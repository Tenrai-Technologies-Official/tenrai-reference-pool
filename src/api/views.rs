use super::Api;
use crate::{
    accounting::cumulative_floor_split,
    address_words::verification_words,
    blocks::{Blocks, Stage},
    chain::{self, Hash384, parse_address},
    config::{Mode, decimal_text, format_trai, format_trai_wide},
    db::{
        BlockStatus, now_millis,
        read::{
            self, BlockDetail as BlockRecord, BlockRow, MinerPayout, PayoutRow, Totals, mode_index,
        },
    },
    pplns::decode_split,
    stats::{
        HISTORY_BUCKET, ModeView, Network, OFFLINE_SHOWN, ProtocolTotals, TopMiner, WorkerView,
    },
    stratum::protocol::Protocol,
};
use anyhow::bail;
use serde::Serialize;
use std::{
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

const DAY_MILLIS: u64 = 86_400_000;
const TOTALS_TTL: Duration = Duration::from_secs(10);
const TOTALS_WAIT: Duration = Duration::from_secs(9);
const RECENT: usize = 10;
const MAX_CREDITS: usize = 200;

static TOTALS: Mutex<Option<(Instant, Totals)>> = Mutex::new(None);
static RELOADING: AtomicBool = AtomicBool::new(false);

#[derive(Serialize)]
pub struct PoolView {
    name: String,
    version: &'static str,
    network: &'static str,
    pool_address: String,
    pool_address_words: String,
    api_rate_limit: u64,
    host: Option<String>,
    urls: Vec<UrlView>,
    explorer: Option<String>,
    uptime: u64,
    ports: Vec<PortView>,
    fees: FeeView,
    pplns_window: String,
    pplns_lookback_minutes: u64,
    share_interval: u64,
    shares_per_block: u64,
    start_difficulty: u64,
    min_difficulty: u64,
    payouts: PayoutPolicy,
    node: Network,
    epochs: Vec<u32>,
    modes: ModesView,
    workers_online: usize,
    miners_online: usize,
    connections: u64,
    protocols: Vec<ProtocolView>,
    blocks_24h: u64,
    rewards_24h: String,
    balances: BalancesView,
    top: Vec<TopMiner>,
}

#[derive(Serialize)]
struct UrlView {
    mode: &'static str,
    url: String,
}

#[derive(Serialize)]
struct PortView {
    mode: &'static str,
    port: u16,
    tls: bool,
    protocol: &'static str,
    web: bool,
}

#[derive(Serialize)]
struct ProtocolView {
    protocol: &'static str,
    #[serde(flatten)]
    totals: ProtocolTotals,
}

#[derive(Serialize)]
struct FeeView {
    pplns: String,
    solo: String,
}

#[derive(Serialize)]
struct PayoutPolicy {
    enabled: bool,
    minimum: String,
    interval_minutes: u64,
    fee: String,
    fee_payer: &'static str,
}

#[derive(Serialize)]
struct ModesView {
    pplns: ModeSummary,
    solo: ModeSummary,
}

#[derive(Serialize)]
struct ModeSummary {
    #[serde(flatten)]
    live: ModeView,
    effort: Option<f64>,
    effort_hour: Option<f64>,
    last_block: Option<u64>,
    blocks_24h: u64,
}

#[derive(Serialize)]
struct BalancesView {
    unpaid: String,
    paid: String,
    payable: u64,
    open_payouts: u64,
    payouts: u64,
}

#[derive(Serialize)]
pub struct BlockItem {
    id: i64,
    hash: String,
    height: u64,
    mode: &'static str,
    status: &'static str,
    stage: &'static str,
    address: String,
    worker: String,
    found_at: u64,
    reward: Option<String>,
    fee: Option<String>,
    effort: Option<f64>,
    cut_height: Option<u64>,
    reason: Option<String>,
}

#[derive(Serialize)]
pub struct BlockPage {
    blocks: Vec<BlockItem>,
    next: Option<i64>,
}

#[derive(Serialize)]
pub struct BlockDetail {
    #[serde(flatten)]
    block: BlockItem,
    network_difficulty: u64,
    round_work: u64,
    fee_percent: String,
    window_work: Option<u64>,
    credits: Vec<CreditItem>,
    credited_addresses: usize,
}

#[derive(Serialize)]
struct CreditItem {
    address: String,
    weight: String,
    percent: f64,
    amount: Option<String>,
}

#[derive(Serialize)]
pub struct PaymentItem {
    id: i64,
    status: &'static str,
    created_at: u64,
    done_at: Option<u64>,
    total: String,
    fee: Option<String>,
    recipients: u32,
    transaction: Option<String>,
}

#[derive(Serialize)]
pub struct PaymentPage {
    payments: Vec<PaymentItem>,
    next: Option<i64>,
}

#[derive(Serialize)]
pub struct MinerView {
    address: String,
    verification_words: String,
    hashrate: f64,
    hashrate_1h: f64,
    workers_online: usize,
    unpaid: String,
    pending: String,
    paid: String,
    earned: String,
    earned_24h: String,
    earnings: Vec<EarningItem>,
    workers: Vec<WorkerView>,
    blocks: Vec<BlockItem>,
    payouts: Vec<MinerPayoutItem>,
    chart: Vec<(u64, f64)>,
}

#[derive(Serialize)]
struct EarningItem {
    day: u64,
    amount: String,
    blocks: u64,
}

#[derive(Serialize)]
struct MinerPayoutItem {
    id: i64,
    status: &'static str,
    created_at: u64,
    amount: String,
    fee_share: String,
    transaction: Option<String>,
}

#[derive(Serialize)]
pub struct History {
    bucket_seconds: u64,
    points: Vec<(u64, f64)>,
}

pub async fn pool(api: &Api) -> anyhow::Result<PoolView> {
    let hub = &api.hub;
    let config = &hub.config;
    let totals = totals(api).await?;
    let (found_24h, rewards_24h) = hub.stats.day()?;
    let network = hub.stats.network();
    let summary = |mode: Mode| {
        let live = hub.stats.mode(mode);
        let effort = config
            .vardiff
            .measures_effort(network.difficulty)
            .then(|| live.round_work as f64 * 100.0 / network.difficulty as f64);
        ModeSummary {
            effort,
            effort_hour: hub.stats.effort_hour(mode),
            last_block: totals.last_found[mode_index(mode)].max(hub.stats.last_found(mode)),
            blocks_24h: found_24h[mode_index(mode)],
            live,
        }
    };
    let (workers_online, miners_online) = hub.stats.online();
    let modes = ModesView {
        pplns: summary(Mode::Pplns),
        solo: summary(Mode::Solo),
    };
    Ok(PoolView {
        name: config.pool_name.clone(),
        version: env!("CARGO_PKG_VERSION"),
        network: hub.network.name(),
        pool_address: hub.pool_address.clone(),
        pool_address_words: words_of(hub.network, &hub.pool_address),
        api_rate_limit: config.web_rate_limit,
        host: config.public_host.clone(),
        urls: config
            .stratum_urls
            .iter()
            .map(|(mode, url)| UrlView {
                mode: mode.name(),
                url: url.clone(),
            })
            .collect(),
        explorer: config.explorer_url.clone(),
        uptime: hub.stats.uptime().as_secs(),
        ports: config
            .listeners
            .iter()
            .map(|listener| PortView {
                mode: listener.mode.name(),
                port: listener.address.port(),
                tls: listener.tls,
                protocol: listener.protocol.name(),
                web: listener.web,
            })
            .collect(),
        fees: FeeView {
            pplns: percent(config.fees.pplns_bps),
            solo: percent(config.fees.solo_bps),
        },
        pplns_window: config.pplns.factor_text(),
        pplns_lookback_minutes: config.pplns.lookback.as_secs() / 60,
        share_interval: config.vardiff.share_interval,
        shares_per_block: config.vardiff.shares_per_block,
        start_difficulty: config.vardiff.start,
        min_difficulty: config.vardiff.min,
        payouts: PayoutPolicy {
            enabled: config.payouts.enabled,
            minimum: format_trai(config.payouts.minimum),
            interval_minutes: config.payouts.interval.as_secs() / 60,
            fee: config.payouts.fee.clone(),
            fee_payer: match config.payouts.fee_payer {
                crate::cli::FeePayer::Pool => "pool",
                crate::cli::FeePayer::Miners => "miners",
            },
        },
        node: network,
        epochs: api.hashers.epochs(),
        modes,
        workers_online,
        miners_online,
        connections: hub.admission.connections(),
        protocols: Protocol::ALL
            .into_iter()
            .filter(|protocol| {
                config
                    .listeners
                    .iter()
                    .any(|listener| listener.protocol == *protocol)
            })
            .map(|protocol| ProtocolView {
                protocol: protocol.name(),
                totals: hub.stats.protocol(protocol),
            })
            .collect(),
        blocks_24h: found_24h.iter().sum(),
        rewards_24h: format_trai_wide(rewards_24h),
        balances: BalancesView {
            unpaid: format_trai(totals.unpaid),
            paid: format_trai_wide(totals.paid),
            payable: totals.payable,
            open_payouts: totals.open_payouts,
            payouts: totals.payouts,
        },
        top: hub.stats.top(RECENT),
    })
}

/// Serve cached totals while their refresh is running.
pub(super) async fn totals(api: &Api) -> anyhow::Result<Totals> {
    let cached = TOTALS.lock().ok().and_then(|cached| cached.clone());
    if cached
        .as_ref()
        .is_none_or(|(at, _)| at.elapsed() >= TOTALS_TTL)
        && !RELOADING.swap(true, Ordering::AcqRel)
    {
        let api = api.clone();
        tokio::spawn(async move {
            let minimum = api.hub.config.payouts.minimum;
            let loaded = api
                .reader
                .read(move |connection| read::totals(connection, minimum))
                .await;
            match (loaded, TOTALS.lock()) {
                (Ok(totals), Ok(mut cached)) => *cached = Some((Instant::now(), totals)),
                (Err(error), _) => tracing::warn!("cannot read the pool totals: {error:#}"),
                _ => {}
            }
            RELOADING.store(false, Ordering::Release);
        });
    }
    if let Some((_, totals)) = cached {
        return Ok(totals);
    }
    let started = Instant::now();
    while started.elapsed() < TOTALS_WAIT {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some((_, totals)) = TOTALS.lock().ok().and_then(|cached| cached.clone()) {
            return Ok(totals);
        }
    }
    bail!("the pool totals are still loading")
}

/// Read one extra row to detect another page.
pub fn block_page(blocks: &Blocks, rows: &[BlockRow], size: usize) -> BlockPage {
    let shown = &rows[..rows.len().min(size)];
    BlockPage {
        blocks: shown.iter().map(|row| block_item(blocks, row)).collect(),
        next: (rows.len() > size)
            .then(|| shown.last().map(|row| row.id))
            .flatten(),
    }
}

/// Report a block as finalized after crediting its reward.
pub(super) fn stage(blocks: &Blocks, row: &BlockRow) -> &'static str {
    match row.status {
        BlockStatus::Confirmed => "finalized",
        BlockStatus::Expired => "expired",
        BlockStatus::Rejected => "rejected",
        BlockStatus::Found | BlockStatus::Pending => row
            .progress
            .or_else(|| Hash384::from_hex(&row.hash).and_then(|id| blocks.stage(&id)))
            .map_or("found", Stage::name),
    }
}

fn block_item(blocks: &Blocks, row: &BlockRow) -> BlockItem {
    BlockItem {
        id: row.id,
        hash: row.hash.clone(),
        height: row.height,
        mode: row.mode.name(),
        status: row.status.name(),
        stage: stage(blocks, row),
        address: row.address.clone(),
        worker: row.worker.clone(),
        found_at: row.found_at,
        reward: row.reward.map(format_trai),
        fee: row.fee.map(format_trai),
        effort: (row.network_difficulty > 0 && row.round_work > 0)
            .then(|| row.round_work as f64 * 100.0 / row.network_difficulty as f64),
        cut_height: row.cut_height,
        reason: row.reason.clone(),
    }
}

pub async fn block_detail(
    api: &Api,
    (row, fee_bps, window_work, split): BlockRecord,
) -> anyhow::Result<BlockDetail> {
    let weights = split.as_deref().and_then(decode_split).unwrap_or_default();
    let total = weights
        .iter()
        .try_fold(0_u128, |sum, (_, weight)| sum.checked_add(*weight))
        .ok_or_else(|| anyhow::anyhow!("stored block split overflow"))?;
    let distributable = row
        .reward
        .zip(row.fee)
        .and_then(|(reward, fee)| reward.checked_sub(fee));
    let amounts = distributable
        .map(|amount| {
            cumulative_floor_split(
                u128::from(amount),
                &weights
                    .iter()
                    .map(|(_, weight)| *weight)
                    .collect::<Vec<_>>(),
            )
        })
        .transpose()?;
    let credited_addresses = weights.len();
    let mut indices = (0..weights.len()).collect::<Vec<_>>();
    indices.sort_by(|left, right| {
        weights[*right]
            .1
            .cmp(&weights[*left].1)
            .then(weights[*left].0.cmp(&weights[*right].0))
    });
    indices.truncate(MAX_CREDITS);
    let mut credits = Vec::with_capacity(indices.len());
    for index in indices {
        let (miner, weight) = weights[index];
        let address = api
            .hub
            .registry
            .address(miner)
            .await?
            .ok_or_else(|| anyhow::anyhow!("stored split names a missing miner"))?;
        credits.push(CreditItem {
            address,
            weight: weight.to_string(),
            percent: if total == 0 {
                0.0
            } else {
                weight as f64 * 100.0 / total as f64
            },
            amount: amounts
                .as_ref()
                .and_then(|amounts| amounts.get(index))
                .and_then(|amount| u64::try_from(*amount).ok())
                .map(format_trai),
        });
    }
    Ok(BlockDetail {
        block: block_item(&api.hub.blocks, &row),
        network_difficulty: row.network_difficulty,
        round_work: row.round_work,
        fee_percent: percent(fee_bps),
        window_work,
        credits,
        credited_addresses,
    })
}

pub fn payment_page(rows: &[PayoutRow], size: usize) -> PaymentPage {
    let shown = &rows[..rows.len().min(size)];
    PaymentPage {
        payments: shown
            .iter()
            .map(|row| PaymentItem {
                id: row.id,
                status: row.status.name(),
                created_at: row.created_at,
                done_at: row.done_at,
                total: format_trai(row.total),
                fee: row.fee.map(format_trai),
                recipients: row.recipients,
                transaction: row.transaction_id.clone(),
            })
            .collect(),
        next: (rows.len() > size)
            .then(|| shown.last().map(|row| row.id))
            .flatten(),
    }
}

pub async fn miner(api: &Api, address: &str) -> anyhow::Result<Option<MinerView>> {
    let workers = api.hub.stats.workers_of(address);
    let verification_words = words_of(api.hub.network, address);
    let Some(id) = api.hub.registry.find(address).await? else {
        if workers.is_empty() {
            return Ok(None);
        }
        return Ok(Some(live_only(address, verification_words, workers)));
    };
    let now = now_millis();
    let today = now / DAY_MILLIS;
    let since = now.saturating_sub(DAY_MILLIS) / HISTORY_BUCKET;
    let shown = u64::try_from(OFFLINE_SHOWN.as_millis()).unwrap_or(u64::MAX);
    let seen_since = now_millis().saturating_sub(shown);
    let (balance, blocks, payouts, earnings, buckets, stored) = api
        .reader
        .read(move |connection| {
            Ok((
                read::balance(connection, id)?,
                read::miner_blocks(connection, id, RECENT)?,
                read::miner_payouts(connection, id, RECENT)?,
                read::earnings(connection, id, today.saturating_sub(6))?,
                read::hashrate(connection, id, since)?,
                read::workers_of(connection, id, seen_since)?,
            ))
        })
        .await?;
    let mut workers = workers;
    for (name, last_share, accepted, rejected, stale) in stored {
        if workers.iter().any(|worker| worker.name == name) {
            continue;
        }
        workers.push(WorkerView {
            name,
            mode: "-",
            protocol: "-",
            online: false,
            agent: String::new(),
            hashrate: 0.0,
            hashrate_1h: 0.0,
            difficulty: 0,
            fixed_difficulty: false,
            accepted,
            rejected,
            stale,
            blocks: 0,
            last_share: (last_share > 0).then_some(last_share),
            connected_at: None,
        });
    }
    let online: Vec<&WorkerView> = workers.iter().filter(|worker| worker.online).collect();
    let earned_24h = earned_last_day(&earnings, now)?;
    Ok(Some(MinerView {
        address: address.to_owned(),
        verification_words,
        hashrate: online.iter().map(|worker| worker.hashrate).sum(),
        hashrate_1h: workers.iter().map(|worker| worker.hashrate_1h).sum(),
        workers_online: online.len(),
        unpaid: format_trai(balance.unpaid),
        pending: format_trai(balance.pending),
        paid: format_trai_wide(balance.paid),
        earned: format_trai_wide(balance.earned),
        earned_24h: format_trai_wide(earned_24h),
        earnings: earnings
            .into_iter()
            .map(|(day, amount, blocks)| EarningItem {
                day,
                amount: format_trai_wide(amount),
                blocks,
            })
            .collect(),
        blocks: blocks
            .iter()
            .map(|row| block_item(&api.hub.blocks, row))
            .collect(),
        payouts: payouts.iter().map(miner_payout).collect(),
        chart: chart(&buckets),
        workers,
    }))
}

/// Weight the prior day's earnings by its overlap with the last 24 hours.
pub(crate) fn earned_last_day(earnings: &[(u64, u128, u64)], now: u64) -> anyhow::Result<u128> {
    let today = now / DAY_MILLIS;
    let denominator = u128::from(DAY_MILLIS);
    let yesterday_share = u128::from(DAY_MILLIS - now % DAY_MILLIS);
    earnings
        .iter()
        .try_fold(0_u128, |total, &(day, amount, _)| {
            let part = match today.checked_sub(day) {
                Some(0) => amount,
                Some(1) => {
                    (amount / denominator) * yesterday_share
                        + (amount % denominator) * yesterday_share / denominator
                }
                _ => 0,
            };
            total
                .checked_add(part)
                .ok_or_else(|| anyhow::anyhow!("daily earnings exceed u128"))
        })
}

fn live_only(address: &str, verification_words: String, workers: Vec<WorkerView>) -> MinerView {
    let zero = format_trai(0);
    MinerView {
        address: address.to_owned(),
        verification_words,
        hashrate: workers.iter().map(|worker| worker.hashrate).sum(),
        hashrate_1h: workers.iter().map(|worker| worker.hashrate_1h).sum(),
        workers_online: workers.iter().filter(|worker| worker.online).count(),
        unpaid: zero.clone(),
        pending: zero.clone(),
        paid: zero.clone(),
        earned: zero.clone(),
        earned_24h: zero,
        earnings: Vec::new(),
        blocks: Vec::new(),
        payouts: Vec::new(),
        chart: Vec::new(),
        workers,
    }
}

fn miner_payout(row: &MinerPayout) -> MinerPayoutItem {
    MinerPayoutItem {
        id: row.id,
        status: row.status.name(),
        created_at: row.created_at,
        amount: format_trai(row.amount.saturating_sub(row.fee_share)),
        fee_share: format_trai(row.fee_share),
        transaction: row.transaction_id.clone(),
    }
}

pub async fn history(api: &Api, miner: u32) -> anyhow::Result<History> {
    let since = now_millis().saturating_sub(DAY_MILLIS) / HISTORY_BUCKET;
    let buckets = api
        .reader
        .read(move |connection| read::hashrate(connection, miner, since))
        .await?;
    Ok(History {
        bucket_seconds: HISTORY_BUCKET / 1_000,
        points: chart(&buckets),
    })
}

/// Return empty verification words for an address from another network.
fn words_of(network: chain::Network, address: &str) -> String {
    parse_address(address, network)
        .map_or_else(|_| String::new(), |lock| verification_words(network, &lock))
}

fn chart(buckets: &[(u64, u64)]) -> Vec<(u64, f64)> {
    let seconds = (HISTORY_BUCKET / 1_000) as f64;
    buckets
        .iter()
        .map(|(bucket, work)| (bucket * HISTORY_BUCKET, *work as f64 / seconds))
        .collect()
}

fn percent(bps: u32) -> String {
    decimal_text(u128::from(bps), 100)
}

#[cfg(test)]
#[path = "../tests/money_views.rs"]
mod money_tests;
