//! Miningcore-compatible pool, block and miner statistics.

use super::{Api, Build, encode, problem, views};
use crate::{
    chain::parse_address,
    config::Mode,
    db::{
        BlockStatus, now_millis,
        read::{self, BlockRow, mode_index},
    },
    stratum::protocol::{Protocol, worker_name},
};
use axum::{
    extract::{Path, Query, State, rejection::QueryRejection},
    http::{HeaderValue, StatusCode, header},
    response::Response,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, time::Duration};

pub const POOLS: [(&str, Mode); 2] = [("trai-pplns", Mode::Pplns), ("trai-solo", Mode::Solo)];
pub const MAX_PAGE_SIZE: usize = 1_000;
const MAX_PAGE: usize = 99;
const DEFAULT_PAGE_SIZE: usize = 15;
pub const PUBLIC_TTL: Duration = Duration::from_secs(10);
const ATOMS: f64 = 100_000_000.0;
const MAX_ADDRESS_BYTES: usize = 128;
const DAY_MILLIS: u64 = 86_400_000;
const PAYOUTS_READ: usize = 100;

#[derive(Serialize)]
struct PoolsResponse {
    pools: Vec<PoolInfo>,
}

#[derive(Serialize)]
struct PoolResponse {
    pool: PoolInfo,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PoolInfo {
    id: &'static str,
    coin: CoinInfo,
    ports: BTreeMap<u16, PortInfo>,
    payment_processing: PaymentProcessing,
    client_connection_timeout: u64,
    pool_fee_percent: f64,
    address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    address_info_link: Option<String>,
    pool_stats: PoolStats,
    network_stats: NetworkStats,
    total_paid: f64,
    total_blocks: u64,
    last_pool_block_time: Option<String>,
    pool_effort: Option<f64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CoinInfo {
    r#type: &'static str,
    name: &'static str,
    symbol: &'static str,
    website: &'static str,
    family: &'static str,
    algorithm: &'static str,
    canonical_name: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PortInfo {
    name: String,
    difficulty: u64,
    tls: bool,
    var_diff: VarDiff,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct VarDiff {
    min_diff: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_diff: Option<u64>,
    target_time: u64,
    variance_percent: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PaymentProcessing {
    enabled: bool,
    minimum_payment: f64,
    payout_scheme: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    payout_scheme_config: Option<SchemeConfig>,
}

#[derive(Serialize)]
struct SchemeConfig {
    factor: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PoolStats {
    connected_miners: usize,
    connected_workers: usize,
    pool_hashrate: f64,
    last_pool_block_time: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NetworkStats {
    network_type: &'static str,
    network_hashrate: f64,
    network_difficulty: f64,
    block_height: u64,
    connected_peers: u32,
    reward_type: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Block {
    pool_id: &'static str,
    payout_scheme: &'static str,
    block_height: u64,
    network_difficulty: f64,
    status: &'static str,
    r#type: &'static str,
    confirmation_progress: f64,
    effort: Option<f64>,
    reward: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    info_link: Option<String>,
    hash: String,
    miner: String,
    created: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MinerStats {
    pending_balance: f64,
    pending_payout: f64,
    total_earned: f64,
    total_paid: f64,
    today_paid: f64,
    last_payment: Option<String>,
    last_payment_link: Option<String>,
    performance: Performance,
}

#[derive(Serialize)]
struct Performance {
    created: String,
    workers: BTreeMap<String, WorkerPerformance>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerPerformance {
    payout_scheme: &'static str,
    hashrate: f64,
    shares_per_second: f64,
}

#[derive(Deserialize)]
pub struct PageQuery {
    page: Option<usize>,
    #[serde(rename = "pageSize")]
    page_size: Option<usize>,
}

pub async fn pools(State(api): State<Api>) -> Response {
    let builder = api.clone();
    let response = api
        .cached(
            "public:pools".into(),
            PUBLIC_TTL,
            Build::Shared,
            || async move {
                let mut pools = Vec::with_capacity(POOLS.len());
                for (id, mode) in POOLS {
                    pools.push(pool_info(&builder, id, mode).await?);
                }
                encode(&PoolsResponse { pools })
            },
        )
        .await;
    public(response)
}

pub async fn pool(State(api): State<Api>, Path(id): Path<String>) -> Response {
    let Some((id, mode)) = find(&id) else {
        return public(problem(StatusCode::NOT_FOUND, "no pool with this id"));
    };
    let builder = api.clone();
    let response = api
        .cached(
            format!("public:pool:{id}"),
            PUBLIC_TTL,
            Build::Shared,
            || async move {
                encode(&PoolResponse {
                    pool: pool_info(&builder, id, mode).await?,
                })
            },
        )
        .await;
    public(response)
}

pub async fn blocks(
    State(api): State<Api>,
    Path(id): Path<String>,
    query: Result<Query<PageQuery>, QueryRejection>,
) -> Response {
    let Some((_, mode)) = find(&id) else {
        return public(problem(StatusCode::NOT_FOUND, "no pool with this id"));
    };
    block_page(api, Some(mode), query).await
}

pub async fn all_blocks(
    State(api): State<Api>,
    query: Result<Query<PageQuery>, QueryRejection>,
) -> Response {
    block_page(api, None, query).await
}

async fn block_page(
    api: Api,
    mode: Option<Mode>,
    query: Result<Query<PageQuery>, QueryRejection>,
) -> Response {
    let Ok(Query(query)) = query else {
        return public(problem(
            StatusCode::BAD_REQUEST,
            "page and pageSize are whole numbers",
        ));
    };
    let page = query.page.unwrap_or(0);
    let size = query.page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    if page > MAX_PAGE || !(1..=MAX_PAGE_SIZE).contains(&size) {
        return public(problem(
            StatusCode::BAD_REQUEST,
            "page goes up to 99 and pageSize from 1 to 1000",
        ));
    }
    let key = format!(
        "public:blocks:{}:{page}:{size}",
        mode.map_or("all", Mode::name)
    );
    let build = if page == 0 && size == DEFAULT_PAGE_SIZE {
        Build::Shared
    } else {
        Build::Counted
    };
    let builder = api.clone();
    let response = api
        .cached(key, PUBLIC_TTL, build, || async move {
            let rows = builder
                .reader
                .read(move |connection| read::blocks_page(connection, mode, page * size, size))
                .await?;
            let explorer = builder.hub.config.explorer_url.clone();
            let blocks: Vec<Block> = rows
                .iter()
                .map(|row| block(&builder, explorer.as_deref(), row))
                .collect();
            encode(&blocks)
        })
        .await;
    public(response)
}

/// Balances span both modes. Worker statistics are mode-specific.
pub async fn miner(
    State(api): State<Api>,
    Path((id, address)): Path<(String, String)>,
) -> Response {
    let Some((_, mode)) = find(&id) else {
        return public(problem(StatusCode::NOT_FOUND, "no pool with this id"));
    };
    miner_stats(api, Some(mode), address).await
}

pub async fn all_miner(State(api): State<Api>, Path(address): Path<String>) -> Response {
    miner_stats(api, None, address).await
}

async fn miner_stats(api: Api, mode: Option<Mode>, address: String) -> Response {
    if address.len() > MAX_ADDRESS_BYTES || parse_address(&address, api.hub.network).is_err() {
        return public(problem(
            StatusCode::BAD_REQUEST,
            "not an address of this network",
        ));
    }
    let known = match api.hub.registry.find(&address).await {
        Ok(known) => known,
        Err(_) => {
            return public(problem(
                StatusCode::SERVICE_UNAVAILABLE,
                "the miner registry is unavailable",
            ));
        }
    };
    if known.is_none() && !api.hub.stats.knows(&address) {
        return public(problem(StatusCode::NOT_FOUND, "no miner with this address"));
    }
    let key = format!("public:miner:{}:{address}", mode.map_or("all", Mode::name));
    let builder = api.clone();
    let response = api
        .cached(key, PUBLIC_TTL, Build::Counted, || async move {
            let mut workers = builder.hub.stats.workers_of(&address);
            workers.retain(|worker| {
                worker.online && mode.is_none_or(|mode| worker.mode == mode.name())
            });
            workers.sort_by_key(|worker| worker.mode != Mode::Pplns.name());
            let interval = builder.hub.config.payouts.interval.as_millis().max(1);
            let rounds = u128::from(DAY_MILLIS) / interval;
            let limit = PAYOUTS_READ.max(usize::try_from(rounds + 2).unwrap_or(usize::MAX));
            let read = known.map(|miner| {
                builder.reader.read(move |connection| {
                    Ok((
                        read::balance(connection, miner)?,
                        read::miner_payouts(connection, miner, limit)?,
                    ))
                })
            });
            let (balance, payouts) = match read {
                Some(read) => read.await?,
                None => Default::default(),
            };
            let today = now_millis() / DAY_MILLIS * DAY_MILLIS;
            let explorer = builder.hub.config.explorer_url.clone();
            let last = payouts.iter().find(|payout| {
                payout.transaction_id.is_some() && payout.status != crate::db::PayoutStatus::Failed
            });
            let mut performance = BTreeMap::new();
            for worker in &workers {
                let scheme = if worker.mode == Mode::Pplns.name() {
                    "PPLNS"
                } else {
                    "SOLO"
                };
                let name = worker_name(&worker.name);
                let key = if performance.contains_key(&name) {
                    format!("{name} ({scheme})")
                } else {
                    name
                };
                let shares = if worker.difficulty > 0 {
                    worker.hashrate / worker.difficulty as f64
                } else {
                    0.0
                };
                performance.insert(
                    key,
                    WorkerPerformance {
                        payout_scheme: scheme,
                        hashrate: worker.hashrate,
                        shares_per_second: shares,
                    },
                );
            }
            let stats = MinerStats {
                pending_balance: balance.unpaid as f64 / ATOMS,
                pending_payout: balance.pending as f64 / ATOMS,
                total_earned: balance.earned as f64 / ATOMS,
                total_paid: balance.paid as f64 / ATOMS,
                today_paid: payouts
                    .iter()
                    .filter(|payout| {
                        payout.created_at >= today && payout.status == crate::db::PayoutStatus::Paid
                    })
                    .try_fold(0_u128, |total, payout| {
                        let amount = payout
                            .amount
                            .checked_sub(payout.fee_share)
                            .ok_or_else(|| anyhow::anyhow!("payout fee exceeds its amount"))?;
                        total
                            .checked_add(u128::from(amount))
                            .ok_or_else(|| anyhow::anyhow!("daily paid total exceeds u128"))
                    })? as f64
                    / ATOMS,
                last_payment: last.map(|payout| iso8601(payout.created_at)),
                last_payment_link: last.and_then(|payout| {
                    Some(format!(
                        "{}#/tx/{}",
                        explorer.as_ref()?,
                        payout.transaction_id.as_ref()?
                    ))
                }),
                performance: Performance {
                    created: iso8601(now_millis()),
                    workers: performance,
                },
            };
            encode(&stats)
        })
        .await;
    public(response)
}

fn find(id: &str) -> Option<(&'static str, Mode)> {
    POOLS.into_iter().find(|(known, _)| *known == id)
}

/// Permit cross-origin reads and cache them for the response TTL.
fn public(mut response: Response) -> Response {
    let success = response.status().is_success();
    let headers = response.headers_mut();
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    if success {
        headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=10"),
        );
    }
    response
}

async fn pool_info(api: &Api, id: &'static str, mode: Mode) -> anyhow::Result<PoolInfo> {
    let hub = &api.hub;
    let config = &hub.config;
    let totals = views::totals(api).await?;
    let network = hub.stats.network();
    let live = hub.stats.mode(mode);
    let last_found = totals.last_found[mode_index(mode)].max(hub.stats.last_found(mode));
    let ports = config
        .listeners
        .iter()
        .filter(|listener| listener.mode == mode)
        .map(|listener| {
            let protocol = match listener.protocol {
                Protocol::V1 if listener.web => "WebSocket",
                Protocol::V1 if listener.tls => "V1 TLS",
                Protocol::V1 => "V1",
                Protocol::V2 => "V2 TLS",
            };
            let port = PortInfo {
                name: format!("{} {protocol}", mode.name().to_uppercase()),
                difficulty: config.vardiff.start,
                tls: listener.tls,
                var_diff: VarDiff {
                    min_diff: config.vardiff.min,
                    max_diff: config.vardiff.max,
                    target_time: config.vardiff.share_interval,
                    variance_percent: config.vardiff.tolerance_percent,
                },
            };
            (listener.address.port(), port)
        })
        .collect();
    Ok(PoolInfo {
        id,
        coin: CoinInfo {
            r#type: "TRAI",
            name: "Tenrai",
            symbol: "TRAI",
            website: "https://tenrai-technologies.org/",
            family: "tenrai",
            algorithm: "TenQ",
            canonical_name: "Tenrai",
        },
        ports,
        payment_processing: PaymentProcessing {
            enabled: config.payouts.enabled,
            minimum_payment: config.payouts.minimum as f64 / ATOMS,
            payout_scheme: match mode {
                Mode::Pplns => "PPLNS",
                Mode::Solo => "SOLO",
            },
            payout_scheme_config: (mode == Mode::Pplns).then(|| SchemeConfig {
                factor: config.pplns.numerator as f64 / config.pplns.denominator as f64,
            }),
        },
        client_connection_timeout: config.idle_timeout.as_secs(),
        pool_fee_percent: f64::from(config.fees.bps(mode)) / 100.0,
        address: hub.pool_address.clone(),
        address_info_link: config
            .explorer_url
            .as_ref()
            .map(|explorer| format!("{explorer}#/address/{}", hub.pool_address)),
        pool_stats: PoolStats {
            connected_miners: live.miners,
            connected_workers: live.workers,
            pool_hashrate: live.hashrate,
            last_pool_block_time: last_found.map(iso8601),
        },
        network_stats: NetworkStats {
            network_type: match hub.network.name() {
                "mainnet" => "Main",
                "development" => "Dev",
                _ => "Test",
            },
            network_hashrate: network.hashrate,
            network_difficulty: network.difficulty as f64,
            block_height: network.height,
            connected_peers: network.peers,
            reward_type: "POW",
        },
        total_paid: totals.paid as f64 / ATOMS,
        total_blocks: live.blocks.found,
        last_pool_block_time: last_found.map(iso8601),
        pool_effort: api
            .hub
            .config
            .vardiff
            .measures_effort(network.difficulty)
            .then(|| live.round_work as f64 / network.difficulty as f64),
    })
}

fn block(api: &Api, explorer: Option<&str>, row: &BlockRow) -> Block {
    let (status, progress) = match views::stage(&api.hub.blocks, row) {
        "finalized" => ("confirmed", 1.0),
        "expired" | "rejected" => ("orphaned", 0.0),
        "executed" => ("pending", 2.0 / 3.0),
        "registered" => ("pending", 1.0 / 3.0),
        _ => ("pending", 0.0),
    };
    let reward = match row.status {
        BlockStatus::Confirmed => row.reward.unwrap_or(0) as f64 / ATOMS,
        _ => 0.0,
    };
    let (pool_id, payout_scheme) = match row.mode {
        Mode::Pplns => ("trai-pplns", "PPLNS"),
        Mode::Solo => ("trai-solo", "SOLO"),
    };
    Block {
        pool_id,
        payout_scheme,
        block_height: row.height,
        network_difficulty: row.network_difficulty as f64,
        status,
        r#type: "block",
        confirmation_progress: progress,
        effort: (row.network_difficulty > 0 && row.round_work > 0)
            .then(|| row.round_work as f64 / row.network_difficulty as f64),
        reward,
        info_link: explorer.map(|explorer| format!("{explorer}#/block/{}", row.hash)),
        hash: row.hash.clone(),
        miner: row.address.clone(),
        created: iso8601(row.found_at),
    }
}

/// Format Unix milliseconds as Miningcore-compatible UTC text.
fn iso8601(millis: u64) -> String {
    let seconds = millis / 1_000;
    let (days, rest) = (seconds / 86_400, seconds % 86_400);
    let (year, month, day) = civil(days as i64);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rest / 3_600,
        rest % 3_600 / 60,
        rest % 60
    )
}

/// Days since 1970-01-01 as a proleptic Gregorian date (Howard Hinnant's algorithm).
fn civil(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_index + 2) / 5 + 1) as u32;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    } as u32;
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
#[path = "../tests/public.rs"]
mod tests;
