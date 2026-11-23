//! Periodic work: the status line, saving counters and history, trimming logs and old rows.

use crate::{
    config::Mode,
    console::TALLY_EVERY,
    db::{Op, now_millis, writer::TRIM_ROWS},
    hub::Hub,
    node::NodeClient,
    pow::target::difficulty_of,
    stats::HISTORY_BUCKET,
};
use std::{sync::Arc, time::Duration};
use tokio::time::{Instant, MissedTickBehavior, interval, interval_at};

const SAVE_EVERY: Duration = Duration::from_secs(60);
const RATES_EVERY: Duration = Duration::from_secs(30);
const PRUNE_EVERY: Duration = Duration::from_secs(600);
const TRIM_PAUSE: Duration = Duration::from_secs(1);
/// Hashrate history kept for the charts.
const HISTORY_KEPT: u64 = 2 * 86_400_000;
/// The share log keeps this many windows of work, in case the difficulty climbs.
const KEEP_WINDOWS: u128 = 4;

pub fn start(hub: Arc<Hub>, node: NodeClient) -> Vec<tokio::task::JoinHandle<()>> {
    let mut jobs = Vec::new();
    let tally = Arc::clone(&hub.tally);
    jobs.push(crate::lifecycle::spawn(hub.shutdown.clone(), async move {
        let mut ticker = interval(TALLY_EVERY);
        loop {
            ticker.tick().await;
            tally.log();
        }
    }));
    let status = Arc::clone(&hub);
    jobs.push(crate::lifecycle::spawn(hub.shutdown.clone(), async move {
        let every = status.config.stats_interval;
        let mut ticker = interval_at(Instant::now() + every, every);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            log_status(&status);
        }
    }));
    let saver = Arc::clone(&hub);
    jobs.push(crate::lifecycle::spawn(hub.shutdown.clone(), async move {
        let mut ticker = interval_at(Instant::now() + SAVE_EVERY, SAVE_EVERY);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            save(&saver);
            trim(&saver);
            saver.stats.prune();
        }
    }));
    let rates = Arc::clone(&hub);
    jobs.push(crate::lifecycle::spawn(hub.shutdown.clone(), async move {
        let mut ticker = interval(RATES_EVERY);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Ok(found) = node.rates().await {
                rates
                    .stats
                    .set_network(|network| network.hashrate = found.network_hashes_per_second);
            }
            let target = node
                .network_mining()
                .await
                .ok()
                .and_then(|mining| mining.target)
                .and_then(|target| hex::decode(target).ok())
                .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok());
            if let Some(target) = target {
                rates
                    .stats
                    .set_network(|network| network.difficulty = difficulty_of(&target));
            }
        }
    }));
    jobs.push(crate::lifecycle::spawn(hub.shutdown.clone(), async move {
        let mut ticker = interval(PRUNE_EVERY);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            prune(&hub).await;
        }
    }));
    jobs
}

/// Saves worker counters, finished hashrate buckets and the open rounds' share work.
pub fn save(hub: &Hub) {
    let counts = hub.stats.take_counts();
    if !counts.is_empty() {
        hub.writer.worker_counts(counts);
    }
    for (bucket, rows) in hub.stats.take_history() {
        hub.writer.hashrate(bucket, rows);
    }
    let [pplns, solo] = hub.stats.round_work();
    hub.writer.round_work(pplns, solo);
    match hub
        .stats
        .saved_blocks()
        .and_then(|saved| serde_json::to_string(&saved).map_err(Into::into))
    {
        Ok(value) => hub.writer.block_figures(value),
        Err(error) => tracing::warn!("cannot save exact block figures: {error:#}"),
    }
}

fn trim(hub: &Hub) {
    let lookback = u64::try_from(hub.config.pplns.lookback.as_millis()).unwrap_or(u64::MAX);
    let difficulty = hub.stats.network().difficulty;
    let keep = hub
        .config
        .pplns
        .window_work(difficulty)
        .saturating_mul(KEEP_WINDOWS);
    if let Ok(mut log) = hub.pplns.lock() {
        log.trim(now_millis().saturating_sub(lookback), keep);
    }
}

async fn prune(hub: &Hub) {
    let now = now_millis();
    let lookback = u64::try_from(hub.config.pplns.lookback.as_millis()).unwrap_or(u64::MAX);
    let before = |kept: std::time::Duration| {
        now.saturating_sub(u64::try_from(kept.as_millis()).unwrap_or(u64::MAX))
    };
    loop {
        let more = hub
            .writer
            .call(|done| Op::PruneAccepted {
                shares_before: now.saturating_sub(lookback.saturating_add(600_000)),
                hashrate_before: now.saturating_sub(HISTORY_KEPT) / HISTORY_BUCKET,
                splits_before: before(hub.config.keep_block_splits),
                sources_before: now.saturating_sub(7 * 86_400_000),
                receipts_before: now
                    .saturating_sub(lookback.max(86_400_000).saturating_add(600_000)),
                height: *hub.heights.borrow(),
                done,
            })
            .await;
        match more {
            Ok(true) => tokio::time::sleep(Duration::from_millis(10)).await,
            Ok(false) => break,
            Err(error) => {
                tracing::warn!("cannot prune accepted-work history: {error:#}");
                break;
            }
        }
    }
    let blocks_before = hub.config.keep_blocks.map(before);
    let keep = hub.config.keep_blocks_max;
    if blocks_before.is_none() && keep.is_none() {
        return;
    }
    loop {
        let trimmed = hub
            .writer
            .call(|done| Op::TrimBlocks {
                before: blocks_before,
                keep,
                done,
            })
            .await;
        match trimmed {
            Ok(rows) if rows >= TRIM_ROWS => tokio::time::sleep(TRIM_PAUSE).await,
            Ok(_) => break,
            Err(error) => {
                tracing::warn!("cannot trim the block list: {error:#}");
                break;
            }
        }
    }
}

fn log_status(hub: &Hub) {
    let network = hub.stats.network();
    let mut parts = vec![format!(
        "height {} | network {}H/s, difficulty {}",
        network.height,
        scaled(network.hashrate),
        scaled(network.difficulty as f64)
    )];
    for mode in Mode::ALL {
        let view = hub.stats.mode(mode);
        if view.workers == 0 && view.accepted == 0 {
            continue;
        }
        let effort = if hub.config.vardiff.measures_effort(network.difficulty) {
            format!(
                ", effort {:.0}%",
                view.round_work as f64 * 100.0 / network.difficulty as f64
            )
        } else {
            String::new()
        };
        parts.push(format!(
            "{} {} workers {}H/s{effort}, shares {}/{}/{}, blocks {} open {} confirmed {} lost",
            mode.label(),
            view.workers,
            scaled(view.hashrate),
            view.accepted,
            view.rejected,
            view.stale,
            view.blocks.open,
            view.blocks.confirmed,
            view.blocks.lost
        ));
    }
    if !network.synced {
        parts.push("node not synchronized".into());
    }
    tracing::info!("{}", parts.join(" | "));
}

/// Short figures for logs: 1.25 M, 980.00 k.
pub fn scaled(value: f64) -> String {
    const UNITS: [(f64, &str); 6] = [
        (1e18, "E"),
        (1e15, "P"),
        (1e12, "T"),
        (1e9, "G"),
        (1e6, "M"),
        (1e3, "k"),
    ];
    UNITS.iter().find(|(scale, _)| value >= *scale).map_or_else(
        || format!("{value:.2} "),
        |(scale, unit)| format!("{:.2} {unit}", value / scale),
    )
}
