//! In-memory statistics: workers, hashrate windows, share counters and network figures.

use crate::{
    config::Mode,
    day::{Day, SavedBlocks},
    db::now_millis,
    rate::RateWindow,
    stratum::protocol::Protocol,
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    net::IpAddr,
    sync::Mutex,
    time::{Duration, Instant},
};

const SHORT_BUCKET: Duration = Duration::from_secs(10);
const SHORT_BUCKETS: u64 = 30;
const LONG_BUCKET: Duration = Duration::from_secs(60);
const LONG_BUCKETS: u64 = 60;
const OFFLINE_KEPT: Duration = Duration::from_secs(24 * 3_600);
/// Offline workers are hidden after this; their counts are kept for a day.
pub const OFFLINE_SHOWN: Duration = Duration::from_secs(3_600);
const EFFORT_WINDOW: Duration = Duration::from_secs(3_600);
/// Hashrate history buckets, in milliseconds.
pub const HISTORY_BUCKET: u64 = 600_000;
const MAX_HISTORY_ROWS: usize = 2 * crate::db::accepted::MAX_ACTIVE_PARTICIPANTS;
const MAX_FINISHED_BUCKETS: usize = 2;

/// Worker id, accepted, rejected and stale shares, and the newest share time.
pub type WorkerCounts = (crate::db::WorkerId, u64, u64, u64, u64);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkerKey {
    pub address: String,
    pub name: String,
    pub mode: Mode,
}

struct Worker {
    worker_id: Option<crate::db::WorkerId>,
    agent: String,
    protocol: Protocol,
    ip: IpAddr,
    sessions: u32,
    connected_at: Option<u64>,
    last_seen: Instant,
    difficulty: u64,
    fixed: bool,
    short: RateWindow,
    long: RateWindow,
    accepted: u64,
    rejected: u64,
    stale: u64,
    blocks: u64,
    last_share: Option<u64>,
    unsaved: (u64, u64, u64),
}

struct ModeTotals {
    short: RateWindow,
    long: RateWindow,
    accepted: u64,
    rejected: u64,
    stale: u64,
    round_work: u128,
    blocks: BlockCounts,
    /// Blocks of the last hour: time, round share work, network difficulty.
    recent: VecDeque<(Instant, u128, u128)>,
    /// Last block found since startup, in Unix milliseconds.
    last_found: Option<u64>,
}

/// Open connections and share outcomes of one wire protocol.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct ProtocolTotals {
    pub connections: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub stale: u64,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct BlockCounts {
    pub found: u64,
    pub open: u64,
    pub confirmed: u64,
    pub lost: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Network {
    pub height: u64,
    pub difficulty: u128,
    pub hashrate: f64,
    pub synced: bool,
    pub peers: u32,
    pub block_rate: u32,
    pub cut_interval_ms: u32,
    pub work_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ModeView {
    pub hashrate: f64,
    pub hashrate_1h: f64,
    pub workers: usize,
    pub miners: usize,
    pub accepted: u64,
    pub rejected: u64,
    pub stale: u64,
    pub round_work: u128,
    pub blocks: BlockCounts,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkerView {
    pub name: String,
    pub mode: &'static str,
    pub protocol: &'static str,
    pub online: bool,
    pub agent: String,
    pub hashrate: f64,
    pub hashrate_1h: f64,
    pub difficulty: u64,
    pub fixed_difficulty: bool,
    pub accepted: u64,
    pub rejected: u64,
    pub stale: u64,
    pub blocks: u64,
    pub last_share: Option<u64>,
    pub connected_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TopMiner {
    pub address: String,
    pub hashrate: f64,
    pub workers: usize,
}

struct Inner {
    /// Ordered by address, so the workers of one miner form a contiguous range.
    workers: BTreeMap<WorkerKey, Worker>,
    modes: [ModeTotals; 2],
    rejects: BTreeMap<&'static str, u64>,
    network: Network,
    protocols: [ProtocolTotals; 2],
    refused: u64,
    bucket: u64,
    history: HashMap<(u32, Mode), u64>,
    history_totals: [u64; 2],
    finished: Vec<HistoryBucket>,
    day: Day,
}

/// One finished ten-minute bucket: work per address id and mode, id 0 holding the pool totals.
pub type HistoryBucket = (u64, Vec<(u32, Mode, u64)>);

pub struct Stats {
    started: Instant,
    inner: Mutex<Inner>,
}

impl Stats {
    pub fn new(blocks: [BlockCounts; 2], round_work: [u128; 2], day: Day) -> Self {
        let now = Instant::now();
        let totals = |index: usize| ModeTotals {
            short: RateWindow::new(SHORT_BUCKET, SHORT_BUCKETS, now),
            long: RateWindow::new(LONG_BUCKET, LONG_BUCKETS, now),
            accepted: 0,
            rejected: 0,
            stale: 0,
            round_work: round_work[index],
            blocks: blocks[index],
            recent: VecDeque::new(),
            last_found: None,
        };
        Self {
            started: now,
            inner: Mutex::new(Inner {
                workers: BTreeMap::new(),
                modes: [totals(0), totals(1)],
                rejects: BTreeMap::new(),
                network: Network::default(),
                protocols: [ProtocolTotals::default(); 2],
                refused: 0,
                bucket: now_millis() / HISTORY_BUCKET,
                history: HashMap::new(),
                history_totals: [0; 2],
                finished: Vec::new(),
                day,
            }),
        }
    }

    pub fn uptime(&self) -> Duration {
        self.started.elapsed()
    }

    fn with<T>(&self, change: impl FnOnce(&mut Inner) -> T) -> T {
        let mut inner = self.inner.lock().expect("statistics lock");
        change(&mut inner)
    }

    pub fn session_opened(&self, protocol: Protocol) {
        self.with(|inner| {
            let count = &mut inner.protocols[slot(protocol)].connections;
            *count = count.saturating_add(1);
        });
    }

    pub fn session_closed(&self, protocol: Protocol) {
        self.with(|inner| {
            let totals = &mut inner.protocols[slot(protocol)];
            totals.connections = totals.connections.saturating_sub(1);
        });
    }

    /// `None` for an accepted share, else whether the rejected one was stale.
    pub fn protocol_share(&self, protocol: Protocol, rejected: Option<bool>) {
        self.with(|inner| {
            let totals = &mut inner.protocols[slot(protocol)];
            match rejected {
                None => totals.accepted = totals.accepted.saturating_add(1),
                Some(true) => totals.stale = totals.stale.saturating_add(1),
                Some(false) => totals.rejected = totals.rejected.saturating_add(1),
            }
        });
    }

    pub fn protocol(&self, protocol: Protocol) -> ProtocolTotals {
        self.with(|inner| inner.protocols[slot(protocol)])
    }

    pub fn refused(&self) {
        self.with(|inner| inner.refused = inner.refused.saturating_add(1));
    }

    pub fn connected(&self, key: &WorkerKey, agent: &str, ip: IpAddr, protocol: Protocol) {
        let now = Instant::now();
        self.with(|inner| {
            let worker = inner
                .workers
                .entry(key.clone())
                .or_insert_with(|| Worker::new(ip, now));
            worker.sessions = worker.sessions.saturating_add(1);
            worker.connected_at.get_or_insert_with(now_millis);
            agent.clone_into(&mut worker.agent);
            worker.protocol = protocol;
            worker.ip = ip;
            worker.last_seen = now;
        });
    }

    pub fn disconnected(&self, key: &WorkerKey) {
        self.with(|inner| {
            let Some(worker) = inner.workers.get_mut(key) else {
                return;
            };
            worker.sessions = worker.sessions.saturating_sub(1);
            worker.last_seen = Instant::now();
            if worker.sessions > 0 {
                return;
            }
            worker.connected_at = None;
            // Drop workers without accepted shares, so logins alone cannot fill memory.
            if worker.accepted == 0 || inner.workers.len() > crate::db::accepted::MAX_REGISTRY_CACHE
            {
                // Persistent counters, names and accepted work remain authoritative on disk.
                inner.workers.remove(key);
            }
        });
    }

    pub fn difficulty(&self, key: &WorkerKey, difficulty: u64, fixed: bool) {
        self.with(|inner| {
            if let Some(worker) = inner.workers.get_mut(key) {
                worker.difficulty = difficulty;
                worker.fixed = fixed;
            }
        });
    }

    pub fn accepted(&self, key: &WorkerKey, ids: (u32, crate::db::WorkerId), weight: u64) {
        let now = Instant::now();
        let millis = now_millis();
        let work = weight as f64;
        self.with(|inner| {
            let mode = &mut inner.modes[index(key.mode)];
            mode.short.add(now, work);
            mode.long.add(now, work);
            mode.accepted = mode.accepted.saturating_add(1);
            mode.round_work = mode.round_work.saturating_add(u128::from(weight));
            roll_history(inner, millis);
            record_history(inner, ids.0, key.mode, weight);
            if let Some(worker) = inner.workers.get_mut(key) {
                worker.worker_id = Some(ids.1);
                worker.short.add(now, work);
                worker.long.add(now, work);
                worker.accepted = worker.accepted.saturating_add(1);
                // Accepted counters were committed with the share; never increment them twice.
                worker.last_share = Some(millis);
                worker.last_seen = now;
            }
        });
    }

    pub fn rejected(&self, key: Option<&WorkerKey>, mode: Mode, reason: &'static str, stale: bool) {
        self.with(|inner| {
            let totals = &mut inner.modes[index(mode)];
            if stale {
                totals.stale = totals.stale.saturating_add(1);
            } else {
                totals.rejected = totals.rejected.saturating_add(1);
            }
            let count = inner.rejects.entry(reason).or_default();
            *count = count.saturating_add(1);
            if let Some(worker) = key.and_then(|key| inner.workers.get_mut(key)) {
                if stale {
                    worker.stale = worker.stale.saturating_add(1);
                    worker.unsaved.2 = worker.unsaved.2.saturating_add(1);
                } else {
                    worker.rejected = worker.rejected.saturating_add(1);
                    worker.unsaved.1 = worker.unsaved.1.saturating_add(1);
                }
            }
        });
    }

    /// Counts a found block and starts a new round; returns its share work, 0 if unmeasured.
    pub fn block_found(
        &self,
        key: &WorkerKey,
        network_difficulty: u128,
        found_at: u64,
        measured: bool,
    ) -> u128 {
        let now = Instant::now();
        self.with(|inner| {
            if let Some(worker) = inner.workers.get_mut(key) {
                worker.blocks = worker.blocks.saturating_add(1);
            }
            inner.day.found(key.mode, found_at);
            let totals = &mut inner.modes[index(key.mode)];
            totals.blocks.found = totals.blocks.found.saturating_add(1);
            totals.blocks.open = totals.blocks.open.saturating_add(1);
            let round = std::mem::take(&mut totals.round_work);
            let round = if measured { round } else { 0 };
            if measured {
                totals.recent.push_back((now, round, network_difficulty));
                while totals.recent.len() > 8192 {
                    totals.recent.pop_front();
                }
            }
            totals.last_found = Some(found_at);
            while totals
                .recent
                .front()
                .is_some_and(|(at, _, _)| now.duration_since(*at) > EFFORT_WINDOW)
            {
                totals.recent.pop_front();
            }
            round
        })
    }

    /// Last block of `mode` found since startup, in Unix milliseconds.
    pub fn last_found(&self, mode: Mode) -> Option<u64> {
        self.with(|inner| inner.modes[index(mode)].last_found)
    }

    /// Share work relative to the work required by the last hour's blocks, in percent.
    pub fn effort_hour(&self, mode: Mode) -> Option<f64> {
        let now = Instant::now();
        self.with(|inner| {
            let (work, needed) = inner.modes[index(mode)]
                .recent
                .iter()
                .filter(|(at, _, _)| now.duration_since(*at) <= EFFORT_WINDOW)
                .fold(
                    (0_u128, 0_u128),
                    |(work, needed), (_, round, difficulty)| {
                        (
                            work.saturating_add(*round),
                            needed.saturating_add(*difficulty),
                        )
                    },
                );
            (needed > 0).then(|| work as f64 * 100.0 / needed as f64)
        })
    }

    pub fn block_closed(&self, mode: Mode, confirmed: bool) {
        self.with(|inner| {
            let blocks = &mut inner.modes[index(mode)].blocks;
            blocks.open = blocks.open.saturating_sub(1);
            if confirmed {
                blocks.confirmed = blocks.confirmed.saturating_add(1);
            } else {
                blocks.lost = blocks.lost.saturating_add(1);
            }
        });
    }

    pub fn block_rewarded(&self, found_at: u64, reward: u64) {
        self.with(|inner| inner.day.rewarded(found_at, reward));
    }

    /// Blocks per mode and their rewards over the last 24 hours.
    pub fn day(&self) -> anyhow::Result<([u64; 2], u128)> {
        let now = now_millis();
        self.with(|inner| inner.day.totals(now))
    }

    pub fn saved_blocks(&self) -> anyhow::Result<SavedBlocks> {
        let now = now_millis();
        self.with(|inner| {
            Ok(SavedBlocks {
                closed: [0, 1].map(|slot| {
                    let blocks = inner.modes[slot].blocks;
                    (blocks.confirmed, blocks.lost)
                }),
                day: inner.day.rows(now)?,
            })
        })
    }

    pub fn set_network(&self, change: impl FnOnce(&mut Network)) {
        self.with(|inner| change(&mut inner.network));
    }

    pub fn network(&self) -> Network {
        self.with(|inner| inner.network.clone())
    }

    pub fn round_work(&self) -> [u128; 2] {
        self.with(|inner| [inner.modes[0].round_work, inner.modes[1].round_work])
    }

    /// Counter increments per worker since the last call.
    pub fn take_counts(&self) -> Vec<WorkerCounts> {
        self.with(|inner| {
            let mut rows = Vec::with_capacity(inner.workers.len());
            for worker in inner.workers.values_mut() {
                if worker.unsaved == (0, 0, 0) {
                    continue;
                }
                let Some(id) = worker.worker_id else { continue };
                let (accepted, rejected, stale) = std::mem::take(&mut worker.unsaved);
                rows.push((
                    id,
                    accepted,
                    rejected,
                    stale,
                    worker.last_share.unwrap_or(0),
                ));
            }
            rows
        })
    }

    pub fn take_history(&self) -> Vec<HistoryBucket> {
        self.with(|inner| {
            roll_history(inner, now_millis());
            std::mem::take(&mut inner.finished)
        })
    }

    pub fn prune(&self) {
        let now = Instant::now();
        self.with(|inner| {
            inner.workers.retain(|_, worker| {
                worker.sessions > 0 || now.duration_since(worker.last_seen) < OFFLINE_KEPT
            });
        });
    }

    pub fn mode(&self, mode: Mode) -> ModeView {
        let now = Instant::now();
        self.with(|inner| {
            let online = inner
                .workers
                .iter()
                .filter(|(key, worker)| key.mode == mode && worker.sessions > 0);
            let totals = &inner.modes[index(mode)];
            ModeView {
                hashrate: totals.short.rate(now),
                hashrate_1h: totals.long.rate(now),
                workers: online.clone().count(),
                miners: online
                    .map(|(key, _)| key.address.as_str())
                    .collect::<BTreeSet<_>>()
                    .len(),
                accepted: totals.accepted,
                rejected: totals.rejected,
                stale: totals.stale,
                round_work: totals.round_work,
                blocks: totals.blocks,
            }
        })
    }

    /// Connected workers and distinct addresses over both modes.
    pub fn online(&self) -> (usize, usize) {
        self.with(|inner| {
            let online = inner
                .workers
                .iter()
                .filter(|(_, worker)| worker.sessions > 0);
            let addresses: BTreeSet<&str> = online
                .clone()
                .map(|(key, _)| key.address.as_str())
                .collect();
            (online.count(), addresses.len())
        })
    }

    pub fn refusals(&self) -> u64 {
        self.with(|inner| inner.refused)
    }

    pub fn rejects(&self) -> BTreeMap<&'static str, u64> {
        self.with(|inner| inner.rejects.clone())
    }

    /// Whether the address has a worker online or seen within the last hour.
    pub fn knows(&self, address: &str) -> bool {
        let now = Instant::now();
        self.with(|inner| {
            of_address(&inner.workers, address).any(|(_, worker)| {
                worker.sessions > 0 || now.duration_since(worker.last_seen) < OFFLINE_SHOWN
            })
        })
    }

    /// Workers of one address that are online or were seen within the last hour, fastest first.
    pub fn workers_of(&self, address: &str) -> Vec<WorkerView> {
        let now = Instant::now();
        let mut workers: Vec<WorkerView> = self.with(|inner| {
            of_address(&inner.workers, address)
                .filter(|(_, worker)| {
                    worker.sessions > 0 || now.duration_since(worker.last_seen) < OFFLINE_SHOWN
                })
                .map(|(key, worker)| worker.view(key, now))
                .collect()
        });
        workers.sort_by(|a, b| b.hashrate.total_cmp(&a.hashrate).then(a.name.cmp(&b.name)));
        workers
    }

    pub fn top(&self, count: usize) -> Vec<TopMiner> {
        let now = Instant::now();
        let mut miners: HashMap<String, (f64, usize)> = HashMap::new();
        self.with(|inner| {
            for (key, worker) in inner
                .workers
                .iter()
                .filter(|(_, worker)| worker.sessions > 0)
            {
                let entry = miners.entry(key.address.clone()).or_default();
                entry.0 += worker.short.rate(now);
                entry.1 = entry.1.saturating_add(1);
            }
        });
        let mut top: Vec<TopMiner> = miners
            .into_iter()
            .map(|(address, (hashrate, workers))| TopMiner {
                address,
                hashrate,
                workers,
            })
            .collect();
        top.sort_by(|a, b| {
            b.hashrate
                .total_cmp(&a.hashrate)
                .then(a.address.cmp(&b.address))
        });
        top.truncate(count);
        top
    }
}

impl Worker {
    fn new(ip: IpAddr, now: Instant) -> Self {
        Self {
            worker_id: None,
            agent: String::new(),
            protocol: Protocol::V1,
            ip,
            sessions: 0,
            connected_at: None,
            last_seen: now,
            difficulty: 0,
            fixed: false,
            short: RateWindow::new(SHORT_BUCKET, SHORT_BUCKETS, now),
            long: RateWindow::new(LONG_BUCKET, LONG_BUCKETS, now),
            accepted: 0,
            rejected: 0,
            stale: 0,
            blocks: 0,
            last_share: None,
            unsaved: (0, 0, 0),
        }
    }

    fn view(&self, key: &WorkerKey, now: Instant) -> WorkerView {
        WorkerView {
            name: key.name.clone(),
            mode: key.mode.name(),
            protocol: self.protocol.name(),
            online: self.sessions > 0,
            agent: self.agent.clone(),
            hashrate: self.short.rate(now),
            hashrate_1h: self.long.rate(now),
            difficulty: self.difficulty,
            fixed_difficulty: self.fixed,
            accepted: self.accepted,
            rejected: self.rejected,
            stale: self.stale,
            blocks: self.blocks,
            last_share: self.last_share,
            connected_at: self.connected_at,
        }
    }
}

fn of_address<'a>(
    workers: &'a BTreeMap<WorkerKey, Worker>,
    address: &'a str,
) -> impl Iterator<Item = (&'a WorkerKey, &'a Worker)> + 'a {
    let first = WorkerKey {
        address: address.to_owned(),
        name: String::new(),
        mode: Mode::Pplns,
    };
    workers
        .range(first..)
        .take_while(move |(key, _)| key.address == address)
}

fn roll_history(inner: &mut Inner, millis: u64) {
    let bucket = millis / HISTORY_BUCKET;
    if bucket == inner.bucket {
        return;
    }
    let totals = std::mem::take(&mut inner.history_totals);
    // These are chart diagnostics. Accepted work, PPLNS and earnings are stored separately.
    // A clock jump or stalled saver must not accumulate unbounded completed buckets.
    if inner.finished.len() < MAX_FINISHED_BUCKETS {
        let mut rows = Vec::with_capacity(inner.history.len() + 2);
        rows.extend(
            inner
                .history
                .drain()
                .map(|((miner, mode), work)| (miner, mode, work)),
        );
        for mode in Mode::ALL {
            if totals[index(mode)] > 0 {
                rows.push((0, mode, totals[index(mode)]));
            }
        }
        if !rows.is_empty() {
            inner.finished.push((inner.bucket, rows));
        }
    } else {
        inner.history.clear();
    }
    inner.bucket = bucket;
}

fn record_history(inner: &mut Inner, miner: u32, mode: Mode, weight: u64) {
    let total = &mut inner.history_totals[index(mode)];
    *total = total.saturating_add(weight);
    // Keep pool totals even when per-miner diagnostic cardinality reaches its bound.
    if inner.history.len() < MAX_HISTORY_ROWS || inner.history.contains_key(&(miner, mode)) {
        let work = inner.history.entry((miner, mode)).or_default();
        *work = work.saturating_add(weight);
    }
}

const fn index(mode: Mode) -> usize {
    match mode {
        Mode::Pplns => 0,
        Mode::Solo => 1,
    }
}

const fn slot(protocol: Protocol) -> usize {
    match protocol {
        Protocol::V1 => 0,
        Protocol::V2 => 1,
    }
}

#[cfg(test)]
#[path = "tests/stats.rs"]
mod tests;
