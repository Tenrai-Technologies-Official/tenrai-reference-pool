//! Work templates and epoch datasets.

mod lifetime;
use lifetime::{BuildLifetime, SourceControl};

use crate::{
    chain::{Hash384, Lock},
    node::{NodeClient, NodeError, SyncStatus},
    outage::{NODE_PATIENCE, NodeOutage, Outage, WAIT_REMINDER, waited},
    pow::{Hasher, Missing, TenQMode, epoch_for, epoch_start, room, template::Template},
    stats::Stats,
};
use anyhow::{Context, Result, anyhow};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc, oneshot, watch},
    time::{MissedTickBehavior, interval},
};

/// Lifetime of a transaction lease on the node.
pub const TEMPLATE_LEASE: Duration = Duration::from_secs(5);
const REQUEST_QUEUE: usize = 8_192;
const SYNC_CHECK: Duration = Duration::from_secs(5);
const NODE_RETRY: Duration = Duration::from_secs(1);
/// Start the next epoch's dataset this many heights in advance.
const PREFETCH_HEIGHTS: u64 = 1_024;
/// Retain the previous epoch for this many heights.
const PREVIOUS_EPOCH_HEIGHTS: u64 = 64;
/// Memory headroom for background dataset builds.
const SPARE_MEMORY_BYTES: u64 = 512 << 20;
const DATASET_RETRY: Duration = Duration::from_secs(60);
const MAX_TRANSACTIONS: usize = 64;

pub struct Work {
    pub template: Template,
    pub hasher: Arc<Hasher>,
    pub fetched: Instant,
}

type Reply = oneshot::Sender<Result<Arc<Work>, String>>;

/// Work reply channel and the last empty template used by the session.
struct Request {
    reply: Reply,
    spent: Option<Hash384>,
}

#[derive(Clone)]
pub struct WorkSource {
    requests: mpsc::Sender<Request>,
    filler: Filler,
    control: Arc<SourceControl>,
}

type Filler = Arc<Mutex<Option<Arc<Work>>>>;

impl WorkSource {
    /// Stop intake and drain work sources and dataset builds before releasing the database.
    pub async fn close(&self) -> Result<()> {
        self.control.close().await?;
        self.filler
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        Ok(())
    }
    pub fn start(
        node: NodeClient,
        payout: (String, Lock),
        chain_id: Hash384,
        hashers: Arc<Hashers>,
        heights: watch::Sender<u64>,
        block_rate: u32,
        stats: Arc<Stats>,
    ) -> Self {
        let (requests, receiver) = mpsc::channel(REQUEST_QUEUE);
        let filler = Filler::default();
        let source = Source {
            node,
            payout,
            chain_id,
            hashers,
            heights,
            stats,
            pending: VecDeque::new(),
            shared: None,
            outage: NodeOutage::new(NODE_PATIENCE, WAIT_REMINDER),
            retry_at: None,
            filler: Arc::clone(&filler),
            filler_due: true,
        };
        let control = SourceControl::start(source, receiver, block_rate);
        Self {
            requests,
            filler,
            control,
        }
    }

    /// Shared empty template after the leased body is consumed.
    pub fn filler(&self) -> Option<Arc<Work>> {
        if self.control.closed() {
            return None;
        }
        self.filler.lock().ok()?.clone()
    }

    /// Return `None` at the queue limit. A spent empty template requires different work.
    pub fn request(
        &self,
        spent: Option<Hash384>,
    ) -> Option<oneshot::Receiver<Result<Arc<Work>, String>>> {
        if self.control.closed() {
            return None;
        }
        let (reply, answer) = oneshot::channel();
        self.requests.try_send(Request { reply, spent }).ok()?;
        Some(answer)
    }
}

struct Source {
    node: NodeClient,
    payout: (String, Lock),
    chain_id: Hash384,
    hashers: Arc<Hashers>,
    heights: watch::Sender<u64>,
    stats: Arc<Stats>,
    pending: VecDeque<Reply>,
    shared: Option<(Arc<Work>, u64)>,
    outage: NodeOutage,
    retry_at: Option<Instant>,
    filler: Filler,
    filler_due: bool,
}

impl Source {
    async fn run(mut self, mut requests: mpsc::Receiver<Request>, block_rate: u32) {
        let slot = Duration::from_nanos(1_000_000_000 / u64::from(block_rate.max(1)));
        let mut ticker = interval(slot);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut sync = interval(SYNC_CHECK);
        sync.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut heights = self.heights.subscribe();
        loop {
            tokio::select! {
                request = requests.recv() => match request {
                    Some(request) => self.take(request),
                    None => return,
                },
                changed = heights.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    drop(heights.borrow_and_update());
                    self.shared = None;
                    self.filler_due = true;
                }
                _ = ticker.tick(), if !self.pending.is_empty() || self.filler_due => {
                    if !self.pending.is_empty() {
                        self.serve().await;
                    }
                    if self.filler_due {
                        self.refill().await;
                    }
                }
                _ = sync.tick() => self.check_sync().await,
            }
        }
    }

    fn take(&mut self, request: Request) {
        let height = *self.heights.borrow();
        match &self.shared {
            Some((work, at))
                if *at == height
                    && work.fetched.elapsed() < TEMPLATE_LEASE
                    && request.spent != Some(work.template.id) =>
            {
                let _ = request.reply.send(Ok(Arc::clone(work)));
            }
            _ => self.pending.push_back(request.reply),
        }
    }

    async fn serve(&mut self) {
        if self.retry_at.is_some_and(|at| Instant::now() < at) {
            return;
        }
        self.pending.retain(|reply| !reply.is_closed());
        if self.pending.is_empty() {
            return;
        }
        let height = *self.heights.borrow();
        match self.fetch(MAX_TRANSACTIONS).await {
            Ok(work) => {
                self.retry_at = None;
                if let Some(failed) = self.outage.recovered() {
                    tracing::info!("miners get new work again after {}", waited(failed));
                }
                self.heights
                    .send_if_modified(|known| raise(known, work.template.height()));
                self.stats.set_network(|network| {
                    network.difficulty = work.template.network_difficulty();
                    network.work_at = Some(crate::db::now_millis());
                });
                if work.template.transactions == 0 {
                    for reply in self.pending.drain(..) {
                        let _ = reply.send(Ok(Arc::clone(&work)));
                    }
                    self.keep_filler(&work);
                    self.shared = Some((work, height));
                } else if let Some(reply) = self.pending.pop_front() {
                    let _ = reply.send(Ok(work));
                }
            }
            Err(error) => {
                self.retry_at = Some(Instant::now() + NODE_RETRY);
                match self.outage.failed() {
                    Outage::Brief | Outage::Quiet => {
                        tracing::debug!("no work from the node: {error:#}");
                    }
                    Outage::Paused => {
                        self.shared = None;
                        tracing::warn!("no new work for miners: {}", self.why(&error).await);
                    }
                    Outage::Lasting(failed) => tracing::info!(
                        "still no new work for miners after {}: {}",
                        waited(failed),
                        self.why(&error).await
                    ),
                }
            }
        }
    }

    async fn why(&self, error: &anyhow::Error) -> String {
        let reason = match error.downcast_ref::<NodeError>() {
            Some(NodeError::Status { message, .. }) => message.clone(),
            Some(NodeError::Transport(_)) => {
                format!("the node at {} does not answer", self.node.mining_url())
            }
            _ => format!("{error:#}"),
        };
        match self.node.sync().await {
            Ok(status) if !status.synced => format!("{reason} ({})", progress(&status)),
            _ => reason,
        }
    }

    /// Fetch one empty template per height while leasing transaction templates.
    async fn refill(&mut self) {
        self.filler_due = false;
        if self.retry_at.is_some_and(|at| Instant::now() < at) {
            return;
        }
        match self.fetch(0).await {
            Ok(work) => self.keep_filler(&work),
            Err(error) => tracing::debug!("no empty template: {error:#}"),
        }
    }

    fn keep_filler(&mut self, work: &Arc<Work>) {
        if work.template.transactions != 0 {
            return;
        }
        self.filler_due = false;
        if let Ok(mut filler) = self.filler.lock() {
            *filler = Some(Arc::clone(work));
        }
    }

    async fn fetch(&self, max_transactions: usize) -> Result<Arc<Work>> {
        let response = self
            .node
            .template(&self.payout.0, max_transactions)
            .await
            .context("template request failed")?;
        let template = Template::parse(response, self.chain_id, &self.payout.1)?;
        let hasher = self.hashers.ensure(template.epoch).await?;
        self.hashers.advance(template.height());
        tracing::debug!(
            "template at height {} with {} transactions",
            template.height(),
            template.transactions
        );
        Ok(Arc::new(Work {
            template,
            hasher,
            fetched: Instant::now(),
        }))
    }

    /// Refresh sync status for the public statistics.
    async fn check_sync(&mut self) {
        let (info, status) = tokio::join!(self.node.info(), self.node.sync());
        match (info, status) {
            (Ok(info), Ok(status)) if info.chain_id == self.chain_id.to_hex() => {
                let height = status.finalized_height.min(info.finalized.height);
                self.heights.send_if_modified(|known| raise(known, height));
                self.stats.set_network(|network| {
                    network.synced = status.synced;
                    network.peers = status.peers;
                });
            }
            (Ok(_), Ok(_)) => {
                tracing::debug!("the work source now serves another chain; height is unchanged");
                self.stats.set_network(|network| network.synced = false);
            }
            (Err(error), _) | (_, Err(error)) => {
                tracing::debug!("cannot read the node's sync state: {error}");
                self.stats.set_network(|network| network.synced = false);
            }
        }
    }
}

fn progress(status: &SyncStatus) -> String {
    if status.peers == 0 {
        return format!("height {}, no peers", status.finalized_height);
    }
    let cuts = if status.behind_by == 1 { "cut" } else { "cuts" };
    let peers = if status.peers == 1 { "peer" } else { "peers" };
    format!(
        "height {}, {} {cuts} behind, {} {peers}",
        status.finalized_height, status.behind_by, status.peers
    )
}

fn raise(known: &mut u64, height: u64) -> bool {
    if height > *known {
        *known = height;
        true
    } else {
        false
    }
}

/// TenQ hashers for adjacent epochs.
pub struct Hashers {
    profile: u32,
    chain: [u8; 48],
    mode: TenQMode,
    threads: usize,
    prefetch: bool,
    free: fn() -> Option<u64>,
    ready: Mutex<BTreeMap<u32, Arc<Hasher>>>,
    building: AsyncMutex<()>,
    lifetime: Arc<BuildLifetime>,
    reported: Mutex<BTreeSet<(u32, bool)>>,
    retried: Mutex<Option<Instant>>,
}

/// A missing memory estimate allows the build to proceed.
fn free_memory() -> Option<u64> {
    tenrai_platform::available_memory_bytes().ok()
}

impl Hashers {
    pub(crate) fn new(
        profile: u32,
        chain: Hash384,
        mode: TenQMode,
        threads: usize,
        prefetch: bool,
        owner: crate::db::ProcessLease,
    ) -> Self {
        Self::new_inner(profile, chain, mode, threads, prefetch, Some(owner))
    }

    #[cfg(test)]
    fn for_test(
        profile: u32,
        chain: Hash384,
        mode: TenQMode,
        threads: usize,
        prefetch: bool,
    ) -> Self {
        Self::new_inner(profile, chain, mode, threads, prefetch, None)
    }
    fn new_inner(
        profile: u32,
        chain: Hash384,
        mode: TenQMode,
        threads: usize,
        prefetch: bool,
        owner: Option<crate::db::ProcessLease>,
    ) -> Self {
        Self {
            profile,
            chain: chain.0,
            mode,
            threads: threads.max(1),
            prefetch,
            free: free_memory,
            ready: Mutex::new(BTreeMap::new()),
            building: AsyncMutex::new(()),
            lifetime: Arc::new(BuildLifetime::new(owner)),
            reported: Mutex::new(BTreeSet::new()),
            retried: Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub fn with_free_memory(mut self, free: fn() -> Option<u64>) -> Self {
        self.free = free;
        self
    }

    pub fn get(&self, epoch: u32) -> Option<Arc<Hasher>> {
        self.ready.lock().ok()?.get(&epoch).cloned()
    }

    /// Use the cache until a full dataset is available.
    pub async fn ensure(&self, epoch: u32) -> Result<Arc<Hasher>> {
        self.lifetime.ensure_open()?;
        if let Some(hasher) = self.get(epoch) {
            return Ok(hasher);
        }
        let _building = self.building.lock().await;
        if let Some(hasher) = self.get(epoch) {
            return Ok(hasher);
        }
        let (hasher, missing) = self.build(epoch, 0).await?;
        if let Some(missing) = missing {
            self.report(epoch, false, &missing);
        }
        self.lifetime.ensure_open()?;
        Ok(self.keep(hasher))
    }

    fn keep(&self, hasher: Hasher) -> Arc<Hasher> {
        let hasher = Arc::new(hasher);
        if let Ok(mut ready) = self.ready.lock() {
            ready.insert(hasher.epoch(), Arc::clone(&hasher));
        }
        hasher
    }

    /// Build the epoch cache and a full dataset if memory allows.
    async fn build(&self, epoch: u32, spare: u64) -> Result<(Hasher, Option<Missing>)> {
        let (profile, chain, mode, threads, free) =
            (self.profile, self.chain, self.mode, self.threads, self.free);
        let started = Instant::now();
        let lease = self.lifetime.begin().await?;
        #[cfg(test)]
        let hook = self.lifetime.take_hook();
        let (hasher, missing, _physical_lease) = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some(hook) = hook {
                hook.wait();
            }
            let mut hasher = Hasher::light(profile, chain, epoch)?;
            hasher.bind_process_owner(lease.process_owner());
            if mode == TenQMode::Light {
                return Some((hasher, None, lease));
            }
            let missing = match room(profile, spare, free()) {
                Ok(()) => {
                    tracing::info!("building the TenQ dataset for epoch {epoch}");
                    hasher.add_dataset(threads).err()
                }
                Err(missing) => Some(missing),
            };
            // Drop abandoned allocations before releasing the OS lease.
            Some((hasher, missing, lease))
        })
        .await?
        .ok_or_else(|| anyhow!("TenQ setup for epoch {epoch} failed"))?;
        if hasher.is_full() {
            tracing::info!(
                "TenQ dataset for epoch {epoch} ready after {} s",
                started.elapsed().as_secs()
            );
        }
        Ok((hasher, missing))
    }

    fn report(&self, epoch: u32, ahead: bool, missing: &Missing) {
        let first = self
            .reported
            .lock()
            .is_ok_and(|mut reported| reported.insert((epoch, ahead)));
        if !first {
            return;
        }
        if ahead {
            tracing::warn!(
                "the TenQ dataset for epoch {epoch} is not built ahead of time: {missing}; the epoch starts from the light cache and the dataset follows when there is room"
            );
        } else {
            tracing::warn!(
                "the TenQ dataset for epoch {epoch} cannot be built: {missing}; shares are checked from the light cache until there is room"
            );
        }
    }

    /// Retire old epochs and prefetch the next one.
    pub fn advance(self: &Arc<Self>, finalized_height: u64) {
        if self.lifetime.closed() {
            return;
        }
        let Some(height) = finalized_height.checked_add(1) else {
            return;
        };
        let Some(epoch) = epoch_for(height) else {
            return;
        };
        let changed = height.saturating_sub(epoch_start(epoch)) < PREVIOUS_EPOCH_HEIGHTS;
        if let Ok(mut ready) = self.ready.lock() {
            ready.retain(|known, _| *known >= epoch || (changed && *known + 1 == epoch));
        }
        if let Ok(mut reported) = self.reported.lock() {
            reported.retain(|(known, _)| *known >= epoch);
        }
        // Check the chain and work version as well as the height.
        if self.get(epoch).is_none() {
            return;
        }
        let Some(next) = epoch.checked_add(1) else {
            return;
        };
        let near = epoch_start(next).saturating_sub(height) <= PREFETCH_HEIGHTS;
        if self.prefetch && near && self.get(next).is_none() {
            self.in_background(next, true);
        } else if self
            .get(epoch)
            .is_some_and(|hasher| self.lacks_dataset(&hasher))
            && self.retry_due()
        {
            self.in_background(epoch, false);
        }
    }

    fn lacks_dataset(&self, hasher: &Hasher) -> bool {
        self.mode == TenQMode::Full && !hasher.is_full()
    }

    fn retry_due(&self) -> bool {
        let Ok(mut retried) = self.retried.lock() else {
            return false;
        };
        let now = Instant::now();
        if retried.is_some_and(|at| now.duration_since(at) < DATASET_RETRY) {
            return false;
        }
        *retried = Some(now);
        true
    }

    fn in_background(self: &Arc<Self>, epoch: u32, ahead: bool) {
        let hashers = Arc::clone(self);
        self.lifetime.spawn_prefetch(move |previous| async move {
            if let Some(previous) = previous
                && let Err(error) = previous.await
            {
                tracing::warn!("previous TenQ setup stopped: {error}");
            }
            if let Err(error) = hashers.prepare(epoch, ahead).await
                && !hashers.lifetime.closed()
            {
                tracing::warn!("cannot prepare TenQ epoch {epoch}: {error:#}");
            }
        });
    }

    /// Keep a light cache when a prefetched dataset exceeds the memory budget.
    async fn prepare(&self, epoch: u32, ahead: bool) -> Result<()> {
        self.lifetime.ensure_open()?;
        let _building = self.building.lock().await;
        match self.get(epoch) {
            Some(hasher) if ahead || !self.lacks_dataset(&hasher) => return Ok(()),
            Some(_) if room(self.profile, SPARE_MEMORY_BYTES, (self.free)()).is_err() => {
                return Ok(());
            }
            _ => {}
        }
        let (hasher, missing) = self.build(epoch, SPARE_MEMORY_BYTES).await?;
        match missing {
            Some(missing) if ahead => self.report(epoch, true, &missing),
            Some(_) => return Ok(()),
            None if !ahead => tracing::info!(
                "the TenQ dataset for epoch {epoch} is ready; shares are checked with it again"
            ),
            None => {}
        }
        self.lifetime.ensure_open()?;
        self.keep(hasher);
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        self.lifetime.close().await?;
        self.ready
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        Ok(())
    }

    pub fn epochs(&self) -> Vec<u32> {
        self.ready
            .lock()
            .map(|ready| ready.keys().copied().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
#[path = "tests/hashers.rs"]
mod tests;
