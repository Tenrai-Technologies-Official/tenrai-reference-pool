//! Commit found work and its share before acknowledging it to the miner.
use crate::{
    chain::Hash384,
    config::Mode,
    db::{
        BlockStatus, Op, Reader,
        accepted::{CommittedBlock, read},
        now_millis,
    },
    hub::Hub,
    node::{NodeClient, Submission},
    pplns::decode_split,
};
use anyhow::{Result, anyhow, ensure};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum Stage {
    Registered,
    Executed,
}
impl Stage {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Registered => "registered",
            Self::Executed => "executed",
        }
    }
}

/// Load settlement metadata while keeping the split on disk.
#[derive(Clone, Debug)]
pub struct OpenBlock {
    pub id: i64,
    pub mode: Mode,
    pub miner: u32,
    pub height: u64,
    pub window_end: u64,
    pub found_at: u64,
    pub fee_bps: u32,
}

pub struct Blocks {
    reader: Reader,
    stages: Mutex<HashMap<Hash384, Stage>>,
}
impl Blocks {
    pub fn new(reader: Reader) -> Self {
        Self {
            reader,
            stages: Mutex::new(HashMap::new()),
        }
    }
    pub async fn get(&self, id: Hash384) -> Result<Option<OpenBlock>> {
        self.reader.read(move |c| read::open(c, id)).await
    }
    pub async fn page(&self, after: i64) -> Result<Vec<(Hash384, OpenBlock)>> {
        self.reader.read(move |c| read::open_page(c, after)).await
    }
    pub async fn weights(&self, block: &OpenBlock) -> Result<Vec<(u32, u128)>> {
        if block.mode == Mode::Solo {
            return Ok(vec![(block.miner, 1)]);
        }
        let id = block.id;
        let bytes = self
            .reader
            .read(move |c| read::split(c, id))
            .await?
            .ok_or_else(|| anyhow!("open PPLNS block has no committed split"))?;
        let weights =
            decode_split(&bytes).ok_or_else(|| anyhow!("invalid committed PPLNS split"))?;
        ensure!(!weights.is_empty(), "empty committed PPLNS split");
        ensure!(
            weights
                .iter()
                .all(|(miner, weight)| *miner > 0 && *weight > 0)
                && weights.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "noncanonical committed PPLNS split"
        );
        Ok(weights)
    }
    /// Display-only stage cache. Settlement reads durable state.
    pub fn advance(&self, id: Hash384, stage: Stage) {
        if let Ok(mut stages) = self.stages.lock() {
            if stages.len() >= 4096 && !stages.contains_key(&id) {
                stages.clear();
            }
            let known = stages.entry(id).or_insert(stage);
            *known = (*known).max(stage);
        }
    }
    pub async fn record_stages(&self, hub: &Hub, rows: &[(Hash384, Stage)]) -> Result<()> {
        for page in rows.chunks(256) {
            hub.writer
                .call(|done| Op::BlockProgress {
                    rows: page.to_vec(),
                    done,
                })
                .await?;
            for &(id, stage) in page {
                self.advance(id, stage);
            }
        }
        Ok(())
    }
    pub fn stage(&self, id: &Hash384) -> Option<Stage> {
        self.stages.lock().ok()?.get(id).copied()
    }
    pub fn close(&self, id: &Hash384) {
        if let Ok(mut stages) = self.stages.lock() {
            stages.remove(id);
        }
    }
    pub fn committed(
        &self,
        hub: &Hub,
        key: &crate::stats::WorkerKey,
        block: &CommittedBlock,
        network: u128,
    ) {
        hub.stats.block_found(
            key,
            network,
            block.found_at,
            hub.config.vardiff.measures_effort(network),
        );
    }
    pub async fn reject(
        &self,
        hub: &Hub,
        block: OpenBlock,
        id: Hash384,
        status: BlockStatus,
        reason: String,
    ) -> Result<()> {
        let changed = hub
            .writer
            .call(|done| Op::BlockOutcome {
                id: block.id,
                retry_height: 0,
                status,
                reason: Some(reason),
                at: now_millis(),
                done,
            })
            .await?;
        if changed {
            self.close(&id);
            hub.stats.block_closed(block.mode, false);
        }
        Ok(())
    }
}

/// Retain pending intents until settlement. Rotate the cursor after each attempt.
/// Retry on a new head or after five seconds.
pub fn start(hub: Arc<Hub>, node: NodeClient, workers: usize) -> Vec<tokio::task::JoinHandle<()>> {
    let mut jobs = Vec::new();
    let workers = workers.clamp(1, 64);
    for slot in 0..workers {
        let hub = Arc::clone(&hub);
        let node = node.clone();
        jobs.push(crate::lifecycle::spawn(hub.shutdown.clone(), async move {
            let mut after = 0_i64;
            loop {
                if *hub.shutdown.borrow() {
                    return;
                }
                let registration_height = hub.registration_height();
                let page = match hub
                    .blocks
                    .reader
                    .read(move |c| {
                        read::payload_partition(c, after, slot, workers, registration_height)
                    })
                    .await
                {
                    Ok(page) => page,
                    Err(error) => {
                        tracing::warn!("cannot read found-work outbox: {error:#}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                if page.is_empty() {
                    after = 0;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                for payload in page {
                    after = payload.block.id;
                    let block = &payload.block;
                    // A lost reply can leave a registered block. Check its status before expiring it.
                    if hub.registration_height() > block.window_end {
                        continue;
                    }
                    let answer = match node.submit(&payload.header, &payload.body_hex).await {
                        Ok(answer) => answer,
                        Err(error) => {
                            tracing::debug!("durable block submission will retry: {error}");
                            continue;
                        }
                    };
                    let (status, reason) = match answer {
                        Submission::Accepted | Submission::Known => {
                            hub.tally.accepted(block.height);
                            (BlockStatus::Pending, None)
                        }
                        Submission::Stale(reason) => {
                            tracing::debug!("found block awaits authoritative lookup: {reason}");
                            continue;
                        }
                        // A rejection can follow an earlier acceptance. Keep the intent until lookup resolves it.
                        Submission::Rejected(reason) => {
                            tracing::debug!("found block awaits authoritative lookup: {reason}");
                            continue;
                        }
                    };
                    if let Err(error) = hub
                        .writer
                        .call(|done| Op::BlockOutcome {
                            id: block.id,
                            retry_height: hub.registration_height().saturating_add(1),
                            status,
                            reason,
                            at: now_millis(),
                            done,
                        })
                        .await
                    {
                        tracing::warn!(
                            "found-work outcome is uncertain; durable intent retained: {error:#}"
                        );
                    }
                }
            }
        }));
    }
    jobs
}

pub fn short(address: &str) -> &str {
    let at = address
        .char_indices()
        .nth(16)
        .map_or(address.len(), |(at, _)| at);
    &address[..at]
}

#[cfg(test)]
#[path = "tests/blocks.rs"]
mod tests;
