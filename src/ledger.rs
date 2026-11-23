//! Settle persisted block rewards in bounded batches.
use crate::{
    accounting::split,
    blocks::Stage,
    chain::Hash384,
    config::format_trai,
    db::{BlockCredit, BlockStatus, Op, now_millis},
    hub::Hub,
    node::{NodeClient, events::ChainEvent},
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, watch},
    time::{MissedTickBehavior, interval, sleep},
};
mod reward_recovery;

const LOOKUP_EVERY: Duration = Duration::from_secs(30);
const LOOKUP_GRACE: u64 = 4;
const SAVE_HEIGHT_EVERY: u64 = 64;
const REPORT_EVERY: Duration = Duration::from_secs(30);
const HEAD_RETRIES: u32 = 10;
const HEAD_RETRY_DELAY: Duration = Duration::from_millis(100);

pub struct Ledger {
    hub: Arc<Hub>,
    node: NodeClient,
    heights: watch::Sender<u64>,
    fee_recipients: Vec<(u32, u32)>,
    saved_height: u64,
    lookup_after: i64,
    report: Report,
    reward_wallet: Option<crate::wallet::Wallet>,
}
#[derive(Default)]
struct Report {
    since: Option<Instant>,
    blocks: u64,
    amount: u64,
    fee: u64,
}
impl Ledger {
    pub fn new(
        hub: Arc<Hub>,
        node: NodeClient,
        heights: watch::Sender<u64>,
        fee_recipients: Vec<(u32, u32)>,
        saved_height: u64,
        reward_wallet: Option<crate::wallet::Wallet>,
    ) -> Self {
        Self {
            hub,
            node,
            heights,
            fee_recipients,
            saved_height,
            lookup_after: 0,
            report: Report::default(),
            reward_wallet,
        }
    }
    pub async fn run(mut self, mut events: mpsc::Receiver<ChainEvent>) {
        let mut lookups = interval(LOOKUP_EVERY);
        lookups.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                event=events.recv()=>match event {
                    Some(ChainEvent::Head(_))=>self.refresh_head().await,
                    Some(ChainEvent::Cut(height))=>self.process_cut_hint(height).await,
                    Some(ChainEvent::Gap)=>{
                        // Missed cuts may have settled open blocks.
                        self.refresh_head().await;
                        self.look_up().await;
                        lookups.reset();
                    }
                    None=>return,
                },
                _=lookups.tick()=>self.look_up().await,
            }
            self.report_if_due();
        }
    }
    fn advance(&self, height: u64) {
        self.heights.send_if_modified(|known| {
            let higher = height > *known;
            if higher {
                *known = height;
            }
            higher
        });
        self.hub
            .stats
            .set_network(|network| network.height = network.height.max(height));
    }
    async fn verified_head(&self) -> Option<u64> {
        match self.node.info().await {
            Ok(info)
                if info.chain_id == self.hub.chain_id.to_hex()
                    && info.network == self.hub.network.name() =>
            {
                Some(info.finalized.height)
            }
            Ok(_) => {
                tracing::warn!("the pool node now serves another chain; reward settlement waits");
                None
            }
            Err(error) => {
                tracing::debug!("cannot verify the node head: {error}");
                None
            }
        }
    }
    async fn refresh_head(&self) {
        if let Some(height) = self.verified_head().await {
            self.advance(height);
        }
    }
    async fn process_cut_hint(&mut self, height: u64) {
        let Some(mut head) = self.verified_head().await else {
            return;
        };
        for _ in 0..HEAD_RETRIES {
            if head >= height {
                break;
            }
            sleep(HEAD_RETRY_DELAY).await;
            let Some(fresh) = self.verified_head().await else {
                return;
            };
            head = fresh;
        }
        self.advance(head);
        if height > head {
            return;
        }
        let cut = match self.node.cut(height).await {
            Ok(cut) => cut,
            Err(error) => {
                tracing::debug!("cannot read cut {height}: {error}");
                return;
            }
        };
        let stages = cut
            .registered_blocks
            .into_iter()
            .map(|id| (id, Stage::Registered))
            .chain(
                cut.executed_blocks
                    .into_iter()
                    .map(|id| (id, Stage::Executed)),
            )
            .collect::<Vec<_>>();
        if let Err(error) = self.hub.blocks.record_stages(&self.hub, &stages).await {
            tracing::warn!("cannot persist finalized work progress: {error:#}");
        }
        let paid = cut.rewards.unwrap_or_default();
        for batch in paid.chunks(16) {
            self.credit_many(height, batch).await;
        }
        if height.saturating_sub(self.saved_height) >= SAVE_HEIGHT_EVERY {
            // Reconcile open blocks even if a cut notification was dropped.
            self.hub.writer.send(Op::CutHeight(height));
            self.saved_height = height;
        }
    }
    /// Materialize up to sixteen splits and settle them in one durable batch.
    async fn credit_many(&mut self, cut: u64, paid: &[(Hash384, u64)]) {
        if paid.is_empty() || paid.len() > 16 {
            return;
        }
        let mut credits = Vec::with_capacity(paid.len());
        let mut closed = Vec::with_capacity(paid.len());
        for &(id, reward) in paid {
            let block = match self.hub.blocks.get(id).await {
                Ok(Some(block)) => block,
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!("cannot read reward intent: {error:#}");
                    continue;
                }
            };
            let weights = match self.hub.blocks.weights(&block).await {
                Ok(weights) => weights,
                Err(error) => {
                    tracing::error!("cannot read committed split of {id}: {error:#}");
                    continue;
                }
            };
            let split = match split(reward, block.fee_bps, &weights, &self.fee_recipients) {
                Ok(split) => split,
                Err(error) => {
                    tracing::error!("cannot split reward of {id}: {error:#}");
                    continue;
                }
            };
            let fee = split.fee;
            credits.push(BlockCredit {
                id: block.id,
                reward,
                fee,
                credits: split.credits,
            });
            closed.push((id, block, reward, fee));
        }
        if credits.is_empty() {
            return;
        }
        match self
            .hub
            .writer
            .call(|done| Op::Credit {
                cut,
                blocks: credits,
                at: now_millis(),
                done,
            })
            .await
        {
            Ok(credited) => {
                self.saved_height = self.saved_height.max(cut);
                // The SQL transaction owns settlement. Skip uncertain statistics.
                let all = credited == closed.len() as u64;
                for (id, block, reward, fee) in closed {
                    self.hub.blocks.close(&id);
                    if all {
                        self.hub.stats.block_closed(block.mode, true);
                        self.hub.stats.block_rewarded(block.found_at, reward);
                        let report = &mut self.report;
                        report.since.get_or_insert_with(Instant::now);
                        report.blocks = report.blocks.saturating_add(1);
                        report.amount = report.amount.saturating_add(reward);
                        report.fee = report.fee.saturating_add(fee);
                    }
                }
            }
            Err(error) => {
                tracing::error!("cannot book bounded reward batch at cut {cut}: {error:#}")
            }
        }
    }
    /// Rotate through all open blocks, including those from missed cuts.
    async fn look_up(&mut self) {
        let mut page = match self.hub.blocks.page(self.lookup_after).await {
            Ok(page) => page,
            Err(error) => {
                tracing::warn!("cannot read open-block page: {error:#}");
                return;
            }
        };
        if page.is_empty() && self.lookup_after != 0 {
            self.lookup_after = 0;
            page = match self.hub.blocks.page(0).await {
                Ok(page) => page,
                Err(error) => {
                    tracing::warn!("cannot read open-block page: {error:#}");
                    return;
                }
            };
        }
        if page.is_empty() {
            return;
        }
        let height = *self.heights.borrow();
        let mut recoveries = 0;
        for (id, block) in page {
            // Leave unprocessed rows for the next pass.
            if recoveries == reward_recovery::MAX_RECOVERIES {
                break;
            }
            self.lookup_after = block.id;
            if height <= block.window_end.saturating_add(LOOKUP_GRACE) {
                continue;
            }
            let state = match self
                .node
                .block_in_registration_window(id, block.height, block.window_end, self.hub.chain_id)
                .await
            {
                Ok(state) => state,
                Err(error) => {
                    tracing::debug!("cannot look up block {id}: {error}");
                    if self.reward_wallet.is_some() {
                        recoveries += 1;
                        self.recover_reward(id).await;
                    }
                    continue;
                }
            };
            if let Some(state) = state {
                let stage = if state.execution_height.is_some() {
                    Some(Stage::Executed)
                } else if state.registration_height.is_some() {
                    Some(Stage::Registered)
                } else {
                    None
                };
                if let Some(stage) = stage
                    && let Err(error) = self
                        .hub
                        .blocks
                        .record_stages(&self.hub, &[(id, stage)])
                        .await
                {
                    tracing::warn!("cannot persist recovered block progress: {error:#}");
                }
                if let Some(reward) = state.reward {
                    match reward.paid() {
                        Ok(Some(amount)) => {
                            let cut = reward
                                .payable_from_height
                                .or(state.execution_height)
                                .unwrap_or(height);
                            self.credit_many(cut, &[(id, amount)]).await;
                        }
                        Ok(None) => {}
                        Err(error) => tracing::warn!("cannot read reward of {id}: {error}"),
                    }
                } else if state.state == "pooled"
                    && state.registration_height.is_none()
                    && state.execution_height.is_none()
                    && let Err(error) = self
                        .hub
                        .blocks
                        .reject(
                            &self.hub,
                            block,
                            id,
                            BlockStatus::Expired,
                            "not registered before the window closed".into(),
                        )
                        .await
                {
                    tracing::warn!("cannot close expired block {id}: {error:#}");
                }
            } else if let Err(error) = self
                .hub
                .blocks
                .reject(
                    &self.hub,
                    block,
                    id,
                    BlockStatus::Expired,
                    "the node does not know the block after its window closed".into(),
                )
                .await
            {
                tracing::warn!("cannot close expired block {id}: {error:#}");
            }
        }
    }
    fn report_if_due(&mut self) {
        let Some(since) = self.report.since else {
            return;
        };
        if since.elapsed() < REPORT_EVERY {
            return;
        }
        let report = std::mem::take(&mut self.report);
        tracing::info!(
            "{} blocks confirmed in {} s: {} TRAI, {} TRAI pool fee",
            report.blocks,
            since.elapsed().as_secs(),
            format_trai(report.amount),
            format_trai(report.fee)
        );
    }
}
