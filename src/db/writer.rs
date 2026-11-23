use super::{BlockIds, BlockStatus, PayoutStatus, int, put_setting};
use crate::{chain::Hash384, config::Mode};
use anyhow::{Result, anyhow};
use rusqlite::{Connection, Transaction, TransactionBehavior, params};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

const QUEUE: usize = 65_536;
const MAX_BATCH: usize = 8_192;
/// Writes are batched for up to this long; every commit rewrites the pages it touched.
const FLUSH_DELAY: Duration = Duration::from_secs(1);
const ACCEPT_FLUSH_DELAY: Duration = Duration::from_millis(20);
const SLOW_COMMIT: Duration = Duration::from_millis(500);
/// Rows deleted per rule and `TrimBlocks` call, which keeps commits short.
pub const TRIM_ROWS: usize = 500;

pub struct NewBlock {
    pub hash: String,
    pub mode: Mode,
    pub miner: u32,
    pub worker: super::WorkerId,
    pub height: u64,
    pub window_end: u64,
    pub found_at: u64,
    pub network_difficulty: u128,
    pub round_work: u128,
    pub fee_bps: u32,
    pub share_id: Option<u64>,
    pub window_work: Option<u128>,
    pub window_shares: Option<u32>,
    pub split: Option<Vec<u8>>,
}

pub struct BlockCredit {
    pub id: i64,
    pub reward: u64,
    pub fee: u64,
    pub credits: Vec<(u32, u64)>,
}

pub enum Op {
    AcceptWork {
        request: Box<super::accepted::AcceptedWork>,
        permit: std::sync::Arc<tokio::sync::OwnedSemaphorePermit>,
        pending: Option<std::sync::Arc<super::accepted::pending::WorkLease>>,
        done: oneshot::Sender<Result<super::accepted::AcceptedReceipt, String>>,
    },
    ReplayWork {
        job: Hash384,
        nonce: [u8; 32],
        address: String,
        pending: std::sync::Arc<super::accepted::pending::PendingWork>,
        permit: std::sync::Arc<tokio::sync::OwnedSemaphorePermit>,
        done: oneshot::Sender<Result<super::accepted::ReplayReceipt, String>>,
    },
    EnsureMiner {
        address: String,
        at: u64,
        done: oneshot::Sender<Result<u32, String>>,
    },
    Miner {
        id: u32,
        address: String,
        at: u64,
    },
    #[cfg(test)]
    Worker {
        id: super::WorkerId,
        miner: u32,
        name: String,
        at: u64,
    },
    Share {
        id: u64,
        time: u64,
        miner: u32,
        worker: super::WorkerId,
        weight: u64,
    },
    Telemetry(Telemetry),
    BlockFound {
        block: NewBlock,
        done: oneshot::Sender<Result<i64, String>>,
    },
    BlockProgress {
        rows: Vec<(Hash384, crate::blocks::Stage)>,
        done: oneshot::Sender<Result<(), String>>,
    },
    BlockOutcome {
        id: i64,
        retry_height: u64,
        status: BlockStatus,
        reason: Option<String>,
        at: u64,
        done: oneshot::Sender<Result<bool, String>>,
    },
    BlockState {
        id: i64,
        status: BlockStatus,
        reason: Option<String>,
        at: u64,
    },
    /// Credits confirmed blocks once; blocks that are no longer open are skipped.
    Credit {
        cut: u64,
        blocks: Vec<BlockCredit>,
        at: u64,
        done: oneshot::Sender<Result<u64, String>>,
    },
    CutHeight(u64),
    PayoutCreate {
        request_prefix: String,
        items: Vec<(u32, u64)>,
        at: u64,
        done: oneshot::Sender<Result<(i64, String), String>>,
    },
    PayoutAttempt {
        id: i64,
        request_id: String,
        scope: crate::wallet::RequestScope,
        done: oneshot::Sender<Result<bool, String>>,
    },
    PayoutSent {
        id: i64,
        transaction: String,
        fee: u64,
        valid_until: Option<u64>,
        fee_shares: Vec<(u32, u64)>,
        at: u64,
        done: oneshot::Sender<Result<(), String>>,
    },
    PayoutNeverSigned {
        id: i64,
        request_id: String,
        scope: crate::wallet::RequestScope,
        at: u64,
        done: oneshot::Sender<Result<(), String>>,
    },
    PayoutClosed {
        id: i64,
        paid: bool,
        error: Option<String>,
        at: u64,
        done: oneshot::Sender<Result<(), String>>,
    },
    PruneAccepted {
        shares_before: u64,
        hashrate_before: u64,
        splits_before: u64,
        sources_before: u64,
        receipts_before: u64,
        height: u64,
        done: oneshot::Sender<Result<bool, String>>,
    },
    Prune {
        shares_before: u64,
        hashrate_before: u64,
        /// Drops the PPLNS split of blocks settled before this time.
        splits_before: u64,
        sources_before: u64,
    },
    /// Deletes old closed blocks beyond the newest `keep`; replies with the count.
    TrimBlocks {
        before: Option<u64>,
        keep: Option<u64>,
        done: oneshot::Sender<Result<usize, String>>,
    },
    Setting {
        key: &'static str,
        value: String,
    },
    Flush(oneshot::Sender<()>),
    Shutdown(oneshot::Sender<()>),
}

#[derive(Clone)]
pub struct Writer {
    sender: mpsc::Sender<Op>,
    accepting: std::sync::Arc<tokio::sync::Semaphore>,
    replaying: std::sync::Arc<tokio::sync::Semaphore>,
    pending: std::sync::Arc<super::accepted::pending::PendingWork>,
    telemetry: telemetry::Budget,
    _checkpoint: std::sync::Arc<super::checkpoint::Owner>,
}

static DROPPED: AtomicU64 = AtomicU64::new(0);

impl Writer {
    pub(crate) fn process_lease(&self) -> Result<super::ProcessLease> {
        self._checkpoint
            .process_lease()
            .ok_or_else(|| anyhow!("physical pool work needs a process database owner"))
    }
    pub(super) fn start(
        connection: Connection,
        ids: BlockIds,
        checkpoint: std::sync::Arc<super::checkpoint::Owner>,
    ) -> Result<(Self, std::thread::JoinHandle<()>)> {
        let (sender, receiver) = mpsc::channel(QUEUE);
        let worker_checkpoint = std::sync::Arc::clone(&checkpoint);
        let thread = std::thread::Builder::new()
            .name("pool-db".into())
            .spawn(move || {
                let _checkpoint = worker_checkpoint;
                run(connection, receiver, &ids);
            })?;
        Ok((
            Self {
                sender,
                accepting: std::sync::Arc::new(tokio::sync::Semaphore::new(
                    super::accepted::MAX_ACCEPTING,
                )),
                replaying: std::sync::Arc::new(tokio::sync::Semaphore::new(
                    super::accepted::pending::MAX_REPLAYING,
                )),
                pending: std::sync::Arc::new(super::accepted::pending::PendingWork::default()),
                telemetry: telemetry::Budget::default(),
                _checkpoint: checkpoint,
            },
            thread,
        ))
    }

    /// Queues a write. If the queue is full, the write is dropped and counted.
    pub fn send(&self, op: Op) {
        if self.sender.try_send(op).is_err()
            && DROPPED
                .fetch_add(1, Ordering::Relaxed)
                .is_multiple_of(10_000)
        {
            tracing::error!("the database falls behind; writes are being dropped");
        }
    }

    pub async fn call<T>(
        &self,
        op: impl FnOnce(oneshot::Sender<Result<T, String>>) -> Op,
    ) -> Result<T> {
        let (done, answer) = oneshot::channel();
        self.sender
            .send(op(done))
            .await
            .map_err(|_| anyhow!("the database writer stopped"))?;
        answer
            .await
            .map_err(|_| anyhow!("the database writer stopped"))?
            .map_err(|error| anyhow!(error))
    }

    /// Queues an acknowledged write without waiting for its commit.
    pub async fn submit<T>(
        &self,
        op: impl FnOnce(oneshot::Sender<Result<T, String>>) -> Op,
    ) -> Result<oneshot::Receiver<Result<T, String>>> {
        let (done, answer) = oneshot::channel();
        self.sender
            .send(op(done))
            .await
            .map_err(|_| anyhow!("the database writer stopped"))?;
        Ok(answer)
    }

    pub async fn flush(&self) {
        let (done, answer) = oneshot::channel();
        if self.sender.send(Op::Flush(done)).await.is_ok() {
            let _ = answer.await;
        }
    }

    pub fn queued(&self) -> usize {
        self.sender.max_capacity() - self.sender.capacity()
    }

    pub fn dropped() -> u64 {
        DROPPED.load(Ordering::Relaxed)
    }
}

fn run(mut connection: Connection, mut receiver: mpsc::Receiver<Op>, ids: &BlockIds) {
    while let Some(first) = receiver.blocking_recv() {
        let mut batch = vec![first];
        let mut deadline = Instant::now()
            + if matches!(batch[0], Op::AcceptWork { .. } | Op::ReplayWork { .. }) {
                ACCEPT_FLUSH_DELAY
            } else {
                FLUSH_DELAY
            };
        while batch.len() < MAX_BATCH && !batch.iter().any(Op::urgent) {
            match receiver.try_recv() {
                Ok(op) => {
                    if matches!(op, Op::AcceptWork { .. } | Op::ReplayWork { .. }) {
                        deadline = deadline.min(Instant::now() + ACCEPT_FLUSH_DELAY);
                    }
                    batch.push(op);
                }
                Err(mpsc::error::TryRecvError::Empty) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
        while batch.len() < MAX_BATCH {
            match receiver.try_recv() {
                Ok(op) => batch.push(op),
                Err(_) => break,
            }
        }
        let stopping = batch.iter().any(|op| matches!(op, Op::Shutdown(_)));
        if stopping {
            receiver.close();
        }
        commit(&mut connection, batch, ids);
        if stopping {
            return;
        }
    }
}

/// Block rows added and removed by a commit, applied to the hash map once it is durable.
#[derive(Default)]
struct BlockRows {
    added: Vec<(Hash384, i64)>,
    removed: Vec<Hash384>,
}

fn commit(connection: &mut Connection, batch: Vec<Op>, ids: &BlockIds) {
    let started = Instant::now();
    let count = batch.len();
    // These permits survive every await cancellation and remain held through physical commit
    // or rollback, even when apply consumes an operation before the transaction finishes.
    let accepting = batch
        .iter()
        .filter_map(|op| match op {
            Op::AcceptWork { permit, .. } | Op::ReplayWork { permit, .. } => {
                Some(std::sync::Arc::clone(permit))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let pending_work = batch
        .iter()
        .filter_map(|op| match op {
            Op::AcceptWork { pending, .. } => pending.clone(),
            _ => None,
        })
        .collect::<Vec<_>>();
    let telemetry = batch
        .iter()
        .filter_map(|op| match op {
            Op::Telemetry(record) => Some(record.keep_alive()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut kinds = BTreeMap::<&str, usize>::new();
    for op in &batch {
        *kinds.entry(op.kind()).or_default() += 1;
    }
    let kinds = kinds
        .iter()
        .map(|(kind, count)| format!("{count} {kind}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut replies = Vec::new();
    let sync = if batch.iter().any(Op::durable) {
        "FULL"
    } else {
        "NORMAL"
    };
    let mut rows = BlockRows::default();
    let result = (|| -> Result<()> {
        // A failed FULL-sync request may never acknowledge monetary intent or effects.
        connection.pragma_update(None, "synchronous", sync)?;
        // IMMEDIATE takes the write lock at BEGIN, where the busy timeout applies; an upgrade would not wait.
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for op in batch {
            apply(&transaction, op, &mut replies, &mut rows)?;
        }
        transaction.commit()?;
        Ok(())
    })();
    if result.is_ok() {
        ids.change(rows.added, rows.removed);
    }
    let failed = result.as_ref().err().map(|error| format!("{error:#}"));
    if let Some(error) = &failed {
        tracing::error!("database write of {count} changes failed: {error}");
    }
    // A batch's places go back on commit or rollback, before any reply.
    drop((accepting, pending_work, telemetry));
    for reply in replies {
        reply(failed.clone());
    }
    let elapsed = started.elapsed();
    if elapsed > SLOW_COMMIT {
        tracing::warn!(
            "database commit of {count} changes ({kinds}) took {} ms",
            elapsed.as_millis()
        );
    } else {
        tracing::debug!(
            "database commit of {count} changes ({kinds}) in {} ms",
            elapsed.as_millis()
        );
    }
}

type Reply = Box<dyn FnOnce(Option<String>)>;

fn answer<T: 'static>(done: oneshot::Sender<Result<T, String>>, value: T) -> Reply {
    Box::new(move |failed: Option<String>| {
        let _ = done.send(match failed {
            Some(error) => Err(error),
            None => Ok(value),
        });
    })
}

fn apply(
    transaction: &Transaction<'_>,
    op: Op,
    replies: &mut Vec<Reply>,
    rows: &mut BlockRows,
) -> Result<()> {
    match op {
        Op::AcceptWork {
            request,
            permit: _,
            pending: _,
            done,
        } => {
            accepted::apply(transaction, *request, done, replies, rows)?;
        }
        Op::ReplayWork {
            job,
            nonce,
            address,
            pending,
            permit: _,
            done,
        } => {
            let receipt = accepted::replay(transaction, job, nonce, &address, &pending)?;
            replies.push(answer(done, receipt));
        }
        Op::EnsureMiner { address, at, done } => {
            let id = super::accepted::miner(transaction, &address, at)?;
            replies.push(answer(done, id));
        }
        Op::Miner { id, address, at } => {
            transaction.execute(
                "INSERT OR IGNORE INTO miners (id, address, first_seen) VALUES (?1, ?2, ?3)",
                params![id, address, int(at.into())],
            )?;
        }
        #[cfg(test)]
        Op::Worker {
            id,
            miner,
            name,
            at,
        } => {
            let inserted = transaction.execute(
                "INSERT OR IGNORE INTO workers (id, miner_id, name, first_seen, last_seen)
                 VALUES (?1, ?2, ?3, ?4, ?4)",
                params![id, miner, name, int(at.into())],
            )?;
            super::worker_lifetime::note_import(transaction, id, inserted)?;
        }
        Op::Share {
            id,
            time,
            miner,
            worker,
            weight,
        } => {
            transaction
                .prepare_cached(
                    "INSERT OR IGNORE INTO shares (id, time, miner_id, worker_id, weight)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )?
                .execute(params![
                    int(id.into()),
                    int(time.into()),
                    miner,
                    worker,
                    int(weight.into())
                ])?;
        }
        Op::Telemetry(record) => record.apply(transaction)?,
        Op::BlockFound { block, done } => {
            accepted::record_block(transaction, block, done, replies, rows)?
        }
        Op::BlockProgress { rows, done } => {
            accepted::progress(transaction, &rows)?;
            replies.push(answer(done, ()));
        }
        Op::BlockOutcome {
            id,
            retry_height,
            status,
            reason,
            at,
            done,
        } => accepted::outcome(
            transaction,
            id,
            retry_height,
            status,
            reason,
            at,
            done,
            replies,
        )?,
        Op::BlockState {
            id,
            status,
            reason,
            at,
        } => {
            let done = matches!(status, BlockStatus::Expired | BlockStatus::Rejected)
                .then(|| int(at.into()));
            transaction
                .prepare_cached(
                    "UPDATE blocks SET status = ?2, reason = COALESCE(?3, reason),
                     done_at = COALESCE(?4, done_at) WHERE id = ?1 AND status < 2",
                )?
                .execute(params![id, status.code(), reason, done])?;
        }
        Op::Credit {
            cut,
            blocks,
            at,
            done,
        } => {
            let credited = credit(transaction, cut, blocks, at)?;
            raise_cut_height(transaction, cut)?;
            replies.push(answer(done, credited));
        }
        Op::CutHeight(height) => raise_cut_height(transaction, height)?,
        Op::PayoutCreate {
            request_prefix,
            items,
            at,
            done,
        } => replies.push(
            match create_payout(transaction, &request_prefix, &items, at) {
                Ok(created) => answer(done, created),
                Err(error) => {
                    let message = format!("{error:#}");
                    Box::new(move |_| {
                        let _ = done.send(Err(message));
                    })
                }
            },
        ),
        Op::PayoutAttempt {
            id,
            request_id,
            scope,
            done,
        } => match payout_attempt(transaction, id, &request_id, &scope) {
            Ok(recovery_only) => replies.push(answer(done, recovery_only)),
            Err(error) => {
                let message = format!("{error:#}");
                replies.push(Box::new(move |_| {
                    let _ = done.send(Err(message));
                }));
            }
        },
        Op::PayoutSent {
            id,
            transaction: transaction_id,
            fee,
            valid_until,
            fee_shares,
            at,
            done,
        } => {
            transaction.execute(
                "UPDATE payouts SET status = ?2, transaction_id = ?3, fee = ?4, sent_at = ?5,
                 valid_until = ?7 WHERE id = ?1 AND status = ?6",
                params![
                    id,
                    PayoutStatus::Sent.code(),
                    transaction_id,
                    super::money::amount(fee)?,
                    int(at.into()),
                    PayoutStatus::Prepared.code(),
                    valid_until.map(|height| int(height.into()))
                ],
            )?;
            for (miner, share) in fee_shares {
                transaction.execute(
                    "UPDATE payout_items SET fee_share = ?3 WHERE payout_id = ?1 AND miner_id = ?2",
                    params![id, miner, super::money::amount(share)?],
                )?;
            }
            replies.push(answer(done, ()));
        }
        Op::PayoutNeverSigned {
            id,
            request_id,
            scope,
            at,
            done,
        } => {
            accounting::close_never_signed(transaction, id, &request_id, &scope, at)?;
            replies.push(answer(done, ()));
        }
        Op::PayoutClosed {
            id,
            paid,
            error,
            at,
            done,
        } => {
            close_payout(transaction, id, paid, error, at)?;
            replies.push(answer(done, ()));
        }
        Op::PruneAccepted {
            shares_before,
            hashrate_before,
            splits_before,
            sources_before,
            receipts_before,
            height,
            done,
        } => {
            let more = retention::prune(
                transaction,
                shares_before,
                hashrate_before,
                splits_before,
                sources_before,
                receipts_before,
                height,
            )?;
            replies.push(answer(done, more));
        }
        Op::Prune {
            shares_before,
            hashrate_before,
            splits_before,
            sources_before,
        } => {
            retention::prune(
                transaction,
                shares_before,
                hashrate_before,
                splits_before,
                sources_before,
                0,
                0,
            )?;
        }
        Op::TrimBlocks { before, keep, done } => {
            let mut deleted = Vec::new();
            if let Some(before) = before {
                deleted.extend(deleted_hashes(
                    transaction,
                    "DELETE FROM blocks WHERE id IN (
                         SELECT id FROM blocks WHERE found_at < ?1 AND status >= 2 AND NOT EXISTS(SELECT 1 FROM accepted_work a WHERE a.block_id=blocks.id) LIMIT ?2)
                     RETURNING hash",
                    int(before.into()),
                )?);
            }
            if let Some(keep) = keep {
                deleted.extend(deleted_hashes(
                    transaction,
                    "DELETE FROM blocks WHERE id IN (
                         SELECT id FROM blocks WHERE status >= 2 AND NOT EXISTS(SELECT 1 FROM accepted_work a WHERE a.block_id=blocks.id) AND id < (
                             SELECT id FROM blocks WHERE status >= 2 ORDER BY id DESC LIMIT 1 OFFSET ?1)
                         LIMIT ?2)
                     RETURNING hash",
                    int(keep.saturating_sub(1).into()),
                )?);
            }
            replies.push(answer(done, deleted.len()));
            rows.removed
                .extend(deleted.iter().filter_map(|hash| Hash384::from_hex(hash)));
        }
        Op::Setting { key, value } => put_setting(transaction, key, &value)?,
        Op::Shutdown(done) | Op::Flush(done) => replies.push(Box::new(move |_| {
            let _ = done.send(());
        })),
    }
    Ok(())
}

fn deleted_hashes(transaction: &Transaction<'_>, sql: &str, bound: i64) -> Result<Vec<String>> {
    let mut statement = transaction.prepare_cached(sql)?;
    let hashes = statement
        .query_map(params![bound, TRIM_ROWS as i64], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    Ok(hashes)
}

/// The cut height the event stream resumes from only moves forward.
fn raise_cut_height(transaction: &Transaction<'_>, height: u64) -> Result<()> {
    let known = super::setting(transaction, "cut_height")?
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    if height > known {
        put_setting(transaction, "cut_height", &height.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
mod money_tests;

mod accounting;
use accounting::*;

mod accepted;

mod retention;

#[cfg(test)]
mod accepted_tests;

mod op;
#[cfg(test)]
mod replay_tests;

mod telemetry;
pub use telemetry::Telemetry;
