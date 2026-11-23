use super::*;
use crate::db::accepted::{AcceptanceError, AcceptedReceipt, AcceptedWork};
use std::sync::Arc;

impl Writer {
    /// Refusal is allowed only before admission. Once queued, lost replies or SQL/commit errors
    /// are an unknown outcome: the caller disconnects and retries the same canonical proof.
    pub async fn accept(
        &self,
        request: AcceptedWork,
    ) -> std::result::Result<AcceptedReceipt, AcceptanceError> {
        self.accept_inner(request, None).await
    }
    pub(crate) async fn accept_claimed(
        &self,
        request: AcceptedWork,
        pending: Arc<crate::db::accepted::pending::WorkLease>,
    ) -> std::result::Result<AcceptedReceipt, AcceptanceError> {
        let nonce = request.header[248..280]
            .try_into()
            .map_err(|_| AcceptanceError::Unknown("invalid canonical header".into()))?;
        if !pending.matches(&self.pending, request.job_id, nonce) {
            return Err(AcceptanceError::Unknown(
                "the proof intake lease changed its identity".into(),
            ));
        }
        self.accept_inner(request, Some(pending)).await
    }
    async fn accept_inner(
        &self,
        request: AcceptedWork,
        pending: Option<Arc<crate::db::accepted::pending::WorkLease>>,
    ) -> std::result::Result<AcceptedReceipt, AcceptanceError> {
        let permit = Arc::new(
            Arc::clone(&self.accepting)
                .try_acquire_owned()
                .map_err(|_| AcceptanceError::Busy)?,
        );
        let (done, answer) = oneshot::channel();
        self.sender
            .try_send(Op::AcceptWork {
                request: Box::new(request),
                permit,
                pending,
                done,
            })
            .map_err(|_| AcceptanceError::Busy)?;
        answer
            .await
            .map_err(|_| AcceptanceError::Unknown("writer reply unavailable".into()))?
            .map_err(AcceptanceError::Unknown)
    }
    pub(crate) fn claim_work(
        &self,
        job: Hash384,
        nonce: [u8; 32],
    ) -> std::result::Result<
        Arc<crate::db::accepted::pending::WorkLease>,
        crate::db::accepted::pending::Refused,
    > {
        self.pending.claim(job, nonce)
    }
    pub fn pending_work(&self) -> usize {
        self.pending.len()
    }
    pub fn replaying_work(&self) -> usize {
        crate::db::accepted::pending::MAX_REPLAYING - self.replaying.available_permits()
    }
    pub async fn replay_work(
        &self,
        job: Hash384,
        nonce: [u8; 32],
        address: String,
    ) -> std::result::Result<crate::db::accepted::ReplayReceipt, AcceptanceError> {
        if address.is_empty() || address.len() > 256 {
            return Err(AcceptanceError::Busy);
        }
        let permit = Arc::new(
            Arc::clone(&self.replaying)
                .try_acquire_owned()
                .map_err(|_| AcceptanceError::Busy)?,
        );
        let (done, answer) = oneshot::channel();
        self.sender
            .try_send(Op::ReplayWork {
                job,
                nonce,
                address,
                pending: Arc::clone(&self.pending),
                permit,
                done,
            })
            .map_err(|_| AcceptanceError::Busy)?;
        answer
            .await
            .map_err(|_| AcceptanceError::Unknown("replay writer reply unavailable".into()))?
            .map_err(AcceptanceError::Unknown)
    }
    pub async fn shutdown(&self) -> Result<()> {
        let (done, answer) = oneshot::channel();
        self.sender
            .send(Op::Shutdown(done))
            .await
            .map_err(|_| anyhow!("database writer already stopped"))?;
        answer
            .await
            .map_err(|_| anyhow!("database shutdown reply unavailable"))
    }
    pub async fn ensure_miner(&self, address: String, at: u64) -> Result<u32> {
        self.call(|done| Op::EnsureMiner { address, at, done })
            .await
    }
}

/// A capacity refusal cannot roll back unrelated miners' work in the same writer batch.
pub(super) fn apply(
    transaction: &Transaction<'_>,
    request: AcceptedWork,
    done: oneshot::Sender<Result<AcceptedReceipt, String>>,
    replies: &mut Vec<Reply>,
    rows: &mut BlockRows,
) -> Result<()> {
    transaction.execute_batch("SAVEPOINT accept_one_work")?;
    match crate::db::accepted::apply(transaction, request) {
        Ok(receipt) => {
            transaction.execute_batch("RELEASE accept_one_work")?;
            if receipt.fresh
                && let Some(block) = &receipt.block
            {
                rows.added.push((block.hash, block.id));
            }
            replies.push(answer(done, receipt));
        }
        Err(error) => {
            transaction.execute_batch("ROLLBACK TO accept_one_work; RELEASE accept_one_work")?;
            let message = format!("{error:#}");
            replies.push(Box::new(move |failed| {
                let _ = done.send(Err(failed.unwrap_or(message)));
            }));
        }
    }
    Ok(())
}

pub(super) fn record_block(
    transaction: &Transaction<'_>,
    block: NewBlock,
    done: oneshot::Sender<Result<i64, String>>,
    replies: &mut Vec<Reply>,
    rows: &mut BlockRows,
) -> Result<()> {
    transaction
        .prepare_cached(
            "INSERT INTO blocks (hash, mode, status, miner_id, worker_id, height, window_end,
                 found_at, network_difficulty, round_work, fee_bps, share_id, window_work,
                 window_shares, split)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        )?
        .execute(params![
            block.hash,
            block.mode.code(),
            BlockStatus::Found.code(),
            block.miner,
            block.worker,
            int(block.height.into()),
            int(block.window_end.into()),
            int(block.found_at.into()),
            int(block.network_difficulty),
            int(block.round_work),
            block.fee_bps,
            block.share_id.map(|id| int(id.into())),
            block.window_work.map(int),
            block.window_shares,
            block.split,
        ])?;
    let id = transaction.last_insert_rowid();
    if let Some(hash) = Hash384::from_hex(&block.hash) {
        rows.added.push((hash, id));
    }
    replies.push(answer(done, id));

    Ok(())
}

pub(super) fn progress(
    transaction: &Transaction<'_>,
    rows: &[(Hash384, crate::blocks::Stage)],
) -> Result<()> {
    anyhow::ensure!(
        rows.len() <= 256,
        "block progress batch exceeds admission bound"
    );
    let mut update = transaction
        .prepare_cached("UPDATE blocks SET progress=MAX(progress,?2) WHERE hash=?1 AND status<2")?;
    for (hash, stage) in rows {
        let code = match stage {
            crate::blocks::Stage::Registered => 1,
            crate::blocks::Stage::Executed => 2,
        };
        update.execute(params![hash.to_hex(), code])?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn outcome(
    transaction: &Transaction<'_>,
    id: i64,
    retry_height: u64,
    status: BlockStatus,
    reason: Option<String>,
    at: u64,
    done: oneshot::Sender<Result<bool, String>>,
    replies: &mut Vec<Reply>,
) -> Result<()> {
    let done_at =
        matches!(status, BlockStatus::Expired | BlockStatus::Rejected).then(|| int(at.into()));
    let changed=transaction.execute("UPDATE blocks SET status=?2,reason=COALESCE(?3,reason),done_at=COALESCE(?4,done_at) WHERE id=?1 AND status<2",
                params![id,status.code(),reason,done_at])?;
    if changed == 1 {
        if matches!(
            status,
            BlockStatus::Expired | BlockStatus::Rejected | BlockStatus::Confirmed
        ) {
            transaction.execute("DELETE FROM found_payload WHERE block_id=?1", [id])?;
        } else {
            transaction.execute("UPDATE found_payload SET retry_height=MAX(retry_height,?2),retry_at=?3 WHERE block_id=?1",params![id,i64::try_from(retry_height)?,int(at.saturating_add(5000).into())])?;
        }
    }
    replies.push(answer(done, changed == 1));

    Ok(())
}

pub(super) fn replay(
    transaction: &Transaction<'_>,
    job: Hash384,
    nonce: [u8; 32],
    address: &str,
    pending: &crate::db::accepted::pending::PendingWork,
) -> Result<crate::db::accepted::ReplayReceipt> {
    use crate::db::accepted::ReplayReceipt;
    Ok(
        match crate::db::accepted::read::replay(transaction, job, nonce, address)? {
            Some(receipt) => ReplayReceipt::Committed(receipt),
            None if pending.contains(job, nonce) => ReplayReceipt::UnknownPending,
            None => ReplayReceipt::Absent,
        },
    )
}
