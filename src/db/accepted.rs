//! Durably record accepted work before optional statistics updates.
use crate::{chain::Hash384, config::Mode, pow::template::HEADER_BYTES, stats::WorkerKey};
use anyhow::{Result, anyhow, ensure};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

mod apply;
pub mod pending;
pub mod read;
mod window;
pub(super) use apply::apply;
pub use window::window;

/// Admission cap for active addresses in the longer of 24 hours and the PPLNS lookback.
/// Retain existing participants' work when the cap is reached.
pub const MAX_ACTIVE_PARTICIPANTS: usize = 16_384;
pub const MAX_SHARE_CACHE: usize = 8_192;
pub const MAX_REGISTRY_CACHE: usize = 4_096;
pub const MAX_ACCEPTING: usize = 128;
pub const MAX_OPEN_BLOCKS: usize = 65_536;
pub const MAX_BODY_HEX: usize = 2 * 46 * 1024;
const ACTIVE_KEPT_MILLIS: u64 = 86_400_000;

pub struct FoundWork {
    pub header: [u8; HEADER_BYTES],
    pub body_hex: String,
    pub height: u64,
    pub window_end: u64,
    pub network_difficulty: u128,
    pub window_work: u128,
    pub fee_bps: u32,
}

pub struct AcceptedWork {
    /// Hash the canonical work header so duplicate proofs share one receipt.
    pub fingerprint: Hash384,
    pub header: [u8; HEADER_BYTES],
    pub job_id: Hash384,
    pub window_end: u64,
    pub key: WorkerKey,
    pub time: u64,
    pub weight: u64,
    pub lookback_millis: u64,
    pub source: Option<[u8; 16]>,
    pub found: Option<FoundWork>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedBlock {
    pub id: i64,
    pub hash: Hash384,
    pub mode: Mode,
    pub miner: u32,
    pub worker: super::WorkerId,
    pub height: u64,
    pub window_end: u64,
    pub found_at: u64,
    pub fee_bps: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedReceipt {
    pub id: u64,
    pub time: u64,
    pub miner: u32,
    pub worker: super::WorkerId,
    pub mode: Mode,
    pub weight: u64,
    pub fresh: bool,
    pub block: Option<CommittedBlock>,
}
impl AcceptedReceipt {
    pub const fn share_id(&self) -> Option<u64> {
        match self.mode {
            Mode::Pplns => Some(self.id),
            Mode::Solo => None,
        }
    }
}

/// Read receipts in writer order to distinguish completed work from pending intake.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplayReceipt {
    Committed(AcceptedReceipt),
    UnknownPending,
    Absent,
}

/// Returned before the writer accepts an operation.
#[derive(Debug, thiserror::Error)]
pub enum AcceptanceError {
    #[error("the accepted-work writer is busy")]
    Busy,
    #[error("accepted-work outcome is unknown; retry the same proof after reconnecting: {0}")]
    Unknown(String),
}

type AcceptedRow = (u64, u64, u32, super::WorkerId, i64, u64, Option<i64>);

pub fn receipt(connection: &Connection, fingerprint: Hash384) -> Result<Option<AcceptedReceipt>> {
    let value: Option<AcceptedRow> = connection.query_row(
        "SELECT id,time,miner_id,worker_id,mode,weight,block_id FROM accepted_work WHERE fingerprint=?1",
        [fingerprint.0.as_slice()], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,
            row.get(4)?,row.get(5)?,row.get(6)?)),
    ).optional()?;
    value
        .map(|(id, time, miner, worker, mode, weight, block)| {
            ensure!(
                id > 0
                    && miner > 0
                    && worker > 0
                    && worker <= super::worker_lifetime::MAX_WORKER_ID
                    && matches!(mode, 0 | 1)
                    && weight > 0,
                "an accepted-work receipt is malformed"
            );
            Ok(AcceptedReceipt {
                id,
                time,
                miner,
                worker,
                mode: Mode::from_code(mode),
                weight,
                fresh: false,
                block: block.map(|id| read_block(connection, id)).transpose()?,
            })
        })
        .transpose()
}

pub fn read_block(connection: &Connection, id: i64) -> Result<CommittedBlock> {
    let block = connection.query_row(
        "SELECT hash,mode,miner_id,worker_id,height,window_end,found_at,fee_bps FROM blocks WHERE id=?1",
        [id], |row| Ok((row.get::<_,String>(0)?,row.get::<_,i64>(1)?,row.get::<_,u32>(2)?,
            row.get::<_,super::WorkerId>(3)?,row.get::<_,u64>(4)?,row.get::<_,u64>(5)?,
            row.get::<_,u64>(6)?,row.get::<_,u32>(7)?)),
    )?;
    ensure!(
        id > 0
            && matches!(block.1, 0 | 1)
            && block.2 > 0
            && block.3 > 0
            && block.3 <= super::worker_lifetime::MAX_WORKER_ID
            && block.5 >= block.4,
        "a committed found-block receipt is malformed"
    );
    Ok(CommittedBlock {
        id,
        hash: Hash384::from_hex(&block.0).ok_or_else(|| anyhow!("malformed block hash"))?,
        mode: Mode::from_code(block.1),
        miner: block.2,
        worker: block.3,
        height: block.4,
        window_end: block.5,
        found_at: block.6,
        fee_bps: block.7,
    })
}

pub(super) fn miner(transaction: &Transaction<'_>, address: &str, at: u64) -> Result<u32> {
    ensure!(
        !address.is_empty() && address.len() <= 256,
        "invalid accepted-work address length"
    );
    if let Some(id) = transaction
        .query_row("SELECT id FROM miners WHERE address=?1", [address], |row| {
            row.get::<_, u32>(0)
        })
        .optional()?
    {
        ensure!(id > 0, "reserved miner ID");
        return Ok(id);
    }
    let last: u64 = transaction.query_row("SELECT COALESCE(MAX(id),0) FROM miners", [], |row| {
        row.get(0)
    })?;
    let id = u32::try_from(
        last.checked_add(1)
            .ok_or_else(|| anyhow!("miner ID exhausted"))?,
    )?;
    transaction.execute(
        "INSERT INTO miners(id,address,first_seen) VALUES(?1,?2,?3)",
        params![id, address, i64::try_from(at)?],
    )?;
    Ok(id)
}
pub(super) fn worker(
    transaction: &Transaction<'_>,
    miner: u32,
    name: &str,
    at: u64,
) -> Result<super::WorkerId> {
    super::worker_lifetime::worker(transaction, miner, name, at)
}

/// Receipts below this durable registration floor can be pruned.
const CLOSED_WINDOWS: &str = "accepted_work_closed_windows_before";

pub(in crate::db) fn closed_windows_before(connection: &Connection) -> Result<u64> {
    let height = crate::db::setting(connection, CLOSED_WINDOWS)?
        .map(|text| text.parse::<u64>())
        .transpose()?
        .unwrap_or(0);
    i64::try_from(height)?;
    Ok(height)
}

pub(in crate::db) fn close_windows_before(
    transaction: &Transaction<'_>,
    height: u64,
) -> Result<()> {
    i64::try_from(height)?;
    if height > closed_windows_before(transaction)? {
        crate::db::put_setting(transaction, CLOSED_WINDOWS, &height.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests;
