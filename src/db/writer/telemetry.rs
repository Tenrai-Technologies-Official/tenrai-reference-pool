//! Bounded, disposable display counters. Monetary accepted work has a separate admission path.
use super::{DROPPED, Op, Writer};
use crate::{config::Mode, db::int, stats::WorkerCounts};
use anyhow::Result;
use rusqlite::{Transaction, params};
use std::{
    mem::size_of,
    sync::{Arc, atomic::Ordering},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_RECORDS: usize = 64;
const CHUNK_ROWS: usize = 4096;
// Includes the envelope, Vec descriptor, Arc allocation and both semaphore permits.
const RECORD_OVERHEAD: usize = 256;

#[derive(Clone)]
pub(super) struct Budget {
    bytes: Arc<Semaphore>,
    records: Arc<Semaphore>,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            bytes: Arc::new(Semaphore::new(MAX_BYTES)),
            records: Arc::new(Semaphore::new(MAX_RECORDS)),
        }
    }
}

pub(super) struct Lease {
    _bytes: OwnedSemaphorePermit,
    _record: OwnedSemaphorePermit,
}

// The constructor is private: callers cannot enqueue an unbudgeted telemetry allocation.
pub struct Telemetry {
    change: Change,
    lease: Arc<Lease>,
}

enum Change {
    WorkerCounts(Vec<WorkerCounts>),
    Hashrate {
        bucket: u64,
        rows: Vec<(u32, Mode, u64)>,
    },
    Setting {
        key: &'static str,
        value: String,
    },
}

impl Change {
    fn bytes(&self) -> Option<usize> {
        let payload = match self {
            Self::Setting { value, .. } => value.capacity(),
            Self::WorkerCounts(rows) => row_bytes(rows)?,
            Self::Hashrate { rows, .. } => row_bytes(rows)?,
        };
        payload.checked_add(RECORD_OVERHEAD)
    }
}

fn row_bytes<T>(rows: &Vec<T>) -> Option<usize> {
    if rows.is_empty() || rows.len() > CHUNK_ROWS {
        return None;
    }
    rows.capacity().checked_mul(size_of::<T>())
}

impl Budget {
    fn take(&self, change: Change) -> Option<Telemetry> {
        let bytes = u32::try_from(change.bytes()?).ok()?;
        let record = Arc::clone(&self.records).try_acquire_owned().ok()?;
        let bytes = Arc::clone(&self.bytes).try_acquire_many_owned(bytes).ok()?;
        Some(Telemetry {
            change,
            lease: Arc::new(Lease {
                _bytes: bytes,
                _record: record,
            }),
        })
    }
}

impl Writer {
    pub fn worker_counts(&self, rows: Vec<WorkerCounts>) {
        self.telemetry_chunks(rows, Change::WorkerCounts);
    }

    pub fn hashrate(&self, bucket: u64, rows: Vec<(u32, Mode, u64)>) {
        self.telemetry_chunks(rows, |rows| Change::Hashrate { bucket, rows });
    }

    pub fn round_work(&self, pplns: u128, solo: u128) {
        self.send_telemetry(Change::Setting {
            key: "round_work",
            value: format!("{pplns},{solo}"),
        });
    }

    pub fn block_figures(&self, value: String) {
        self.send_telemetry(Change::Setting {
            key: "blocks",
            value,
        });
    }

    fn send_telemetry(&self, change: Change) -> bool {
        let Some(record) = self.telemetry.take(change) else {
            if DROPPED
                .fetch_add(1, Ordering::Relaxed)
                .is_multiple_of(10_000)
            {
                tracing::warn!(
                    "the database telemetry budget is full; display counters were dropped"
                );
            }
            return false;
        };
        self.send(Op::Telemetry(record));
        true
    }

    fn telemetry_chunks<T>(&self, rows: Vec<T>, make: impl Fn(Vec<T>) -> Change) {
        let mut rows = rows.into_iter();
        loop {
            // Borrowing the iterator prevents collect from reusing the entire source allocation.
            let part: Vec<_> = rows.by_ref().take(CHUNK_ROWS).collect();
            if part.is_empty() {
                return;
            }
            if !self.send_telemetry(make(part)) {
                return;
            }
        }
    }
}

impl Telemetry {
    pub(super) fn keep_alive(&self) -> Arc<Lease> {
        Arc::clone(&self.lease)
    }

    pub(super) const fn kind(&self) -> &'static str {
        match self.change {
            Change::WorkerCounts(_) => "worker counts",
            Change::Hashrate { .. } => "hashrate",
            Change::Setting { .. } => "display settings",
        }
    }

    pub(super) fn apply(self, transaction: &Transaction<'_>) -> Result<()> {
        match self.change {
            Change::Setting { key, value } => crate::db::put_setting(transaction, key, &value)?,
            Change::WorkerCounts(rows) => {
                let mut statement = transaction.prepare_cached(
                    "UPDATE workers SET accepted = MIN(9223372036854775807,accepted + ?2), rejected = MIN(9223372036854775807,rejected + ?3),
                     stale = MIN(9223372036854775807,stale + ?4), last_seen = MAX(last_seen, ?5) WHERE id = ?1",
                )?;
                for (worker, accepted, rejected, stale, seen) in rows {
                    statement.execute(params![
                        worker,
                        int(accepted.into()),
                        int(rejected.into()),
                        int(stale.into()),
                        int(seen.into())
                    ])?;
                }
            }
            Change::Hashrate { bucket, rows } => {
                let mut statement = transaction.prepare_cached(
                    "INSERT INTO hashrate (miner_id, bucket, mode, work) VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT (miner_id, bucket, mode) DO UPDATE SET work = MIN(9223372036854775807,work + excluded.work)",
                )?;
                for (miner, mode, work) in rows {
                    statement.execute(params![
                        miner,
                        int(bucket.into()),
                        mode.code(),
                        int(work.into())
                    ])?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
