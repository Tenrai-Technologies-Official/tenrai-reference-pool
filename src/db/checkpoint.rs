use super::{CHECKPOINT_EVERY, WAL_LIMIT_BYTES};
use anyhow::Result;
use rusqlite::Connection;
use std::{
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
    time::{Duration, Instant},
};

struct Stop {
    requested: Mutex<bool>,
    wake: Condvar,
}

/// Only real reader/writer owners retain this object; the worker retains Stop only.
pub(super) struct Owner {
    stop: Arc<Stop>,
    worker: Mutex<Option<JoinHandle<()>>>,
    _process_owner: Option<Arc<super::process_owner::ProcessOwner>>,
}

impl Owner {
    pub(super) fn process_lease(&self) -> Option<super::ProcessLease> {
        self._process_owner
            .as_ref()
            .map(|owner| super::ProcessLease {
                _owner: Arc::clone(owner),
            })
    }
    pub(super) fn start(
        connection: Connection,
        process_owner: Option<Arc<super::process_owner::ProcessOwner>>,
    ) -> Result<Arc<Self>> {
        let stop = Arc::new(Stop {
            requested: Mutex::new(false),
            wake: Condvar::new(),
        });
        let signal = Arc::clone(&stop);
        let worker = std::thread::Builder::new()
            .name("pool-db-checkpoint".into())
            .spawn(move || checkpoint(connection, signal))?;
        Ok(Arc::new(Self {
            stop,
            worker: Mutex::new(Some(worker)),
            _process_owner: process_owner,
        }))
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        *self
            .stop
            .requested
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        self.stop.wake.notify_all();
        if let Some(worker) = self
            .worker
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            && worker.join().is_err()
        {
            tracing::warn!("database checkpoint worker stopped after a panic");
        }
    }
}

fn checkpoint(connection: Connection, stop: Arc<Stop>) {
    let page_bytes: i64 = connection
        .pragma_query_value(None, "page_size", |row| row.get(0))
        .unwrap_or(4_096);
    loop {
        let requested = stop
            .requested
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (requested, _) = stop
            .wake
            .wait_timeout_while(requested, CHECKPOINT_EVERY, |requested| !*requested)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *requested {
            return;
        }
        drop(requested);
        let started = Instant::now();
        let passive = connection.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            row.get::<_, i64>(1)
        });
        let done = match passive {
            Ok(frames) if frames.saturating_mul(page_bytes) > WAL_LIMIT_BYTES => connection
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
                .map(|()| true),
            Ok(_) => Ok(false),
            Err(error) => Err(error),
        };
        match done {
            Ok(truncated) if truncated || started.elapsed() > Duration::from_secs(2) => {
                tracing::debug!(
                    "database checkpoint{} took {} ms",
                    if truncated { " with truncation" } else { "" },
                    started.elapsed().as_millis()
                );
            }
            Ok(_) => {}
            Err(error) => tracing::warn!("database checkpoint failed: {error}"),
        }
    }
}

#[cfg(test)]
mod tests;
