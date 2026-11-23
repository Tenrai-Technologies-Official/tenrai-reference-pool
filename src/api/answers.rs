use axum::body::Bytes;
use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

const MAX_KEPT: usize = 4_096;
/// Total cached response budget.
const MAX_BYTES: usize = 64 << 20;
const SWEEP_EVERY: Duration = Duration::from_secs(1);

type Gate = Arc<tokio::sync::Mutex<()>>;

struct Kept {
    bytes: Bytes,
    built: Instant,
    ttl: Duration,
    used: Instant,
}

struct Store {
    entries: HashMap<String, Kept>,
    bytes: usize,
    swept: Instant,
}

/// Coalesce concurrent misses for the same key.
#[derive(Clone)]
pub struct Answers {
    kept: Arc<Mutex<Store>>,
    building: Arc<Mutex<HashMap<String, Gate>>>,
}

fn locked<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn weight(key: &str, value: &Bytes) -> usize {
    key.len() + value.len()
}

impl Default for Answers {
    fn default() -> Self {
        Self {
            kept: Arc::new(Mutex::new(Store {
                entries: HashMap::new(),
                bytes: 0,
                swept: Instant::now(),
            })),
            building: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Answers {
    pub async fn get_or_build<F, Fut, E>(
        &self,
        key: &str,
        ttl: Duration,
        build: F,
    ) -> Result<Option<Bytes>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Option<Bytes>, E>>,
    {
        if let Some(bytes) = self.lookup(key, ttl) {
            return Ok(Some(bytes));
        }
        let gate = Arc::clone(locked(&self.building).entry(key.to_owned()).or_default());
        let turn = gate.lock().await;
        let answer = match self.lookup(key, ttl) {
            Some(bytes) => Ok(Some(bytes)),
            None => build().await.inspect(|built| {
                if let Some(bytes) = built {
                    self.store(key, ttl, bytes.clone());
                }
            }),
        };
        drop(turn);
        let mut building = locked(&self.building);
        // The map and this caller own the only references to the gate.
        if Arc::strong_count(&gate) <= 2 {
            building.remove(key);
        }
        answer
    }

    fn lookup(&self, key: &str, ttl: Duration) -> Option<Bytes> {
        let mut store = locked(&self.kept);
        let kept = store.entries.get_mut(key)?;
        let now = Instant::now();
        if now.duration_since(kept.built) >= ttl {
            return None;
        }
        kept.used = now;
        Some(kept.bytes.clone())
    }

    fn store(&self, key: &str, ttl: Duration, bytes: Bytes) {
        let size = weight(key, &bytes);
        if size > MAX_BYTES {
            return;
        }
        let mut store = locked(&self.kept);
        store.remove(key);
        let now = Instant::now();
        if store.full(size) && now.duration_since(store.swept) >= SWEEP_EVERY {
            store.sweep(now);
        }
        while store.full(size) && store.drop_least_used() {}
        store.bytes += size;
        store.entries.insert(
            key.to_owned(),
            Kept {
                bytes,
                built: now,
                ttl,
                used: now,
            },
        );
    }
}

impl Store {
    fn full(&self, adding: usize) -> bool {
        self.entries.len() >= MAX_KEPT || self.bytes + adding > MAX_BYTES
    }

    fn remove(&mut self, key: &str) {
        if let Some(kept) = self.entries.remove(key) {
            self.bytes -= weight(key, &kept.bytes);
        }
    }

    fn sweep(&mut self, now: Instant) {
        self.swept = now;
        self.entries
            .retain(|_, kept| now.duration_since(kept.built) < kept.ttl);
        self.bytes = self
            .entries
            .iter()
            .map(|(key, kept)| weight(key, &kept.bytes))
            .sum();
    }

    fn drop_least_used(&mut self) -> bool {
        let Some(key) = self
            .entries
            .iter()
            .min_by_key(|(_, kept)| kept.used)
            .map(|(key, _)| key.clone())
        else {
            return false;
        };
        self.remove(&key);
        true
    }
}

#[cfg(test)]
#[path = "../tests/answers.rs"]
mod tests;
