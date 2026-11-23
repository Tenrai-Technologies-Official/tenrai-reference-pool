//! Bounded metadata caches. SQLite assigns IDs and remains authoritative after eviction.
use crate::db::{Reader, Writer, accepted::MAX_REGISTRY_CACHE, now_millis};
use anyhow::Result;
use rusqlite::OptionalExtension;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Mutex,
        atomic::{AtomicU32, Ordering},
    },
};

pub struct Registry {
    inner: Mutex<Cache>,
    reader: Reader,
    writer: Writer,
    maximum_miner: AtomicU32,
}
#[derive(Default)]
struct Cache {
    miners: HashMap<String, u32>,
    addresses: HashMap<u32, String>,
    order: VecDeque<u32>,
}
impl Registry {
    pub async fn load(reader: Reader, writer: Writer) -> Result<Self> {
        let maximum = reader
            .read(|connection| {
                connection.query_row("SELECT COALESCE(MAX(id),0) FROM miners", [], |row| {
                    row.get::<_, u32>(0)
                })
            })
            .await?;
        Ok(Self {
            inner: Mutex::new(Cache::default()),
            reader,
            writer,
            maximum_miner: AtomicU32::new(maximum),
        })
    }
    /// Fee recipients are trusted startup inputs; ingress IDs join accepted work atomically.
    pub async fn miner(&self, address: &str) -> Result<u32> {
        if let Some(id) = self.find(address).await? {
            return Ok(id);
        }
        let id = self
            .writer
            .ensure_miner(address.to_owned(), now_millis())
            .await?;
        self.remember(address, id);
        Ok(id)
    }
    pub fn remember(&self, address: &str, id: u32) {
        if id == 0 {
            return;
        }
        self.maximum_miner.fetch_max(id, Ordering::Relaxed);
        let mut cache = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cache.miners.get(address) == Some(&id) {
            return;
        }
        if cache.miners.contains_key(address) || cache.addresses.contains_key(&id) {
            return;
        }
        while cache.order.len() >= MAX_REGISTRY_CACHE {
            if let Some(old) = cache.order.pop_front()
                && let Some(address) = cache.addresses.remove(&old)
            {
                cache.miners.remove(&address);
            }
        }
        cache.order.push_back(id);
        cache.miners.insert(address.to_owned(), id);
        cache.addresses.insert(id, address.to_owned());
    }
    pub async fn find(&self, address: &str) -> Result<Option<u32>> {
        if let Some(id) = self
            .inner
            .lock()
            .ok()
            .and_then(|cache| cache.miners.get(address).copied())
        {
            return Ok(Some(id));
        }
        let name = address.to_owned();
        let id = self
            .reader
            .read(move |connection| {
                connection
                    .query_row("SELECT id FROM miners WHERE address=?1", [name], |row| {
                        row.get::<_, u32>(0)
                    })
                    .optional()
            })
            .await?;
        if let Some(id) = id {
            self.remember(address, id);
        }
        Ok(id)
    }
    pub async fn address(&self, id: u32) -> Result<Option<String>> {
        if let Some(address) = self
            .inner
            .lock()
            .ok()
            .and_then(|cache| cache.addresses.get(&id).cloned())
        {
            return Ok(Some(address));
        }
        let address = self
            .reader
            .read(move |connection| {
                connection
                    .query_row("SELECT address FROM miners WHERE id=?1", [id], |row| {
                        row.get::<_, String>(0)
                    })
                    .optional()
            })
            .await?;
        if let Some(address) = &address {
            self.remember(address, id);
        }
        Ok(address)
    }
    pub async fn worker_name(&self, worker: crate::db::WorkerId) -> Result<Option<String>> {
        self.reader
            .read(move |connection| {
                connection
                    .query_row("SELECT name FROM workers WHERE id=?1", [worker], |row| {
                        row.get(0)
                    })
                    .optional()
            })
            .await
    }
    pub fn miners(&self) -> usize {
        self.maximum_miner.load(Ordering::Relaxed) as usize
    }
    pub fn cached(&self) -> usize {
        self.inner.lock().map_or(0, |cache| cache.order.len())
    }
}

#[cfg(test)]
mod tests;
