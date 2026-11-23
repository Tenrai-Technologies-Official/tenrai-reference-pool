//! Bounded intake identities survive async cancellation until physical proof/write completion.
use crate::chain::Hash384;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

pub const MAX_PENDING_WORK: usize = 128;
pub const MAX_REPLAYING: usize = 8;
type Key = (Hash384, [u8; 32]);

#[derive(Default)]
pub struct PendingWork {
    keys: Mutex<HashSet<Key>>,
}

/// Opaque: network code cannot change the alias after claiming it. Clones retain real ownership.
pub struct WorkLease {
    key: Key,
    owner: Arc<PendingWork>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Refused {
    Busy,
    Pending,
}

impl PendingWork {
    pub(crate) fn claim(
        self: &Arc<Self>,
        job: Hash384,
        nonce: [u8; 32],
    ) -> Result<Arc<WorkLease>, Refused> {
        let key = (job, nonce);
        let mut keys = self
            .keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if keys.contains(&key) {
            return Err(Refused::Pending);
        }
        if keys.len() >= MAX_PENDING_WORK {
            return Err(Refused::Busy);
        }
        keys.insert(key);
        Ok(Arc::new(WorkLease {
            key,
            owner: Arc::clone(self),
        }))
    }
    pub(crate) fn contains(&self, job: Hash384, nonce: [u8; 32]) -> bool {
        self.keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&(job, nonce))
    }
    pub(crate) fn len(&self) -> usize {
        self.keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}
impl WorkLease {
    pub(crate) fn matches(&self, owner: &Arc<PendingWork>, job: Hash384, nonce: [u8; 32]) -> bool {
        Arc::ptr_eq(&self.owner, owner) && self.key == (job, nonce)
    }
}
impl Drop for WorkLease {
    fn drop(&mut self) {
        self.owner
            .keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.key);
    }
}
