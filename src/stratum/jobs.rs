use super::protocol::RejectReason;
use crate::{chain::Hash384, work::Work};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    sync::Arc,
};

/// Jobs a session keeps open for shares that arrive late.
pub const SESSION_JOBS: usize = 8;
const MAX_SEEN_SHARES: usize = 4_096;

#[derive(Clone)]
pub struct Job {
    pub id: Hash384,
    pub work: Arc<Work>,
    pub extranonce: [u8; 32],
    pub share_target: [u8; 32],
    /// Share difficulty of `share_target`, the weight an accepted share counts with.
    pub weight: u64,
}

/// One connection's open jobs, oldest first, and the nonces already submitted for them.
#[derive(Default)]
pub struct JobBook {
    jobs: BTreeMap<Hash384, Job>,
    order: VecDeque<Hash384>,
    seen: HashSet<(Hash384, [u8; 32])>,
    next_issue: u64,
}

impl JobBook {
    pub fn next_issue(&mut self) -> u64 {
        self.next_issue += 1;
        self.next_issue
    }

    pub fn insert(&mut self, job: Job) {
        while self.jobs.len() >= SESSION_JOBS {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.remove(oldest);
        }
        self.order.push_back(job.id);
        self.jobs.insert(job.id, job);
    }

    pub fn contains(&self, id: Hash384) -> bool {
        self.jobs.contains_key(&id)
    }

    /// Reserves one share before its TenQ check; the same nonce on the same job counts once.
    pub fn claim(
        &mut self,
        id: Hash384,
        nonce: [u8; 32],
        registration_height: u64,
    ) -> Result<Job, RejectReason> {
        let job = self.jobs.get(&id).ok_or(RejectReason::StaleJob)?;
        if job.work.template.registration_window_end < registration_height {
            self.remove(id);
            return Err(RejectReason::StaleJob);
        }
        if self.seen.contains(&(id, nonce)) {
            return Err(RejectReason::Duplicate);
        }
        if self.seen.len() >= MAX_SEEN_SHARES {
            return Err(RejectReason::RateLimited);
        }
        let job = job.clone();
        self.seen.insert((id, nonce));
        Ok(job)
    }

    pub fn release(&mut self, id: Hash384, nonce: [u8; 32]) {
        self.seen.remove(&(id, nonce));
    }

    /// A body can carry only one block, so jobs on the same template end.
    pub fn retire_template(&mut self, template: Hash384) {
        let retired: Vec<Hash384> = self
            .jobs
            .values()
            .filter(|job| job.work.template.id == template)
            .map(|job| job.id)
            .collect();
        for id in retired {
            self.remove(id);
        }
    }

    pub fn prune(&mut self, registration_height: u64) {
        let expired: Vec<Hash384> = self
            .jobs
            .values()
            .filter(|job| job.work.template.registration_window_end < registration_height)
            .map(|job| job.id)
            .collect();
        for id in expired {
            self.remove(id);
        }
    }

    fn remove(&mut self, id: Hash384) {
        if self.jobs.remove(&id).is_some() {
            self.order.retain(|known| *known != id);
            self.seen.retain(|(job, _)| *job != id);
        }
    }
}

#[cfg(test)]
#[path = "../tests/jobs.rs"]
mod tests;
