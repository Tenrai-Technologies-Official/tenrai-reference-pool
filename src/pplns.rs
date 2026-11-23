//! PPLNS share log and block windows.

use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Entry {
    id: u64,
    time: u64,
    miner: u32,
    weight: u64,
}

/// Accepted PPLNS shares in arrival order; ids grow by one per share.
pub struct ShareLog {
    entries: VecDeque<Entry>,
    total: u128,
    next_id: u64,
}

/// The share work behind one block, per address, in address-id order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Window {
    pub weights: Vec<(u32, u128)>,
    pub total: u128,
    pub shares: u32,
    pub first: u64,
    /// False when the lookback ended the walk before `work` was covered.
    pub complete: bool,
}

impl ShareLog {
    pub fn new(next_id: u64) -> Self {
        Self {
            entries: VecDeque::new(),
            total: 0,
            next_id: next_id.max(1),
        }
    }
    /// A bounded cache update from a durable database receipt. Older evicted receipts do not
    /// re-enter the cache; the database still holds their full eligible PPLNS work.
    pub fn record(&mut self, id: u64, time: u64, miner: u32, weight: u64) -> anyhow::Result<()> {
        anyhow::ensure!(
            id > 0 && miner > 0 && weight > 0,
            "malformed accepted share cache row"
        );
        if self.entries.len() >= crate::db::accepted::MAX_SHARE_CACHE
            && self.entries.front().is_some_and(|first| id < first.id)
        {
            return Ok(());
        }
        let next = id
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("share sequence exhausted"))?;
        let row = Entry {
            id,
            time,
            miner,
            weight,
        };
        match self.entries.binary_search_by_key(&id, |row| row.id) {
            Ok(at) => anyhow::ensure!(
                self.entries[at] == row,
                "conflicting accepted share receipt"
            ),
            Err(at) => {
                self.total = self
                    .total
                    .checked_add(u128::from(weight))
                    .ok_or_else(|| anyhow::anyhow!("share cache work overflow"))?;
                self.entries.insert(at, row);
            }
        }
        self.next_id = self.next_id.max(next);
        while self.entries.len() > crate::db::accepted::MAX_SHARE_CACHE {
            if let Some(old) = self.entries.pop_front() {
                self.total = self
                    .total
                    .checked_sub(u128::from(old.weight))
                    .ok_or_else(|| anyhow::anyhow!("share cache accounting underflow"))?;
            }
        }
        Ok(())
    }
    pub fn restore(
        &mut self,
        rows: impl IntoIterator<Item = (u64, u64, u32, u64)>,
    ) -> anyhow::Result<()> {
        for (id, time, miner, weight) in rows {
            if self.entries.back().is_some_and(|last| last.id >= id) {
                continue;
            }
            self.record(id, time, miner, weight)?;
        }
        Ok(())
    }
    /// Only tests or trusted import tools allocate synthetic cache IDs. Production records the
    /// checked monotonic ID returned by the same transaction that accepted the actual work.
    pub fn push(&mut self, time: u64, miner: u32, weight: u64) -> anyhow::Result<u64> {
        let id = self.next_id;
        self.record(id, time, miner, weight)?;
        Ok(id)
    }
    /// A cache lookup is not monetary evidence. A missing end or incomplete cache must use the
    /// streaming SQLite window; accepted work is never replaced by an empty/finder-only split.
    pub fn window(&self, end: u64, work: u128, since: u64) -> anyhow::Result<Window> {
        anyhow::ensure!(work > 0, "PPLNS work is zero");
        let last = self
            .entries
            .binary_search_by_key(&end, |row| row.id)
            .map_err(|_| anyhow::anyhow!("PPLNS cache miss; read the durable window"))?;
        let mut weights = BTreeMap::<u32, u128>::new();
        let mut window = Window::default();
        let mut ended_by_time = false;
        for entry in self.entries.range(..=last).rev() {
            if entry.time < since {
                ended_by_time = true;
                break;
            }
            let take = u128::from(entry.weight).min(
                work.checked_sub(window.total)
                    .ok_or_else(|| anyhow::anyhow!("PPLNS cache underflow"))?,
            );
            let share = weights.entry(entry.miner).or_default();
            *share = share
                .checked_add(take)
                .ok_or_else(|| anyhow::anyhow!("PPLNS cache miner overflow"))?;
            window.total = window
                .total
                .checked_add(take)
                .ok_or_else(|| anyhow::anyhow!("PPLNS cache work overflow"))?;
            window.shares = window
                .shares
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("PPLNS cache count exhausted"))?;
            window.first = entry.id;
            if window.total == work {
                window.complete = true;
                break;
            }
        }
        anyhow::ensure!(
            window.complete || ended_by_time || self.entries.front().is_some_and(|row| row.id == 1),
            "PPLNS cache does not cover the whole eligible history; read the durable window"
        );
        window.weights = weights.into_iter().collect();
        Ok(window)
    }
    pub fn trim(&mut self, before: u64, keep_work: u128) {
        while let Some(front) = self.entries.front() {
            let after = self.total.saturating_sub(u128::from(front.weight));
            if front.time >= before && after < keep_work {
                break;
            }
            self.total = after;
            self.entries.pop_front();
        }
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn work_since(&self, after: u64) -> u128 {
        self.entries
            .iter()
            .rev()
            .take_while(|row| row.id > after)
            .fold(0_u128, |sum, row| {
                sum.saturating_add(u128::from(row.weight))
            })
    }
}

/// LEB128 pairs of address id and weight.
pub fn encode_split(weights: &[(u32, u128)]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(weights.len() * 8);
    for (miner, weight) in weights {
        leb128(&mut bytes, u128::from(*miner));
        leb128(&mut bytes, *weight);
    }
    bytes
}

pub fn decode_split(bytes: &[u8]) -> Option<Vec<(u32, u128)>> {
    if bytes.len() > crate::db::accepted::MAX_ACTIVE_PARTICIPANTS * 24 {
        return None;
    }
    let mut weights = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        if weights.len() >= crate::db::accepted::MAX_ACTIVE_PARTICIPANTS {
            return None;
        }
        let miner = u32::try_from(read_leb128(bytes, &mut at)?).ok()?;
        let weight = read_leb128(bytes, &mut at)?;
        weights.push((miner, weight));
    }
    Some(weights)
}

fn leb128(bytes: &mut Vec<u8>, mut value: u128) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            bytes.push(byte);
            return;
        }
        bytes.push(byte | 0x80);
    }
}

fn read_leb128(bytes: &[u8], at: &mut usize) -> Option<u128> {
    let mut value = 0_u128;
    for shift in (0..128).step_by(7) {
        let byte = *bytes.get(*at)?;
        *at += 1;
        let payload = u128::from(byte & 0x7f);
        // checked_shl checks the shift count, not discarded high payload bits.
        if payload > (u128::MAX >> shift) {
            return None;
        }
        value |= payload << shift;
        if byte & 0x80 == 0 {
            if shift != 0 && payload == 0 {
                return None; // An extra zero group aliases a shorter encoding.
            }
            return Some(value);
        }
    }
    None
}

#[cfg(test)]
#[path = "tests/pplns.rs"]
mod tests;
