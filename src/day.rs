//! Rolling 24-hour block and reward totals in one-minute buckets.

use crate::{config::Mode, db::read::mode_index};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MINUTE: u64 = 60_000;
const DAY: u64 = 86_400_000;

pub type MinuteRow = (u64, [u64; 2], u128);

/// Persist daily totals after pruning closed block rows.
#[derive(Default, Deserialize, Serialize)]
pub struct SavedBlocks {
    pub closed: [(u64, u64); 2],
    pub day: Vec<MinuteRow>,
}

#[derive(Default)]
pub struct Day {
    minutes: BTreeMap<u64, ([u64; 2], u128)>,
    invalid: bool,
}

impl Day {
    pub const fn start(now: u64) -> u64 {
        now.saturating_sub(DAY) / MINUTE * MINUTE
    }

    pub fn load(rows: Vec<MinuteRow>, now: u64) -> Self {
        let mut day = Self::default();
        let first = Self::start(now) / MINUTE;
        let last = now / MINUTE;
        for (minute, found, rewards) in rows {
            if minute < first || minute > last {
                continue;
            }
            if day.minutes.insert(minute, (found, rewards)).is_some() {
                day.invalid = true;
            }
        }
        day.keep(now);
        day
    }

    pub fn rows(&mut self, now: u64) -> anyhow::Result<Vec<MinuteRow>> {
        self.totals(now)?;
        Ok(self
            .minutes
            .iter()
            .map(|(minute, (found, rewards))| (*minute, *found, *rewards))
            .collect())
    }

    pub fn found(&mut self, mode: Mode, at: u64) {
        self.keep(at);
        let count = &mut self.minutes.entry(at / MINUTE).or_default().0[mode_index(mode)];
        match count.checked_add(1) {
            Some(value) => *count = value,
            None => self.invalid = true,
        }
    }

    /// Add a reward to the block's original minute if it is still retained.
    pub fn rewarded(&mut self, found_at: u64, reward: u64) {
        if let Some(entry) = self.minutes.get_mut(&(found_at / MINUTE)) {
            match entry.1.checked_add(u128::from(reward)) {
                Some(value) => entry.1 = value,
                None => self.invalid = true,
            }
        }
    }

    pub fn totals(&mut self, now: u64) -> anyhow::Result<([u64; 2], u128)> {
        self.keep(now);
        anyhow::ensure!(
            !self.invalid,
            "daily block figures are invalid; refusing monetary statistics"
        );
        self.minutes.values().try_fold(
            ([0_u64; 2], 0_u128),
            |(found, rewards), (blocks, reward)| {
                let add = |a: u64, b: u64| {
                    a.checked_add(b)
                        .ok_or_else(|| anyhow::anyhow!("daily block count exceeds u64"))
                };
                Ok((
                    [add(found[0], blocks[0])?, add(found[1], blocks[1])?],
                    rewards
                        .checked_add(*reward)
                        .ok_or_else(|| anyhow::anyhow!("daily rewards exceed u128"))?,
                ))
            },
        )
    }

    fn keep(&mut self, now: u64) {
        let first = Self::start(now) / MINUTE;
        while self
            .minutes
            .first_key_value()
            .is_some_and(|(minute, _)| *minute < first)
        {
            self.minutes.pop_first();
        }
        // Bound future buckets after a backwards clock adjustment.
        let last = now / MINUTE;
        while self
            .minutes
            .last_key_value()
            .is_some_and(|(minute, _)| *minute > last)
        {
            self.minutes.pop_last();
        }
    }
}

#[cfg(test)]
#[path = "tests/day.rs"]
mod tests;
