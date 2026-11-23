use crate::config::VardiffSettings;
use std::time::{Duration, Instant};

/// A fixed difficulty above this share rate falls back to vardiff.
const MAX_FIXED_SHARES_PER_MINUTE: f64 = 240.0;
/// A fixed difficulty is checked against that rate this often.
const FIXED_CHECK: Duration = Duration::from_secs(30);
/// Standard deviations from the target rate before the difficulty changes.
const CONFIDENCE: f64 = 2.5;
/// Shares needed for a measurement, or the same number of target intervals without shares.
const MIN_SHARES: f64 = 12.0;

/// Per-worker share difficulty that changes only when the share rate is clearly off target.
pub struct Vardiff {
    settings: VardiffSettings,
    difficulty: u64,
    ceiling: u64,
    fixed: bool,
    since: Instant,
    shares: f64,
    work: f64,
}

impl Vardiff {
    pub fn new(settings: VardiffSettings, now: Instant) -> Self {
        Self {
            difficulty: settings.start,
            ceiling: u64::MAX,
            settings,
            fixed: false,
            since: now,
            shares: 0.0,
            work: 0.0,
        }
    }

    /// The difficulty to issue, never above the ceiling and never below the minimum.
    pub fn difficulty(&self) -> u64 {
        self.difficulty.min(self.ceiling).max(self.settings.min)
    }

    pub const fn is_fixed(&self) -> bool {
        self.fixed
    }

    /// A miner-chosen difficulty within the configured bounds.
    pub fn fix(&mut self, requested: u64) {
        let maximum = self.settings.max.unwrap_or(u64::MAX);
        self.difficulty = requested.clamp(self.settings.min, maximum);
        self.fixed = true;
    }

    /// Caps a share at `1 / shares_per_block` of a block, so PPLNS windows hold many shares.
    pub fn set_ceiling(&mut self, network_difficulty: u128) {
        let part = network_difficulty / u128::from(self.settings.shares_per_block.max(1));
        let network = u64::try_from(part).unwrap_or(u64::MAX).max(1);
        self.ceiling = self.settings.max.map_or(network, |max| max.min(network));
    }

    /// Counts a submitted share, checked or not, so a flood raises the difficulty quickly.
    pub fn record(&mut self, now: Instant, weight: u64) -> bool {
        self.shares += 1.0;
        self.work += weight as f64;
        self.retarget(now)
    }

    pub fn retarget(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.since).as_secs_f64();
        if self.fixed {
            if elapsed < FIXED_CHECK.as_secs_f64() && self.shares < MIN_SHARES {
                return false;
            }
            if self.shares * 60.0 / elapsed <= MAX_FIXED_SHARES_PER_MINUTE {
                self.restart(now);
                return false;
            }
            self.fixed = false;
            tracing::debug!("a fixed difficulty sent too many shares; vardiff takes over");
        }
        let aim = self.settings.share_interval as f64;
        let current = self.difficulty() as f64;
        let found = self.work / current;
        let ratio = if self.shares >= MIN_SHARES {
            found * aim / elapsed.max(1.0)
        } else if elapsed >= MIN_SHARES * aim {
            (found + 1.0) * aim / elapsed
        } else {
            return false;
        };
        let tolerance = f64::from(self.settings.tolerance_percent) / 100.0;
        let doubt = CONFIDENCE / self.shares.max(1.0).sqrt();
        if ratio.ln().abs() <= (1.0 + tolerance.max(doubt)).ln() {
            self.forget(now, elapsed);
            return false;
        }
        let step = f64::from(self.settings.max_step);
        let maximum = self.settings.max.unwrap_or(u64::MAX).min(self.ceiling);
        let next = ((current * ratio.clamp(1.0 / step, step)).round() as u64)
            .clamp(self.settings.min, maximum.max(self.settings.min));
        self.restart(now);
        if next == self.difficulty() {
            return false;
        }
        self.difficulty = next;
        true
    }

    /// Halves old evidence once it exceeds the history limit, so later hashrate changes still show.
    fn forget(&mut self, now: Instant, elapsed: f64) {
        if self.shares > self.settings.history_shares as f64 {
            self.shares /= 2.0;
            self.work /= 2.0;
            self.since = now - Duration::from_secs_f64(elapsed / 2.0);
        }
    }

    fn restart(&mut self, now: Instant) {
        self.since = now;
        self.shares = 0.0;
        self.work = 0.0;
    }
}

#[cfg(test)]
#[path = "../tests/vardiff.rs"]
mod tests;
