use std::{
    collections::VecDeque,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

const TOKEN: u128 = 1 << 32;
const NANOS_PER_MINUTE: u128 = 60_000_000_000;
/// Share (1/N) of the verify threads available to sessions without proven work.
const UNPROVEN_PART: u64 = 4;
/// Longer checks waited on something other than TenQ and count as this long.
const SLOWEST_CHECK_MICROS: u64 = 250_000;

/// Work per second over a sliding window of fixed-width buckets.
pub struct RateWindow {
    width: Duration,
    buckets: u64,
    start: Instant,
    recent: VecDeque<(u64, f64)>,
}

impl RateWindow {
    pub fn new(width: Duration, buckets: u64, now: Instant) -> Self {
        Self {
            width,
            buckets: buckets.max(1),
            start: now,
            recent: VecDeque::new(),
        }
    }

    pub fn add(&mut self, now: Instant, work: f64) {
        let slot = self.slot(now);
        match self.recent.back_mut() {
            Some((last, total)) if *last == slot => *total += work,
            _ => self.recent.push_back((slot, work)),
        }
        let oldest = self.oldest(slot);
        while self.recent.front().is_some_and(|(slot, _)| *slot < oldest) {
            self.recent.pop_front();
        }
    }

    /// Divides by the time covered so far, so a new worker is not averaged against idle history.
    pub fn rate(&self, now: Instant) -> f64 {
        let slot = self.slot(now);
        let oldest = self.oldest(slot);
        let work: f64 = self
            .recent
            .iter()
            .filter(|(slot, _)| *slot >= oldest)
            .map(|(_, work)| work)
            .sum();
        let skipped = self
            .width
            .saturating_mul(u32::try_from(oldest).unwrap_or(u32::MAX));
        let covered = now
            .saturating_duration_since(self.start)
            .saturating_sub(skipped)
            .max(self.width);
        work / covered.as_secs_f64()
    }

    fn slot(&self, now: Instant) -> u64 {
        let elapsed = now.saturating_duration_since(self.start).as_nanos();
        u64::try_from(elapsed / self.width.as_nanos().max(1)).unwrap_or(u64::MAX)
    }

    fn oldest(&self, slot: u64) -> u64 {
        slot.saturating_sub(self.buckets - 1)
    }
}

/// Integer token bucket refilled per minute. `refund` returns a spent token.
pub struct TokenBucket {
    tokens: u128,
    per_minute: u128,
    burst: u128,
    remainder: u128,
    refilled: Instant,
}

impl TokenBucket {
    pub fn per_minute(count: u64, burst: u32) -> Option<Self> {
        if count == 0 || burst == 0 {
            return None;
        }
        let burst = u128::from(burst) * TOKEN;
        Some(Self {
            tokens: burst,
            per_minute: u128::from(count) * TOKEN,
            burst,
            remainder: 0,
            refilled: Instant::now(),
        })
    }

    pub fn allow(&mut self) -> bool {
        self.allow_at(Instant::now())
    }

    pub fn allow_at(&mut self, now: Instant) -> bool {
        self.refill(now);
        if self.tokens < TOKEN {
            return false;
        }
        self.tokens -= TOKEN;
        true
    }

    pub fn refund(&mut self) {
        self.tokens = self.tokens.saturating_add(TOKEN).min(self.burst);
    }

    /// Adds the tokens of requests that queued up while the receiver stalled.
    pub fn stalled(&mut self, stalled: Duration) {
        self.refill(Instant::now());
        let earned = stalled.as_nanos() * self.per_minute / NANOS_PER_MINUTE;
        self.tokens = self.tokens.saturating_add(earned);
    }

    pub fn set_per_minute(&mut self, count: u64) {
        self.per_minute = u128::from(count) * TOKEN;
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.refilled).as_nanos();
        self.refilled = now;
        if elapsed == 0 || self.tokens >= self.burst {
            return;
        }
        let accrued = elapsed * self.per_minute + self.remainder;
        let earned = accrued / NANOS_PER_MINUTE;
        self.remainder = accrued % NANOS_PER_MINUTE;
        self.tokens = self.tokens.saturating_add(earned).min(self.burst);
        if self.tokens == self.burst {
            self.remainder = 0;
        }
    }

    #[cfg(test)]
    pub fn rewind(&mut self, elapsed: Duration) {
        self.refilled -= elapsed;
    }
}

/// Pool-wide check budget for unproven sessions; it shrinks while checks are slow.
pub struct CheckBudget {
    bucket: Mutex<TokenBucket>,
    per_minute: u64,
    threads: u64,
    micros: AtomicU64,
}

impl CheckBudget {
    pub fn new(per_minute: u64, burst: u32, threads: usize) -> Option<Self> {
        Some(Self {
            bucket: Mutex::new(TokenBucket::per_minute(per_minute, burst)?),
            per_minute,
            threads: threads as u64,
            micros: AtomicU64::new(0),
        })
    }

    pub fn take(&self) -> bool {
        let rate = checks_per_minute(
            self.per_minute,
            self.threads,
            self.micros.load(Ordering::Relaxed),
        );
        self.bucket.lock().is_ok_and(|mut bucket| {
            bucket.set_per_minute(rate);
            bucket.allow()
        })
    }

    pub fn refund(&self) {
        if let Ok(mut bucket) = self.bucket.lock() {
            bucket.refund();
        }
    }

    /// Records a finished check. The average adapts to a mode change within a few checks.
    pub fn checked(&self, took: Duration) {
        let sample = u64::try_from(took.as_micros())
            .unwrap_or(u64::MAX)
            .clamp(1, SLOWEST_CHECK_MICROS);
        let _ = self
            .micros
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |known| {
                Some(if known == 0 {
                    sample
                } else {
                    (known * 7).saturating_add(sample) / 8
                })
            });
    }
}

/// Checks per minute that fit the unproven share of the verify threads.
pub fn checks_per_minute(per_minute: u64, threads: u64, micros: u64) -> u64 {
    if micros == 0 {
        return per_minute;
    }
    let micros = micros.min(SLOWEST_CHECK_MICROS);
    (threads.max(1).saturating_mul(60_000_000) / UNPROVEN_PART / micros).clamp(1, per_minute.max(1))
}

#[cfg(test)]
#[path = "tests/rate.rs"]
mod tests;
