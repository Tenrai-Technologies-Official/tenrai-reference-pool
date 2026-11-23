//! Node outage tracking with rate-limited logging.

use std::time::{Duration, Instant};

/// Miners keep their current jobs through node failures shorter than this.
pub const NODE_PATIENCE: Duration = Duration::from_secs(5);
/// Minimum interval between reminders while work waits for the node.
pub const WAIT_REMINDER: Duration = Duration::from_secs(60);

/// Effect of another failed node request on the current work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outage {
    /// Short enough to keep the current work.
    Brief,
    /// Work pauses now.
    Paused,
    /// Still paused and a reminder is due; carries the outage duration.
    Lasting(Duration),
    /// Still paused, nothing to log.
    Quiet,
}

/// Keeps work running through brief node failures and pauses it after `patience`.
#[derive(Clone, Debug)]
pub struct NodeOutage {
    patience: Duration,
    reminder: Duration,
    since: Option<Instant>,
    paused: bool,
    reported: Option<Instant>,
}

impl NodeOutage {
    pub const fn new(patience: Duration, reminder: Duration) -> Self {
        Self {
            patience,
            reminder,
            since: None,
            paused: false,
            reported: None,
        }
    }

    pub fn failed(&mut self) -> Outage {
        self.failed_at(Instant::now())
    }

    pub fn failed_at(&mut self, now: Instant) -> Outage {
        let since = *self.since.get_or_insert(now);
        if !self.paused {
            if now.saturating_duration_since(since) < self.patience {
                return Outage::Brief;
            }
            self.paused = true;
            self.reported = Some(now);
            return Outage::Paused;
        }
        if self
            .reported
            .is_some_and(|at| now.saturating_duration_since(at) < self.reminder)
        {
            return Outage::Quiet;
        }
        self.reported = Some(now);
        Outage::Lasting(now.saturating_duration_since(since))
    }

    /// Marks the node as answering again. Returns the outage duration if work had paused.
    pub fn recovered(&mut self) -> Option<Duration> {
        self.recovered_at(Instant::now())
    }

    pub fn recovered_at(&mut self, now: Instant) -> Option<Duration> {
        let since = self.since.take();
        self.reported = None;
        std::mem::take(&mut self.paused)
            .then(|| since.map_or(Duration::ZERO, |since| now.saturating_duration_since(since)))
    }

    pub const fn is_paused(&self) -> bool {
        self.paused
    }
}

/// `40 s`, `3 min`, `2 h 5 min`.
pub fn waited(duration: Duration) -> String {
    let seconds = duration.as_secs();
    match seconds {
        0..=59 => format!("{seconds} s"),
        60..=3_599 => format!("{} min", seconds / 60),
        _ => format!("{} h {} min", seconds / 3_600, seconds % 3_600 / 60),
    }
}

#[cfg(test)]
#[path = "tests/outage.rs"]
mod tests;
