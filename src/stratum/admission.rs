//! Connection limits, nonce prefixes, per-address budgets and the optional autoban.

use crate::rate::TokenBucket;
use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    net::{IpAddr, Ipv6Addr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const MAX_TRACKED: usize = 65_536;
const IDLE: Duration = Duration::from_secs(15 * 60);
/// Unverified shares one session may send in a burst; a valid share gives its token back.
pub const SESSION_SHARE_BURST: u32 = 8;
/// Bad requests before a ban, and the batch of checked shares judged for invalid ones.
const STRIKES: u32 = 10;
const CHECK_SHARES: u32 = 20;
const INVALID_PERCENT: u32 = 50;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Refusal {
    Full,
    PerIp,
    TooFast,
    Banned,
}

pub struct Admission {
    total: u32,
    /// Total nonce prefixes. Each connection holds one, so the used ones count the connections.
    prefixes: u64,
    per_ip: u32,
    shares_per_minute: u64,
    ban: Option<Duration>,
    state: Mutex<State>,
}

struct State {
    connections: BTreeMap<IpAddr, u32>,
    free_prefixes: BTreeSet<u16>,
    connects: Buckets,
    verification: Buckets,
    bans: BTreeMap<IpAddr, Instant>,
    strikes: BTreeMap<IpAddr, (u32, Instant)>,
}

#[derive(Default)]
struct Buckets(BTreeMap<IpAddr, (TokenBucket, Instant)>);

/// Holds a connection slot and its nonce prefix until dropped.
pub struct Lease {
    admission: Arc<Admission>,
    key: IpAddr,
    prefix: u16,
}

/// Counts one session's checked shares in batches.
#[derive(Default)]
pub struct Judge {
    checked: u32,
    invalid: u32,
}

impl Admission {
    pub fn new(
        total: u32,
        per_ip: u32,
        shares_per_minute: u64,
        ban: Option<Duration>,
    ) -> Arc<Self> {
        let last = u16::try_from(total.saturating_sub(1)).unwrap_or(u16::MAX);
        Arc::new(Self {
            total,
            prefixes: u64::from(last) + 1,
            per_ip,
            shares_per_minute: shares_per_minute.max(1),
            ban,
            state: Mutex::new(State {
                connections: BTreeMap::new(),
                free_prefixes: (0..=last).collect(),
                connects: Buckets::default(),
                verification: Buckets::default(),
                bans: BTreeMap::new(),
                strikes: BTreeMap::new(),
            }),
        })
    }

    pub fn admit(self: &Arc<Self>, ip: IpAddr) -> Result<Lease, Refusal> {
        let key = client_key(ip);
        let mut state = self.state.lock().map_err(|_| Refusal::Full)?;
        if let Some(until) = state.bans.get(&key).copied() {
            if until > Instant::now() {
                return Err(Refusal::Banned);
            }
            state.bans.remove(&key);
        }
        if state.free_prefixes.is_empty() || self.connections_in(&state) >= u64::from(self.total) {
            return Err(Refusal::Full);
        }
        let count = state.connections.get(&key).copied().unwrap_or(0);
        if count >= self.per_ip {
            return Err(Refusal::PerIp);
        }
        let per_ip = self.per_ip;
        if !state
            .connects
            .allow(key, || TokenBucket::per_minute(u64::from(per_ip), per_ip))
        {
            return Err(Refusal::TooFast);
        }
        let prefix = state.free_prefixes.pop_first().ok_or(Refusal::Full)?;
        state.connections.insert(key, count + 1);
        Ok(Lease {
            admission: Arc::clone(self),
            key,
            prefix,
        })
    }

    fn connections_in(&self, state: &State) -> u64 {
        let free = u64::try_from(state.free_prefixes.len()).unwrap_or(u64::MAX);
        self.prefixes.saturating_sub(free)
    }

    pub fn connections(&self) -> u64 {
        self.state
            .lock()
            .map_or(0, |state| self.connections_in(&state))
    }

    /// Takes a token for an unverified share; it is refunded if the share is valid.
    pub fn take_verification(&self, ip: IpAddr) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        let rate = self
            .shares_per_minute
            .saturating_mul(u64::from(self.per_ip));
        let burst = SESSION_SHARE_BURST.saturating_mul(self.per_ip);
        state
            .verification
            .allow(client_key(ip), || TokenBucket::per_minute(rate, burst))
    }

    pub fn refund_verification(&self, ip: IpAddr) {
        if let Ok(mut state) = self.state.lock() {
            state.verification.refund(client_key(ip));
        }
    }

    pub const fn bans_enabled(&self) -> bool {
        self.ban.is_some()
    }

    /// Counts a bad request. Returns true if the address got banned.
    pub fn strike(&self, ip: IpAddr) -> bool {
        let Some(duration) = self.ban else {
            return false;
        };
        if canonical(ip).is_loopback() {
            return false;
        }
        let key = client_key(ip);
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        let now = Instant::now();
        if state.strikes.len() >= MAX_TRACKED && !state.strikes.contains_key(&key) {
            state
                .strikes
                .retain(|_, (_, at)| now.duration_since(*at) < IDLE);
            if state.strikes.len() >= MAX_TRACKED {
                state.strikes.clear();
            }
        }
        let entry = state.strikes.entry(key).or_insert((0, now));
        if now.duration_since(entry.1) > IDLE {
            *entry = (0, now);
        }
        entry.0 += 1;
        entry.1 = now;
        if entry.0 < STRIKES {
            return false;
        }
        state.strikes.remove(&key);
        Self::ban_locked(&mut state, key, now + duration)
    }

    pub fn ban(&self, ip: IpAddr) -> bool {
        let Some(duration) = self.ban else {
            return false;
        };
        if canonical(ip).is_loopback() {
            return false;
        }
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        Self::ban_locked(&mut state, client_key(ip), Instant::now() + duration)
    }

    /// Returns false if the list is full of active bans; it never grows past `MAX_TRACKED`.
    fn ban_locked(state: &mut State, key: IpAddr, until: Instant) -> bool {
        if state.bans.len() >= MAX_TRACKED && !state.bans.contains_key(&key) {
            let now = Instant::now();
            state.bans.retain(|_, until| *until > now);
            if state.bans.len() >= MAX_TRACKED {
                return false;
            }
        }
        state.bans.insert(key, until);
        true
    }

    pub fn banned(&self) -> usize {
        let now = Instant::now();
        self.state.lock().map_or(0, |state| {
            state.bans.values().filter(|until| **until > now).count()
        })
    }
}

impl Judge {
    /// Returns true as soon as the batch of checked shares can no longer pass.
    pub fn record(&mut self, valid: bool) -> bool {
        self.checked += 1;
        self.invalid += u32::from(!valid);
        if self.invalid * 100 >= CHECK_SHARES * INVALID_PERCENT {
            *self = Self::default();
            return true;
        }
        if self.checked >= CHECK_SHARES {
            *self = Self::default();
        }
        false
    }
}

impl Buckets {
    fn allow(&mut self, key: IpAddr, create: impl FnOnce() -> Option<TokenBucket>) -> bool {
        let now = Instant::now();
        if !self.0.contains_key(&key) && self.0.len() >= MAX_TRACKED {
            self.0
                .retain(|_, (_, touched)| now.duration_since(*touched) < IDLE);
            // Full of active entries: clear rather than refuse every new miner.
            if self.0.len() >= MAX_TRACKED {
                self.0.clear();
            }
        }
        let entry = match self.0.entry(key) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => match create() {
                Some(bucket) => entry.insert((bucket, now)),
                None => return false,
            },
        };
        entry.1 = now;
        entry.0.allow_at(now)
    }

    fn refund(&mut self, key: IpAddr) {
        if let Some((bucket, _)) = self.0.get_mut(&key) {
            bucket.refund();
        }
    }
}

impl Lease {
    pub const fn prefix(&self) -> [u8; 2] {
        self.prefix.to_be_bytes()
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let Ok(mut state) = self.admission.state.lock() else {
            return;
        };
        if let Some(count) = state.connections.get_mut(&self.key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.connections.remove(&self.key);
            }
        }
        state.free_prefixes.insert(self.prefix);
    }
}

/// IPv4 clients of a dual-stack listener arrive as IPv4-mapped IPv6 addresses.
pub fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        v4 => v4,
    }
}

/// Limits and bans count per IPv4 address or IPv6 /64.
pub fn client_key(ip: IpAddr) -> IpAddr {
    match canonical(ip) {
        IpAddr::V6(v6) => IpAddr::V6(Ipv6Addr::from(u128::from(v6) & !((1_u128 << 64) - 1))),
        v4 => v4,
    }
}

#[cfg(test)]
#[path = "../tests/admission.rs"]
mod tests;
