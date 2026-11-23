pub mod target;
pub mod template;
#[allow(
    clippy::manual_is_multiple_of,
    clippy::needless_range_loop,
    clippy::unusual_byte_groupings,
    dead_code,
    unsafe_code,
    reason = "the TenQ engine of the node repository, byte for byte; its one unsafe call is a cache prefetch"
)]
pub mod tenq;

use crate::chain::Network;
use std::fmt;

pub const PREIMAGE_BYTES: usize = tenq::POW_PREIMAGE_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TenQMode {
    Full,
    Light,
}

/// Development uses the test profile. Other networks use the 2 GiB profile.
pub const fn profile_for(network: Network) -> u32 {
    match network {
        Network::Development => tenq::PROFILE_TEST,
        Network::Testnet | Network::Testnet100 | Network::Mainnet => tenq::PROFILE_PROD2G,
    }
}

pub fn epoch_for(registration_height: u64) -> Option<u32> {
    tenq::epoch_of_anchor_height_checked(registration_height.checked_sub(1)?)
}

pub fn epoch_start(epoch: u32) -> u64 {
    u64::from(epoch) * tenq::EPOCH_HEIGHTS + 1
}

/// TenQ cache and optional full dataset for one epoch.
pub struct Hasher {
    profile: u32,
    chain: [u8; 48],
    epoch: u32,
    seed: [u8; 64],
    cache: Vec<u32>,
    dataset: Option<Vec<u32>>,
    // Drop the allocations before releasing the process lease.
    _process_owner: Option<crate::db::ProcessLease>,
}

#[derive(Debug)]
pub enum Missing {
    Room { needed: u64, free: u64 },
    Allocation { bytes: u64 },
}

impl fmt::Display for Missing {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Room { needed, free } => write!(
                formatter,
                "it needs {} MiB of free memory, {} MiB are free",
                needed >> 20,
                free >> 20
            ),
            Self::Allocation { bytes } => {
                write!(formatter, "its {} MiB could not be allocated", bytes >> 20)
            }
        }
    }
}

/// Check memory for the dataset and the requested headroom.
pub fn room(profile: u32, spare: u64, free: Option<u64>) -> Result<(), Missing> {
    let bytes = tenq::dataset_words(profile).map_or(0, |words| words as u64 * 4);
    let needed = bytes.saturating_add(spare);
    match free {
        Some(free) if free < needed => Err(Missing::Room { needed, free }),
        _ => Ok(()),
    }
}

impl Hasher {
    pub fn build(
        profile: u32,
        chain: [u8; 48],
        epoch: u32,
        mode: TenQMode,
        threads: usize,
    ) -> Option<Self> {
        let mut hasher = Self::light(profile, chain, epoch)?;
        if mode == TenQMode::Full {
            hasher.add_dataset(threads).ok()?;
        }
        Some(hasher)
    }

    pub fn light(profile: u32, chain: [u8; 48], epoch: u32) -> Option<Self> {
        Some(Self {
            profile,
            chain,
            epoch,
            seed: tenq::epoch_seed(profile, &chain, epoch)?,
            cache: tenq::make_cache(profile, &chain, epoch)?,
            dataset: None,
            _process_owner: None,
        })
    }

    pub(crate) fn bind_process_owner(&mut self, owner: Option<crate::db::ProcessLease>) {
        self._process_owner = owner;
    }

    pub fn add_dataset(&mut self, threads: usize) -> Result<(), Missing> {
        let bytes = tenq::dataset_words(self.profile).map_or(0, |words| words as u64 * 4);
        self.dataset = Some(
            build_dataset(self.profile, &self.cache, &self.seed, threads)
                .ok_or(Missing::Allocation { bytes })?,
        );
        Ok(())
    }

    pub const fn is_full(&self) -> bool {
        self.dataset.is_some()
    }

    pub const fn epoch(&self) -> u32 {
        self.epoch
    }

    pub fn evaluate(&self, preimage: &[u8; PREIMAGE_BYTES]) -> Option<[u8; 32]> {
        if preimage[tenq::OFF_CHAIN..tenq::OFF_CHAIN + 48] != self.chain {
            return None;
        }
        match &self.dataset {
            Some(dataset) => tenq::evaluate(self.profile, preimage, &self.seed, dataset),
            None => tenq::evaluate_light(self.profile, preimage, &self.seed, &self.cache),
        }
    }
}

fn build_dataset(profile: u32, cache: &[u32], seed: &[u8; 64], threads: usize) -> Option<Vec<u32>> {
    let profile = tenq::profile(profile)?;
    let words = profile.dataset_words();
    if cache.len() != profile.cache_words() {
        return None;
    }
    let items = words / tenq::ITEM_WORDS;
    let threads = threads.clamp(1, items);
    let mut dataset = Vec::new();
    dataset.try_reserve_exact(words).ok()?;
    dataset.resize(words, 0);
    let per_thread = items.div_ceil(threads) * tenq::ITEM_WORDS;
    std::thread::scope(|scope| {
        for (slot, chunk) in dataset.chunks_mut(per_thread).enumerate() {
            let first = slot * per_thread / tenq::ITEM_WORDS;
            scope.spawn(move || {
                let (items, _) = chunk.as_chunks_mut::<{ tenq::ITEM_WORDS }>();
                for (offset, item) in items.iter_mut().enumerate() {
                    tenq::dataset_item(&profile, cache, seed, (first + offset) as u32, item);
                }
            });
        }
    });
    Some(dataset)
}

#[cfg(test)]
#[path = "../tests/pow.rs"]
mod tests;
