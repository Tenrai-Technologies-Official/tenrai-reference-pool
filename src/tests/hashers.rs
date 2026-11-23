use super::*;
use crate::{
    chain::Network,
    pow::{profile_for, tenq},
    test_support::CHAIN,
};
use std::sync::atomic::{AtomicU64, Ordering};

fn hashers(mode: TenQMode, free: fn() -> Option<u64>) -> Arc<Hashers> {
    let profile = profile_for(Network::Development);
    Arc::new(Hashers::for_test(profile, CHAIN, mode, 2, true).with_free_memory(free))
}

async fn settled(hashers: &Hashers) {
    while hashers.lifetime.prefetch_running() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn without_room_shares_are_checked_light_and_the_dataset_follows() {
    static FREE: AtomicU64 = AtomicU64::new(0);
    let hashers = hashers(TenQMode::Full, || Some(FREE.load(Ordering::Relaxed)));
    let light = hashers.ensure(0).await.unwrap();
    assert!(!light.is_full());

    hashers.advance(1);
    settled(&hashers).await;
    assert!(!hashers.get(0).unwrap().is_full());

    FREE.store(u64::MAX, Ordering::Relaxed);
    hashers.advance(2);
    settled(&hashers).await;
    assert!(
        !hashers.get(0).unwrap().is_full(),
        "no second try within the retry interval"
    );

    *hashers.retried.lock().unwrap() = None;
    hashers.advance(3);
    settled(&hashers).await;
    let full = hashers.get(0).unwrap();
    assert!(full.is_full());

    let preimage = {
        let mut preimage = [3_u8; crate::pow::PREIMAGE_BYTES];
        preimage[..11].copy_from_slice(b"TRAI_POW_V1");
        preimage[tenq::OFF_CHAIN..tenq::OFF_CHAIN + 48].copy_from_slice(&CHAIN.0);
        preimage[tenq::OFF_ALGO] = tenq::ALGORITHM_ID;
        preimage
    };
    assert!(light.evaluate(&preimage).is_some());
    assert_eq!(light.evaluate(&preimage), full.evaluate(&preimage));
}

#[tokio::test]
async fn the_next_epoch_without_spare_memory_is_kept_with_its_cache() {
    let hashers = hashers(TenQMode::Full, || Some(64 << 20));
    assert!(hashers.ensure(0).await.unwrap().is_full());

    hashers.advance(epoch_start(1) - 2);
    settled(&hashers).await;
    assert!(!hashers.get(1).unwrap().is_full());
    assert!(hashers.get(0).unwrap().is_full());
}

#[tokio::test]
async fn node_heights_do_not_prepare_tables_before_a_checked_template() {
    let hashers = hashers(TenQMode::Full, || None);
    let height = epoch_start(1) - PREFETCH_HEIGHTS - 1;

    hashers.advance(height);
    let background_started = hashers.lifetime.prefetch_running();
    settled(&hashers).await;
    assert!(!background_started, "heights alone must not start a build");
    assert!(hashers.epochs().is_empty());

    assert!(hashers.ensure(0).await.unwrap().is_full());
    hashers.advance(height);
    settled(&hashers).await;
    assert!(hashers.get(0).unwrap().is_full());
    assert!(hashers.get(1).unwrap().is_full());
}

#[tokio::test]
async fn the_next_epoch_is_built_ahead_when_memory_allows() {
    let hashers = hashers(TenQMode::Full, || None);
    hashers.ensure(0).await.unwrap();
    hashers.advance(epoch_start(1) - PREFETCH_HEIGHTS - 2);
    settled(&hashers).await;
    assert!(hashers.get(1).is_none());

    hashers.advance(epoch_start(1) - PREFETCH_HEIGHTS - 1);
    settled(&hashers).await;
    assert!(hashers.get(1).unwrap().is_full());
}

#[tokio::test]
async fn the_last_epoch_goes_shortly_after_the_change() {
    let hashers = hashers(TenQMode::Light, || None);
    hashers.ensure(0).await.unwrap();
    hashers.ensure(1).await.unwrap();
    let first = epoch_start(1);

    hashers.advance(first - 1);
    assert_eq!(hashers.epochs(), [0, 1]);
    hashers.advance(first + PREVIOUS_EPOCH_HEIGHTS - 2);
    assert_eq!(hashers.epochs(), [0, 1]);
    hashers.advance(first + PREVIOUS_EPOCH_HEIGHTS - 1);
    assert_eq!(hashers.epochs(), [1]);
}

#[path = "hashers/lifetime.rs"]
mod lifetime;

#[path = "hashers/http_identity.rs"]
mod http_identity;
