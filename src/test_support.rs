use crate::{
    chain::{Hash384, LOCK_BYTES, Lock, Network, address_text},
    pow::{
        Hasher, TenQMode, profile_for,
        template::{Template, TemplateResponse},
    },
    work::Work,
};
use std::{sync::Arc, time::Instant};

pub const CHAIN: Hash384 = Hash384([9; 48]);

pub fn lock(marker: u8) -> Lock {
    let mut lock = [marker; LOCK_BYTES];
    lock[0] = 1;
    lock
}

pub fn address(marker: u8) -> String {
    address_text(&lock(marker), Network::Development)
}

/// An empty body: no parents, no transactions.
pub const EMPTY_BODY: &str = "0000000000";

pub fn response(payout: &Lock, target: [u8; 32], registration_height: u64) -> TemplateResponse {
    TemplateResponse {
        job_id: Hash384([2; 48]).to_hex(),
        chain_id: CHAIN.to_hex(),
        pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
        spec_revision: crate::pow::tenq::SPEC_REVISION,
        work_ref: Hash384([3; 48]).to_hex(),
        target: hex::encode(target),
        registration_height,
        registration_window_end: registration_height + 95,
        parents_root: Hash384([4; 48]).to_hex(),
        payload_root: Hash384([5; 48]).to_hex(),
        payload_length: 5,
        payload_mass: 7,
        reward_lock: hex::encode(payout),
        body_hex: EMPTY_BODY.into(),
    }
}

pub fn template(target: [u8; 32], registration_height: u64) -> Template {
    Template::parse(
        response(&lock(1), target, registration_height),
        CHAIN,
        &lock(1),
    )
    .unwrap()
}

pub fn hasher() -> Arc<Hasher> {
    Arc::new(
        Hasher::build(
            profile_for(Network::Development),
            CHAIN.0,
            0,
            TenQMode::Light,
            1,
        )
        .expect("a development hasher"),
    )
}

pub fn work(target: [u8; 32], registration_height: u64) -> Arc<Work> {
    Arc::new(Work {
        template: template(target, registration_height),
        hasher: hasher(),
        fetched: Instant::now(),
    })
}

pub fn temp_path(name: &str) -> std::path::PathBuf {
    let mut bytes = [0_u8; 6];
    rand_core::RngCore::fill_bytes(&mut rand_core::OsRng, &mut bytes);
    std::env::temp_dir().join(format!("tenrai-pool-{name}-{}", hex::encode(bytes)))
}
