use super::{
    target::{difficulty_of, meets, target_for},
    template::{Template, TemplateError, block_id, preimage},
    *,
};
use crate::{
    chain::Hash384,
    test_support::{CHAIN, lock, response},
};
use serde_json::Value;

const VECTORS: &str = include_str!("../../tests/vectors/tenq_vectors.json");
const NODE_HEADER: &str = "0909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090001030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050000004d00000000000004d2010707070707070707070707070707070707070707070707070707070707070707070707070707070708080808080808080808080808080808080808080808080808080808080808080606060606060606060606060606060606060606060606060606060606060606";
const NODE_BLOCK_ID: &str = "f8d3e79007076a76a52802ce9e16bcd883c2f3a426ec38bc9c4a9f65e44b31c7618bfc8cfd1733613e14ddb83245d106";

/// The preimage every TenQ implementation is tested with, as tenq_vectors.json describes it.
fn fixture() -> [u8; PREIMAGE_BYTES] {
    let mut pre = [0_u8; PREIMAGE_BYTES];
    pre[..11].copy_from_slice(b"TRAI_POW_V1");
    for i in 0..48 {
        pre[11 + i] = ((i * 3 + 1) % 256) as u8;
        pre[62 + i] = 0xa5 ^ i as u8;
    }
    pre[59] = 0;
    pre[60] = 1;
    pre[61] = tenq::ALGORITHM_ID;
    for (i, byte) in pre.iter_mut().enumerate().take(259).skip(110) {
        *byte = ((i * 17 + 23) % 256) as u8;
    }
    for i in 0..32 {
        pre[259 + i] = i as u8;
        pre[291 + i] = ((i * 5) % 256) as u8;
    }
    pre
}

/// The test profile's epoch 7: the fixture's value, and the value with nonce bytes 0..8 zero.
fn expected() -> ([u8; 32], [u8; 32]) {
    let vectors: Value = serde_json::from_str(VECTORS).unwrap();
    let profile = vectors["profiles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|profile| profile["name"] == "test")
        .unwrap();
    let epoch = profile["epochs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|epoch| epoch["epoch"] == 7)
        .unwrap();
    let nonce_zero = epoch["values"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["nonce"] == "0" && case["extranonce_xor"] == 0)
        .unwrap();
    let decode = |value: &Value| {
        hex::decode(value.as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap()
    };
    (
        decode(&epoch["work_value_fixture"]),
        decode(&nonce_zero["value"]),
    )
}

#[test]
fn tenq_matches_the_reference_vectors_in_both_modes() {
    let pre = fixture();
    let chain: [u8; 48] = pre[11..59].try_into().unwrap();
    let mut nonce_zero = pre;
    nonce_zero[259..267].fill(0);
    let (fixture_value, nonce_zero_value) = expected();
    for mode in [TenQMode::Light, TenQMode::Full] {
        let hasher = Hasher::build(tenq::PROFILE_TEST, chain, 7, mode, 2).unwrap();
        assert_eq!(hasher.evaluate(&pre), Some(fixture_value), "{mode:?}");
        assert_eq!(
            hasher.evaluate(&nonce_zero),
            Some(nonce_zero_value),
            "{mode:?}"
        );
    }
}

#[test]
fn a_hasher_refuses_a_preimage_of_another_chain() {
    let pre = fixture();
    let hasher = Hasher::build(tenq::PROFILE_TEST, [1; 48], 7, TenQMode::Light, 1).unwrap();
    assert_eq!(hasher.epoch(), 7);
    assert_eq!(hasher.evaluate(&pre), None);
}

#[test]
fn epochs_follow_the_anchor_height() {
    assert_eq!(epoch_for(0), None);
    assert_eq!(epoch_for(1), Some(0));
    assert_eq!(epoch_for(tenq::EPOCH_HEIGHTS), Some(0));
    assert_eq!(epoch_for(tenq::EPOCH_HEIGHTS + 1), Some(1));
    assert_eq!(epoch_start(1), tenq::EPOCH_HEIGHTS + 1);
    assert_eq!(epoch_for(epoch_start(5)), Some(5));
}

#[test]
fn the_header_and_block_id_match_the_node() {
    let mut wire = response(&lock(7), [u8::MAX; 32], 10);
    wire.payload_mass = 1_234;
    let mut template = Template::parse(wire, CHAIN, &lock(7)).unwrap();
    template.payload_length = 77;
    let header = template.header(&[6; 32], &[8; 32]);
    assert_eq!(hex::encode(header), NODE_HEADER);
    assert_eq!(block_id(&header).to_hex(), NODE_BLOCK_ID);
    let pre = preimage(&header);
    assert_eq!(&pre[..11], b"TRAI_POW_V1");
    assert_eq!(pre[11..], header);
}

#[test]
fn templates_are_checked_before_use() {
    let good = response(&lock(1), [u8::MAX; 32], 10);
    let template = Template::parse(good.clone(), CHAIN, &lock(1)).unwrap();
    assert_eq!(template.transactions, 0);
    assert_eq!(template.height(), 9);
    assert_eq!(template.epoch, 0);
    assert_eq!(
        Template::parse(good.clone(), Hash384([1; 48]), &lock(1)).unwrap_err(),
        TemplateError::ChainId
    );
    assert_eq!(
        Template::parse(good.clone(), CHAIN, &lock(2)).unwrap_err(),
        TemplateError::Payout
    );
    let mut other = good.clone();
    other.pow_algorithm_id = tenq::ALGORITHM_ID + 1;
    other.spec_revision = tenq::SPEC_REVISION + 1;
    assert_eq!(
        Template::parse(other, CHAIN, &lock(1)).unwrap_err(),
        TemplateError::UnsupportedTenq {
            algorithm_id: tenq::ALGORITHM_ID + 1,
            spec_revision: tenq::SPEC_REVISION + 1,
        }
    );
    let mut short = good.clone();
    short.payload_length = 6;
    assert_eq!(
        Template::parse(short, CHAIN, &lock(1)).unwrap_err(),
        TemplateError::Body
    );
    let mut window = good.clone();
    window.registration_window_end = 9;
    assert_eq!(
        Template::parse(window, CHAIN, &lock(1)).unwrap_err(),
        TemplateError::Window
    );
    let mut busy = good;
    busy.body_hex = format!("01{}00000003", "ab".repeat(48));
    busy.payload_length = 53;
    assert_eq!(
        Template::parse(busy, CHAIN, &lock(1)).unwrap().transactions,
        3
    );
}

#[test]
fn difficulty_and_target_convert_both_ways() {
    assert_eq!(target_for(0), [u8::MAX; 32]);
    assert_eq!(target_for(1), [u8::MAX; 32]);
    assert_eq!(difficulty_of(&[u8::MAX; 32]), 1);
    for difficulty in [2_u128, 3, 16, 65_536, 1 << 40, u128::from(u64::MAX)] {
        assert_eq!(
            difficulty_of(&target_for(difficulty)),
            difficulty,
            "{difficulty}"
        );
    }
    assert!(target_for(1_000) < target_for(999));
    let target = target_for(16);
    assert!(meets(&target, &target));
    assert!(meets(&target_for(17), &target));
    assert!(!meets(&target_for(15), &target));
}

#[test]
fn a_raw_template_cannot_bypass_mining_reward_profile_checks() {
    for profile in [0, 3, 4, 255] {
        let mut unsupported = lock(7);
        unsupported[0] = profile;
        let wire = response(&unsupported, [u8::MAX; 32], 10);
        assert_eq!(
            Template::parse(wire, CHAIN, &unsupported).unwrap_err(),
            TemplateError::RewardLockProfile
        );
    }
}

#[test]
fn oversized_body_is_rejected_before_hex_decoding() {
    let mut wire = response(&lock(1), [u8::MAX; 32], 10);
    wire.body_hex = "z".repeat(46 * 1024 * 2 + 2);
    assert_eq!(
        Template::parse(wire, CHAIN, &lock(1)).unwrap_err(),
        TemplateError::Body
    );
}

#[test]
fn opaque_template_body_budget_has_an_inclusive_limit() {
    // The pool deliberately leaves contents opaque; this checks only the byte budget.
    let mut wire = response(&lock(1), [u8::MAX; 32], 10);
    wire.body_hex = "00".repeat(46 * 1024);
    wire.payload_length = 46 * 1024;
    assert_eq!(
        Template::parse(wire.clone(), CHAIN, &lock(1))
            .unwrap()
            .transactions,
        0
    );
    wire.body_hex.push_str("00");
    wire.payload_length += 1;
    assert_eq!(
        Template::parse(wire, CHAIN, &lock(1)).unwrap_err(),
        TemplateError::Body
    );
}
