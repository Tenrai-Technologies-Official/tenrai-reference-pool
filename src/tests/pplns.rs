use super::*;

fn log_of(shares: &[(u64, u32, u64)]) -> ShareLog {
    let mut log = ShareLog::new(1);
    for (time, miner, weight) in shares {
        log.push(*time, *miner, *weight).unwrap();
    }
    log
}

#[test]
fn shares_count_by_their_difficulty() {
    let log = log_of(&[(1, 1, 100), (2, 2, 400), (3, 1, 100), (4, 3, 400)]);
    let window = log.window(4, 1_000, 0).unwrap();
    assert!(window.complete);
    assert_eq!(window.total, 1_000);
    assert_eq!(window.weights, vec![(1, 200), (2, 400), (3, 400)]);
    assert_eq!(window.shares, 4);
    assert_eq!(window.first, 1);
}

#[test]
fn the_boundary_share_counts_only_as_far_as_the_window_needs() {
    let log = log_of(&[(1, 1, 500), (2, 2, 300), (3, 3, 300)]);
    let window = log.window(3, 1_000, 0).unwrap();
    assert!(window.complete);
    assert_eq!(window.weights, vec![(1, 400), (2, 300), (3, 300)]);
    assert_eq!(window.total, 1_000);
}

#[test]
fn an_exact_boundary_takes_the_whole_share_and_stops() {
    let log = log_of(&[(1, 9, 100), (2, 1, 600), (3, 2, 400)]);
    let window = log.window(3, 1_000, 0).unwrap();
    assert!(window.complete);
    assert_eq!(window.weights, vec![(1, 600), (2, 400)]);
    assert_eq!(window.shares, 2);
    assert_eq!(window.first, 2);
}

#[test]
fn shares_after_the_block_never_enter_its_window() {
    let mut log = log_of(&[(1, 1, 100), (2, 2, 100)]);
    let block_share = 2;
    log.push(3, 3, 5_000).unwrap();
    log.push(4, 4, 5_000).unwrap();
    let window = log.window(block_share, 150, 0).unwrap();
    assert_eq!(window.weights, vec![(1, 50), (2, 100)]);
    assert!(!window.weights.iter().any(|(miner, _)| *miner >= 3));
}

#[test]
fn the_lookback_ends_a_window_that_cannot_be_filled() {
    let log = log_of(&[(100, 1, 300), (900, 2, 300), (950, 3, 300)]);
    let window = log.window(3, 10_000, 500).unwrap();
    assert!(!window.complete);
    assert_eq!(window.total, 600);
    assert_eq!(window.weights, vec![(2, 300), (3, 300)]);
}

#[test]
fn a_block_right_after_another_shares_most_of_its_window() {
    let log = log_of(&[
        (1, 1, 100),
        (2, 2, 100),
        (3, 1, 100),
        (4, 3, 100),
        (5, 2, 100),
    ]);
    let first = log.window(4, 300, 0).unwrap();
    let second = log.window(5, 300, 0).unwrap();
    assert_eq!(first.weights, vec![(1, 100), (2, 100), (3, 100)]);
    assert_eq!(second.weights, vec![(1, 100), (2, 100), (3, 100)]);
    assert_eq!(first.first, 2);
    assert_eq!(second.first, 3);
}

#[test]
fn workers_joining_and_leaving_and_hashrate_changes_weigh_by_work() {
    let mut shares = Vec::new();
    for time in 0..50 {
        shares.push((time, 1, 1_000));
    }
    for time in 50..60 {
        shares.push((time, 2, 10_000));
    }
    let log = log_of(&shares);
    let window = log.window(60, 100_000, 0).unwrap();
    assert!(window.complete);
    assert_eq!(window.weights, vec![(2, 100_000)]);
    let longer = log.window(60, 140_000, 0).unwrap();
    assert_eq!(longer.weights, vec![(1, 40_000), (2, 100_000)]);
}

#[test]
fn an_unknown_end_requires_the_durable_window() {
    let log = log_of(&[(1, 1, 100)]);
    assert!(log.window(7, 100, 0).is_err());
}

#[test]
fn trimming_keeps_what_later_windows_can_reach() {
    let mut log = log_of(&[(1, 1, 100), (2, 1, 100), (3, 1, 100), (4, 1, 100)]);
    log.trim(0, 250);
    assert_eq!(log.len(), 3);
    log.trim(4, 1_000);
    assert_eq!(log.len(), 1);
    assert_eq!(log.window(4, 100, 0).unwrap().total, 100);
}

#[test]
fn a_restored_log_continues_its_numbering() {
    let mut log = ShareLog::new(1);
    log.restore(vec![(10, 1, 1, 50), (11, 2, 2, 50), (11, 3, 3, 50)])
        .unwrap();
    assert_eq!(log.len(), 2);
    assert_eq!(log.push(4, 1, 50).unwrap(), 12);
    assert_eq!(log.work_since(10), 100);
}

#[test]
fn splits_survive_encoding() {
    let weights = vec![
        (1, 1),
        (300, u128::from(u64::MAX) * 7),
        (70_000, 123_456_789),
    ];
    assert_eq!(decode_split(&encode_split(&weights)), Some(weights));
    assert_eq!(decode_split(&[0x80]), None);
    assert_eq!(decode_split(&[]), Some(Vec::new()));
}

#[test]
fn split_work_rejects_bits_above_u128_instead_of_truncating_them() {
    for high in 4_u8..=127 {
        let mut bytes = vec![1]; // miner 1
        bytes.extend_from_slice(&[0x80; 18]);
        bytes.push(high);
        assert_eq!(decode_split(&bytes), None, "overflowing final group {high}");
    }
    let mut maximum = vec![1];
    maximum.extend_from_slice(&[0xff; 18]);
    maximum.push(3);
    assert_eq!(decode_split(&maximum), Some(vec![(1, u128::MAX)]));
    *maximum.last_mut().unwrap() = 0x83;
    assert_eq!(
        decode_split(&maximum),
        None,
        "a continuation cannot extend u128"
    );
}

#[test]
fn split_work_and_miner_ids_have_one_canonical_varint_encoding() {
    for alias in [
        vec![0x81, 0, 1],
        vec![1, 0x81, 0],
        vec![1, 0x80, 0],
        vec![1, 0xff, 0x80, 0],
    ] {
        assert_eq!(decode_split(&alias), None);
    }
    assert_eq!(decode_split(&[1, 0]), Some(vec![(1, 0)]));
    for work in [
        0,
        1,
        127,
        128,
        u128::from(u64::MAX),
        1_u128 << 127,
        u128::MAX,
    ] {
        let pairs = vec![(u32::MAX, work)];
        assert_eq!(decode_split(&encode_split(&pairs)), Some(pairs));
    }
}
