use super::*;

fn sum(split: &Split) -> u64 {
    split.credits.iter().map(|(_, amount)| amount).sum()
}

#[test]
fn pplns_pays_in_proportion_to_share_work() {
    let split = split(1_000_000, 0, &[(1, 100), (2, 300), (3, 600)], &[]).unwrap();
    assert_eq!(split.fee, 0);
    assert_eq!(
        split.credits,
        vec![(1, 100_000), (2, 300_000), (3, 600_000)]
    );
}

#[test]
fn rounding_never_creates_or_loses_an_atom() {
    let weights = [(1, 1), (2, 1), (3, 1)];
    let even = split(100, 0, &weights, &[]).unwrap();
    assert_eq!(sum(&even), 100);
    assert_eq!(even.credits, vec![(1, 33), (2, 33), (3, 34)]);
    for reward in [1_u64, 7, 99_999_999, 785_432_109] {
        let weights = [(1, 3), (2, 7), (3, 11), (4, 13)];
        let uneven = split(reward, 137, &weights, &[(9, 2_500)]).unwrap();
        assert_eq!(sum(&uneven) + uneven.retained, reward, "{reward}");
    }
}

#[test]
fn the_pool_fee_is_withheld_and_stays_in_the_wallet() {
    let split = split(1_000_000, 100, &[(1, 1)], &[]).unwrap();
    assert_eq!(split.fee, 10_000);
    assert_eq!(split.retained, 10_000);
    assert_eq!(split.credits, vec![(1, 990_000)]);
}

#[test]
fn fee_recipients_share_the_fee_and_the_rest_stays() {
    let split = split(1_000_000, 200, &[(1, 1)], &[(7, 5_000), (8, 2_500)]).unwrap();
    assert_eq!(split.fee, 20_000);
    assert_eq!(split.credits, vec![(1, 980_000), (7, 10_000), (8, 5_000)]);
    assert_eq!(split.retained, 5_000);
    assert_eq!(sum(&split) + split.retained, 1_000_000);
}

#[test]
fn a_fee_recipient_who_also_mines_gets_one_merged_credit() {
    let split = split(1_000, 1_000, &[(7, 1), (2, 1)], &[(7, 10_000)]).unwrap();
    assert_eq!(split.credits, vec![(2, 450), (7, 550)]);
    assert_eq!(split.retained, 0);
}

#[test]
fn solo_pays_the_finder_less_the_fee() {
    let split = split(785_000_000, 150, &[(42, 1)], &[]).unwrap();
    assert_eq!(split.fee, 11_775_000);
    assert_eq!(split.credits, vec![(42, 773_225_000)]);
}

#[test]
fn a_full_fee_leaves_nothing_to_the_miners() {
    let split = split(1_000, 10_000, &[(1, 5)], &[]).unwrap();
    assert_eq!(split.fee, 1_000);
    assert!(split.credits.is_empty());
}

#[test]
fn a_split_needs_weight() {
    assert_eq!(split(1_000, 0, &[], &[]), Err(SplitError::ZeroWeight));
    assert_eq!(split(1_000, 0, &[(1, 0)], &[]), Err(SplitError::ZeroWeight));
}

#[test]
fn huge_weights_do_not_overflow() {
    let weights = [(1, u128::MAX / 4), (2, u128::MAX / 4)];
    let split = split(u64::MAX / 2, 0, &weights, &[]).unwrap();
    assert_eq!(sum(&split), u64::MAX / 2);
}

#[test]
fn mul_div_matches_plain_arithmetic_where_it_fits() {
    for (a, b, d) in [
        (7_u128, 9, 4),
        (0, 5, 3),
        (1 << 60, 1 << 60, 1 << 70),
        (123, 456, 1),
    ] {
        assert_eq!(mul_div_floor(a, b, d), Some(a * b / d));
    }
    assert_eq!(
        mul_div_floor(u128::MAX, u128::MAX, u128::MAX),
        Some(u128::MAX)
    );
    assert_eq!(mul_div_floor(1, 1, 0), None);
    assert_eq!(mul_div_floor(u128::MAX, 2, 1), None);
}
