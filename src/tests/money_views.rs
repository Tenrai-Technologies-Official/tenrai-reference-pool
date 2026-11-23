use super::*;

#[test]
fn wide_amount_formatting_preserves_all_u128_digits_and_the_existing_u64_contract() {
    assert_eq!(
        crate::config::format_trai_wide(u128::MAX),
        "3402823669209384634633746074317.68211455"
    );
    assert_eq!(
        crate::config::format_trai_wide(u128::from(u64::MAX) + 1),
        "184467440737.09551616"
    );
    for amount in [0, 1, 100_000_000, u64::MAX] {
        assert_eq!(
            crate::config::format_trai(amount),
            crate::config::format_trai_wide(u128::from(amount))
        );
    }
}

#[test]
fn daily_earnings_use_exact_wide_interpolation_without_an_intermediate_product_overflow() {
    let earnings = [(9, u128::MAX, 1)];
    assert_eq!(
        earned_last_day(&earnings, 10 * DAY_MILLIS).unwrap(),
        u128::MAX
    );
    assert_eq!(
        earned_last_day(&earnings, 10 * DAY_MILLIS + DAY_MILLIS / 2).unwrap(),
        u128::MAX / 2
    );
    assert_eq!(
        earned_last_day(&[(10, u128::MAX, 1)], 10 * DAY_MILLIS).unwrap(),
        u128::MAX
    );
    assert!(earned_last_day(&[(9, 1, 1), (10, u128::MAX, 1)], 10 * DAY_MILLIS).is_err());
}
