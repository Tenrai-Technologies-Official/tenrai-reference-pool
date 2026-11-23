use super::*;

#[test]
fn a_bucket_refills_at_its_rate() {
    let mut bucket = TokenBucket::per_minute(20, 2).unwrap();
    assert!(bucket.allow());
    assert!(bucket.allow());
    assert!(!bucket.allow());
    bucket.rewind(Duration::from_secs(3));
    assert!(bucket.allow());
    assert!(!bucket.allow());
}

#[test]
fn a_refunded_token_can_be_spent_again_up_to_the_burst() {
    let mut bucket = TokenBucket::per_minute(1, 1).unwrap();
    assert!(bucket.allow());
    assert!(!bucket.allow());
    bucket.refund();
    bucket.refund();
    assert!(bucket.allow());
    assert!(!bucket.allow());
}

#[test]
fn a_stalled_receiver_takes_what_queued_up_at_the_allowed_rate() {
    let mut bucket = TokenBucket::per_minute(600, 2).unwrap();
    bucket.stalled(Duration::from_secs(3));
    let taken = (0..40).filter(|_| bucket.allow()).count();
    assert_eq!(taken, 32, "the burst and three seconds at ten a second");
    bucket.rewind(Duration::from_secs(60));
    let taken = (0..40).filter(|_| bucket.allow()).count();
    assert_eq!(taken, 2, "afterwards the burst is the limit again");
}

#[test]
fn the_check_budget_follows_what_one_check_costs() {
    assert_eq!(
        checks_per_minute(24_576, 12, 0),
        24_576,
        "nothing measured yet"
    );
    assert_eq!(
        checks_per_minute(24_576, 12, 2_500),
        24_576,
        "full mode keeps the configured rate"
    );
    assert_eq!(
        checks_per_minute(24_576, 12, 31_000),
        5_806,
        "light mode: a quarter of twelve threads"
    );
    assert_eq!(checks_per_minute(24_576, 1, 31_000), 483);

    let budget = CheckBudget::new(6_000, 2, 1).unwrap();
    assert!(budget.take() && budget.take() && !budget.take());
    budget.refund();
    assert!(budget.take());
    budget.checked(Duration::from_millis(31));
    budget.checked(Duration::from_millis(31));
    assert_eq!(budget.micros.load(Ordering::Relaxed), 31_000);
}

#[test]
fn the_check_budget_stays_within_its_bounds_whatever_is_measured() {
    assert_eq!(
        checks_per_minute(24_576, 12, 1),
        24_576,
        "a check too fast to be true keeps the configured rate"
    );
    assert_eq!(
        checks_per_minute(24_576, 12, u64::MAX),
        720,
        "a stalled check leaves one a second per thread"
    );
    assert_eq!(checks_per_minute(24_576, 1, u64::MAX), 60);
    assert_eq!(
        checks_per_minute(24_576, 5, 2_500),
        24_576,
        "full mode from five threads up"
    );
    assert_eq!(checks_per_minute(24_576, 4, 2_500), 24_000);
    assert_eq!(
        checks_per_minute(30, 12, 31_000),
        30,
        "never above the configured rate"
    );
    assert_eq!(checks_per_minute(24_576, u64::MAX, 1), 24_576);

    let budget = CheckBudget::new(6_000, 2, 1).unwrap();
    budget.checked(Duration::ZERO);
    assert_eq!(budget.micros.load(Ordering::Relaxed), 1);
    let budget = CheckBudget::new(6_000, 2, 1).unwrap();
    for _ in 0..8 {
        budget.checked(Duration::from_millis(31));
    }
    budget.checked(Duration::from_secs(3_600));
    assert_eq!(
        budget.micros.load(Ordering::Relaxed),
        58_375,
        "one stall moves the average by an eighth of the cap"
    );
}

#[test]
fn empty_buckets_are_refused_at_construction() {
    assert!(TokenBucket::per_minute(0, 2).is_none());
    assert!(TokenBucket::per_minute(20, 0).is_none());
}

#[test]
fn the_rate_divides_by_the_covered_time() {
    let start = Instant::now();
    let mut window = RateWindow::new(Duration::from_secs(10), 30, start);
    window.add(start, 1_000.0);
    window.add(start + Duration::from_secs(15), 1_000.0);
    let rate = window.rate(start + Duration::from_secs(20));
    assert!((rate - 100.0).abs() < 1e-9, "{rate}");
}

#[test]
fn old_buckets_leave_the_window() {
    let start = Instant::now();
    let mut window = RateWindow::new(Duration::from_secs(10), 3, start);
    window.add(start, 9_000.0);
    let later = start + Duration::from_secs(100);
    window.add(later, 300.0);
    let rate = window.rate(later);
    assert!(rate < 50.0, "{rate}");
}
