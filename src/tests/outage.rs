use super::{NodeOutage, Outage, waited};
use std::time::{Duration, Instant};

const SECOND: Duration = Duration::from_secs(1);

#[test]
fn a_brief_failure_keeps_the_work() {
    let start = Instant::now();
    let mut outage = NodeOutage::new(5 * SECOND, 60 * SECOND);
    assert_eq!(outage.failed_at(start), Outage::Brief);
    assert_eq!(outage.failed_at(start + 4 * SECOND), Outage::Brief);
    assert!(!outage.is_paused());
    assert_eq!(outage.recovered_at(start + 4 * SECOND), None);
    assert_eq!(outage.failed_at(start + 8 * SECOND), Outage::Brief);
}

#[test]
fn a_long_failure_pauses_once_and_reminds_once_a_minute() {
    let start = Instant::now();
    let mut outage = NodeOutage::new(5 * SECOND, 60 * SECOND);
    assert_eq!(outage.failed_at(start), Outage::Brief);
    assert_eq!(outage.failed_at(start + 5 * SECOND), Outage::Paused);
    assert!(outage.is_paused());
    let quiet = (6..65)
        .map(|second| outage.failed_at(start + second * SECOND))
        .filter(|step| *step != Outage::Quiet)
        .count();
    assert_eq!(quiet, 0);
    assert_eq!(
        outage.failed_at(start + 65 * SECOND),
        Outage::Lasting(65 * SECOND)
    );
    assert_eq!(outage.failed_at(start + 100 * SECOND), Outage::Quiet);
    assert_eq!(
        outage.recovered_at(start + 130 * SECOND),
        Some(130 * SECOND)
    );
    assert!(!outage.is_paused());
    assert_eq!(outage.recovered_at(start + 131 * SECOND), None);
}

#[test]
fn every_new_outage_starts_with_its_own_patience() {
    let start = Instant::now();
    let mut outage = NodeOutage::new(5 * SECOND, 60 * SECOND);
    outage.failed_at(start);
    assert_eq!(outage.failed_at(start + 6 * SECOND), Outage::Paused);
    outage.recovered_at(start + 7 * SECOND);
    assert_eq!(outage.failed_at(start + 10 * SECOND), Outage::Brief);
    assert_eq!(outage.failed_at(start + 15 * SECOND), Outage::Paused);
}

#[test]
fn waits_read_in_seconds_minutes_and_hours() {
    assert_eq!(waited(Duration::from_secs(40)), "40 s");
    assert_eq!(waited(Duration::from_secs(179)), "2 min");
    assert_eq!(waited(Duration::from_secs(7_500)), "2 h 5 min");
}
