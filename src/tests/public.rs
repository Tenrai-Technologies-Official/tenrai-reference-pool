use super::*;

#[test]
fn the_last_day_takes_today_and_the_part_of_yesterday_inside_it() {
    let earnings = [(8, 1_000, 1), (9, 400, 1), (10, 200, 1)];
    let earned = |now| crate::api::views::earned_last_day(&earnings, now).unwrap();
    assert_eq!(earned(10 * DAY_MILLIS), 400 + 200);
    assert_eq!(earned(10 * DAY_MILLIS + DAY_MILLIS / 4), 300 + 200);
    assert_eq!(earned(11 * DAY_MILLIS - 1), 200);
    assert_eq!(earned(12 * DAY_MILLIS), 0);
}

#[test]
fn times_are_written_like_miningcore() {
    assert_eq!(iso8601(0), "1970-01-01T00:00:00Z");
    assert_eq!(iso8601(1_791_059_445_000), "2026-10-03T20:30:45Z");
    assert_eq!(iso8601(1_709_208_000_999), "2024-02-29T12:00:00Z");
    assert_eq!(iso8601(951_825_600_000), "2000-02-29T12:00:00Z");
    assert_eq!(iso8601(4_102_444_799_000), "2099-12-31T23:59:59Z");
}

#[test]
fn only_the_two_pool_ids_exist() {
    assert_eq!(find("trai-pplns"), Some(("trai-pplns", Mode::Pplns)));
    assert_eq!(find("trai-solo"), Some(("trai-solo", Mode::Solo)));
    assert_eq!(find("TRAI-PPLNS"), None);
    assert_eq!(find("../trai-pplns"), None);
}
