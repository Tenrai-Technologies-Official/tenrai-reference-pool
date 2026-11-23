use super::*;

const NOW: u64 = 1_800_000_000_000;

#[test]
fn blocks_count_per_mode_and_rewards_by_the_minute_their_block_was_found() {
    let mut day = Day::default();
    day.found(Mode::Pplns, NOW - 2 * MINUTE);
    day.found(Mode::Pplns, NOW - MINUTE);
    day.found(Mode::Solo, NOW);
    day.rewarded(NOW - 2 * MINUTE, 500);
    day.rewarded(NOW, 70);
    assert_eq!(day.totals(NOW).unwrap(), ([2, 1], 570));
}

#[test]
fn the_window_slides_and_late_rewards_of_older_blocks_stay_out() {
    let first = Day::start(NOW) / MINUTE;
    let mut day = Day::load(
        vec![
            (first - 1, [5, 0], 900),
            (first + 10, [0, 2], 40),
            ((NOW - 60 * MINUTE) / MINUTE, [3, 0], 300),
        ],
        NOW,
    );
    assert_eq!(day.totals(NOW).unwrap(), ([3, 2], 340));
    assert_eq!(
        Day::load(day.rows(NOW).unwrap(), NOW).totals(NOW).unwrap(),
        ([3, 2], 340)
    );
    day.rewarded((first - 1) * MINUTE, 1_000);
    assert_eq!(day.totals(NOW + 11 * MINUTE).unwrap(), ([3, 0], 300));
    day.found(Mode::Pplns, NOW + DAY);
    assert_eq!(day.minutes.len(), 1);
    assert_eq!(day.totals(NOW + DAY).unwrap(), ([1, 0], 0));
}

#[test]
fn daily_rewards_and_saved_rows_remain_exact_above_u64() {
    let mut day = Day::default();
    day.found(Mode::Pplns, NOW);
    day.found(Mode::Solo, NOW);
    day.rewarded(NOW, u64::MAX);
    day.rewarded(NOW, u64::MAX);
    let expected = 2 * u128::from(u64::MAX);
    assert_eq!(day.totals(NOW).unwrap(), ([1, 1], expected));
    let saved = SavedBlocks {
        closed: [(0, 0); 2],
        day: day.rows(NOW).unwrap(),
    };
    let decoded: SavedBlocks =
        serde_json::from_str(&serde_json::to_string(&saved).unwrap()).unwrap();
    assert_eq!(Day::load(decoded.day, NOW).totals(NOW).unwrap().1, expected);
}

#[test]
fn exhausted_or_duplicate_daily_figures_are_refused_instead_of_saturated_or_published() {
    let minute = NOW / MINUTE;
    let mut exhausted = Day::load(vec![(minute, [1, 0], u128::MAX)], NOW);
    exhausted.rewarded(NOW, 1);
    assert!(exhausted.totals(NOW).is_err());
    assert!(exhausted.rows(NOW).is_err());
    let mut counts = Day::load(vec![(minute, [u64::MAX, 0], 1)], NOW);
    counts.found(Mode::Pplns, NOW);
    assert!(counts.totals(NOW).is_err());
    assert!(counts.rows(NOW).is_err());
    let mut duplicate = Day::load(vec![(minute, [1, 0], 1), (minute, [1, 0], 2)], NOW);
    assert!(duplicate.totals(NOW).is_err());
    let mut sum = Day::load(
        vec![(minute, [1, 0], u128::MAX), (minute - 1, [1, 0], 1)],
        NOW,
    );
    assert!(sum.totals(NOW).is_err());
    assert!(sum.rows(NOW).is_err());
}

#[test]
fn loading_chart_rows_keeps_only_the_closed_day_interval_and_preserves_current_exact_values() {
    let first = Day::start(NOW) / MINUTE;
    let last = NOW / MINUTE;
    let mut rows: Vec<_> = (first.saturating_sub(10)..=last + 10)
        .map(|m| (m, [1, 0], 2))
        .collect();
    rows.push((last + 1, [u64::MAX, 0], u128::MAX));
    let mut day = Day::load(rows, NOW);
    let count = last - first + 1;
    assert_eq!(day.minutes.len() as u64, count);
    assert_eq!(
        day.totals(NOW).unwrap(),
        ([count, 0], 2 * u128::from(count))
    );
    assert!(
        day.rows(NOW)
            .unwrap()
            .iter()
            .all(|(minute, _, _)| *minute >= first && *minute <= last)
    );
}

#[test]
fn repeated_backward_clock_steps_cannot_accumulate_future_minute_rows() {
    let mut day = Day::default();
    day.found(Mode::Pplns, NOW + DAY);
    day.rewarded(NOW + DAY, 7);
    assert_eq!(day.totals(NOW + DAY).unwrap(), ([1, 0], 7));
    assert_eq!(
        day.totals(NOW).unwrap(),
        ([0, 0], 0),
        "future events are outside the current rolling chart"
    );
    for offset in 0..3000 {
        let at = NOW - offset * MINUTE;
        day.found(Mode::Solo, at);
        assert!(day.minutes.len() <= 1441);
        assert!(day.minutes.keys().all(|minute| *minute <= at / MINUTE));
    }
    assert_eq!(day.minutes.len(), 1);
    assert_eq!(day.totals(NOW - 2999 * MINUTE).unwrap(), ([0, 1], 0));
}
