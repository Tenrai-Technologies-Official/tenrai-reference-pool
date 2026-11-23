use super::*;

#[test]
fn the_effort_weighs_every_block_of_the_last_hour() {
    let stats = Stats::new([BlockCounts::default(); 2], [0; 2], Day::default());
    let key = WorkerKey {
        address: "dtrai1example".into(),
        name: "rig".into(),
        mode: Mode::Pplns,
    };
    assert_eq!(stats.effort_hour(Mode::Pplns), None);
    stats.accepted(&key, (1, 1), 300);
    assert_eq!(stats.block_found(&key, 100, 1_000, true), 300);
    stats.accepted(&key, (1, 1), 100);
    stats.accepted(&key, (1, 1), 100);
    assert_eq!(stats.block_found(&key, 300, 2_000, true), 200);
    assert_eq!(stats.effort_hour(Mode::Pplns), Some(125.0));
    assert_eq!(stats.effort_hour(Mode::Solo), None);
}

#[test]
fn a_round_of_block_hard_shares_has_no_effort() {
    let stats = Stats::new([BlockCounts::default(); 2], [0; 2], Day::default());
    let key = WorkerKey {
        address: "dtrai1example".into(),
        name: "rig".into(),
        mode: Mode::Pplns,
    };
    stats.accepted(&key, (1, 1), 32);
    assert_eq!(stats.block_found(&key, 32, 1_000, false), 0);
    assert_eq!(stats.effort_hour(Mode::Pplns), None);
    let settings = crate::config::VardiffSettings {
        start: 50,
        min: 50,
        max: None,
        shares_per_block: 16,
        share_interval: 20,
        tolerance_percent: 30,
        max_step: 4,
        history_shares: 64,
    };
    assert!(!settings.measures_effort(32));
    assert!(!settings.measures_effort(50));
    assert!(settings.measures_effort(51));
}

#[test]
fn the_saved_block_figures_hold_the_closed_counts_and_the_day() {
    let stats = Stats::new([BlockCounts::default(); 2], [0; 2], Day::default());
    let key = WorkerKey {
        address: "dtrai1example".into(),
        name: "rig".into(),
        mode: Mode::Solo,
    };
    let now = now_millis();
    stats.block_found(&key, 100, now, true);
    stats.block_found(&key, 100, now, true);
    stats.block_closed(Mode::Solo, true);
    stats.block_rewarded(now, 70);
    let saved: SavedBlocks =
        serde_json::from_str(&serde_json::to_string(&stats.saved_blocks().unwrap()).unwrap())
            .unwrap();
    assert_eq!(saved.closed, [(0, 0), (1, 0)]);
    assert_eq!(Day::load(saved.day, now).totals(now).unwrap(), ([0, 2], 70));
}

#[test]
fn a_login_without_an_accepted_share_leaves_nothing_behind_once_it_ends() {
    let stats = Stats::new([BlockCounts::default(); 2], [0; 2], Day::default());
    let key = |address: &str| WorkerKey {
        address: address.into(),
        name: "rig".into(),
        mode: Mode::Pplns,
    };
    let ip = IpAddr::from([127, 0, 0, 1]);
    for address in ["dtrai1", "dtrai1a", "dtrai1ab"] {
        stats.connected(&key(address), "test", ip, Protocol::V1);
    }
    stats.connected(&key("dtrai1a"), "test", ip, Protocol::V1);
    assert_eq!(
        stats.workers_of("dtrai1a").len(),
        1,
        "neighbouring addresses stay apart"
    );
    stats.disconnected(&key("dtrai1a"));
    assert!(
        stats.knows("dtrai1a"),
        "one of its two sessions is still open"
    );
    stats.disconnected(&key("dtrai1a"));
    assert!(!stats.knows("dtrai1a"));
    assert!(stats.knows("dtrai1ab"));
    assert_eq!(stats.with(|inner| inner.workers.len()), 2);
}

#[test]
fn a_worker_offline_for_an_hour_leaves_the_miner_page() {
    let stats = Stats::new([BlockCounts::default(); 2], [0; 2], Day::default());
    let key = |name: &str| WorkerKey {
        address: "dtrai1example".into(),
        name: name.into(),
        mode: Mode::Pplns,
    };
    let ip = IpAddr::from([127, 0, 0, 1]);
    for name in ["online", "recent", "gone"] {
        stats.connected(&key(name), "test", ip, Protocol::V1);
        stats.accepted(&key(name), (1, 1), 10);
    }
    stats.disconnected(&key("recent"));
    stats.disconnected(&key("gone"));
    let ago = |minutes: u64| {
        Instant::now()
            .checked_sub(Duration::from_secs(minutes * 60))
            .expect("an hour ago")
    };
    stats.with(|inner| {
        inner
            .workers
            .get_mut(&key("recent"))
            .expect("recent")
            .last_seen = ago(59);
        inner.workers.get_mut(&key("gone")).expect("gone").last_seen = ago(61);
    });
    let mut names: Vec<String> = stats
        .workers_of("dtrai1example")
        .into_iter()
        .map(|worker| worker.name)
        .collect();
    names.sort();
    assert_eq!(names, ["online", "recent"]);
    stats.prune();
    assert_eq!(
        stats.with(|inner| inner.workers.len()),
        3,
        "the counts stay for a day"
    );
}

#[test]
fn per_miner_chart_cardinality_is_bounded_without_losing_the_pool_aggregate() {
    let stats = Stats::new([BlockCounts::default(); 2], [0; 2], Day::default());
    stats.with(|inner| {
        inner.bucket = 100;
        for miner in 1..=MAX_HISTORY_ROWS + 5 {
            record_history(inner, miner as u32, Mode::Pplns, 2);
        }
        assert_eq!(inner.history.len(), MAX_HISTORY_ROWS);
        record_history(inner, 1, Mode::Pplns, 3);
        assert_eq!(
            inner.history[&(1, Mode::Pplns)],
            5,
            "known rows remain updateable at capacity"
        );
        roll_history(inner, 101 * HISTORY_BUCKET);
        let (_, rows) = &inner.finished[0];
        assert_eq!(rows.len(), MAX_HISTORY_ROWS + 1);
        assert_eq!(rows.capacity(), MAX_HISTORY_ROWS + 2);
        assert!(rows.contains(&(0, Mode::Pplns, 2 * (MAX_HISTORY_ROWS as u64 + 5) + 3)));
        assert_eq!(inner.history_totals, [0, 0]);
        assert!(inner.history.is_empty());
    });
}

#[test]
fn clock_bucket_churn_cannot_accumulate_unbounded_completed_diagnostic_vectors() {
    let stats = Stats::new([BlockCounts::default(); 2], [0; 2], Day::default());
    stats.with(|inner| {
        inner.bucket = 100;
        for step in 0..1000 {
            record_history(inner, 1, Mode::Solo, 7);
            let bucket = if step % 2 == 0 { 101 } else { 100 };
            roll_history(inner, bucket * HISTORY_BUCKET);
            assert!(inner.finished.len() <= MAX_FINISHED_BUCKETS);
            assert!(inner.history.is_empty());
            assert_eq!(inner.history_totals, [0, 0]);
        }
        assert_eq!(inner.finished.len(), MAX_FINISHED_BUCKETS);
        assert_eq!(
            inner.finished[0].1,
            vec![(1, Mode::Solo, 7), (0, Mode::Solo, 7)]
        );
        inner.finished.clear();
        record_history(inner, 2, Mode::Pplns, 11);
        roll_history(inner, 102 * HISTORY_BUCKET);
        assert_eq!(
            inner.finished[0].1,
            vec![(2, Mode::Pplns, 11), (0, Mode::Pplns, 11)]
        );
        assert_eq!(
            inner.modes[0].round_work, 0,
            "discarded chart rows cannot create PPLNS work"
        );
        assert_eq!(
            inner.modes[1].accepted, 0,
            "chart recovery cannot create accepted shares"
        );
    });
}
