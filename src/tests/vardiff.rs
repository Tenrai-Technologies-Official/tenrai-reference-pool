use super::*;

fn settings() -> VardiffSettings {
    VardiffSettings {
        start: 1_000,
        min: 10,
        max: None,
        shares_per_block: 1,
        share_interval: 10,
        tolerance_percent: 25,
        max_step: 4,
        history_shares: 64,
    }
}

/// Feeds shares of the current difficulty at a fixed interval.
fn mine(vardiff: &mut Vardiff, start: Instant, every: Duration, count: u32) -> Instant {
    let mut now = start;
    for _ in 0..count {
        now += every;
        let weight = vardiff.difficulty();
        vardiff.record(now, weight);
    }
    now
}

#[test]
fn a_share_stays_a_part_of_a_block() {
    let mut vardiff = Vardiff::new(
        VardiffSettings {
            shares_per_block: 16,
            ..settings()
        },
        Instant::now(),
    );
    vardiff.set_ceiling(16_000);
    assert_eq!(vardiff.difficulty(), 1_000);
    vardiff.set_ceiling(8_000);
    assert_eq!(vardiff.difficulty(), 500);
    vardiff.set_ceiling(4);
    assert_eq!(vardiff.difficulty(), 10, "the minimum still holds");
}

#[test]
fn a_worker_starts_at_the_start_difficulty() {
    let vardiff = Vardiff::new(settings(), Instant::now());
    assert_eq!(vardiff.difficulty(), 1_000);
    assert!(!vardiff.is_fixed());
}

#[test]
fn fast_shares_raise_the_difficulty_by_a_bounded_step() {
    let start = Instant::now();
    let mut vardiff = Vardiff::new(settings(), start);
    mine(&mut vardiff, start, Duration::from_millis(100), 12);
    assert_eq!(vardiff.difficulty(), 4_000);
}

#[test]
fn slow_shares_lower_the_difficulty() {
    let start = Instant::now();
    let mut vardiff = Vardiff::new(settings(), start);
    mine(&mut vardiff, start, Duration::from_secs(30), 3);
    assert_eq!(
        vardiff.difficulty(),
        1_000,
        "three shares prove nothing yet"
    );
    assert!(vardiff.retarget(start + Duration::from_secs(130)));
    assert!(vardiff.difficulty() < 1_000, "{}", vardiff.difficulty());
}

#[test]
fn a_rate_near_the_aim_keeps_the_difficulty() {
    let start = Instant::now();
    let mut vardiff = Vardiff::new(settings(), start);
    mine(&mut vardiff, start, Duration::from_secs(10), 3);
    assert_eq!(vardiff.difficulty(), 1_000);
}

#[test]
fn a_steady_miner_settles_on_its_hashrate() {
    let start = Instant::now();
    let mut vardiff = Vardiff::new(settings(), start);
    let hashrate = 5_000.0;
    let mut now = start;
    for _ in 0..200 {
        let seconds = vardiff.difficulty() as f64 / hashrate;
        now += Duration::from_secs_f64(seconds);
        let weight = vardiff.difficulty();
        vardiff.record(now, weight);
    }
    let aim = hashrate * 10.0;
    let difficulty = vardiff.difficulty() as f64;
    assert!(
        (difficulty / aim - 1.0).abs() < 0.3,
        "{difficulty} vs {aim}"
    );
}

#[test]
fn the_network_difficulty_caps_and_the_minimum_floors() {
    let start = Instant::now();
    let mut vardiff = Vardiff::new(settings(), start);
    vardiff.set_ceiling(300);
    assert_eq!(vardiff.difficulty(), 300);
    vardiff.set_ceiling(5);
    assert_eq!(vardiff.difficulty(), 10);
    vardiff.set_ceiling(1_000_000);
    assert_eq!(vardiff.difficulty(), 1_000);
}

#[test]
fn a_fixed_difficulty_stays_inside_the_bounds() {
    let mut vardiff = Vardiff::new(
        VardiffSettings {
            max: Some(50_000),
            ..settings()
        },
        Instant::now(),
    );
    vardiff.fix(1);
    assert_eq!(vardiff.difficulty(), 10);
    vardiff.fix(1_000_000);
    assert_eq!(vardiff.difficulty(), 50_000);
    assert!(vardiff.is_fixed());
}

#[test]
fn a_fixed_difficulty_that_floods_falls_back_to_vardiff() {
    let start = Instant::now();
    let mut vardiff = Vardiff::new(settings(), start);
    vardiff.fix(10);
    mine(&mut vardiff, start, Duration::from_millis(20), 12);
    assert!(!vardiff.is_fixed());
    assert!(vardiff.difficulty() > 10);
}

#[test]
fn a_fixed_difficulty_at_a_normal_rate_stays() {
    let start = Instant::now();
    let mut vardiff = Vardiff::new(settings(), start);
    vardiff.fix(500);
    mine(&mut vardiff, start, Duration::from_secs(1), 40);
    assert!(vardiff.is_fixed());
    assert_eq!(vardiff.difficulty(), 500);
}

struct Simulation {
    state: u64,
}

impl Simulation {
    fn uniform(&mut self) -> f64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        ((self.state >> 11) as f64 + 0.5) / (1_u64 << 53) as f64
    }

    /// Seconds until the next share of a miner with this hashrate at this difficulty.
    fn wait(&mut self, hashrate: f64, difficulty: u64) -> f64 {
        -self.uniform().ln() * difficulty as f64 / hashrate
    }
}

struct Outcome {
    seconds_per_share: f64,
    changes_per_hour: f64,
    /// Share of the time the difficulty stays within 30 % of the ideal one.
    near: f64,
}

/// Simulates mining with hashrate changes; counts after `warm` seconds.
fn simulate(
    settings: VardiffSettings,
    rates: &[(f64, f64)],
    end: f64,
    warm: f64,
    seed: u64,
) -> Outcome {
    let start = Instant::now();
    let aim = settings.share_interval as f64;
    let mut vardiff = Vardiff::new(settings, start);
    let mut random = Simulation { state: seed };
    let hashrate_at = |second: f64| {
        rates
            .iter()
            .rev()
            .find(|(from, _)| *from <= second)
            .map_or(rates[0].1, |(_, rate)| *rate)
    };
    let mut next = random.wait(hashrate_at(0.0), vardiff.difficulty());
    let (mut shares, mut changes, mut near) = (0_u64, 0_u32, 0_u64);
    let mut second = 0.0;
    while second < end {
        second += 1.0;
        while next <= second {
            let now = start + Duration::from_secs_f64(next);
            let weight = vardiff.difficulty();
            let changed = vardiff.record(now, weight);
            if next >= warm {
                shares += 1;
                changes += u32::from(changed);
            }
            next += random.wait(hashrate_at(next), vardiff.difficulty());
        }
        let changed = vardiff.retarget(start + Duration::from_secs_f64(second));
        if changed {
            next = second + random.wait(hashrate_at(second), vardiff.difficulty());
        }
        if second >= warm {
            changes += u32::from(changed);
            let ideal = hashrate_at(second) * aim;
            near += u64::from((vardiff.difficulty() as f64 / ideal - 1.0).abs() <= 0.3);
        }
    }
    let counted = end - warm;
    Outcome {
        seconds_per_share: counted / shares as f64,
        changes_per_hour: f64::from(changes) * 3_600.0 / counted,
        near: near as f64 / counted,
    }
}

/// Four hours at a 20 s aim, counted from `warm` seconds on.
fn aimed(rates: &[(f64, f64)], warm: f64, seed: u64) -> Outcome {
    let settings = VardiffSettings {
        start: 65_536,
        min: 50,
        share_interval: 20,
        ..settings()
    };
    simulate(settings, rates, 4.0 * 3_600.0, warm, seed * 7_919)
}

#[test]
fn the_share_rate_settles_on_the_interval() {
    for hashrate in [700.0, 2_000.0, 50_000.0, 5_000_000.0] {
        let runs: Vec<Outcome> = (1..=12)
            .map(|seed| aimed(&[(0.0, hashrate)], 1_800.0, seed))
            .collect();
        let mean =
            |pick: fn(&Outcome) -> f64| runs.iter().map(pick).sum::<f64>() / runs.len() as f64;
        let seconds = mean(|run| run.seconds_per_share);
        assert!(
            (18.0..=22.0).contains(&seconds),
            "{hashrate} H/s: {seconds} s per share"
        );
        assert!(
            mean(|run| run.changes_per_hour) < 2.0,
            "{hashrate} H/s changes too often"
        );
        assert!(
            mean(|run| run.near) > 0.85,
            "{hashrate} H/s: {}",
            mean(|run| run.near)
        );
    }
}

#[test]
fn a_hashrate_change_is_followed() {
    let settled = 7_200.0 + 900.0;
    for rates in [
        [(0.0, 40_000.0), (7_200.0, 10_000.0)],
        [(0.0, 10_000.0), (7_200.0, 20_000.0)],
    ] {
        let runs: Vec<Outcome> = (1..=12).map(|seed| aimed(&rates, settled, seed)).collect();
        let mean =
            |pick: fn(&Outcome) -> f64| runs.iter().map(pick).sum::<f64>() / runs.len() as f64;
        let seconds = mean(|run| run.seconds_per_share);
        assert!(
            (17.0..=23.0).contains(&seconds),
            "{rates:?}: {seconds} s per share"
        );
        assert!(
            mean(|run| run.near) > 0.75,
            "{rates:?}: {}",
            mean(|run| run.near)
        );
    }
}

#[test]
fn a_start_far_too_low_climbs_within_minutes() {
    let start = Instant::now();
    let mut vardiff = Vardiff::new(
        VardiffSettings {
            start: 65_536,
            share_interval: 20,
            ..settings()
        },
        start,
    );
    let hashrate = 5_000_000.0;
    let ideal = hashrate * 20.0;
    let mut now = start;
    for (minutes, off) in [(5, 2.0), (15, 1.3)] {
        while now < start + Duration::from_secs(minutes * 60) {
            now += Duration::from_secs_f64(vardiff.difficulty() as f64 / hashrate);
            let weight = vardiff.difficulty();
            vardiff.record(now, weight);
        }
        let ratio = vardiff.difficulty() as f64 / ideal;
        assert!(
            (1.0 / off..=off).contains(&ratio),
            "after {minutes} min: {ratio}"
        );
    }
}
