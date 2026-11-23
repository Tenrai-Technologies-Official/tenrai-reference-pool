use super::*;
use axum::http::HeaderMap;
use std::net::Ipv6Addr;

fn peer(text: &str) -> SocketAddr {
    SocketAddr::new(text.parse().unwrap(), 40_000)
}

fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        headers.insert(*name, value.parse().unwrap());
    }
    headers
}

#[test]
fn a_client_gets_its_burst_and_then_waits() {
    let limits = Limits::new(60);
    let now = Instant::now();
    let client: IpAddr = "203.0.113.7".parse().unwrap();
    for _ in 0..20 {
        assert!(limits.allow(client, now));
    }
    assert!(!limits.allow(client, now), "the burst of 20 is spent");
    assert!(
        limits.allow("203.0.113.8".parse().unwrap(), now),
        "another client has its own bucket"
    );
    assert!(
        limits.allow(client, now + Duration::from_secs(2)),
        "one request a second comes back"
    );
}

/// One request per /64 every 100 µs, slow enough for the total limit.
fn flood(limits: &Limits, start: Instant, count: usize) -> Instant {
    let mut now = start;
    for host in 0..count {
        let prefix = (0x2001_0db8_u128 << 96) | ((host as u128) << 64);
        now = start + Duration::from_micros(100 * host as u64);
        assert!(limits.allow(IpAddr::V6(Ipv6Addr::from(prefix)), now));
    }
    now
}

#[test]
fn a_flood_of_addresses_never_locks_out_a_new_client() {
    let limits = Limits::new(300);
    let end = flood(&limits, Instant::now(), MAX_CLIENTS);
    assert!(
        limits.allow("198.51.100.77".parse().unwrap(), end),
        "a table full of busy buckets is cleared rather than refusing the newcomer"
    );
    assert_eq!(limits.clients.lock().unwrap().len(), 1);
}

#[test]
fn buckets_that_filled_up_again_make_room_first() {
    let limits = Limits::new(300);
    let start = Instant::now();
    let old: IpAddr = "203.0.113.1".parse().unwrap();
    let busy: IpAddr = "203.0.113.2".parse().unwrap();
    assert!(limits.allow(old, start));
    let later = start + limits.refill + Duration::from_secs(1);
    while limits.allow(busy, later) {}
    let end = flood(&limits, later, MAX_CLIENTS - 2);
    assert!(limits.allow("198.51.100.77".parse().unwrap(), end));
    let clients = limits.clients.lock().unwrap();
    assert!(
        !clients.contains_key(&old),
        "its bucket had filled up again"
    );
    assert!(clients.contains_key(&busy));
    drop(clients);
    let mut left = 0;
    while limits.allow(busy, end) {
        left += 1;
    }
    assert!(
        left < limits.burst,
        "the busy client kept its bucket: {left} left, not a fresh burst"
    );
}

#[test]
fn slow_builds_spend_the_build_time_sooner_than_quick_ones() {
    let mut budget = BuildTime::new(Duration::from_secs(2));
    let start = Instant::now();
    assert!(budget.admit(start));
    budget.charge(Duration::from_millis(2_500));
    assert!(!budget.admit(start), "half a second in debt");
    assert!(!budget.admit(start + Duration::from_millis(400)));
    assert!(
        budget.admit(start + Duration::from_millis(600)),
        "the clock paid the debt back"
    );
}

#[test]
fn the_build_debt_stops_at_one_burst() {
    let mut budget = BuildTime::new(Duration::from_secs(2));
    let start = Instant::now();
    budget.charge(Duration::from_secs(100));
    assert!(!budget.admit(start + Duration::from_millis(1_900)));
    assert!(
        budget.admit(start + Duration::from_millis(2_100)),
        "two seconds of debt at most"
    );
}

#[test]
fn zero_leaves_only_the_total_limit() {
    let limits = Limits::new(0);
    let now = Instant::now();
    let client: IpAddr = "203.0.113.7".parse().unwrap();
    assert!((0..1_000).all(|_| limits.allow(client, now)));
}

#[test]
fn only_a_proxy_on_this_host_may_name_the_client() {
    let named = headers(&[("x-real-ip", "198.51.100.4")]);
    assert_eq!(
        client(peer("127.0.0.1"), &named),
        "198.51.100.4".parse::<IpAddr>().unwrap()
    );
    assert_eq!(
        client(peer("203.0.113.9"), &named),
        "203.0.113.9".parse::<IpAddr>().unwrap()
    );
    let chain = headers(&[("x-forwarded-for", "10.0.0.1, 198.51.100.5")]);
    assert_eq!(
        client(peer("::1"), &chain),
        "198.51.100.5".parse::<IpAddr>().unwrap()
    );
    let garbage = headers(&[("x-real-ip", "not an address")]);
    assert_eq!(
        client(peer("127.0.0.1"), &garbage),
        "127.0.0.1".parse::<IpAddr>().unwrap()
    );
}

#[test]
fn ipv6_clients_count_per_64() {
    let one = client(peer("2001:db8:1:2::1"), &HeaderMap::new());
    let two = client(peer("2001:db8:1:2:ffff::9"), &HeaderMap::new());
    let other = client(peer("2001:db8:1:3::1"), &HeaderMap::new());
    assert_eq!(one, two);
    assert_ne!(one, other);
}

#[test]
fn bodies_are_refused() {
    assert!(carries_body(&headers(&[("content-length", "12")])));
    assert!(carries_body(&headers(&[("transfer-encoding", "chunked")])));
    assert!(!carries_body(&headers(&[("content-length", "0")])));
    assert!(!carries_body(&HeaderMap::new()));
}
