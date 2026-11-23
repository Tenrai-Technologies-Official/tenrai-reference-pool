use super::*;
use std::net::Ipv4Addr;

fn ip(last: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(203, 0, 113, last))
}

#[test]
fn connections_are_limited_per_address_and_in_total() {
    let admission = Admission::new(3, 2, 6, None);
    let first = admission.admit(ip(1)).unwrap();
    let second = admission.admit(ip(1)).unwrap();
    assert_eq!(admission.admit(ip(1)).err(), Some(Refusal::PerIp));
    let third = admission.admit(ip(2)).unwrap();
    assert_eq!(admission.admit(ip(3)).err(), Some(Refusal::Full));
    assert_eq!(admission.connections(), 3);
    drop(second);
    assert!(admission.admit(ip(3)).is_ok());
    drop((first, third));
}

#[test]
fn a_flood_of_addresses_never_refuses_a_new_miner() {
    let admission = Admission::new(4, 4, 60, Some(Duration::from_secs(60)));
    let flood = |host: usize| {
        let prefix = (0x2001_0db8_u128 << 96) | ((host as u128) << 64);
        IpAddr::V6(std::net::Ipv6Addr::from(prefix))
    };
    for host in 0..MAX_TRACKED + 10 {
        drop(admission.admit(flood(host)).unwrap());
        admission.strike(flood(host));
        assert!(admission.take_verification(flood(host)));
    }
    assert!(admission.admit(ip(9)).is_ok(), "a new miner still gets in");
    let state = admission.state.lock().unwrap();
    assert!(state.connects.0.len() <= MAX_TRACKED);
    assert!(state.verification.0.len() <= MAX_TRACKED);
    assert!(state.strikes.len() <= MAX_TRACKED);
}

#[test]
fn the_ban_list_never_grows_past_its_size() {
    let admission = Admission::new(4, 4, 60, Some(Duration::from_secs(600)));
    let banned = |host: usize| {
        IpAddr::V6(std::net::Ipv6Addr::from(
            (0x2001_0db8_u128 << 96) | ((host as u128) << 64),
        ))
    };
    for host in 0..MAX_TRACKED {
        assert!(admission.ban(banned(host)));
    }
    assert!(!admission.ban(banned(MAX_TRACKED)), "full of running bans");
    assert_eq!(admission.banned(), MAX_TRACKED);
    assert!(
        admission.ban(banned(0)),
        "a running ban can still be renewed"
    );
}

#[test]
fn every_connection_gets_its_own_nonce_prefix() {
    let admission = Admission::new(16, 16, 6, None);
    let leases: Vec<Lease> = (0..16).map(|_| admission.admit(ip(1)).unwrap()).collect();
    let mut prefixes: Vec<[u8; 2]> = leases.iter().map(Lease::prefix).collect();
    prefixes.sort_unstable();
    prefixes.dedup();
    assert_eq!(prefixes.len(), 16);
    assert_eq!(admission.admit(ip(2)).err(), Some(Refusal::Full));
    drop(leases);
    assert!(admission.admit(ip(2)).is_ok());
}

#[test]
fn ipv6_counts_per_64_prefix() {
    let one: IpAddr = "2001:db8:1:2::1".parse().unwrap();
    let two: IpAddr = "2001:db8:1:2:ffff::9".parse().unwrap();
    let other: IpAddr = "2001:db8:1:3::1".parse().unwrap();
    assert_eq!(client_key(one), client_key(two));
    assert_ne!(client_key(one), client_key(other));
    let mapped: IpAddr = "::ffff:203.0.113.7".parse().unwrap();
    assert_eq!(client_key(mapped), ip(7));
}

#[test]
fn repeated_bad_requests_ban_an_address_when_bans_are_on() {
    let admission = Admission::new(8, 8, 6, Some(Duration::from_secs(60)));
    for _ in 0..9 {
        assert!(!admission.strike(ip(1)));
    }
    assert!(admission.strike(ip(1)));
    assert_eq!(admission.admit(ip(1)).err(), Some(Refusal::Banned));
    assert!(admission.admit(ip(2)).is_ok());
    assert_eq!(admission.banned(), 1);
}

#[test]
fn bans_stay_off_by_default_and_loopback_is_never_banned() {
    let off = Admission::new(8, 8, 6, None);
    for _ in 0..50 {
        assert!(!off.strike(ip(1)));
    }
    assert!(off.admit(ip(1)).is_ok());
    let on = Admission::new(8, 8, 6, Some(Duration::from_secs(60)));
    assert!(!on.ban(IpAddr::V4(Ipv4Addr::LOCALHOST)));
}

#[test]
fn a_judge_flags_a_mostly_invalid_batch() {
    let mut judge = Judge::default();
    assert!((0..20).any(|index| judge.record(index % 3 == 0)));
    let mut spam = Judge::default();
    assert_eq!((0..20).position(|_| spam.record(false)), Some(9));
    let mut honest = Judge::default();
    let flagged = (0..20).any(|index| honest.record(index != 5));
    assert!(!flagged);
}

#[test]
fn unverified_shares_of_one_address_are_budgeted() {
    let admission = Admission::new(8, 1, 6, None);
    let allowed = (0..100)
        .filter(|_| admission.take_verification(ip(1)))
        .count();
    assert_eq!(allowed, SESSION_SHARE_BURST as usize);
    admission.refund_verification(ip(1));
    assert!(admission.take_verification(ip(1)));
}
