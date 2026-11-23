use super::*;

fn ip(text: &str) -> IpAddr {
    text.parse().unwrap()
}

#[test]
fn a_client_gets_its_share_of_connections_and_the_proxy_only_the_total() {
    let open = Arc::new(Open::default());
    let client: Vec<Slot> = (0..CONNECTIONS_PER_CLIENT)
        .map(|_| Open::take(&open, ip("203.0.113.7")).unwrap())
        .collect();
    assert!(Open::take(&open, ip("203.0.113.7")).is_none());
    assert!(
        Open::take(&open, ip("2001:db8:1:2::9")).is_some(),
        "another client has its own share"
    );
    let proxy: Vec<Slot> = (0..CONNECTIONS_PER_CLIENT * 2)
        .map(|_| Open::take(&open, ip("127.0.0.1")).unwrap())
        .collect();
    drop(client);
    assert!(
        Open::take(&open, ip("203.0.113.7")).is_some(),
        "closed connections give their place back"
    );
    drop(proxy);
    assert_eq!(open.counts().0, 0);
    assert!(open.counts().1.is_empty());
}

#[test]
fn ipv6_clients_share_one_count_per_64() {
    let open = Arc::new(Open::default());
    let _held: Vec<Slot> = (0..CONNECTIONS_PER_CLIENT)
        .map(|host| Open::take(&open, ip(&format!("2001:db8:1:2::{host:x}"))).unwrap())
        .collect();
    assert!(Open::take(&open, ip("2001:db8:1:2:ffff::1")).is_none());
}

#[test]
fn no_connection_beyond_the_total() {
    let open = Arc::new(Open::default());
    let _held: Vec<Slot> = (0..MAX_CONNECTIONS)
        .map(|_| Open::take(&open, ip("::1")).unwrap())
        .collect();
    assert!(Open::take(&open, ip("::1")).is_none());
    assert!(Open::take(&open, ip("198.51.100.4")).is_none());
}
