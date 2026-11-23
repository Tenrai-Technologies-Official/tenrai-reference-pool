use super::*;
use std::collections::BTreeSet;

#[test]
fn every_network_has_its_own_ports() {
    let mut seen = BTreeSet::new();
    for network in [Network::Mainnet, Network::Testnet, Network::Development] {
        let ports = network.ports();
        for port in [
            ports.node_api,
            ports.node_mining,
            ports.wallet,
            ports.pplns,
            ports.pplns_tls,
            ports.pplns_v2,
            ports.pplns_ws,
            ports.solo,
            ports.solo_tls,
            ports.solo_v2,
            ports.solo_ws,
            ports.web,
        ] {
            assert!((28_700..28_780).contains(&port), "{port}");
            assert!(seen.insert(port), "{port} is used twice");
        }
    }
    assert_eq!(Network::Mainnet.ports().pplns, 28_740);
    assert_eq!(Network::Testnet.ports().solo_ws, 28_755);
    assert_eq!(Network::Development.ports().node_mining, 28_712);
    assert_eq!(Network::Development.ports().web, 28_778);
    assert_eq!(Network::Testnet100.ports(), Network::Testnet.ports());
}

#[test]
fn an_ip_alone_takes_the_default_port() {
    assert_eq!(
        bind_address("0.0.0.0", 28_740),
        "0.0.0.0:28740".parse().ok()
    );
    assert_eq!(bind_address("0.0.0.0:9", 28_740), "0.0.0.0:9".parse().ok());
    assert_eq!(bind_address("[::1]", 28_744), "[::1]:28744".parse().ok());
    assert_eq!(bind_address("pool.example", 28_740), None);
    assert_eq!(bind_address("0.0.0.0:0", 28_740), None);
}

#[test]
fn a_host_alone_is_plain_http_on_the_default_port() {
    assert_eq!(http_origin("127.0.0.1", 28_702), "http://127.0.0.1:28702");
    assert_eq!(http_origin("localhost:9", 28_703), "http://localhost:9");
    assert_eq!(http_origin("::1", 28_701), "http://[::1]:28701");
    assert_eq!(
        http_origin("https://node.example", 28_701),
        "https://node.example"
    );
    assert_eq!(http_origin("node example", 28_701), "node example");
}

#[test]
fn malformed_hosts_are_refused() {
    for malformed in [
        "",
        "999.1.1.1",
        "node..example",
        "node.example:0",
        "node.example:port",
    ] {
        assert_eq!(host_port(malformed, 28_740), None, "{malformed}");
    }
}
