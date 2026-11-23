use super::*;
use crate::{cli, test_support::temp_path};
use std::ffi::OsString;

fn config(extra: &[&str]) -> Result<Config> {
    let token = temp_path("token");
    std::fs::write(&token, "secret\n").unwrap();
    let mut args: Vec<OsString> = vec![
        "tenrai-pool".into(),
        "--wallet-token-file".into(),
        token.clone().into(),
    ];
    args.extend(extra.iter().map(OsString::from));
    let result = Config::build(&cli::parse(args).unwrap(), None);
    let _ = std::fs::remove_file(token);
    result
}

#[test]
fn the_defaults_make_a_valid_config() {
    let config = config(&[]).unwrap();
    assert_eq!(config.wallet_token.as_deref(), Some("secret"));
    assert!(config.wallet_check);
    assert_eq!(config.payouts.interval, Duration::from_secs(15 * 60));
    assert_eq!(config.listeners.len(), 2);
    assert_eq!(config.listeners[0].address.port(), 28740);
    assert_eq!(config.listeners[0].mode, Mode::Pplns);
    assert_eq!(config.listeners[1].address.port(), 28744);
    assert_eq!((config.fees.pplns_bps, config.fees.solo_bps), (250, 300));
    assert_eq!(
        (config.vardiff.min, config.vardiff.share_interval),
        (50, 20)
    );
    assert_eq!(config.payouts.minimum, ATOMS_PER_TRAI);
    assert_eq!(config.pplns.window_work(1_000), 2_000);
    assert_eq!(config.pplns.lookback, Duration::from_secs(3_600));
    assert_eq!(config.vardiff.shares_per_block, 16);
    assert_eq!(config.keep_block_splits, Duration::ZERO);
    assert_eq!(config.keep_blocks, Some(Duration::from_secs(2 * 86_400)));
    assert_eq!(config.keep_blocks_max, Some(10_000));
    assert_eq!(
        config.explorer_url.as_deref(),
        Some("https://explorer.tenrai-technologies.org/")
    );
    assert!(config.stratum_urls.is_empty());
}

#[test]
fn v1_stays_on_beside_v2_until_switched_off() {
    let tls = ["--tls-cert", "pool.crt", "--tls-key", "pool.key"];
    let both = config(&[&["--pplns-listen-v2-tls", "0.0.0.0:28742"], &tls[..]].concat()).unwrap();
    let described: Vec<_> = both
        .listeners
        .iter()
        .map(|listener| {
            (
                listener.address.port(),
                listener.mode,
                listener.tls,
                listener.protocol,
            )
        })
        .collect();
    assert_eq!(
        described,
        [
            (28742, Mode::Pplns, true, Protocol::V2),
            (28740, Mode::Pplns, false, Protocol::V1),
            (28744, Mode::Solo, false, Protocol::V1),
        ]
    );
    let only = config(
        &[
            &["--no-v1", "--solo-listen-v2-tls", "0.0.0.0:28746"],
            &tls[..],
        ]
        .concat(),
    )
    .unwrap();
    assert_eq!(only.listeners.len(), 1);
    assert_eq!(only.listeners[0].protocol, Protocol::V2);

    let own = config(&[
        "--pplns-listen-v2-tls",
        "192.0.2.7:28742",
        "--public-host",
        "pool.example",
        "--solo-url",
        "stratum2+tls://[2001:db8::7]:28746",
        "--db",
        "state/pool.sqlite",
    ])
    .unwrap();
    assert_eq!(
        own.tls,
        Some((
            PathBuf::from("state/stratum.crt"),
            PathBuf::from("state/stratum.key")
        ))
    );
    assert_eq!(
        own.own_certificate.unwrap(),
        [
            "localhost",
            "127.0.0.1",
            "::1",
            "192.0.2.7",
            "pool.example",
            "2001:db8::7"
        ]
    );
    assert!(both.own_certificate.is_none());
    assert!(config(&["--no-v1"]).is_err());
    assert!(config(&["--no-v1", "--pplns-listen", "0.0.0.0:28740"]).is_err());
    assert!(config(&["--pplns-url", "stratum2+tcp://pool.example:28742"]).is_err());
    let shown = config(&["--pplns-url", "stratum2+tls://pool.example:28742"]).unwrap();
    assert_eq!(shown.stratum_urls[0].1, "stratum2+tls://pool.example:28742");
}

#[test]
fn web_pages_get_websocket_ports_only_when_asked() {
    let plain = config(&[]).unwrap();
    assert!(plain.listeners.iter().all(|listener| !listener.web));

    let web = config(&[
        "--pplns-listen-ws",
        "127.0.0.1:28743",
        "--solo-listen-ws",
        "127.0.0.1:28747",
        "--ws-origin",
        "https://Miner.Example",
    ])
    .unwrap();
    let ports: Vec<_> = web
        .listeners
        .iter()
        .filter(|listener| listener.web)
        .collect();
    assert_eq!(ports.len(), 2);
    assert!(
        ports
            .iter()
            .all(|listener| !listener.tls && listener.protocol == Protocol::V1)
    );
    assert_eq!(ports[0].mode, Mode::Pplns);
    assert_eq!(ports[1].mode, Mode::Solo);
    assert!(
        web.listeners.iter().any(|listener| !listener.web),
        "the default V1 ports stay"
    );
    assert!(web.web_origins.admits(Some("https://miner.example")));
    assert!(!web.web_origins.admits(Some("https://other.example")));

    assert!(
        config(&[
            "--pplns-listen-ws",
            "127.0.0.1:28743",
            "--ws-origin",
            "miner.example"
        ])
        .is_err()
    );
    assert!(
        config(&["--pplns-listen-ws", "127.0.0.1:28740"]).is_err(),
        "the default PPLNS port"
    );
}

#[test]
fn website_links_and_stratum_urls_are_checked() {
    let custom = config(&[
        "--pplns-url",
        "stratum+tls://pool.example:28741",
        "--solo-url",
        "stratum+tls://pool.example:28745",
        "--explorer-url",
        "https://explorer.example/path",
    ])
    .unwrap();
    assert_eq!(
        custom.stratum_urls,
        vec![
            (Mode::Pplns, "stratum+tls://pool.example:28741".to_owned()),
            (Mode::Solo, "stratum+tls://pool.example:28745".to_owned()),
        ]
    );
    assert_eq!(
        custom.explorer_url.as_deref(),
        Some("https://explorer.example/path/")
    );
    assert_eq!(config(&["--explorer-url", ""]).unwrap().explorer_url, None);
    assert!(config(&["--pplns-url", "http://pool.example:28740"]).is_err());
    assert!(config(&["--solo-url", "stratum+tcp://pool.example:28744"]).is_err());
    assert!(config(&["--solo-url", "stratum+tcp://127.0.0.1:28744"]).is_ok());
    assert!(config(&["--solo-url", "stratum+tcp://pool example:28744"]).is_err());
    assert!(config(&["--explorer-url", "javascript:alert(1)"]).is_err());
    assert!(config(&["--explorer-url", "https://explorer.example/?a=1"]).is_err());
    assert!(config(&["--shares-per-block", "0"]).is_err());
}

#[test]
fn unsafe_combinations_are_refused() {
    assert!(config(&["--pplns-fee", "100.01"]).is_err());
    assert!(config(&["--fee-recipient", "a=60", "--fee-recipient", "b=50"]).is_err());
    assert!(config(&["--payout-batch-size", "0"]).is_err());
    assert!(config(&["--min-difficulty", "0"]).is_err());
    assert!(config(&["--start-difficulty", "10", "--min-difficulty", "20"]).is_err());
    assert!(config(&["--pplns-window", "0"]).is_err());
    assert!(config(&["--pplns-window", "1.23456"]).is_err());
    assert!(config(&["--min-payout", "0"]).is_err());
    assert!(
        config(&[
            "--pplns-listen",
            "127.0.0.1:5555",
            "--solo-listen",
            "127.0.0.1:5555"
        ])
        .is_err()
    );
    assert!(config(&["--pplns-listen", "0.0.0.0:28740"]).is_err());
    assert!(config(&["--pplns-listen-ws", "0.0.0.0:28743"]).is_err());
    assert!(config(&["--node", "http://203.0.113.5:28702"]).is_err());
    assert!(config(&["--node", "https://node.example.org/path"]).is_err());
    assert!(config(&["--payout-fee", "fast"]).is_err());
}

#[test]
fn a_remote_node_needs_https() {
    assert!(http_url("http://127.0.0.1:28702", "--node").is_ok());
    assert!(http_url("http://localhost:28702", "--node").is_ok());
    assert!(http_url("http://[::1]:28702", "--node").is_ok());
    assert!(http_url("https://node.example.org", "--node").is_ok());
    assert!(http_url("http://node.example.org", "--node").is_err());
    assert!(http_url("ftp://127.0.0.1", "--node").is_err());
}

#[test]
fn a_remote_node_needs_an_identity_pin() {
    let chain_id = "11".repeat(48);
    let identity = "ab".repeat(32);
    let remote = [
        "--node",
        "https://mining.example.org",
        "--node-api",
        "https://api.example.org",
    ];
    assert!(config(&remote).is_err());

    let mainnet = config(&[
        "--node",
        "https://mining.example.org",
        "--node-api",
        "https://api.example.org",
        "--node-identity",
        &identity,
    ])
    .unwrap();
    assert_eq!(mainnet.chain_id.to_hex(), crate::chain::MAINNET_CHAIN_ID);

    let pinned = config(&[
        "--node",
        "https://mining.example.org",
        "--node-api",
        "https://api.example.org",
        "--chain-id",
        &chain_id,
        "--node-identity",
        &identity,
    ])
    .unwrap();
    assert_eq!(pinned.chain_id, Hash384([0x11; 48]));
    assert_eq!(pinned.node_identity, Some([0xab; 32]));

    let uppercase_identity = identity.to_uppercase();
    assert!(
        config(&[
            "--node",
            "https://mining.example.org",
            "--node-api",
            "https://api.example.org",
            "--chain-id",
            &chain_id,
            "--node-identity",
            &uppercase_identity,
        ])
        .is_err()
    );
}

#[test]
fn percentages_become_basis_points_exactly() {
    assert_eq!(percent_bps("1").unwrap(), 100);
    assert_eq!(percent_bps("0.25").unwrap(), 25);
    assert_eq!(percent_bps("1.5").unwrap(), 150);
    assert_eq!(percent_bps("100").unwrap(), 10_000);
    assert_eq!(percent_bps("0").unwrap(), 0);
    assert!(percent_bps("1.234").is_err());
    assert!(percent_bps("-1").is_err());
    assert!(percent_bps("1e2").is_err());
}

#[test]
fn trai_amounts_become_atoms_exactly() {
    assert_eq!(trai_atoms("1").unwrap(), 100_000_000);
    assert_eq!(trai_atoms("0.00000001").unwrap(), 1);
    assert_eq!(trai_atoms("12.5").unwrap(), 1_250_000_000);
    assert!(trai_atoms("0.000000001").is_err());
    assert!(trai_atoms("abc").is_err());
    assert_eq!(format_trai(1_250_000_001), "12.50000001");
}

#[test]
fn factors_print_without_trailing_zeros() {
    assert_eq!(decimal_text(2, 1), "2");
    assert_eq!(decimal_text(15, 10), "1.5");
    assert_eq!(decimal_text(1_250, 1_000), "1.25");
    assert_eq!(decimal_text(100, 100), "1");
    assert_eq!(decimal_text(5, 100), "0.05");
}

#[test]
fn payouts_need_the_wallet_token_unless_they_are_off() {
    let started = |extra: &[&str], usual: Option<PathBuf>| {
        let mut args: Vec<OsString> = vec!["tenrai-pool".into()];
        args.extend(extra.iter().map(OsString::from));
        Config::build(&cli::parse(args).unwrap(), usual)
    };
    let bare = |extra: &[&str]| started(extra, None);
    let refused = bare(&[]).unwrap_err().to_string();
    assert!(refused.contains("--wallet-token-file"), "{refused}");
    assert!(refused.contains("tenrai-wallet daemon"), "{refused}");
    let missing = temp_path("no-token");
    let refused = started(&[], Some(missing.clone())).unwrap_err().to_string();
    assert!(
        refused.contains(&missing.display().to_string()),
        "{refused}"
    );
    let off = bare(&["--no-payouts"]).unwrap();
    assert!(off.wallet_token.is_none());
    assert!(!off.payouts.enabled);
    let unchecked = config(&["--no-wallet-check", "--pool-address", " dtrai1abc "]).unwrap();
    assert!(!unchecked.wallet_check);
    assert_eq!(unchecked.pool_address.as_deref(), Some("dtrai1abc"));
}

#[test]
fn a_network_brings_its_own_ports_and_a_testnet_needs_its_chain_id() {
    let refused = config(&["--network", "testnet"]).unwrap_err().to_string();
    assert!(refused.contains("--chain-id"), "{refused}");

    let chain = "de".repeat(48);
    let development =
        config(&["--network", "development", "--pplns-listen-ws", "127.0.0.1"]).unwrap();
    assert_eq!(development.chain_id.to_hex(), chain);
    assert_eq!(chain, crate::chain::DEVELOPMENT_CHAIN_ID);
    assert_eq!(development.node.as_str(), "http://127.0.0.1:28712/");
    assert_eq!(development.node_api.as_str(), "http://127.0.0.1:28711/");
    assert_eq!(
        development
            .listeners
            .iter()
            .map(|listener| listener.address.port())
            .collect::<Vec<_>>(),
        [28759, 28756, 28760]
    );
    assert_eq!(development.web.unwrap().port(), 28778);

    let testnet = config(&["--network", "testnet100", "--chain-id", &chain]).unwrap();
    assert_eq!(testnet.wallet.as_str(), "http://127.0.0.1:28708/");
    assert_eq!(testnet.listeners[0].address.port(), 28748);
}

#[test]
fn the_token_of_this_machines_wallet_daemon_is_found_without_naming_it() {
    let profile = temp_path("profile");
    let token = crate::home::wallet_token_file_in(&profile, Network::Mainnet);
    assert!(token.ends_with("wallet/mainnet/wallet.vault.token"));
    std::fs::create_dir_all(token.parent().unwrap()).unwrap();
    std::fs::write(&token, "from the daemon\n").unwrap();
    let args: Vec<OsString> = vec!["tenrai-pool".into()];
    let cli = cli::parse(args).unwrap();
    let found = Config::build(&cli, Some(token.clone())).unwrap();
    assert_eq!(found.wallet_token.as_deref(), Some("from the daemon"));
    assert!(found.payouts.enabled);
    let mut off = cli;
    off.no_payouts = true;
    assert!(
        Config::build(&off, Some(token))
            .unwrap()
            .wallet_token
            .is_none()
    );
    let _ = std::fs::remove_dir_all(profile);
}

#[test]
fn the_shipped_example_builds_its_config() {
    let example = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("pool.example.json");
    let config = config(&["--config", example.to_str().unwrap()]).unwrap();
    assert_eq!(config.node.as_str(), "http://127.0.0.1:28702/");
    assert_eq!(config.node_api.as_str(), "http://127.0.0.1:28701/");
    assert_eq!(config.wallet.as_str(), "http://127.0.0.1:28703/");
    assert_eq!(
        config.wallet_token.as_deref(),
        Some("secret"),
        "the command line's token file wins"
    );
    let tls: Vec<_> = config
        .listeners
        .iter()
        .filter(|listener| listener.tls)
        .map(|listener| (listener.address.port(), listener.mode))
        .collect();
    assert_eq!(tls, [(28741, Mode::Pplns), (28745, Mode::Solo)]);
    assert_eq!((config.max_clients, config.max_clients_per_ip), (8192, 256));
    assert_eq!(config.tag.as_deref(), Some("Tenrai Pool"));
    assert_eq!(
        (
            config.vardiff.start,
            config.vardiff.min,
            config.vardiff.share_interval
        ),
        (1_048_576, 1_024, 20)
    );
    assert_eq!(config.pplns.lookback, Duration::from_secs(3_600));
    assert_eq!((config.fees.pplns_bps, config.fees.solo_bps), (250, 300));
    assert!(config.fees.recipients.is_empty());
    assert_eq!(config.payouts.interval, Duration::from_secs(15 * 60));
    assert_eq!(config.payouts.minimum, ATOMS_PER_TRAI);
    assert_eq!(
        config.db,
        std::path::PathBuf::from("/var/lib/tenrai-pool/pool.sqlite")
    );
    assert_eq!(config.web.map(|web| web.port()), Some(28772));
    assert_eq!(config.pool_name, "Tenrai Pool");
    assert_eq!(config.public_host.as_deref(), Some("pool.example.org"));
}
