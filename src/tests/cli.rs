use super::*;
use crate::test_support::temp_path;

fn args(items: &[&str]) -> Vec<OsString> {
    std::iter::once("tenrai-pool")
        .chain(items.iter().copied())
        .map(OsString::from)
        .collect()
}

#[test]
fn defaults_need_no_options() {
    let cli = parse(args(&[])).unwrap();
    assert_eq!(cli.network, crate::chain::Network::Mainnet);
    assert!(cli.node.is_none() && cli.node_api.is_none() && cli.wallet.is_none());
    assert_eq!(cli.pplns_window, "2");
    assert_eq!(cli.payout_batch_size, 7);
    assert!(cli.pplns_listen.is_empty());
}

#[test]
fn a_config_file_sets_options_and_the_command_line_wins() {
    let path = temp_path("config.json");
    std::fs::write(
        &path,
        r#"{"pplns-fee": "2", "solo-fee": 1.5, "fee-recipient": ["a=10", "b=20"], "no-payouts": true, "autoban": false}"#,
    )
    .unwrap();
    let file = path.to_str().unwrap();
    let cli = parse(args(&["--config", file, "--pplns-fee", "3"])).unwrap();
    assert_eq!(cli.pplns_fee, "3");
    assert_eq!(cli.solo_fee, "1.5");
    assert_eq!(cli.fee_recipient, ["a=10", "b=20"]);
    assert!(cli.no_payouts);
    assert!(!cli.autoban);
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_config_file_must_be_an_object_of_plain_values() {
    let path = temp_path("bad.json");
    std::fs::write(&path, r#"{"pplns-fee": {"nested": 1}}"#).unwrap();
    let file = path.to_str().unwrap();
    assert!(parse(args(&["--config", file])).is_err());
    std::fs::write(&path, "[1, 2]").unwrap();
    assert!(parse(args(&["--config", file])).is_err());
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_pool_tag_is_checked_like_a_miner_tag() {
    assert!(parse(args(&["--tag", "Tenrai Pool"])).is_ok());
    assert!(parse(args(&["--tag", "<b>pool</b>"])).is_err());
}
