use super::*;
use crate::test_support::lock;

const MAINNET: &str =
    "trai1qyqswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurscucsz2";
const TESTNET: &str =
    "ttrai1qyqswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qursxf0a4d";
const DEVELOPMENT: &str =
    "dtrai1qyqswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswdy689";
const THRESHOLD: &str =
    "dtrai1qyp95kj6tfd95kj6tfd95kj6tfd95kj6tfd95kj6tfd95kj6tfd95kj6tfd95kj6tfdq5sw2dk";

#[test]
fn addresses_are_encoded_like_the_node_encodes_them() {
    assert_eq!(address_text(&lock(7), Network::Mainnet), MAINNET);
    assert_eq!(address_text(&lock(7), Network::Testnet), TESTNET);
    assert_eq!(address_text(&lock(7), Network::Testnet100), TESTNET);
    assert_eq!(address_text(&lock(7), Network::Development), DEVELOPMENT);
}

#[test]
fn a_valid_address_parses_to_its_lock() {
    assert_eq!(
        parse_address(DEVELOPMENT, Network::Development),
        Ok(lock(7))
    );
    assert_eq!(parse_address(MAINNET, Network::Mainnet), Ok(lock(7)));
    let threshold = parse_address(THRESHOLD, Network::Development).unwrap();
    assert_eq!(threshold[0], 2);
    assert_eq!(threshold[1..], [0x5a; 40]);
}

#[test]
fn an_address_of_another_network_is_refused() {
    assert_eq!(
        parse_address(DEVELOPMENT, Network::Mainnet),
        Err(AddressError::Network)
    );
    assert_eq!(
        parse_address(MAINNET, Network::Development),
        Err(AddressError::Network)
    );
    assert_eq!(parse_address(TESTNET, Network::Testnet100), Ok(lock(7)));
}

#[test]
fn a_changed_or_missing_character_breaks_the_checksum() {
    let mut changed = DEVELOPMENT.to_owned();
    changed.replace_range(20..21, "x");
    assert_eq!(
        parse_address(&changed, Network::Development),
        Err(AddressError::Encoding)
    );
    let shortened = &DEVELOPMENT[..DEVELOPMENT.len() - 1];
    assert_eq!(
        parse_address(shortened, Network::Development),
        Err(AddressError::Encoding)
    );
    assert!(parse_address("", Network::Development).is_err());
    assert!(parse_address("dtrai1", Network::Development).is_err());
    assert!(parse_address("not an address", Network::Development).is_err());
}

#[test]
fn upper_case_is_refused() {
    assert_eq!(
        parse_address(&DEVELOPMENT.to_uppercase(), Network::Development),
        Err(AddressError::Uppercase)
    );
}

#[test]
fn an_unknown_lock_profile_is_refused() {
    let mut unknown = lock(7);
    unknown[0] = 4;
    let text = address_text(&unknown, Network::Development);
    assert_eq!(
        parse_address(&text, Network::Development),
        Err(AddressError::Payload)
    );
}

#[test]
fn the_domain_hash_matches_the_node() {
    assert_eq!(
        domain_hash(b"TRAI_TEST", &[b"a", b"bc"]).to_hex(),
        "b301908a8ace36fa1aa6bee1777d04d98617efc628c435dec97513e1e36d6ccff51a341e70563d02facab5b6bdace67d"
    );
}

#[test]
fn hashes_parse_only_from_96_hex_characters() {
    let hash = Hash384([0xab; 48]);
    assert_eq!(Hash384::from_hex(&hash.to_hex()), Some(hash));
    assert_eq!(Hash384::from_hex(&"ab".repeat(47)), None);
    assert_eq!(Hash384::from_hex(&"zz".repeat(48)), None);
}

#[test]
fn miner_tags_follow_the_node_rule() {
    assert!(is_miner_tag(b"Tenrai Ref v0.1.0"));
    assert!(is_miner_tag(b"pool@tenrai (eu)!"));
    assert!(!is_miner_tag(b" leading"));
    assert!(!is_miner_tag(b"trailing "));
    assert!(!is_miner_tag(b"<script>"));
    assert!(!is_miner_tag(b"a tag of 21 characters"));
    assert!(!is_miner_tag(b""));
    assert!(check_tag("").is_ok());
    assert!(check_tag("bad\"quote").is_err());
}

#[test]
fn the_extranonce_carries_the_tag_and_random_bytes() {
    let first = fresh_extranonce("pool");
    assert_eq!(first[0], 4);
    assert_eq!(&first[1..5], b"pool");
    let second = fresh_extranonce("pool");
    assert_ne!(first[5..], second[5..]);
    assert_eq!(fresh_extranonce("<bad>")[0], 0);
}

#[test]
fn native_addresses_are_valid_utxo_locks_but_not_mining_reward_addresses() {
    let mut native = lock(7);
    native[0] = 3;
    for network in [
        Network::Mainnet,
        Network::Testnet,
        Network::Testnet100,
        Network::Development,
    ] {
        assert_eq!(
            parse_address(&address_text(&native, network), network),
            Err(AddressError::NativeRewardLock)
        );
    }
}
