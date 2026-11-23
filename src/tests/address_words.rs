use super::*;
use crate::chain::parse_address;
use serde_json::Value;
use sha2::{Digest, Sha256};

const VECTORS: &str = include_str!("../../tests/vectors/address-words-v1.json");

#[test]
fn the_word_list_is_the_published_one() {
    let vectors: Value = serde_json::from_str(VECTORS).unwrap();
    assert_eq!(
        hex::encode(Sha256::digest(WORD_LIST.as_bytes())),
        vectors["word_list_sha256"]
    );
    assert_eq!(word_list().len(), 2_048);
}

#[test]
fn the_words_match_the_node_vectors() {
    let vectors: Value = serde_json::from_str(VECTORS).unwrap();
    let cases = vectors["cases"].as_array().unwrap();
    assert!(!cases.is_empty());
    for case in cases {
        let network = Network::parse(case["network"].as_str().unwrap()).unwrap();
        let lock = parse_address(case["address"].as_str().unwrap(), network).unwrap();
        assert_eq!(
            verification_words(network, &lock),
            case["words"],
            "{}",
            case["name"]
        );
    }
}
