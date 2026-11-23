use super::*;

#[test]
fn requests_parse_into_messages() {
    let subscribe = r#"{"id": 1, "method": "mining.subscribe", "params": ["tenrai-miner/0.1.0", "rig tag", 3, 9]}"#;
    assert_eq!(
        ClientMessage::parse(subscribe).unwrap(),
        ClientMessage::Subscribe {
            pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
            spec_revision: crate::pow::tenq::SPEC_REVISION,
            id: json!(1),
            agent: "tenrai-miner/0.1.0".into(),
            tag: "rig tag".into()
        }
    );
    let authorize =
        r#"{"id": "a", "method": "mining.authorize", "params": ["dtrai1x.rig1", "d=512"]}"#;
    assert!(matches!(
        ClientMessage::parse(authorize).unwrap(),
        ClientMessage::Authorize { login, password, .. } if login == "dtrai1x.rig1" && password == "d=512"
    ));
    let submit = r#"{"id": 3, "method": "mining.submit", "params": ["rig1", "ab", "cd"]}"#;
    assert!(matches!(
        ClientMessage::parse(submit).unwrap(),
        ClientMessage::Submit { job_id, nonce, .. } if job_id == "ab" && nonce == "cd"
    ));
    assert!(matches!(
        ClientMessage::parse(r#"{"id": 4, "method": "mining.extranonce.subscribe"}"#).unwrap(),
        ClientMessage::Unknown { .. }
    ));
}

#[test]
fn malformed_requests_are_errors() {
    assert!(ClientMessage::parse("not json").is_err());
    assert!(ClientMessage::parse(r#"{"id": {}, "method": "mining.subscribe"}"#).is_err());
    assert!(ClientMessage::parse(r#"{"id": 1}"#).is_err());
    assert!(
        ClientMessage::parse(r#"{"id": 1, "method": "mining.submit", "params": ["w"]}"#).is_err()
    );
    assert!(
        ClientMessage::parse(r#"{"id": 1, "method": "mining.authorize", "params": []}"#).is_err()
    );
}

#[test]
fn answers_use_the_bridge_wire_format() {
    let reject = ServerMessage::reject(json!(7), RejectReason::Duplicate).to_line();
    assert_eq!(
        reject,
        r#"{"error":[22,"duplicate share",null],"id":7,"result":null}"#
    );
    let target = ServerMessage::SetTarget("ff".into()).to_line();
    assert_eq!(
        target,
        r#"{"id":null,"method":"mining.set_target","params":["ff"]}"#
    );
    let subscribe = serde_json::to_value(SubscribeResult {
        pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
        spec_revision: crate::pow::tenq::SPEC_REVISION,
        chain_id: hex::encode([0x44; 48]),
        protocol: PROTOCOL.into(),
        nonce_prefix: "0001".into(),
        pool_address: "dtrai1pool".into(),
    })
    .unwrap();
    assert_eq!(subscribe["pool_address"], "dtrai1pool");
    assert_eq!(RejectReason::InvalidAddress.code(), 27);
}

#[test]
fn logins_split_into_address_and_a_safe_worker_name() {
    assert_eq!(
        split_login("dtrai1abc.rig01"),
        ("dtrai1abc", "rig01".to_owned())
    );
    assert_eq!(
        split_login("dtrai1abc"),
        ("dtrai1abc", DEFAULT_WORKER.to_owned())
    );
    assert_eq!(
        split_login("dtrai1abc.rig.2"),
        ("dtrai1abc", "rig.2".to_owned())
    );
    assert_eq!(
        split_login("dtrai1abc.<script>alert(1)</script>").1,
        "_script_alert_1___script_"
    );
    assert_eq!(
        split_login(&format!("a.{}", "w".repeat(80))).1.len(),
        MAX_WORKER_BYTES
    );
}

#[test]
fn a_fixed_difficulty_comes_from_the_password() {
    assert_eq!(requested_difficulty("d=512"), Some(512));
    assert_eq!(requested_difficulty("x,d=65536"), Some(65_536));
    assert_eq!(requested_difficulty("d=1.5"), Some(1));
    assert_eq!(requested_difficulty("d=0"), None);
    assert_eq!(requested_difficulty("d=lots"), None);
    assert_eq!(requested_difficulty("x"), None);
    assert_eq!(requested_difficulty(""), None);
}

#[test]
fn miner_text_keeps_printable_ascii_only() {
    assert_eq!(printable("rig\u{202e}1\n", 64), "rig1");
    assert_eq!(printable("abcdef", 3), "abc");
}

#[test]
fn subscribe_requires_four_correctly_typed_identity_parameters() {
    for params in [
        json!([]),
        json!(["miner", "tag"]),
        json!(["miner", "tag", 3]),
        json!(["miner", "tag", 3, 8, "extra"]),
        json!([1, "tag", 3, 8]),
        json!(["miner", null, 3, 8]),
        json!(["miner", "tag", "3", 8]),
        json!(["miner", "tag", 256, 8]),
        json!(["miner", "tag", -1, 8]),
        json!(["miner", "tag", 3, 8.0]),
        json!(["miner", "tag", 3, "8"]),
        json!(["miner", "tag", 3, 4_294_967_296_u64]),
    ] {
        let line = json!({"id": 1, "method": "mining.subscribe", "params": params}).to_string();
        assert!(ClientMessage::parse(&line).is_err(), "{line}");
    }
}

#[test]
fn subscribe_answers_require_the_pow_and_chain_identity() {
    let result = json!({
        "protocol": PROTOCOL, "nonce_prefix": "0001", "pool_address": "dtrai1pool",
        "pow_algorithm_id": crate::pow::tenq::ALGORITHM_ID,
        "spec_revision": crate::pow::tenq::SPEC_REVISION, "chain_id": "44".repeat(48),
    });
    assert!(serde_json::from_value::<SubscribeResult>(result.clone()).is_ok());
    for field in ["pow_algorithm_id", "spec_revision", "chain_id"] {
        let mut missing = result.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<SubscribeResult>(missing).is_err(),
            "{field}"
        );
    }
}

#[test]
fn subscribe_answers_reject_a_noncanonical_chain_identity() {
    for chain in [
        String::new(),
        "44".repeat(47),
        "gg".repeat(48),
        "AA".repeat(48),
    ] {
        let result = json!({
            "protocol": PROTOCOL, "nonce_prefix": "0001", "pool_address": "dtrai1pool",
            "pow_algorithm_id": crate::pow::tenq::ALGORITHM_ID,
            "spec_revision": crate::pow::tenq::SPEC_REVISION, "chain_id": chain,
        });
        assert!(serde_json::from_value::<SubscribeResult>(result).is_err());
    }
}
