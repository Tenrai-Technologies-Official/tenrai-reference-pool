use super::*;
use crate::test_support::{CHAIN, address};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const BLOCK: Hash384 = Hash384([7; 48]);

fn document() -> Value {
    let lock = parse_address(&address(1), Network::Development).unwrap();
    let point = domain_hash(b"TRAI_WORK_REWARD_V1", &[&CHAIN.0, &BLOCK.0]);
    json!({"block":BLOCK.to_hex(),"chain_id":CHAIN.to_hex(),"reward_lock":hex::encode(lock),
        "reward_address":address(1),"anchor":{"height":100,"cut_id":"11".repeat(48),
        "state_root":"22".repeat(48),"validator_set_id":"33".repeat(48)},"evidence":"unspent",
        "reward":{"outpoint":{"transaction_id":point.to_hex(),"output_index":0},"amount_atoms":"1000000",
        "source":"work_reward","created_finalized_height":20,"mature_at_height":20,"mature":true,"confirmations":81}})
}
fn parse(value: Value, minimum: u64) -> Result<Option<WorkReward>, WalletError> {
    checked(
        serde_json::from_value(value).map_err(|_| malformed())?,
        BLOCK,
        CHAIN,
        Network::Development,
        &address(1),
        minimum,
    )
}

#[test]
fn the_positive_contract_preserves_exact_amounts_and_settlement_height() {
    let reward = parse(document(), 100).unwrap().unwrap();
    assert_eq!(
        (reward.amount, reward.created, reward.anchor.height),
        (1_000_000, 20, 100)
    );
    for amount in [20_000_000_000_000_000_u64, u64::MAX] {
        let mut value = document();
        value["reward"]["amount_atoms"] = json!(amount.to_string());
        assert_eq!(
            parse(value, 100).unwrap().unwrap().amount,
            amount,
            "the client never clamps or silently narrows a supported RPC atom string"
        );
    }
}

#[test]
fn absence_is_unknown_and_never_a_negative_registration_fact() {
    let mut value = document();
    value["reward"] = Value::Null;
    assert!(parse(value.clone(), 100).is_err());
    value["evidence"] = json!("unknown");
    assert!(parse(value.clone(), 100).unwrap().is_none());
    value["reward"] = document()["reward"].clone();
    assert!(parse(value, 100).is_err());
}

#[test]
fn every_request_and_monetary_binding_is_checked_before_credit() {
    for (pointer, replacement) in [
        ("/block", json!("77".repeat(48))),
        ("/chain_id", json!("88".repeat(48))),
        ("/reward_lock", json!(format!("01{}", "02".repeat(40)))),
        ("/reward_address", json!(address(2))),
        ("/anchor/height", json!(99)),
        ("/anchor/cut_id", json!("AA".repeat(48))),
        ("/anchor/state_root", json!("f")),
        ("/anchor/validator_set_id", json!("g".repeat(96))),
        ("/evidence", json!("paid")),
        ("/reward/source", json!("transaction")),
        ("/reward/outpoint/transaction_id", json!("99".repeat(48))),
        ("/reward/outpoint/output_index", json!(1)),
        ("/reward/amount_atoms", json!("01")),
        ("/reward/amount_atoms", json!("0")),
        ("/reward/amount_atoms", json!("+1")),
        ("/reward/amount_atoms", json!("18446744073709551616")),
        ("/reward/created_finalized_height", json!(0)),
        ("/reward/created_finalized_height", json!(101)),
        ("/reward/mature_at_height", json!(21)),
        ("/reward/mature", json!(false)),
        ("/reward/confirmations", json!(80)),
    ] {
        let mut value = document();
        *value.pointer_mut(pointer).unwrap() = replacement;
        assert!(parse(value, 100).is_err(), "{pointer}");
    }
}

/// Actual bounded HTTP/RPC transport; cryptographic verification belongs to the daemon suite.
#[tokio::test]
async fn a_shared_wallet_client_requests_read_only_evidence_once_and_preserves_retryable_errors() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let physical = tokio::spawn(async move {
        let mut calls = Vec::new();
        for result in [
            json!({"result":document()}),
            json!({"error":{"code":-32001,"message":"proof incomplete"}}),
            json!({"error":{"code":-32005,"message":"locked"}}),
        ] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 4096];
            let call = loop {
                let read = socket.read(&mut buffer).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buffer[..read]);
                assert!(request.len() <= 65_536);
                if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&request[..end]).unwrap();
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse().unwrap())
                        })
                        .unwrap();
                    if request.len() >= end + 4 + length {
                        break serde_json::from_slice::<Value>(&request[end + 4..end + 4 + length])
                            .unwrap();
                    }
                }
            };
            assert_eq!(call["method"], "work_reward_evidence");
            assert_eq!(call["params"]["block"], BLOCK.to_hex());
            assert_eq!(call["params"]["minimum_height"], 100);
            assert_eq!(call["params"]["reward_address"], address(1));
            calls.push(call);
            let body = serde_json::to_vec(&result).unwrap();
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(header.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
            socket.shutdown().await.unwrap();
        }
        calls
    });
    let wallet = Wallet::new(endpoint, "public-test-token".into()).unwrap();
    let shared = wallet.clone();
    assert_eq!(
        wallet
            .work_reward_evidence(BLOCK, CHAIN, Network::Development, &address(1), 100)
            .await
            .unwrap()
            .unwrap()
            .amount,
        1_000_000
    );
    assert!(matches!(
        shared
            .work_reward_evidence(BLOCK, CHAIN, Network::Development, &address(1), 100)
            .await,
        Err(WalletError::Rpc { code: -32001, .. })
    ));
    assert!(matches!(
        wallet
            .work_reward_evidence(BLOCK, CHAIN, Network::Development, &address(1), 100)
            .await,
        Err(WalletError::Rpc { code: -32005, .. })
    ));
    let calls = physical.await.unwrap();
    assert_eq!(calls.len(), 3, "no automatic POST retry or payment call");
    assert_eq!(
        calls
            .iter()
            .map(|call| call["id"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}
