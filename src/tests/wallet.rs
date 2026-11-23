use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

#[tokio::test]
async fn a_daemon_with_missing_or_false_durability_cannot_enable_payouts() {
    for capability in [None, Some(false)] {
        let mut status = json!({"network":"development","chain_id":crate::test_support::CHAIN.to_hex(),
            "live":true,"outputs":2,"pending_sends":0});
        if let Some(capability) = capability {
            status["durable_request_ids"] = json!(capability);
        }
        let (url, task) = server(vec![response(
            &serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":status})).unwrap(),
        )])
        .await;
        let wallet = Wallet::new(url, "public_test_token".into()).unwrap();
        assert!(matches!(
            wallet
                .checked_address(Network::Development, crate::test_support::CHAIN)
                .await,
            Err(CheckError::DurableRequestsRequired)
        ));
        assert_eq!(
            task.await.unwrap().len(),
            1,
            "no payment or address request follows an unsafe daemon"
        );
    }
}

#[tokio::test]
async fn durable_request_capability_preserves_the_chain_and_address_checks() {
    let address = crate::test_support::address(1);
    let status = json!({"network":"development","chain_id":crate::test_support::CHAIN.to_hex(),
        "live":true,"outputs":2,"pending_sends":0,"durable_request_ids":true});
    let (url, task) = server(vec![
        response(&serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":status})).unwrap()),
        response(
            &serde_json::to_vec(&json!({"jsonrpc":"2.0","id":2,"result":{"address":address}}))
                .unwrap(),
        ),
    ])
    .await;
    let wallet = Wallet::new(url, "public_test_token".into()).unwrap();
    assert_eq!(
        wallet
            .checked_address(Network::Development, crate::test_support::CHAIN)
            .await
            .unwrap(),
        address
    );
    let requests = task.await.unwrap();
    assert_eq!(requests[0]["method"], "status");
    assert_eq!(requests[1]["method"], "receive");
}

async fn server(replies: Vec<Vec<u8>>) -> (Url, JoinHandle<Vec<Value>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for reply in replies {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 4096];
            loop {
                let read = socket.read(&mut buffer).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buffer[..read]);
                assert!(request.len() <= 65_536);
                if let Some(end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
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
                        requests.push(
                            serde_json::from_slice(&request[end + 4..end + 4 + length]).unwrap(),
                        );
                        break;
                    }
                }
            }
            // Oversized replies may be cut off immediately by the capped client.
            let _ = socket.write_all(&reply).await;
            let _ = socket.shutdown().await;
        }
        requests
    });
    (Url::parse(&format!("http://{address}/")).unwrap(), task)
}

fn response(body: &[u8]) -> Vec<u8> {
    let mut bytes = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).into_bytes();
    bytes.extend_from_slice(body);
    bytes
}

fn assert_unknown(error: WalletError) {
    assert!(matches!(error, WalletError::Answer(_)), "{error}");
    assert!(error.to_string().contains("exceeds"), "{error}");
}

#[tokio::test]
async fn declared_oversized_wallet_reply_is_refused_before_body_or_json() {
    let reply = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        MAX_WALLET_RESPONSE_BYTES + 1
    )
    .into_bytes();
    let (url, task) = server(vec![reply]).await;
    let wallet = Wallet::new(url, "public-test-token".into()).unwrap();
    assert_unknown(wallet.call("status", json!({})).await.unwrap_err());
    assert_eq!(task.await.unwrap().len(), 1);
}

#[tokio::test]
async fn chunked_wallet_reply_cannot_grow_past_the_streaming_budget() {
    let mut reply =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
    for _ in 0..MAX_WALLET_RESPONSE_BYTES / 65_536 {
        reply.extend_from_slice(b"10000\r\n");
        reply.extend(std::iter::repeat_n(b'x', 65_536));
        reply.extend_from_slice(b"\r\n");
    }
    reply.extend_from_slice(b"1\r\nx\r\n0\r\n\r\n");
    let (url, task) = server(vec![reply]).await;
    let wallet = Wallet::new(url, "public-test-token".into()).unwrap();
    assert_unknown(wallet.call("status", json!({})).await.unwrap_err());
    assert_eq!(task.await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_valid_wallet_document_at_the_exact_limit_remains_readable() {
    let prefix = br#"{"jsonrpc":"2.0","id":1,"result":{"balance":"2.00000000","available":"1.00000000","outputs":2},"padding":""#;
    let suffix = br#""}"#;
    let mut body = prefix.to_vec();
    body.resize(MAX_WALLET_RESPONSE_BYTES - suffix.len(), b'x');
    body.extend_from_slice(suffix);
    assert_eq!(body.len(), MAX_WALLET_RESPONSE_BYTES);
    let (url, task) = server(vec![response(&body)]).await;
    let wallet = Wallet::new(url, "public-test-token".into()).unwrap();
    let balance = wallet.balance().await.unwrap();
    assert_eq!(balance.balance, 200_000_000);
    assert_eq!(balance.available, 100_000_000);
    assert_eq!(balance.outputs, 2);
    assert_eq!(task.await.unwrap().len(), 1);
}

#[tokio::test]
async fn an_oversized_send_ack_retries_the_identical_payment_request_id() {
    let oversized = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        MAX_WALLET_RESPONSE_BYTES + 1
    )
    .into_bytes();
    let transaction = "ab".repeat(48);
    let receipt = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":2,"result":{"transaction":transaction,"fee":"0.00000010","valid_until":123,"duplicate":true}})).unwrap();
    let capability = response(
        &serde_json::to_vec(&json!({"jsonrpc":"2.0","result":{
        "network":"development","chain_id":crate::test_support::CHAIN.to_hex(),"live":true,
        "outputs":2,"pending_sends":0,"durable_request_ids":true}}))
        .unwrap(),
    );
    let (url, task) = server(vec![
        capability.clone(),
        oversized,
        capability,
        response(&receipt),
    ])
    .await;
    let wallet = Wallet::new(url, "public-test-token".into()).unwrap();
    let recipients = [("public-test-recipient".to_owned(), 1_000_000)];
    let send = || {
        wallet.send_many(
            &recipients,
            "minimum",
            "public-test-stable-id",
            false,
            false,
        )
    };
    assert_unknown(send().await.unwrap_err());
    let retried = send().await.unwrap();
    assert_eq!(retried.transaction, transaction);
    assert_eq!(retried.fee, 10);
    assert_eq!(retried.valid_until, 123);
    assert!(retried.duplicate);
    let all_requests = task.await.unwrap();
    assert_eq!(
        all_requests
            .iter()
            .map(|request| request["method"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["status", "send_many", "status", "send_many"]
    );
    let requests = [&all_requests[1], &all_requests[3]];
    assert_eq!(requests[0]["params"], requests[1]["params"]);
    assert_eq!(requests[0]["params"]["request_id"], "public-test-stable-id");
    assert_eq!(requests[0]["params"]["dry_run"], false);
    assert_ne!(
        requests[0]["id"], requests[1]["id"],
        "transport correlation IDs differ; payment idempotency ID does not"
    );
}

#[tokio::test]
async fn wallet_output_totals_check_overflow_after_selecting_the_largest() {
    // As the daemon answers `largest`: at most that many outputs, from a complete snapshot.
    let document = |amounts: &[u64]| {
        let outputs: Vec<Value> = amounts
            .iter()
            .map(|amount| json!({"amount":format_trai(*amount),"reserved":false}))
            .collect();
        let result = json!({"outputs":outputs,"complete":true,"snapshot_complete":true});
        response(&serde_json::to_vec(&json!({"jsonrpc":"2.0","result":result})).unwrap())
    };
    let (url, task) = server(vec![
        document(&[u64::MAX, 1]),
        document(&[u64::MAX - 1, 1]),
        document(&[u64::MAX]),
    ])
    .await;
    let wallet = Wallet::new(url, "public-test-token".into()).unwrap();
    let error = wallet.largest_outputs(2).await.unwrap_err();
    assert!(matches!(error, WalletError::Answer(_)));
    assert!(error.to_string().contains("overflows"));
    assert_eq!(wallet.largest_outputs(2).await.unwrap(), u64::MAX);
    assert_eq!(wallet.largest_outputs(1).await.unwrap(), u64::MAX);
    assert_eq!(task.await.unwrap().len(), 3);
}

#[tokio::test]
async fn repeated_fee_quotes_never_request_a_signed_export_or_a_payment_identity() {
    let quote = response(
        &serde_json::to_vec(&json!({"jsonrpc":"2.0","result":{
        "unsigned":true,"submitted":false,"fee":"0.00000010"}}))
        .unwrap(),
    );
    let (url, task) = server(vec![quote.clone(), quote]).await;
    let wallet = Wallet::new(url, "public-test-token".into()).unwrap();
    let recipients = [("public-test-recipient".into(), 1_000_000)];
    for subtract in [false, true] {
        assert_eq!(
            wallet
                .quote_send_many(&recipients, "minimum", subtract)
                .await
                .unwrap(),
            10
        );
    }
    for call in task.await.unwrap() {
        assert_eq!(call["method"], "quote_send_many");
        assert_eq!(call["params"]["recipients"][0]["amount"], "0.01000000");
        for forbidden in ["dry_run", "request_id", "recovery_only", "compound_first"] {
            assert!(call["params"].get(forbidden).is_none());
        }
    }
}
#[tokio::test]
async fn a_signed_export_or_malformed_fee_is_not_accepted_as_a_quote() {
    let invalid = [
        json!({"fee":"0.00000010","transaction":"ab".repeat(48)}),
        json!({"unsigned":true,"submitted":false,"fee":"0.00000010","wire_hex":"00"}),
        json!({"unsigned":true,"submitted":true,"fee":"0.00000010"}),
        json!({"unsigned":true,"submitted":false,"fee":"184467440738"}),
    ];
    let replies = invalid
        .iter()
        .map(|value| {
            response(&serde_json::to_vec(&json!({"jsonrpc":"2.0","result":value})).unwrap())
        })
        .collect();
    let (url, task) = server(replies).await;
    let wallet = Wallet::new(url, "public-test-token".into()).unwrap();
    for _ in invalid {
        assert!(wallet.quote_send_many(&[], "minimum", false).await.is_err());
    }
    assert_eq!(task.await.unwrap().len(), 4);
}

#[tokio::test]
async fn every_real_payout_attempt_carries_the_same_exact_signing_fee_cap() {
    let receipt = response(
        &serde_json::to_vec(&json!({"jsonrpc":"2.0","result":{
        "transaction":"ab".repeat(48),"fee":"0.00000020","valid_until":123}}))
        .unwrap(),
    );
    let (url, task) = server(vec![receipt.clone(), receipt]).await;
    let wallet = Wallet::new(url, "public-test-token".into()).unwrap();
    let scope = RequestScope {
        archive_id: "69".repeat(32),
        account: 0,
        chain_id: crate::test_support::CHAIN.to_hex(),
    };
    let recipients = [("public-test-recipient".into(), 1_000_000)];
    for recovery in [false, true] {
        assert_eq!(
            wallet
                .send_many_bound(
                    &recipients,
                    "normal",
                    "capped-original",
                    false,
                    &scope,
                    recovery,
                    Some(20)
                )
                .await
                .unwrap()
                .fee,
            20
        );
    }
    let calls = task.await.unwrap();
    for call in &calls {
        assert_eq!(call["method"], "send_many");
        assert_eq!(call["params"]["max_fee"], "0.00000020");
        assert_eq!(call["params"]["request_id"], "capped-original");
        assert_eq!(call["params"]["fee"], "normal");
        assert_eq!(call["params"]["dry_run"], false);
    }
    assert_eq!(calls[0]["params"]["recovery_only"], false);
    assert_eq!(calls[1]["params"]["recovery_only"], true);
}
#[tokio::test]
async fn an_over_budget_receipt_stays_unresolved_instead_of_becoming_a_success() {
    let receipt = response(
        &serde_json::to_vec(&json!({"jsonrpc":"2.0","result":{
        "transaction":"ab".repeat(48),"fee":"0.00000021","valid_until":123}}))
        .unwrap(),
    );
    let (url, task) = server(vec![receipt]).await;
    let wallet = Wallet::new(url, "public-test-token".into()).unwrap();
    let scope = RequestScope {
        archive_id: "69".repeat(32),
        account: 0,
        chain_id: crate::test_support::CHAIN.to_hex(),
    };
    assert!(
        wallet
            .send_many_bound(
                &[],
                "normal",
                "capped-original",
                false,
                &scope,
                false,
                Some(20)
            )
            .await
            .is_err()
    );
    assert_eq!(task.await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_complete_owned_never_signed_result_is_not_a_transaction_or_an_unknown_error() {
    let scope = RequestScope {
        archive_id: "69".repeat(32),
        account: 0,
        chain_id: crate::test_support::CHAIN.to_hex(),
    };
    let receipt = json!({"outcome":"never_signed","reason":"fee_limit_exceeded","request_id":"budget-original",
        "request_archive_id":scope.archive_id,"account":0,"chain_id":scope.chain_id,
        "fingerprint":"ab".repeat(48),"maximum_fee_atoms":"20","planned_fee_atoms":"21"});
    let mut wrong = receipt.clone();
    wrong["request_archive_id"] = json!("cd".repeat(32));
    let replies = [receipt, wrong]
        .iter()
        .map(|result| {
            response(&serde_json::to_vec(&json!({"jsonrpc":"2.0","result":result})).unwrap())
        })
        .collect();
    let (url, task) = server(replies).await;
    let wallet = Wallet::new(url, "public-test-token".into()).unwrap();
    let error = wallet
        .send_many_bound(
            &[],
            "normal",
            "budget-original",
            false,
            &scope,
            true,
            Some(20),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, WalletError::NeverSigned(_)));
    let error = wallet
        .send_many_bound(
            &[],
            "normal",
            "budget-original",
            false,
            &scope,
            true,
            Some(20),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, WalletError::Answer(_)),
        "wrong archive cannot authorize an accounting release"
    );
    assert_eq!(task.await.unwrap().len(), 2);
}
