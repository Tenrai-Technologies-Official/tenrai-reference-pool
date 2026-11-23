use super::*;
use rusqlite::{Connection, params};
use tokio::net::TcpStream;

async fn read_rpc(socket: &mut TcpStream) -> Value {
    let mut input = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        let read = socket.read(&mut buffer).await.unwrap();
        assert!(read > 0);
        input.extend_from_slice(&buffer[..read]);
        assert!(input.len() <= 65_536);
        if let Some(end) = input.windows(4).position(|window| window == b"\r\n\r\n") {
            let length: usize = std::str::from_utf8(&input[..end])
                .unwrap()
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            if input.len() >= end + 4 + length {
                return serde_json::from_slice(&input[end + 4..end + 4 + length]).unwrap();
            }
        }
    }
}

enum Step {
    Capability(Option<bool>),
    StatusError,
    LoseAcknowledgement,
    MalformedAcknowledgement(String),
    RecoverReceipt,
}

#[tokio::test]
async fn a_daemon_swap_keeps_the_original_payout_reserved_until_durable_id_recovery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let journal_path =
        test_support::temp_path("payout-daemon-rid-journal").with_extension("sqlite");
    let path = test_support::temp_path("payout-daemon-capability-swap").with_extension("sqlite");
    let server_journal = journal_path.clone();
    let malformed = [
        "€".repeat(8),
        "AB".repeat(48),
        "ab".repeat(47),
        "gg".repeat(48),
    ];
    let invalid_count = malformed.len();
    let mut steps = vec![
        Step::Capability(Some(true)),
        Step::LoseAcknowledgement,
        Step::Capability(Some(false)),
        Step::Capability(None),
        Step::StatusError,
    ];
    for invalid in malformed {
        steps.push(Step::Capability(Some(true)));
        steps.push(Step::MalformedAcknowledgement(invalid));
    }
    steps.push(Step::Capability(Some(true)));
    steps.push(Step::RecoverReceipt);
    let server = tokio::spawn(async move {
        // An independently durable stand-in daemon journal makes duplicate monetary attempts visible.
        // It models RPC idempotency, not a blockchain or its consensus.
        let journal = Connection::open(server_journal).unwrap();
        journal.pragma_update(None, "synchronous", "FULL").unwrap();
        journal
            .execute_batch(
                "CREATE TABLE payments(request_id TEXT PRIMARY KEY, transaction_id TEXT NOT NULL)",
            )
            .unwrap();
        let transaction = "ab".repeat(48);
        let mut original_params = None;
        let mut requests = Vec::new();
        for step in steps {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_rpc(&mut socket).await;
            let result = match step {
                Step::Capability(capability) => {
                    assert_eq!(request["method"], "status");
                    let mut status = json!({"network":"development", "chain_id":test_support::CHAIN.to_hex(),
                        "live":true,"outputs":2,"pending_sends":0,"account":0,"request_archive_id":"69".repeat(32)});
                    if let Some(capability) = capability {
                        status["durable_request_ids"] = json!(capability);
                    }
                    Some(json!({"jsonrpc":"2.0","id":request["id"],"result":status}))
                }
                Step::StatusError => {
                    assert_eq!(request["method"], "status");
                    Some(
                        json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,"message":"status unavailable"}}),
                    )
                }
                step => {
                    assert_eq!(request["method"], "send_many");
                    assert_eq!(request["params"]["dry_run"], false);
                    let mut parameters = request["params"].clone();
                    let recovery_only = parameters
                        .as_object_mut()
                        .unwrap()
                        .remove("recovery_only")
                        .unwrap();
                    if let Some(original) = &original_params {
                        assert_eq!(&parameters, original);
                        assert_eq!(recovery_only, true);
                    } else {
                        assert_eq!(recovery_only, false);
                        original_params = Some(parameters);
                    }
                    let request_id = request["params"]["request_id"].as_str().unwrap();
                    journal
                        .execute(
                            "INSERT OR IGNORE INTO payments VALUES(?1,?2)",
                            params![request_id, &transaction],
                        )
                        .unwrap();
                    match step {
                        Step::LoseAcknowledgement => None,
                        Step::MalformedAcknowledgement(invalid) => {
                            Some(json!({"jsonrpc":"2.0","id":request["id"],
                            "result":{"transaction":invalid,"fee":"0.00000010","valid_until":123,"duplicate":true}}))
                        }
                        Step::RecoverReceipt => Some(json!({"jsonrpc":"2.0","id":request["id"],
                            "result":{"transaction":&transaction,"fee":"0.00000010","valid_until":123,"duplicate":true}})),
                        _ => unreachable!(),
                    }
                }
            };
            requests.push(request);
            if let Some(result) = result {
                socket
                    .write_all(&reply("200 OK", &serde_json::to_vec(&result).unwrap()))
                    .await
                    .unwrap();
            }
            let _ = socket.shutdown().await;
        }
        assert_eq!(
            journal
                .query_row("SELECT COUNT(*) FROM payments", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        requests
    });
    let (mut payouts, mut writer_thread) = service(&path, base.clone(), true).await;
    let original = payouts.reader.read(read::open_payouts).await.unwrap();
    let id = original[0].id;
    let request_id = original[0].request_id.clone();
    let recipients = [(test_support::address(2), RESERVED)];
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Unknown
    );
    assert_reserved(&payouts, id, &request_id).await;
    drop(payouts);
    writer_thread.join().unwrap();
    (payouts, writer_thread) = service(&path, base.clone(), false).await;
    for _ in 0..3 + invalid_count {
        assert_eq!(
            payouts.send(id, &request_id, &recipients, &[1]).await,
            Sending::Unknown
        );
        assert_reserved(&payouts, id, &request_id).await;
    }
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Sent
    );
    let recorded = payouts.reader.read(read::open_payouts).await.unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].id, id);
    assert_eq!(recorded[0].request_id, request_id);
    assert_eq!(recorded[0].status, PayoutStatus::Sent);
    assert_eq!(
        recorded[0].transaction_id.as_deref(),
        Some("ab".repeat(48).as_str())
    );
    let balance = payouts
        .reader
        .read(|connection| read::balance(connection, 1))
        .await
        .unwrap();
    assert_eq!(
        (
            balance.unpaid,
            balance.pending,
            balance.paid,
            balance.earned
        ),
        (0, RESERVED, 0, u128::from(RESERVED))
    );
    assert!(
        payouts
            .reader
            .read(|connection| read::payable(connection, ANY, 10))
            .await
            .unwrap()
            .is_empty()
    );
    let requests = server.await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["method"] == "send_many")
            .count(),
        invalid_count + 2
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["method"] == "status")
            .count(),
        invalid_count + 5
    );
    drop(payouts);
    writer_thread.join().unwrap();
    let (payouts, writer_thread) = service(&path, base, false).await;
    let recorded = payouts.reader.read(read::open_payouts).await.unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].request_id, request_id);
    assert_eq!(recorded[0].status, PayoutStatus::Sent);
    let journal = Connection::open(journal_path).unwrap();
    assert_eq!(
        journal
            .query_row("SELECT COUNT(*) FROM payments", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    drop(journal);
    drop(payouts);
    writer_thread.join().unwrap();
}

#[path = "context.rs"]
mod context_tests;

#[path = "archive.rs"]
mod archive_tests;
