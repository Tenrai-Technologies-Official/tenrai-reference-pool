use super::*;

#[path = "payout_uncertain/capability.rs"]
mod capability_tests;
use crate::{db, test_support};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::watch,
};
use url::Url;

const RESERVED: u64 = 500;
const ANY: u64 = 1;

fn reply(status: &str, body: &[u8]) -> Vec<u8> {
    let mut bytes = format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).into_bytes();
    bytes.extend_from_slice(body);
    bytes
}

async fn service(path: &Path, base: Url, fresh: bool) -> (Payouts, std::thread::JoinHandle<()>) {
    let (writer, reader, thread) = db::open(path).unwrap();
    if fresh {
        writer.send(Op::Miner {
            id: 1,
            address: test_support::address(2),
            at: 1,
        });
        writer.flush().await;
        let connection = db::connect(path, false).unwrap();
        connection
            .execute(
                "INSERT INTO balances(miner_id,unpaid,pending,paid,earned) VALUES(1,?1,0,0,?1)",
                [RESERVED as i64],
            )
            .unwrap();
        drop(connection);
        writer
            .call(|done| Op::PayoutCreate {
                request_prefix: "public-test-stable-".into(),
                items: vec![(1, RESERVED)],
                at: 1,
                done,
            })
            .await
            .unwrap();
    }
    let (_, heights) = watch::channel(1_u64);
    let (_, shutdown) = watch::channel(false);
    (
        Payouts {
            wallet: Wallet::new(base.clone(), "public-test-token".into()).unwrap(),
            checked: true,
            node: crate::node::NodeClient::new(base.clone(), base).unwrap(),
            reader,
            writer,
            settings: PayoutSettings {
                enabled: true,
                check_every: Duration::from_secs(20),
                interval: Duration::from_secs(60),
                minimum: ANY,
                fee: "minimum".into(),
                fee_max: None,
                fee_payer: FeePayer::Pool,
                batch_size: 1,
                batches: 1,
                compound_above: 0,
                compound_open: 1,
                compound_every: Duration::from_secs(5),
            },
            network: Network::Development,
            chain_id: test_support::CHAIN,
            pool_address: test_support::address(1),
            request_prefix: "public-test-stable-".into(),
            heights,
            shutdown,
            unresolved: Default::default(),
        },
        thread,
    )
}

async fn assert_reserved(service: &Payouts, id: i64, request_id: &str) {
    let open = service.reader.read(read::open_payouts).await.unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].id, id);
    assert_eq!(open[0].request_id, request_id);
    assert_eq!(open[0].status, PayoutStatus::Prepared);
    let balance = service
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
        service
            .reader
            .read(|connection| read::payable(connection, ANY, 10))
            .await
            .unwrap()
            .is_empty(),
        "a later round must not allocate a second payment"
    );
}

#[tokio::test]
async fn prepared_payment_survives_every_rpc_error_and_reopen_without_recrediting() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let mut replies = vec![
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            3 * 1024 * 1024
        )
        .into_bytes(),
        reply("401 Unauthorized", b""),
    ];
    // These describe this call, never the absence of an earlier payment under the same ID.
    for code in [
        -32000, -32601, -32602, -32004, -32001, -32002, -32003, -32005,
    ] {
        replies.push(reply(
            "200 OK",
            &serde_json::to_vec(
                &json!({"jsonrpc":"2.0","error":{"code":code,"message":"public-test-error"}}),
            )
            .unwrap(),
        ));
    }
    replies.push(reply("200 OK", b"not JSON"));
    replies.push(reply("200 OK", b""));
    replies.push(Vec::new()); // Actual transport close without an acknowledgement.
    let errors = replies.len();
    let transaction = "ab".repeat(48);
    replies.push(reply("200 OK", &serde_json::to_vec(&json!({"jsonrpc":"2.0","result":{"transaction":transaction,"fee":"0.00000010","valid_until":123,"duplicate":true}})).unwrap()));
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        let calls = replies.len() * 2;
        let mut replies = replies.into_iter();
        for _ in 0..calls {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut input = Vec::new();
            let mut buffer = [0; 4096];
            loop {
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                input.extend_from_slice(&buffer[..count]);
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
                        requests.push(
                            serde_json::from_slice::<Value>(&input[end + 4..end + 4 + length])
                                .unwrap(),
                        );
                        break;
                    }
                }
            }
            let bytes = if requests.last().unwrap()["method"] == "status" {
                reply("200 OK", &serde_json::to_vec(&json!({"jsonrpc":"2.0","result":{
                    "network":"development","chain_id":test_support::CHAIN.to_hex(),"live":true,
                    "outputs":2,"pending_sends":0,"durable_request_ids":true,"account":0,"request_archive_id":"69".repeat(32)}})).unwrap())
            } else {
                assert_eq!(requests.last().unwrap()["method"], "send_many");
                replies.next().unwrap()
            };
            let _ = socket.write_all(&bytes).await;
            let _ = socket.shutdown().await;
        }
        requests
    });
    let path = test_support::temp_path("prepared-rpc-errors").with_extension("sqlite");
    let (mut payouts, mut writer_thread) = service(&path, base.clone(), true).await;
    let open = payouts.reader.read(read::open_payouts).await.unwrap();
    let id = open[0].id;
    let request_id = open[0].request_id.clone();
    let recipients = [(test_support::address(2), RESERVED)];
    for index in 0..errors {
        assert_eq!(
            payouts.send(id, &request_id, &recipients, &[1]).await,
            Sending::Unknown,
            "reply {index} must not release the original reservation"
        );
        assert_reserved(&payouts, id, &request_id).await;
        if index == 0 {
            drop(payouts);
            writer_thread.join().unwrap();
            (payouts, writer_thread) = service(&path, base.clone(), false).await;
            assert_reserved(&payouts, id, &request_id).await;
        }
    }
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Sent
    );
    let open = payouts.reader.read(read::open_payouts).await.unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].id, id);
    assert_eq!(open[0].request_id, request_id);
    assert_eq!(open[0].status, PayoutStatus::Sent);
    assert_eq!(
        open[0].transaction_id.as_deref(),
        Some(transaction.as_str())
    );
    assert_eq!(
        payouts
            .reader
            .read(|connection| connection
                .query_row("SELECT COUNT(*) FROM payouts", [], |row| row
                    .get::<_, i64>(0)))
            .await
            .unwrap(),
        1
    );
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), (errors + 1) * 2);
    let requests: Vec<_> = requests
        .iter()
        .filter(|request| request["method"] == "send_many")
        .collect();
    assert_eq!(requests.len(), errors + 1);
    let mut original_params = requests[0]["params"].clone();
    original_params
        .as_object_mut()
        .unwrap()
        .remove("recovery_only");
    for (index, request) in requests.iter().enumerate() {
        assert_eq!(request["method"], "send_many");
        assert_eq!(request["params"]["recovery_only"], index != 0);
        let mut parameters = request["params"].clone();
        parameters.as_object_mut().unwrap().remove("recovery_only");
        assert_eq!(parameters, original_params);
        assert_eq!(request["params"]["request_id"], request_id);
    }
    drop(payouts);
    writer_thread.join().unwrap();
    // Production checkpoint readers retain these task-only temporary files until process exit.
}
