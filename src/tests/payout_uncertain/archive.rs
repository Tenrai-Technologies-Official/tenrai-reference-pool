use super::*;
use rusqlite::OptionalExtension;
use std::sync::{Arc, Mutex};

struct Daemon {
    archive: String,
    account: u32,
    chain: String,
    lose_ack: bool,
    calls: Vec<Value>,
}

async fn serving_archive(
    pool: &Path,
    journal: &Path,
) -> (Url, Arc<Mutex<Daemon>>, tokio::task::JoinHandle<()>) {
    let authority = Connection::open(journal).unwrap();
    authority
        .pragma_update(None, "synchronous", "FULL")
        .unwrap();
    authority.execute_batch("CREATE TABLE receipts(archive_id TEXT,account INTEGER,request_id TEXT,transaction_id TEXT,PRIMARY KEY(archive_id,account,request_id)); CREATE TABLE effects(request_id TEXT NOT NULL);").unwrap();
    drop(authority);
    let daemon = Arc::new(Mutex::new(Daemon {
        archive: "69".repeat(32),
        account: 0,
        chain: test_support::CHAIN.to_hex(),
        lose_ack: true,
        calls: Vec::new(),
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server_state = Arc::clone(&daemon);
    let pool = pool.to_owned();
    let journal = journal.to_owned();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_rpc(&mut socket).await;
            let response = {
                let mut daemon = server_state.lock().unwrap();
                let result = match request["method"].as_str().unwrap() {
                    "status" => Some(Ok(json!({"network":"development","chain_id":daemon.chain,
                        "live":true,"outputs":2,"pending_sends":0,"durable_request_ids":true,
                        "request_archive_id":daemon.archive,"account":daemon.account}))),
                    "send_many" => {
                        let parameters = &request["params"];
                        assert_eq!(parameters["dry_run"], false);
                        let request_id = parameters["request_id"].as_str().unwrap();
                        let durable = db::connect(&pool, false).unwrap();
                        let (attempted, archive, account, chain): (bool, String, u32, String) = durable.query_row(
                            "SELECT a.attempted,a.archive_id,a.account,a.chain_id FROM payout_attempts a JOIN payouts p ON p.id=a.payout_id WHERE p.request_id=?1",
                            [request_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                        ).unwrap();
                        assert!(
                            attempted,
                            "a committed attempt intent must be visible BEFORE the RPC POST"
                        );
                        assert_eq!(parameters["expected_request_archive_id"], archive);
                        assert_eq!(parameters["expected_account"], account);
                        assert_eq!(parameters["expected_chain_id"], chain);
                        if parameters["expected_request_archive_id"] != daemon.archive
                            || parameters["expected_account"] != daemon.account
                            || parameters["expected_chain_id"] != daemon.chain
                        {
                            Some(Err("request scope mismatch"))
                        } else {
                            let mut authority = Connection::open(&journal).unwrap();
                            authority
                                .pragma_update(None, "synchronous", "FULL")
                                .unwrap();
                            let transaction = authority.transaction().unwrap();
                            let existing: Option<String> = transaction.query_row(
                                "SELECT transaction_id FROM receipts WHERE archive_id=?1 AND account=?2 AND request_id=?3",
                                params![daemon.archive, daemon.account, request_id], |row| row.get(0),
                            ).optional().unwrap();
                            if existing.is_none() && parameters["recovery_only"] == true {
                                Some(Err(
                                    "the original request is absent; retain its reservation",
                                ))
                            } else {
                                let duplicate = existing.is_some();
                                let tx = existing.unwrap_or_else(|| "ab".repeat(48));
                                if !duplicate {
                                    transaction
                                        .execute(
                                            "INSERT INTO receipts VALUES(?1,?2,?3,?4)",
                                            params![daemon.archive, daemon.account, request_id, tx],
                                        )
                                        .unwrap();
                                    transaction
                                        .execute("INSERT INTO effects VALUES(?1)", [request_id])
                                        .unwrap();
                                }
                                transaction.commit().unwrap();
                                if daemon.lose_ack {
                                    daemon.lose_ack = false;
                                    None // Close a real TCP connection after committing the monetary effect.
                                } else {
                                    Some(Ok(
                                        json!({"transaction":tx,"fee":"0.00000010","valid_until":123,"duplicate":duplicate}),
                                    ))
                                }
                            }
                        }
                    }
                    method => panic!("unexpected fixture RPC {method}"),
                };
                daemon.calls.push(request.clone());
                result.map(|result| match result {
                    Ok(result) => json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
                    Err(message) => json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32002,"message":message}}),
                })
            };
            if let Some(document) = response {
                let bytes = reply("200 OK", &serde_json::to_vec(&document).unwrap());
                socket.write_all(&bytes).await.unwrap();
                socket.shutdown().await.unwrap();
            }
        }
    });
    (base, daemon, task)
}

fn sends(daemon: &Arc<Mutex<Daemon>>) -> Vec<Value> {
    daemon
        .lock()
        .unwrap()
        .calls
        .iter()
        .filter(|call| call["method"] == "send_many")
        .map(|call| call["params"].clone())
        .collect()
}

fn effects(journal: &Path) -> i64 {
    Connection::open(journal)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM effects", [], |row| row.get(0))
        .unwrap()
}

#[tokio::test]
async fn archive_swap_and_same_id_rollback_never_replace_a_lost_ack_payout_after_reopen() {
    let path = test_support::temp_path("archive-bound-pool").with_extension("sqlite");
    let journal = test_support::temp_path("archive-bound-daemon").with_extension("sqlite");
    let (base, daemon, server) = serving_archive(&path, &journal).await;
    let (payouts, writer) = service(&path, base.clone(), true).await;
    let original = payouts.reader.read(read::open_payouts).await.unwrap();
    let id = original[0].id;
    let request_id = original[0].request_id.clone();
    let recipients = [(test_support::address(2), RESERVED)];
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Unknown
    );
    assert_eq!(effects(&journal), 1);
    assert_eq!(sends(&daemon).len(), 1);
    assert_eq!(sends(&daemon)[0]["recovery_only"], false);
    assert_reserved(&payouts, id, &request_id).await;
    drop(payouts);
    writer.join().unwrap();
    let (payouts, writer) = service(&path, base.clone(), false).await;
    // Both backends truthfully advertise permanent IDs; the new empty journal is foreign.
    daemon.lock().unwrap().archive = "70".repeat(32);
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Unknown
    );
    assert_eq!(
        sends(&daemon).len(),
        1,
        "no POST may cross a persisted archive binding"
    );
    daemon.lock().unwrap().archive = "69".repeat(32);
    daemon.lock().unwrap().account = 1;
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Unknown
    );
    assert_eq!(sends(&daemon).len(), 1);
    daemon.lock().unwrap().account = 0;
    daemon.lock().unwrap().chain = "71".repeat(48);
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Unknown
    );
    assert_eq!(sends(&daemon).len(), 1);
    daemon.lock().unwrap().chain = test_support::CHAIN.to_hex();
    // Restore the same archive ID to a point before this request; effects is the independent oracle.
    Connection::open(&journal)
        .unwrap()
        .execute("DELETE FROM receipts", [])
        .unwrap();
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Unknown
    );
    assert_eq!(effects(&journal), 1);
    let attempted = sends(&daemon);
    assert_eq!(attempted.len(), 2);
    assert_eq!(attempted[1]["recovery_only"], true);
    let mut first = attempted[0].clone();
    let mut retry = attempted[1].clone();
    first.as_object_mut().unwrap().remove("recovery_only");
    retry.as_object_mut().unwrap().remove("recovery_only");
    assert_eq!(
        first, retry,
        "only the recovery mode changes; scope, RID and payment stay immutable"
    );
    assert_reserved(&payouts, id, &request_id).await;
    drop(payouts);
    writer.join().unwrap();
    let (payouts, writer) = service(&path, base, false).await;
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Unknown
    );
    assert_eq!(effects(&journal), 1);
    assert_reserved(&payouts, id, &request_id).await;
    // Restoring the real original receipt permits recovery, never a second effect.
    Connection::open(&journal)
        .unwrap()
        .execute(
            "INSERT INTO receipts VALUES(?1,0,?2,?3)",
            params!["69".repeat(32), request_id, "ab".repeat(48)],
        )
        .unwrap();
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Sent
    );
    assert_eq!(effects(&journal), 1);
    assert!(
        sends(&daemon)
            .iter()
            .skip(1)
            .all(|params| params["recovery_only"] == true)
    );
    let open = payouts.reader.read(read::open_payouts).await.unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].request_id, request_id);
    assert_eq!(open[0].status, PayoutStatus::Sent);
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
    drop(payouts);
    writer.join().unwrap();
    server.abort();
}

#[tokio::test]
async fn crash_after_intent_before_post_and_unbound_old_rows_remain_reserved_without_new_payments()
{
    let path = test_support::temp_path("intent-before-post").with_extension("sqlite");
    let journal = test_support::temp_path("intent-empty-daemon").with_extension("sqlite");
    let (base, daemon, server) = serving_archive(&path, &journal).await;
    let (payouts, writer) = service(&path, base.clone(), true).await;
    let original = payouts.reader.read(read::open_payouts).await.unwrap();
    let (id, request_id) = (original[0].id, original[0].request_id.clone());
    let scope = payouts
        .wallet
        .payment_scope(payouts.network, payouts.chain_id)
        .await
        .unwrap();
    assert!(
        !payouts
            .writer
            .call(|done| Op::PayoutAttempt {
                id,
                request_id: request_id.clone(),
                scope,
                done,
            })
            .await
            .unwrap()
    );
    assert!(sends(&daemon).is_empty());
    drop(payouts);
    writer.join().unwrap();
    let (payouts, writer) = service(&path, base.clone(), false).await;
    let recipients = [(test_support::address(2), RESERVED)];
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Unknown
    );
    assert_eq!(
        effects(&journal),
        0,
        "a missing recovery cannot plan an initial payment"
    );
    assert_eq!(sends(&daemon).len(), 1);
    assert_eq!(sends(&daemon)[0]["recovery_only"], true);
    assert_reserved(&payouts, id, &request_id).await;
    // A pre-upgrade Prepared row has no marker. It cannot be silently attached to any new archive.
    db::connect(&path, false)
        .unwrap()
        .execute("DELETE FROM payout_attempts WHERE payout_id=?1", [id])
        .unwrap();
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Unknown
    );
    assert_eq!(sends(&daemon).len(), 1);
    assert_eq!(effects(&journal), 0);
    assert_reserved(&payouts, id, &request_id).await;
    drop(payouts);
    writer.join().unwrap();
    let (payouts, writer) = service(&path, base, false).await;
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Unknown
    );
    assert_eq!(sends(&daemon).len(), 1);
    assert_reserved(&payouts, id, &request_id).await;
    drop(payouts);
    writer.join().unwrap();
    server.abort();
}

#[tokio::test]
async fn failed_intent_commit_never_posts_and_a_real_success_persists_before_the_first_post() {
    let path = test_support::temp_path("intent-commit-failure").with_extension("sqlite");
    let journal = test_support::temp_path("intent-commit-daemon").with_extension("sqlite");
    let (base, daemon, server) = serving_archive(&path, &journal).await;
    daemon.lock().unwrap().lose_ack = false;
    let (payouts, writer) = service(&path, base, true).await;
    let original = payouts.reader.read(read::open_payouts).await.unwrap();
    let (id, request_id) = (original[0].id, original[0].request_id.clone());
    db::connect(&path, false).unwrap().execute_batch(
        "CREATE TRIGGER fail_attempt BEFORE UPDATE ON payout_attempts BEGIN SELECT RAISE(ABORT,'controlled durable intent failure'); END;"
    ).unwrap();
    let recipients = [(test_support::address(2), RESERVED)];
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Unknown
    );
    assert!(sends(&daemon).is_empty());
    assert_eq!(effects(&journal), 0);
    assert_reserved(&payouts, id, &request_id).await;
    let attempted: bool = payouts
        .reader
        .read(move |connection| {
            connection.query_row(
                "SELECT attempted FROM payout_attempts WHERE payout_id=?1",
                [id],
                |row| row.get(0),
            )
        })
        .await
        .unwrap();
    assert!(!attempted);
    db::connect(&path, false)
        .unwrap()
        .execute_batch("DROP TRIGGER fail_attempt")
        .unwrap();
    // RAISE(IGNORE) is not an SQL error: it returns Ok(0). An acknowledged intent
    // still requires exactly one changed row before a POST can be authorized.
    db::connect(&path, false).unwrap().execute_batch(
        "CREATE TRIGGER ignore_attempt BEFORE UPDATE ON payout_attempts BEGIN SELECT RAISE(IGNORE); END;"
    ).unwrap();
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Unknown
    );
    assert!(sends(&daemon).is_empty());
    assert_eq!(effects(&journal), 0);
    assert_reserved(&payouts, id, &request_id).await;
    let attempted: bool = payouts
        .reader
        .read(move |connection| {
            connection.query_row(
                "SELECT attempted FROM payout_attempts WHERE payout_id=?1",
                [id],
                |row| row.get(0),
            )
        })
        .await
        .unwrap();
    assert!(!attempted);
    db::connect(&path, false)
        .unwrap()
        .execute_batch("DROP TRIGGER ignore_attempt")
        .unwrap();
    assert_eq!(
        payouts.send(id, &request_id, &recipients, &[1]).await,
        Sending::Sent
    );
    assert_eq!(effects(&journal), 1);
    assert_eq!(sends(&daemon).len(), 1);
    assert_eq!(sends(&daemon)[0]["recovery_only"], false);
    drop(payouts);
    writer.join().unwrap();
    server.abort();
}

#[tokio::test]
async fn overflowing_or_unstorable_recipient_totals_cannot_post_or_mark_attempted() {
    let path = test_support::temp_path("payout-total-overflow").with_extension("sqlite");
    let journal = test_support::temp_path("payout-total-daemon").with_extension("sqlite");
    let (base, daemon, server) = serving_archive(&path, &journal).await;
    daemon.lock().unwrap().lose_ack = false;
    let (payouts, writer) = service(&path, base, true).await;
    let original = payouts.reader.read(read::open_payouts).await.unwrap();
    let (id, request_id) = (original[0].id, original[0].request_id.clone());
    for amounts in [vec![u64::MAX, 1], vec![i64::MAX as u64 + 1]] {
        let recipients: Vec<_> = amounts
            .into_iter()
            .map(|amount| (test_support::address(2), amount))
            .collect();
        assert_eq!(
            payouts.send(id, &request_id, &recipients, &[1]).await,
            Sending::Unknown
        );
        assert!(
            daemon.lock().unwrap().calls.is_empty(),
            "invalid total is refused before status or intent"
        );
        assert_eq!(effects(&journal), 0);
        assert_reserved(&payouts, id, &request_id).await;
        let attempted: bool = payouts
            .reader
            .read(move |connection| {
                connection.query_row(
                    "SELECT attempted FROM payout_attempts WHERE payout_id=?1",
                    [id],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert!(!attempted);
    }
    assert_eq!(
        payouts
            .send(
                id,
                &request_id,
                &[(test_support::address(2), RESERVED)],
                &[1]
            )
            .await,
        Sending::Sent
    );
    assert_eq!(effects(&journal), 1);
    assert_eq!(sends(&daemon).len(), 1);
    drop(payouts);
    writer.join().unwrap();
    server.abort();
}
