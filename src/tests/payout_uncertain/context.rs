use super::*;
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::Response,
};
use libcrux_ml_dsa::ml_dsa_65::{self, MLDSA65SigningKey};
use sha2::{Digest, Sha384};
use sha3::{
    Shake256,
    digest::{ExtendableOutput, Update, XofReader},
};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy)]
enum Scenario {
    RebuiltBehind,
    UnknownExpired,
    HistoryUnavailable,
    LaggingStatus,
    RolledBack,
    ChangedSameHeight,
    FutureFinal,
    FinalWithoutCut,
    BrokenSignature,
    Finalized,
    AppliedFailure,
}

struct FixtureState {
    scenario: Scenario,
    heads: usize,
    sends: Vec<String>,
    requests: Vec<String>,
}

struct Backend {
    state: Mutex<FixtureState>,
    signing: MLDSA65SigningKey,
    public_key: [u8; 1952],
    identity: [u8; 32],
}

fn backend() -> Arc<Backend> {
    let mut signing = MLDSA65SigningKey::zero();
    let mut public_key = [0; 1952];
    ml_dsa_65::portable::generate_key_pair_mut([0x69; 32], signing.as_ref_mut(), &mut public_key);
    let mut identity_hash = Shake256::default();
    Update::update(&mut identity_hash, b"TRAI/P2P/MLDSA65/NODE-ID/1");
    Update::update(&mut identity_hash, &public_key);
    let mut identity = [0; 32];
    identity_hash.finalize_xof().read(&mut identity);
    Arc::new(Backend {
        state: Mutex::new(FixtureState {
            scenario: Scenario::UnknownExpired,
            heads: 0,
            sends: Vec::new(),
            requests: Vec::new(),
        }),
        signing,
        public_key,
        identity,
    })
}

fn scenario(state: &Arc<Backend>, scenario: Scenario) {
    let mut state = state.state.lock().unwrap();
    state.scenario = scenario;
    state.heads = 0;
}

fn canonical_head(height: u64, marker: u8) -> Value {
    json!({"chain_id":test_support::CHAIN.to_hex(),"height":height,
        "cut_id":format!("{marker:02x}").repeat(48),"state_root":"22".repeat(48),
        "validator_set_id":"33".repeat(48),"params_hash":"44".repeat(48)})
}

#[allow(clippy::too_many_arguments)]
fn signed_reply(
    backend: &Backend,
    request_headers: &HeaderMap,
    method: &str,
    target: &str,
    request_body: &[u8],
    status: StatusCode,
    document: Value,
    served_height: u64,
    damaged: bool,
) -> Response {
    let bytes = serde_json::to_vec(&document).unwrap();
    let mut response = Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(bytes.clone()))
        .unwrap();
    if let Some(nonce) = request_headers.get("x-tenrai-request-nonce") {
        let nonce: [u8; 32] = hex::decode(nonce.to_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        // Actual published response-envelope encoding, independently signed by the fixture key.
        let mut message = b"TRAI/API/ENVELOPE/1".to_vec();
        message.extend_from_slice(&1_u16.to_le_bytes());
        message.extend_from_slice(&test_support::CHAIN.0);
        message.push(u8::try_from(method.len()).unwrap());
        message.extend_from_slice(method.as_bytes());
        message.extend_from_slice(&u16::try_from(target.len()).unwrap().to_le_bytes());
        message.extend_from_slice(target.as_bytes());
        message.extend_from_slice(&Sha384::digest(request_body));
        message.extend_from_slice(&status.as_u16().to_le_bytes());
        message.extend_from_slice(&Sha384::digest(&bytes));
        message.extend_from_slice(&backend.identity);
        message.extend_from_slice(&nonce);
        message.extend_from_slice(&served_height.to_le_bytes());
        let signature = ml_dsa_65::portable::sign(
            &backend.signing,
            &message,
            b"TRAI/API/RESPONSE/1",
            [0x70; 32],
        )
        .unwrap();
        let mut signature = signature.as_ref().to_vec();
        if damaged {
            signature[0] ^= 1;
        }
        for (name, value) in [
            ("x-tenrai-node-identity", hex::encode(backend.identity)),
            ("x-tenrai-request-nonce", hex::encode(nonce)),
            ("x-tenrai-tip-height", served_height.to_string()),
            ("x-tenrai-signature", hex::encode(signature)),
            ("x-tenrai-envelope-version", "1".into()),
        ] {
            response
                .headers_mut()
                .insert(name, HeaderValue::from_str(&value).unwrap());
        }
    }
    response
}

async fn respond(State(backend): State<Arc<Backend>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let bytes = to_bytes(body, 65_536).await.unwrap();
    let path = parts.uri.path();
    let (status, document, served, damaged) = {
        let mut state = backend.state.lock().unwrap();
        state.requests.push(path.into());
        let height = if matches!(state.scenario, Scenario::RebuiltBehind) {
            5
        } else {
            200
        };
        if path == "/v1/node/identity" {
            (
                StatusCode::OK,
                json!({"api_version":1,"network":"development","chain_id":test_support::CHAIN.to_hex(),
                "identity":hex::encode(backend.identity),"public_key":hex::encode(backend.public_key),"key_scheme":"mldsa65"}),
                height,
                false,
            )
        } else if path == "/v1/chain/head" {
            let call = state.heads;
            state.heads += 1;
            let height = if call > 0 && matches!(state.scenario, Scenario::RolledBack) {
                190
            } else {
                height
            };
            let marker = if call > 0 && matches!(state.scenario, Scenario::ChangedSameHeight) {
                0x12
            } else {
                0x11
            };
            (
                StatusCode::OK,
                canonical_head(height, marker),
                height,
                false,
            )
        } else if path.starts_with("/v1/transactions/") && path.ends_with("/status") {
            match state.scenario {
                Scenario::LaggingStatus => (
                    StatusCode::OK,
                    json!({"lifecycle":"finalized","finalized_cut":{"height":3},"failure":null}),
                    5,
                    false,
                ),
                Scenario::RolledBack => (
                    StatusCode::OK,
                    json!({"lifecycle":"failed","finalized_cut":{"height":190},"failure":"conflict"}),
                    height,
                    false,
                ),
                Scenario::ChangedSameHeight => (
                    StatusCode::OK,
                    json!({"lifecycle":"finalized","finalized_cut":{"height":70},"failure":null}),
                    height,
                    false,
                ),
                Scenario::HistoryUnavailable => (
                    StatusCode::NOT_FOUND,
                    json!({"code":"not_found","message":"history unavailable","reason":"history_not_held"}),
                    height,
                    false,
                ),
                Scenario::Finalized
                | Scenario::BrokenSignature
                | Scenario::FutureFinal
                | Scenario::FinalWithoutCut => {
                    let cut = match state.scenario {
                        Scenario::FinalWithoutCut => Value::Null,
                        Scenario::FutureFinal => json!({"height":201}),
                        _ => json!({"height":70}),
                    };
                    (
                        StatusCode::OK,
                        json!({"lifecycle":"finalized","finalized_cut":cut,"failure":null}),
                        height,
                        matches!(state.scenario, Scenario::BrokenSignature),
                    )
                }
                Scenario::AppliedFailure => (
                    StatusCode::OK,
                    json!({"lifecycle":"failed","finalized_cut":{"height":190},"failure":"conflict"}),
                    height,
                    false,
                ),
                _ => (
                    StatusCode::NOT_FOUND,
                    json!({"code":"not_found","message":"unknown"}),
                    height,
                    false,
                ),
            }
        } else if path == "/v1/transactions/manifest" {
            (
                StatusCode::OK,
                json!({"transaction_limits":{"max_inputs":8,"max_outputs":8},"max_validity_cuts":1800}),
                height,
                false,
            )
        } else if path == "/v1/storage" {
            // Retain this tempting full-history answer: old expiry logic used it to recredit.
            (
                StatusCode::OK,
                json!({"body_history_floor":0}),
                height,
                false,
            )
        } else if path == "/v1/rpc" {
            let request: Value = serde_json::from_slice(&bytes).unwrap();
            let result = match request["method"].as_str().unwrap() {
                "status" => json!({"network":"development","chain_id":test_support::CHAIN.to_hex(),
                    "live":true,"outputs":2,"pending_sends":0,"durable_request_ids":true,"account":0,"request_archive_id":"69".repeat(32)}),
                "balance" => json!({"balance":"10.00000000","available":"10.00000000","outputs":2}),
                "outputs" => json!({"outputs":[{"amount":"10.00000000","reserved":false}]}),
                "quote_send_many" => json!({"unsigned":true,"submitted":false,"fee":"0.00000010"}),
                "send_many" => {
                    if request["params"]["dry_run"] != true {
                        state
                            .sends
                            .push(request["params"]["request_id"].as_str().unwrap().into());
                    }
                    json!({"transaction":"cd".repeat(48),"fee":"0.00000010","valid_until":400})
                }
                method => panic!("unexpected wallet method {method}"),
            };
            (
                StatusCode::OK,
                json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
                height,
                false,
            )
        } else {
            panic!("unexpected fixture request {path}");
        }
    };
    let target = parts.uri.path_and_query().unwrap().as_str();
    signed_reply(
        &backend,
        &parts.headers,
        parts.method.as_str(),
        target,
        &bytes,
        status,
        document,
        served,
        damaged,
    )
}

async fn serving() -> (Arc<Backend>, Url, tokio::task::JoinHandle<()>) {
    let backend = backend();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let app = Router::new()
        .fallback(respond)
        .with_state(Arc::clone(&backend));
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (backend, base, task)
}

async fn sent_service(
    path: &Path,
    base: Url,
    fresh: bool,
    pinned: bool,
    backend: &Arc<Backend>,
) -> (Payouts, std::thread::JoinHandle<()>) {
    let (mut payouts, writer) = service(path, base.clone(), fresh).await;
    if fresh {
        let original = payouts.reader.read(read::open_payouts).await.unwrap();
        payouts
            .writer
            .call(|done| Op::PayoutSent {
                id: original[0].id,
                transaction: "ab".repeat(48),
                fee: 10,
                valid_until: Some(100),
                fee_shares: Vec::new(),
                at: 2,
                done,
            })
            .await
            .unwrap();
    }
    let (_, cached_ahead) = watch::channel(300);
    payouts.heights = cached_ahead;
    if pinned {
        payouts
            .node
            .establish_trust(test_support::CHAIN, backend.identity)
            .await
            .unwrap();
    }
    (payouts, writer)
}

async fn still_reserved(payouts: &Payouts, request_id: &str) {
    let open = payouts.reader.read(read::open_payouts).await.unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].status, PayoutStatus::Sent);
    assert_eq!(open[0].request_id, request_id);
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
}

#[tokio::test]
async fn unknown_expiry_backend_rollback_and_missing_history_never_recredit_or_create_a_new_id() {
    for pinned in [false, true] {
        let (backend, base, task) = serving().await;
        let path = test_support::temp_path("payout-fresh-context").with_extension("sqlite");
        let (mut payouts, mut writer) =
            sent_service(&path, base.clone(), true, pinned, &backend).await;
        let original = payouts.reader.read(read::open_payouts).await.unwrap();
        let request_id = original[0].request_id.clone();
        let id = original[0].id;
        for next in [
            Scenario::RebuiltBehind,
            Scenario::UnknownExpired,
            Scenario::HistoryUnavailable,
            Scenario::LaggingStatus,
            Scenario::RolledBack,
            Scenario::ChangedSameHeight,
            Scenario::FutureFinal,
            Scenario::FinalWithoutCut,
        ] {
            if !pinned && matches!(next, Scenario::LaggingStatus) {
                continue; // An unsigned local status has no authenticated tip to compare.
            }
            scenario(&backend, next);
            if matches!(next, Scenario::HistoryUnavailable) {
                assert!(
                    payouts.node.payout_status(&"ab".repeat(48)).await.is_err(),
                    "missing history is not converted to a complete unknown status"
                );
            }
            if pinned && matches!(next, Scenario::LaggingStatus) {
                assert_eq!(
                    payouts
                        .node
                        .payout_status(&"ab".repeat(48))
                        .await
                        .unwrap()
                        .authenticated_height,
                    Some(5)
                );
            }
            payouts.follow(id, &"ab".repeat(48), Some(100)).await;
            still_reserved(&payouts, &request_id).await;
            payouts.round().await.unwrap();
            still_reserved(&payouts, &request_id).await;
            assert!(backend.state.lock().unwrap().sends.is_empty());
        }
        drop(payouts);
        writer.join().unwrap();
        (payouts, writer) = sent_service(&path, base.clone(), false, pinned, &backend).await;
        still_reserved(&payouts, &request_id).await;
        if pinned {
            scenario(&backend, Scenario::BrokenSignature);
            payouts.follow(id, &"ab".repeat(48), Some(100)).await;
            still_reserved(&payouts, &request_id).await;
        }
        scenario(&backend, Scenario::Finalized);
        payouts.follow(id, &"ab".repeat(48), Some(100)).await;
        payouts.follow(id, &"ab".repeat(48), Some(100)).await;
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
            (0, 0, u128::from(RESERVED), u128::from(RESERVED))
        );
        assert!(payouts.unresolved.lock().unwrap().is_empty());
        assert!(backend.state.lock().unwrap().sends.is_empty());
        drop(payouts);
        writer.join().unwrap();
        let (payouts, writer) = sent_service(&path, base, false, pinned, &backend).await;
        assert!(
            payouts
                .reader
                .read(read::open_payouts)
                .await
                .unwrap()
                .is_empty()
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
            (0, 0, u128::from(RESERVED), u128::from(RESERVED))
        );
        drop(payouts);
        writer.join().unwrap();
        task.abort();
        let _ = task.await;
    }
}

#[tokio::test]
async fn a_context_bound_terminal_failure_returns_credit_once_and_allows_one_new_payment() {
    let (backend, base, task) = serving().await;
    let path = test_support::temp_path("payout-terminal-failure-context").with_extension("sqlite");
    let (payouts, writer) = sent_service(&path, base, true, true, &backend).await;
    let original = payouts.reader.read(read::open_payouts).await.unwrap();
    let id = original[0].id;
    let request_id = original[0].request_id.clone();
    scenario(&backend, Scenario::AppliedFailure);
    payouts.follow(id, &"ab".repeat(48), Some(100)).await;
    payouts.follow(id, &"ab".repeat(48), Some(100)).await;
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
        (RESERVED, 0, 0, u128::from(RESERVED))
    );
    payouts.round().await.unwrap();
    let open = payouts.reader.read(read::open_payouts).await.unwrap();
    assert_eq!(open.len(), 1);
    assert_ne!(open[0].request_id, request_id);
    assert_eq!(open[0].status, PayoutStatus::Sent);
    assert_eq!(
        backend.state.lock().unwrap().sends.as_slice(),
        [open[0].request_id.clone()]
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
    drop(payouts);
    writer.join().unwrap();
    task.abort();
    let _ = task.await;
}

#[path = "bounds.rs"]
mod bounds_tests;
