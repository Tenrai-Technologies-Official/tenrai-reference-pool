use super::{choose_pool_address, passing, wait_for_mining_rates};
use crate::node::{NodeClient, NodeError};
use axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::get};
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

const DAEMON: &str = "dtrai1daemon";
const OTHER: &str = "dtrai1other";

#[test]
fn the_daemon_address_wins_and_must_match_what_is_known() {
    assert_eq!(
        choose_pool_address(None, Some(DAEMON), None).unwrap(),
        DAEMON
    );
    assert_eq!(
        choose_pool_address(Some(DAEMON), Some(DAEMON), Some(DAEMON)).unwrap(),
        DAEMON
    );
    let given = choose_pool_address(Some(OTHER), Some(DAEMON), None).unwrap_err();
    assert!(given.to_string().contains("--pool-address"), "{given}");
    let stored = choose_pool_address(None, Some(DAEMON), Some(OTHER)).unwrap_err();
    assert!(
        stored.to_string().contains("this database belongs"),
        "{stored}"
    );
}

#[test]
fn without_the_daemon_the_given_or_stored_address_serves() {
    assert_eq!(
        choose_pool_address(Some(DAEMON), None, None).unwrap(),
        DAEMON
    );
    assert_eq!(
        choose_pool_address(None, None, Some(DAEMON)).unwrap(),
        DAEMON
    );
    assert!(choose_pool_address(Some(OTHER), None, Some(DAEMON)).is_err());
    let missing = choose_pool_address(None, None, None).unwrap_err();
    assert!(missing.to_string().contains("--pool-address"), "{missing}");
}

#[tokio::test]
async fn a_node_that_is_busy_at_the_start_is_asked_again() {
    let asked = Arc::new(AtomicU32::new(0));
    let counter = Arc::clone(&asked);
    let router = Router::new().route(
        "/v1/network/mining",
        get(move || {
            let counter = Arc::clone(&counter);
            async move {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    let busy = serde_json::json!({"code": "unavailable", "message": "the node is busy; retry shortly"});
                    return (StatusCode::SERVICE_UNAVAILABLE, Json(busy)).into_response();
                }
                Json(serde_json::json!({"work_blocks_per_second": 50, "target_cut_interval_millis": 2500})).into_response()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url: url::Url = format!("http://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    let node = NodeClient::new(url.clone(), url).unwrap();
    let rates = wait_for_mining_rates(&node).await.unwrap();
    assert_eq!(rates.work_blocks_per_second, 50);
    assert_eq!(asked.load(Ordering::SeqCst), 2);
}

#[test]
fn only_outages_and_busy_answers_are_asked_again() {
    let status = |status: StatusCode| NodeError::Status {
        status,
        message: String::new(),
    };
    assert!(passing(&status(StatusCode::SERVICE_UNAVAILABLE)));
    assert!(passing(&status(StatusCode::TOO_MANY_REQUESTS)));
    assert!(passing(&NodeError::Busy));
    assert!(!passing(&status(StatusCode::NOT_FOUND)));
    assert!(!passing(&NodeError::Answer("not JSON".into())));
}
