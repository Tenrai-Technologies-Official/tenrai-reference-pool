use super::*;
use axum::{Json, Router, extract::State, response::IntoResponse, routing::get};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

const CHAIN: Hash384 = Hash384([9; 48]);
struct Scenario {
    floors: [u64; 2],
    heights: [u64; 2],
    wrong_chain: bool,
    changed_tip: bool,
    positive: bool,
    pooled: bool,
    typed: bool,
    floor_calls: AtomicUsize,
    head_calls: AtomicUsize,
}
impl Default for Scenario {
    fn default() -> Self {
        Self {
            floors: [0, 0],
            heights: [110, 110],
            wrong_chain: false,
            changed_tip: false,
            positive: false,
            pooled: false,
            typed: true,
            floor_calls: AtomicUsize::new(0),
            head_calls: AtomicUsize::new(0),
        }
    }
}
struct Service {
    client: NodeClient,
    stop: oneshot::Sender<()>,
    server: JoinHandle<()>,
}
impl Service {
    async fn close(self) {
        let Self {
            client,
            stop,
            server,
        } = self;
        drop(client);
        let _ = stop.send(());
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
    }
}
async fn service(scenario: Scenario) -> Service {
    let app = Router::new()
        .route("/v1/chain/head", get(head))
        .route("/v1/storage", get(storage))
        .route("/v1/blocks/{id}", get(block))
        .with_state(Arc::new(scenario));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    Service {
        client: NodeClient::new(url.clone(), url).unwrap(),
        stop,
        server,
    }
}
async fn head(State(state): State<Arc<Scenario>>) -> Json<serde_json::Value> {
    let step = state.head_calls.fetch_add(1, Ordering::SeqCst).min(1);
    let height = state.heights[step];
    let tip = if state.changed_tip && step == 1 { 8 } else { 7 };
    Json(
        serde_json::json!({"chain_id":if state.wrong_chain { Hash384([99;48]).to_hex() } else { CHAIN.to_hex() },
        "height":height,"cut_id":Hash384([tip;48]).to_hex(),"state_root":"11".repeat(48),
        "validator_set_id":"22".repeat(48),"params_hash":"33".repeat(48)}),
    )
}
async fn storage(State(state): State<Arc<Scenario>>) -> Json<serde_json::Value> {
    let step = state.floor_calls.fetch_add(1, Ordering::SeqCst).min(1);
    Json(serde_json::json!({"body_history_floor":state.floors[step]}))
}
async fn block(State(state): State<Arc<Scenario>>) -> axum::response::Response {
    if state.pooled {
        return Json(serde_json::json!({"state":"pooled","registration_height":null,"execution_height":null,"reward":null})).into_response();
    }
    if state.positive {
        return Json(
            serde_json::json!({"state":"executed","registration_height":6,"execution_height":7,
            "reward":{"state":"paid","fees":"5","subsidy":"10","payable_from_height":9}}),
        )
        .into_response();
    }
    let mut value =
        serde_json::json!({"code":"not_found","message":"unknown or outside retained history"});
    if state.typed {
        value["reason"] = serde_json::json!("history_not_held");
    }
    (StatusCode::NOT_FOUND, Json(value)).into_response()
}

// Genuine loopback HTTP with bounded fixture metadata; no consensus-certificate claim.
#[tokio::test]
async fn typed_history_gaps_never_expire_old_or_inconsistently_observed_work() {
    for scenario in [
        Scenario {
            floors: [7, 7],
            ..Default::default()
        },
        Scenario {
            floors: [6, 7],
            ..Default::default()
        },
        Scenario {
            heights: [110, 109],
            ..Default::default()
        },
        Scenario {
            heights: [100, 100],
            ..Default::default()
        },
        Scenario {
            wrong_chain: true,
            ..Default::default()
        },
        Scenario {
            changed_tip: true,
            ..Default::default()
        },
        Scenario {
            typed: false,
            ..Default::default()
        },
    ] {
        let fixture = service(scenario).await;
        assert!(
            fixture
                .client
                .block_in_registration_window(Hash384([1; 48]), 5, 100, CHAIN)
                .await
                .is_err()
        );
        fixture.close().await;
    }
}
#[tokio::test]
async fn the_first_registration_cut_is_inclusive_and_supports_the_genesis_anchor() {
    for (anchor, floor, end) in [(0, 1, 1), (5, 6, 100)] {
        let fixture = service(Scenario {
            floors: [floor, floor],
            ..Default::default()
        })
        .await;
        assert!(
            fixture
                .client
                .block_in_registration_window(Hash384([1; 48]), anchor, end, CHAIN)
                .await
                .unwrap()
                .is_none()
        );
        fixture.close().await;
    }
}
#[tokio::test]
async fn an_old_positive_reward_is_usable_after_its_raw_history_horizon() {
    let fixture = service(Scenario {
        floors: [10000, 10000],
        positive: true,
        ..Default::default()
    })
    .await;
    let state = fixture
        .client
        .block_in_registration_window(Hash384([1; 48]), 5, 100, CHAIN)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.reward.unwrap().paid().unwrap(), Some(15));
    fixture.close().await;
}
#[tokio::test]
async fn an_unscoped_block_query_cannot_promote_a_404_history_gap_to_absence() {
    let fixture = service(Scenario::default()).await;
    assert!(fixture.client.block(Hash384([1; 48])).await.is_err());
    fixture.close().await;
}

#[tokio::test]
async fn an_unregistered_pool_entry_requires_the_same_complete_window_as_a_missing_one() {
    for scenario in [
        Scenario {
            pooled: true,
            floors: [7, 7],
            ..Default::default()
        },
        Scenario {
            pooled: true,
            heights: [100, 100],
            ..Default::default()
        },
    ] {
        let fixture = service(scenario).await;
        assert!(
            fixture
                .client
                .block_in_registration_window(Hash384([1; 48]), 5, 100, CHAIN)
                .await
                .is_err()
        );
        fixture.close().await;
    }
}
