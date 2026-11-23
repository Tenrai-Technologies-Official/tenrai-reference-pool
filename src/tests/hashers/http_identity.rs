use super::*;
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use std::sync::atomic::AtomicU32;

fn document(revision: u32, registration_height: u64) -> Value {
    let response = crate::test_support::response(
        &crate::test_support::lock(1),
        [0xff; 32],
        registration_height,
    );
    json!({
        "job_id": response.job_id,
        "chain_id": response.chain_id,
        "pow_algorithm_id": response.pow_algorithm_id,
        "spec_revision": revision,
        "work_ref": response.work_ref,
        "target": response.target,
        "registration_height": response.registration_height,
        "registration_window_end": response.registration_window_end,
        "parents_root": response.parents_root,
        "payload_root": response.payload_root,
        "payload_length": response.payload_length,
        "payload_mass": response.payload_mass,
        "reward_lock": response.reward_lock,
        "body_hex": response.body_hex,
    })
}

#[tokio::test]
async fn http_work_identity_is_checked_before_tables_and_enables_prefetch() {
    let height = epoch_start(1) - PREFETCH_HEIGHTS - 1;
    let revision = Arc::new(AtomicU32::new(tenq::SPEC_REVISION - 1));
    let served_revision = Arc::clone(&revision);
    let router = Router::new().route(
        "/v1/mining/templates",
        post(move || {
            let revision = served_revision.load(Ordering::SeqCst);
            async move { Json(document(revision, height + 1)) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url: url::Url = format!("http://{}/", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let (stop, stopping) = oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = stopping.await;
            })
            .await
            .unwrap();
    });
    let node = NodeClient::new(url.clone(), url).unwrap();
    let hashers = hashers(TenQMode::Full, || None);
    let (heights, _) = watch::channel(height);
    let source = Source {
        node,
        payout: (
            crate::test_support::address(1),
            crate::test_support::lock(1),
        ),
        chain_id: CHAIN,
        hashers: Arc::clone(&hashers),
        heights,
        stats: Arc::new(Stats::new(
            Default::default(),
            [0, 0],
            crate::day::Day::default(),
        )),
        pending: VecDeque::new(),
        shared: None,
        outage: NodeOutage::new(NODE_PATIENCE, WAIT_REMINDER),
        retry_at: None,
        filler: Filler::default(),
        filler_due: true,
    };

    let Err(error) = source.fetch(0).await else {
        panic!("previous-revision HTTP work must be rejected");
    };
    assert!(error.to_string().contains("unsupported TenQ"));
    assert!(!hashers.lifetime.prefetch_running());
    assert!(hashers.epochs().is_empty());

    revision.store(tenq::SPEC_REVISION, Ordering::SeqCst);
    let work = source.fetch(0).await.unwrap();
    settled(&hashers).await;
    assert_eq!(work.template.height(), height);
    assert!(work.hasher.is_full());
    assert!(hashers.get(0).unwrap().is_full());
    assert!(hashers.get(1).unwrap().is_full());

    hashers.close().await.unwrap();
    stop.send(()).unwrap();
    server.await.unwrap();
}
