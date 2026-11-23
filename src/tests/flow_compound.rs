use super::*;
use crate::wallet::Wallet;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compound_waves_overlap_up_to_the_open_limit() {
    let mut rig = rig().await;
    rig.config.payouts.enabled = false;
    rig.config.payouts.compound_above = 10;
    rig.config.payouts.compound_open = 128;
    rig.config.payouts.compound_every = Duration::from_millis(100);
    {
        let mut daemon = rig.daemon.lock().unwrap();
        daemon.outputs = 5_000;
        daemon.open_compounds = 128;
    }
    let (stop, pool) = start(&rig.config);
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(rig.daemon.lock().unwrap().compounds, 0);

    rig.daemon.lock().unwrap().open_compounds = 64;
    until("a wave while 64 compounds are open", async || {
        (rig.daemon.lock().unwrap().compounds >= 3).then_some(())
    })
    .await;

    rig.daemon.lock().unwrap().outputs = 10;
    let waves = rig.daemon.lock().unwrap().compounds;
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(rig.daemon.lock().unwrap().compounds <= waves + 1);
    let _ = stop.send(());
    pool.await.unwrap().unwrap();
}

#[tokio::test]
async fn the_largest_outputs_come_only_from_the_daemons_largest_filter() {
    let daemon = FakeDaemon::default();
    let address = serve_router(
        Router::new()
            .route("/v1/rpc", post(rpc))
            .with_state(Arc::clone(&daemon)),
    )
    .await;
    let wallet = Wallet::new(
        url::Url::parse(&format!("http://{address}")).unwrap(),
        TOKEN.to_owned(),
    )
    .unwrap();
    assert_eq!(wallet.largest_outputs(8).await.unwrap(), 5_100_000_000);
    assert_eq!(wallet.largest_outputs(1).await.unwrap(), 5_000_000_000);
    // A daemon without the filter would hand over an unbounded page; the pool asks nothing else.
    daemon.lock().unwrap().without_largest = true;
    assert!(wallet.largest_outputs(8).await.is_err());
}
