//! The public Miningcore-style API and the request limits, against a running pool.

use super::*;
use axum::http::StatusCode as Status;

async fn status_of(request: reqwest::RequestBuilder) -> (u16, reqwest::header::HeaderMap) {
    let response = request.send().await.unwrap();
    (response.status().as_u16(), response.headers().clone())
}

fn stop_and_clean(rig: &Rig) {
    for suffix in ["", "-wal", "-shm"] {
        let mut name = rig.database.as_os_str().to_owned();
        name.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(name));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_public_api_speaks_miningcore() {
    let rig = rig().await;
    let (stop, pool) = start(&rig.config);
    let web = rig.config.web.unwrap();
    let pools = until("the public pools", async || {
        get_json(format!("http://{web}/api/pools")).await
    })
    .await;
    let pplns = &pools["pools"][0];
    assert_eq!(pplns["id"], "trai-pplns");
    assert_eq!(pplns["coin"]["type"], "TRAI");
    assert_eq!(pplns["coin"]["algorithm"], "TenQ");
    assert_eq!(pplns["paymentProcessing"]["payoutScheme"], "PPLNS");
    assert_eq!(
        pplns["paymentProcessing"]["payoutSchemeConfig"]["factor"],
        2.0
    );
    assert_eq!(pplns["poolFeePercent"], 1.0);
    assert_eq!(pplns["address"], address(POOL));
    assert!(pplns["poolStats"]["poolHashrate"].is_number());
    assert!(pplns["poolStats"]["connectedMiners"].is_number());
    assert_eq!(pplns["networkStats"]["networkType"], "Dev");
    assert!(pplns["networkStats"]["blockHeight"].is_number());
    assert_eq!(pools["pools"][1]["id"], "trai-solo");
    assert_eq!(
        pools["pools"][1]["paymentProcessing"]["payoutScheme"],
        "SOLO"
    );

    let solo = get_json(format!("http://{web}/api/pools/trai-solo"))
        .await
        .unwrap();
    assert_eq!(solo["pool"]["id"], "trai-solo");

    let miner = address(9);
    let mut stratum = Client::connect(rig.config.listeners[0].address.port()).await;
    stratum.login(&format!("{miner}.rig")).await;
    let job = stratum.job.clone().unwrap();
    let nonce = hex::encode(stratum.share_nonce());
    assert_eq!(
        stratum
            .call("mining.submit", json!(["rig", job["job_id"], nonce]))
            .await["result"],
        true
    );
    let stats = until("the miner in the public API", async || {
        get_json(format!("http://{web}/api/pools/trai-pplns/miners/{miner}")).await
    })
    .await;
    assert!(
        stats["performance"]["workers"]["rig"]["hashrate"].is_number(),
        "{stats}"
    );
    assert_eq!(stats["pendingBalance"], 0.0);
    let other_mode = get_json(format!("http://{web}/api/pools/trai-solo/miners/{miner}"))
        .await
        .unwrap();
    assert!(
        other_mode["performance"]["workers"]
            .as_object()
            .unwrap()
            .is_empty()
    );
    let both = get_json(format!("http://{web}/api/miners/{miner}"))
        .await
        .unwrap();
    assert_eq!(
        both["performance"]["workers"]["rig"]["payoutScheme"], "PPLNS",
        "{both}"
    );
    let all = get_json(format!("http://{web}/api/blocks?pageSize=1000"))
        .await
        .unwrap();
    assert!(all.as_array().unwrap().is_empty(), "{all}");
    let blocks = get_json(format!(
        "http://{web}/api/pools/trai-pplns/blocks?page=0&pageSize=5"
    ))
    .await
    .unwrap();
    assert!(blocks.as_array().unwrap().is_empty(), "{blocks}");

    let client = crate::tls::http_client(Duration::from_secs(5)).unwrap();
    let (code, headers) = status_of(client.get(format!("http://{web}/api/pools"))).await;
    assert_eq!(code, Status::OK.as_u16());
    assert_eq!(headers["access-control-allow-origin"], "*");
    assert_eq!(headers["cache-control"], "public, max-age=10");
    let unknown = address(10);
    let uppercase = miner.to_uppercase();
    let long = "a".repeat(200);
    for (path, want) in [
        (format!("/api/pools/trai-pplns/miners/{unknown}"), Status::NOT_FOUND),
        (format!("/api/pools/trai-pplns/miners/{uppercase}"), Status::BAD_REQUEST),
        (format!("/api/pools/trai-pplns/miners/{long}"), Status::BAD_REQUEST),
        ("/api/pools/trai-pplns/miners/trai1qyqswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurscucsz2".to_owned(), Status::BAD_REQUEST),
        ("/api/pools/trai-pplns/miners/%3Cscript%3E".to_owned(), Status::BAD_REQUEST),
        ("/api/pools/trai-other/miners/x".to_owned(), Status::NOT_FOUND),
        (format!("/api/miners/{unknown}"), Status::NOT_FOUND),
        (format!("/api/miners/{uppercase}"), Status::BAD_REQUEST),
        ("/api/miners/%22%3E%3Cimg%20src%3Dx%3E".to_owned(), Status::BAD_REQUEST),
        ("/api/blocks?pageSize=1001".to_owned(), Status::BAD_REQUEST),
        ("/api/blocks?page=abc".to_owned(), Status::BAD_REQUEST),
    ] {
        let (code, _) = status_of(client.get(format!("http://{web}{path}"))).await;
        assert_eq!(code, want.as_u16(), "{path}");
    }
    for (path, want) in [
        ("/api/pools/trai-other", Status::NOT_FOUND),
        (
            "/api/pools/trai-pplns/blocks?pageSize=1001",
            Status::BAD_REQUEST,
        ),
        (
            "/api/pools/trai-pplns/blocks?pageSize=0",
            Status::BAD_REQUEST,
        ),
        ("/api/pools/trai-pplns/blocks?page=100", Status::BAD_REQUEST),
        (
            "/api/pools/trai-pplns/blocks?pageSize=-1",
            Status::BAD_REQUEST,
        ),
        ("/api/pools/trai-pplns/blocks?page=x", Status::BAD_REQUEST),
    ] {
        let (code, _) = status_of(client.get(format!("http://{web}{path}"))).await;
        assert_eq!(code, want.as_u16(), "{path}");
    }
    let long = format!("http://{web}/api/pools?{}", "a".repeat(600));
    assert_eq!(
        status_of(client.get(long)).await.0,
        Status::URI_TOO_LONG.as_u16()
    );
    let with_body = client
        .get(format!("http://{web}/api/pools"))
        .body("x".repeat(64));
    assert_eq!(
        status_of(with_body).await.0,
        Status::PAYLOAD_TOO_LARGE.as_u16()
    );
    let posted = client.post(format!("http://{web}/api/pools"));
    assert_eq!(
        status_of(posted).await.0,
        Status::METHOD_NOT_ALLOWED.as_u16()
    );

    let _ = stop.send(());
    pool.await.unwrap().unwrap();
    stop_and_clean(&rig);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flood_from_one_client_meets_the_limit() {
    let mut rig = rig().await;
    rig.config.web_rate_limit = 60;
    let (stop, pool) = start(&rig.config);
    let web = rig.config.web.unwrap();
    until("the website", async || {
        get_json(format!("http://{web}/api/pools")).await
    })
    .await;
    let client = crate::tls::http_client(Duration::from_secs(5)).unwrap();
    let mut refused = None;
    for _ in 0..40 {
        let (code, headers) = status_of(client.get(format!("http://{web}/api/pools"))).await;
        if code == Status::TOO_MANY_REQUESTS.as_u16() {
            refused = Some(headers);
            break;
        }
    }
    let headers = refused.expect("the burst of 20 runs out");
    assert_eq!(headers["retry-after"], "10");
    let behind_proxy = client
        .get(format!("http://{web}/api/pools"))
        .header("x-real-ip", "198.51.100.23");
    assert_eq!(
        status_of(behind_proxy).await.0,
        Status::OK.as_u16(),
        "another client behind the local proxy"
    );

    let _ = stop.send(());
    pool.await.unwrap().unwrap();
    stop_and_clean(&rig);
}
