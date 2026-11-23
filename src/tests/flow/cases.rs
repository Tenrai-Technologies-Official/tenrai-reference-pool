use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shares_blocks_credits_and_payouts_flow_end_to_end() {
    let rig = rig().await;
    let (stop, pool) = start(&rig.config);
    let pplns_port = rig.config.listeners[0].address.port();
    let solo_port = rig.config.listeners[1].address.port();
    let miner = address(5);
    let soloist = address(6);

    let mut stranger = Client::connect(pplns_port).await;
    stranger
        .call(
            "mining.subscribe",
            json!([
                "flow-miner/1",
                "",
                crate::pow::tenq::ALGORITHM_ID,
                crate::pow::tenq::SPEC_REVISION
            ]),
        )
        .await;
    let refused = stranger
        .call("mining.authorize", json!(["trai1qyqswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurscucsz2.rig", "x"]))
        .await;
    assert_eq!(
        refused["error"][0], 27,
        "a mainnet address is refused on development"
    );
    let garbage = stranger
        .call("mining.authorize", json!(["dtrai1notanaddress.rig", "x"]))
        .await;
    assert_eq!(garbage["error"][0], 27);

    let mut client = Client::connect(pplns_port).await;
    let subscribed = client.login(&format!("{miner}.rig")).await;
    assert_eq!(subscribed["pool_address"], address(POOL));
    let job = client.job.clone().unwrap();
    let nonce = hex::encode(client.share_nonce());
    let first = client
        .call("mining.submit", json!(["rig", job["job_id"], nonce]))
        .await;
    assert_eq!(first["result"], true);
    let duplicate = client
        .call("mining.submit", json!(["rig", job["job_id"], nonce]))
        .await;
    assert_eq!(
        duplicate["result"], true,
        "a durable retry returns its original receipt"
    );
    let mut reconnect = Client::connect(pplns_port).await;
    reconnect.login(&format!("{miner}.reconnected")).await;
    let retry = reconnect
        .call(
            "mining.submit",
            json!(["reconnected", job["job_id"], nonce]),
        )
        .await;
    assert_eq!(
        retry["result"], true,
        "a new nonce prefix does not hide the original committed receipt"
    );
    drop(reconnect);
    let connection = crate::db::connect(&rig.database, true).unwrap();
    let durable:(u64,u64,u64)=connection.query_row("SELECT (SELECT COUNT(*) FROM shares),(SELECT COUNT(*) FROM workers),(SELECT SUM(accepted) FROM workers)",[],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).unwrap();
    assert_eq!(
        durable,
        (1, 1, 1),
        "neither replay creates another accepted share or worker"
    );
    drop(connection);
    let unknown = client
        .call("mining.submit", json!(["rig", "ab".repeat(48), nonce]))
        .await;
    assert_eq!(unknown["error"][0], 21);
    let foreign = client
        .call(
            "mining.submit",
            json!(["rig", job["job_id"], "00".repeat(32)]),
        )
        .await;
    assert_eq!(
        foreign["error"][0], 26,
        "a nonce without the session prefix is malformed"
    );

    until("the event stream", async || {
        (rig.node.listeners() > 0).then_some(())
    })
    .await;
    let block = client.mine_block(0).await;
    until("the candidate", async || {
        rig.node.candidates().contains(&block).then_some(())
    })
    .await;
    until_stage(&rig.config, block, "found").await;
    rig.node.cut(&[(block, REWARD)]);
    let credited = until("the PPLNS credit", async || {
        let view = miner_view(&rig.config, &miner).await?;
        (view["earned"] != "0.00000000").then_some(view)
    })
    .await;
    assert_eq!(
        credited["earned"], "0.00990000",
        "one miner earns the reward less 1 %"
    );
    assert_eq!(credited["workers"][0]["name"], "rig");

    let mut solo = Client::connect(solo_port).await;
    solo.login(&format!("{soloist}.box")).await;
    let solo_block = solo.mine_block(1_000).await;
    until("the SOLO candidate", async || {
        rig.node.candidates().contains(&solo_block).then_some(())
    })
    .await;
    until_stage(&rig.config, solo_block, "found").await;
    rig.node.cut(&[(solo_block, REWARD)]);
    let solo_view = until("the SOLO credit", async || {
        let view = miner_view(&rig.config, &soloist).await?;
        (view["earned"] != "0.00000000").then_some(view)
    })
    .await;
    assert_eq!(solo_view["earned"], "0.00990000");
    let after = miner_view(&rig.config, &miner).await.unwrap();
    assert_eq!(
        after["earned"], "0.00990000",
        "a SOLO block pays no PPLNS miner"
    );

    let paid = until("both payouts", async || {
        let view = miner_view(&rig.config, &miner).await?;
        let other = miner_view(&rig.config, &soloist).await?;
        (view["paid"] == "0.00990000" && other["paid"] == "0.00990000").then_some(view)
    })
    .await;
    assert_eq!(paid["unpaid"], "0.00000000");
    let sends = rig.daemon.lock().unwrap().sends.clone();
    let total: usize = sends.iter().map(|(_, recipients)| recipients.len()).sum();
    assert_eq!(total, 2, "{sends:?}");
    assert!(
        sends
            .iter()
            .all(|(request, _)| request.starts_with("pool-"))
    );

    let _ = stop.send(());
    pool.await.unwrap().unwrap();
    let (stop, pool) = start(&rig.config);
    let restarted = until("the restarted pool", async || {
        miner_view(&rig.config, &miner).await
    })
    .await;
    assert_eq!(restarted["paid"], "0.00990000");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        rig.daemon.lock().unwrap().sends.len(),
        sends.len(),
        "a restart pays nothing twice"
    );
    let history = get_json(format!("http://{}/api/v1/blocks", rig.config.web.unwrap()))
        .await
        .unwrap();
    let statuses: Vec<&str> = history["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|block| block["status"].as_str().unwrap())
        .collect();
    assert_eq!(statuses, ["confirmed", "confirmed"]);
    let _ = stop.send(());
    pool.await.unwrap().unwrap();
    for suffix in ["", "-wal", "-shm"] {
        let mut name = rig.database.as_os_str().to_owned();
        name.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(name));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_used_leased_body_hands_the_miner_the_empty_template_at_once() {
    let rig = rig().await;
    rig.node.chain.lock().unwrap().leased = true;
    let (stop, pool) = start(&rig.config);
    let mut client = Client::connect(rig.config.listeners[0].address.port()).await;
    client.login(&format!("{}.rig", address(8))).await;
    let leased = LEASED_ROOT.to_hex();
    let carries = |job: &Value| job["header"].as_str().unwrap().contains(&leased);
    while !carries(client.job.as_ref().unwrap()) {
        client.read().await;
    }

    client.mine_block(0).await;
    let next = loop {
        let message = client.read().await;
        if message["method"] == "mining.notify" {
            break message["params"].clone();
        }
    };
    assert!(
        !carries(&next),
        "the miner goes on with the empty template while the next leased one is fetched"
    );
    let _ = stop.send(());
    pool.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_on_the_shared_empty_template_brings_the_miner_fresh_work() {
    let rig = rig().await;
    let (stop, pool) = start(&rig.config);
    let mut client = Client::connect(rig.config.listeners[0].address.port()).await;
    client.login(&format!("{}.rig", address(8))).await;
    assert_eq!(
        client.job.as_ref().unwrap()["single_block"],
        true,
        "an empty template ends with its first block, too"
    );
    let fetched = rig.node.templates();

    client.mine_block(0).await;
    let next = loop {
        let message = client.read().await;
        if message["method"] == "mining.notify" {
            break message["params"].clone();
        }
    };
    assert!(
        rig.node.templates() > fetched,
        "the pool fetched other work instead of handing out the template the block was found on"
    );
    assert_eq!(next["single_block"], true);
    let _ = stop.send(());
    pool.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_block_waits_for_the_cut_that_pays_it() {
    let rig = rig().await;
    let (stop, pool) = start(&rig.config);
    let miner = address(7);
    let mut client = Client::connect(rig.config.listeners[0].address.port()).await;
    client.login(&format!("{miner}.rig")).await;
    until("the event stream", async || {
        (rig.node.listeners() > 0).then_some(())
    })
    .await;
    let block = client.mine_block(0).await;
    until("the candidate", async || {
        rig.node.candidates().contains(&block).then_some(())
    })
    .await;
    until_stage(&rig.config, block, "found").await;

    rig.node.cut(&[]);
    until_stage(&rig.config, block, "registered").await;
    tokio::time::sleep(HINT_SPACING).await;
    rig.node.execute(&[block]);
    until_stage(&rig.config, block, "executed").await;

    let _ = stop.send(());
    pool.await.unwrap().unwrap();
    let (stop, pool) = start(&rig.config);
    until_stage(&rig.config, block, "executed").await;
    until("the new event stream", async || {
        (rig.node.subscriptions() > 1).then_some(())
    })
    .await;

    rig.node.describe(
        block,
        json!({"state": "executed", "registration_height": 6, "execution_height": 7,
            "reward": {"state": "queued", "fees": "0", "subsidy": REWARD.to_string(), "payable_from_height": 12}}),
    );
    let lookups = rig.node.lookups();
    // The pool looks a block up only after its registration window; these cuts are missed.
    for _ in 0..100 {
        rig.node.skip();
    }
    rig.node.cut(&[]);
    until("the lookup after the gap", async || {
        (rig.node.lookups() > lookups).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let waiting = pool_block(&rig.config, block).await.unwrap();
    assert_eq!(
        waiting["status"], "pending",
        "a queued reward is not booked: {waiting}"
    );
    assert_eq!(waiting["stage"], "executed");
    assert_eq!(
        miner_view(&rig.config, &miner).await.unwrap()["earned"],
        "0.00000000"
    );

    tokio::time::sleep(HINT_SPACING).await;
    rig.node.cut(&[(block, REWARD)]);
    let paid = until_stage(&rig.config, block, "finalized").await;
    assert_eq!(paid["status"], "confirmed");
    assert_eq!(paid["reward"], "0.01000000");
    let credited = until("the PPLNS credit", async || {
        let view = miner_view(&rig.config, &miner).await?;
        (view["earned"] != "0.00000000").then_some(view)
    })
    .await;
    assert_eq!(credited["earned"], "0.00990000");

    let _ = stop.send(());
    pool.await.unwrap().unwrap();
    for suffix in ["", "-wal", "-shm"] {
        let mut name = rig.database.as_os_str().to_owned();
        name.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(name));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_without_the_wallet_daemon_needs_leave_and_an_address() {
    let mut rig = rig().await;
    rig.config.wallet = url::Url::parse(&format!("http://127.0.0.1:{}", free_port())).unwrap();
    let refused = crate::serve(rig.config.clone(), std::future::pending::<()>())
        .await
        .unwrap_err();
    assert!(
        format!("{refused:#}").contains("--no-wallet-check"),
        "{refused:#}"
    );

    rig.config.wallet_check = false;
    let unknown = crate::serve(rig.config.clone(), std::future::pending::<()>())
        .await
        .unwrap_err();
    assert!(
        format!("{unknown:#}").contains("--pool-address"),
        "{unknown:#}"
    );

    rig.config.pool_address = Some(address(POOL));
    let (stop, pool) = start(&rig.config);
    let view = until("the pool without its daemon", async || {
        get_json(format!("http://{}/api/v1/pool", rig.config.web.unwrap())).await
    })
    .await;
    assert_eq!(view["pool_address"], address(POOL));
    assert_eq!(rig.daemon.lock().unwrap().sends.len(), 0);
    let _ = stop.send(());
    pool.await.unwrap().unwrap();
    for suffix in ["", "-wal", "-shm"] {
        let mut name = rig.database.as_os_str().to_owned();
        name.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(name));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committed_found_intent_survives_complete_pool_stop_and_reopen() {
    let rig = rig().await;
    rig.node.chain.lock().unwrap().submit_busy = true;
    let (stop, pool) = start(&rig.config);
    let port = rig.config.listeners[0].address.port();
    let mut client = Client::connect(port).await;
    client.login(&format!("{}.rig", address(8))).await;
    let block = client.mine_block(0).await;
    let stored = {
        let c = crate::db::connect(&rig.database, true).unwrap();
        c.query_row("SELECT (SELECT COUNT(*) FROM shares),(SELECT COUNT(*) FROM accepted_work),(SELECT COUNT(*) FROM blocks),(SELECT COUNT(*) FROM found_payload)",[],|r|Ok((r.get::<_,u64>(0)?,r.get::<_,u64>(1)?,r.get::<_,u64>(2)?,r.get::<_,u64>(3)?))).unwrap()
    };
    assert_eq!((stored.2, stored.3), (1, 1));
    assert!(!rig.node.candidates().contains(&block));
    // Keep the client open: shutdown must own, stop and join its listener session as well.
    stop.send(()).unwrap();
    tokio::time::timeout(WAIT, pool)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(client);
    rig.node.chain.lock().unwrap().submit_busy = false;
    let (stop, pool) = start(&rig.config);
    until("durable outbox resubmission", async || {
        rig.node.candidates().contains(&block).then_some(())
    })
    .await;
    let c = crate::db::connect(&rig.database, true).unwrap();
    let held: u64 = c
        .query_row("SELECT COUNT(*) FROM found_payload", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        held, 1,
        "a mempool ACK is not finality and cannot discard the recovery payload"
    );
    drop(c);
    until("restored event stream", async || {
        (rig.node.listeners() > 0).then_some(())
    })
    .await;
    rig.node.cut(&[(block, REWARD)]);
    until("finalized intent retirement",async||{
        let c=crate::db::connect(&rig.database,true).unwrap();
        let state:(u64,u64)=c.query_row("SELECT (SELECT COUNT(*) FROM found_payload),(SELECT COUNT(*) FROM blocks WHERE status=2)",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
        (state==(0,1)).then_some(())
    }).await;
    let c = crate::db::connect(&rig.database, true).unwrap();
    let restored:(u64,u64,u64)=c.query_row("SELECT (SELECT COUNT(*) FROM shares),(SELECT COUNT(*) FROM accepted_work),(SELECT COUNT(*) FROM blocks)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!(
        restored,
        (stored.0, stored.1, 1),
        "submission recovery cannot create new shares, work receipts or block claims"
    );
    drop(c);
    stop.send(()).unwrap();
    tokio::time::timeout(WAIT, pool)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnect_during_a_real_verified_share_commit_never_reports_a_false_rejection() {
    let rig = rig().await;
    let (stop, pool) = start(&rig.config);
    let port = rig.config.listeners[0].address.port();
    let owner = address(9);
    let mut original = Client::connect(port).await;
    original.login(&format!("{owner}.first")).await;
    let job = original.job.as_ref().unwrap()["job_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let nonce = hex::encode(original.share_nonce());
    let lock = crate::db::connect(&rig.database, false).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (first_job, first_nonce) = (job.clone(), nonce.clone());
    let submitting = tokio::spawn(async move {
        let result = original
            .call("mining.submit", json!(["first", first_job, first_nonce]))
            .await;
        (original, result)
    });
    until("physically pending real share", async || {
        let client = crate::tls::http_client(Duration::from_secs(2)).unwrap();
        let response = client
            .get(format!("http://{}/metrics", rig.config.web.unwrap()))
            .send()
            .await
            .ok()?;
        let text = response.text().await.ok()?;
        text.lines()
            .find_map(|line| line.strip_prefix("tenrai_pool_acceptances_in_flight "))?
            .parse::<u64>()
            .ok()
            .filter(|count| *count > 0)
            .map(|_| ())
    })
    .await;
    let mut replay = Client::connect(port).await;
    replay.login(&format!("{owner}.replay")).await;
    let request = json!({"id":9001,"method":"mining.submit","params":["replay",job,nonce]})
        .to_string()
        + "\n";
    replay.writer.write_all(request.as_bytes()).await.unwrap();
    let before_commit = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            let Some(line) = replay.lines.next_line().await? else {
                return Ok::<Option<Value>, std::io::Error>(None);
            };
            let message: Value = serde_json::from_str(&line).unwrap();
            if message["id"] == json!(9001) {
                return Ok(Some(message));
            }
        }
    })
    .await;
    match before_commit {
        Err(_) => {}       // The writer FIFO fence is still behind the original physical commit.
        Ok(Ok(None)) => {} // The pre-write physical verifier is pending: typed unknown disconnect.
        other => panic!(
            "an unresolved physical share received a definite reply before commit: {other:?}"
        ),
    }
    lock.execute_batch("COMMIT").unwrap();
    drop(lock);
    drop(replay);
    let (original, accepted) = submitting.await.unwrap();
    assert_eq!(accepted["result"], true);
    drop(original);
    let mut retry = Client::connect(port).await;
    retry.login(&format!("{owner}.retry")).await;
    let answer = retry
        .call("mining.submit", json!(["retry", job, nonce]))
        .await;
    assert_eq!(answer["result"], true);
    let c = crate::db::connect(&rig.database, true).unwrap();
    let amounts:(u64,u64,u64)=c.query_row("SELECT (SELECT COUNT(*) FROM accepted_work),(SELECT COUNT(*) FROM shares),(SELECT SUM(accepted) FROM workers)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!(amounts, (1, 1, 1));
    drop(c);
    drop(retry);
    stop.send(()).unwrap();
    tokio::time::timeout(WAIT, pool)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mismatched_pow_identity_never_subscribes_or_enables_work() {
    let rig = rig().await;
    let (stop, pool) = start(&rig.config);
    let port = rig.config.listeners[0].address.port();
    for (algorithm, revision) in [
        (
            crate::pow::tenq::ALGORITHM_ID,
            crate::pow::tenq::SPEC_REVISION - 1,
        ),
        (
            crate::pow::tenq::ALGORITHM_ID ^ 1,
            crate::pow::tenq::SPEC_REVISION,
        ),
    ] {
        let mut client = Client::connect(port).await;
        let authorized = client
            .call(
                "mining.authorize",
                json!([format!("{}.rig", address(31)), "x"]),
            )
            .await;
        assert_eq!(
            authorized["error"][0], 24,
            "authorization needs a valid subscription"
        );
        let subscribed = client
            .call(
                "mining.subscribe",
                json!(["wrong-pow/1", "", algorithm, revision]),
            )
            .await;
        assert_eq!(subscribed["error"][0], 20, "{subscribed}");
        assert!(client.job.is_none());
        let authorized = client
            .call(
                "mining.authorize",
                json!([format!("{}.rig", address(31)), "x"]),
            )
            .await;
        assert_eq!(
            authorized["error"][0], 24,
            "a rejected identity cannot authorize"
        );
        let submitted = client
            .call(
                "mining.submit",
                json!(["rig", "00".repeat(48), "00".repeat(32)]),
            )
            .await;
        assert_eq!(
            submitted["error"][0], 24,
            "identity rejection must leave the session unsubscribed"
        );
    }
    let _ = stop.send(());
    pool.await.unwrap().unwrap();
    for suffix in ["", "-wal", "-shm"] {
        let mut name = rig.database.as_os_str().to_owned();
        name.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(name));
    }
}
