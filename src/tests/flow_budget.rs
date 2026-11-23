//! The budgets for failed share checks, against a running pool.

use super::*;

impl Client {
    /// Submits a nonce of the current job whose value meets its share target, or one that does not. A share that
    /// is a block as well ends its job, as for any miner, so the client then waits for the next one.
    async fn submit(&mut self, counter: &mut u64, share: bool) -> Value {
        let hasher = hasher();
        let job = self.job.clone().unwrap();
        let mut header: [u8; HEADER_BYTES] = hex::decode(job["header"].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        let target: [u8; 32] = hex::decode(job["share_target"].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        let block_target: [u8; 32] = hex::decode(job["target"].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        let (nonce, block) = loop {
            *counter += 1;
            let nonce = self.nonce(*counter);
            header[248..280].copy_from_slice(&nonce);
            let value = hasher.evaluate(&preimage(&header)).unwrap();
            if meets(&value, &target) == share {
                break (nonce, meets(&value, &block_target));
            }
        };
        let answer = self
            .call(
                "mining.submit",
                json!(["rig", job["job_id"], hex::encode(nonce)]),
            )
            .await;
        if block && answer["result"] == true {
            while self.job.as_ref() == Some(&job) {
                self.read().await;
            }
        }
        answer
    }
}

/// One token for the whole pool every fifteen seconds, eight at once.
async fn tight_rig() -> Rig {
    let mut rig = rig().await;
    rig.config.max_clients = 4;
    rig.config.vardiff.share_interval = 60;
    rig.config.vardiff.start = 64;
    rig.config.vardiff.min = 64;
    rig
}

async fn finish(rig: Rig, stop: oneshot::Sender<()>, pool: JoinHandle<anyhow::Result<()>>) {
    let _ = stop.send(());
    pool.await.unwrap().unwrap();
    for suffix in ["", "-wal", "-shm"] {
        let mut name = rig.database.as_os_str().to_owned();
        name.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(name));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn guesses_cannot_spend_the_shared_budget_of_a_miner_whose_shares_hold() {
    let rig = tight_rig().await;
    let (stop, pool) = start(&rig.config);
    let port = rig.config.listeners[0].address.port();
    let mut counter = 0;

    let mut miner = Client::connect(port).await;
    miner.login(&format!("{}.rig", address(5))).await;
    assert_eq!(miner.submit(&mut counter, true).await["result"], true);
    let mut late = Client::connect(port).await;
    late.login(&format!("{}.rig", address(8))).await;

    let mut refused = Vec::new();
    for marker in [6, 7] {
        let mut guesser = Client::connect(port).await;
        guesser.login(&format!("{}.rig", address(marker))).await;
        for _ in 0..8 {
            refused.push(guesser.submit(&mut counter, false).await["error"][0].as_i64());
        }
    }
    assert_eq!(
        refused[..8],
        [Some(23); 8],
        "the first session's guesses are checked"
    );
    assert_eq!(
        refused[8..],
        [Some(25); 8],
        "then the pool-wide budget is spent"
    );

    assert_eq!(
        late.submit(&mut counter, true).await["error"][0],
        25,
        "a session that has shown no work waits for the budget"
    );
    assert_eq!(
        miner.submit(&mut counter, true).await["result"],
        true,
        "the miner whose last share held is checked without it"
    );
    finish(rig, stop, pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_miner_whose_shares_hold_never_waits_for_a_budget() {
    let rig = tight_rig().await;
    let (stop, pool) = start(&rig.config);
    let mut miner = Client::connect(rig.config.listeners[0].address.port()).await;
    miner.login(&format!("{}.rig", address(5))).await;
    let mut counter = 0;
    for share in 0..24 {
        let answer = miner.submit(&mut counter, true).await;
        assert_eq!(answer["result"], true, "share {share}: {answer}");
    }
    finish(rig, stop, pool).await;
}
