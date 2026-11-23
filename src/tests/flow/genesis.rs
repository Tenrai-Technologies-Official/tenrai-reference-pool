use super::*;

/// Real TenQ + Stratum + SQLite; the node/wallet are the existing bounded service fixtures.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_genesis_anchor_work_is_durable_before_the_pool_acknowledges_it() {
    let rig = rig().await;
    {
        let mut chain = rig.node.chain.lock().unwrap();
        chain.height = 0;
        chain.announced = 0;
    }
    let (stop, pool) = start(&rig.config);
    let mut miner = Client::connect(rig.config.listeners[0].address.port()).await;
    miner.login(&format!("{}.genesis", address(5))).await;
    let hash = miner.mine_block(1).await; // Returns only after the real Stratum accepted ACK.
    let connection = crate::db::connect(&rig.database, true).unwrap();
    let (height, payloads, proofs): (u64, u64, u64) = connection
        .query_row(
            "SELECT height,(SELECT COUNT(*) FROM found_payload WHERE block_id=b.id),
         (SELECT COUNT(*) FROM accepted_work WHERE block_id=b.id) FROM blocks b WHERE hash=?1",
            [hash.to_hex()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!((height, payloads, proofs), (0, 1, 1));
    drop((connection, miner));
    let _ = stop.send(());
    pool.await.unwrap().unwrap();
    for suffix in ["", "-wal", "-shm"] {
        let mut name = rig.database.as_os_str().to_owned();
        name.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(name));
    }
}
