use super::*;
use crate::{
    chain::domain_hash,
    db::{self, Op},
    stats::WorkerKey,
    test_support::temp_path,
};

pub(crate) fn request(number: u64, mode: Mode, address: &str) -> AcceptedWork {
    let mut header = [0; 312];
    header[..8].copy_from_slice(&number.to_le_bytes());
    header[248..256].copy_from_slice(&number.to_le_bytes());
    AcceptedWork {
        fingerprint: domain_hash(b"TRAI_POOL_ACCEPTED_WORK_V1", &[&header]),
        header,
        job_id: Hash384([7; 48]),
        window_end: 100,
        key: WorkerKey {
            address: address.into(),
            name: "rig".into(),
            mode,
        },
        time: 1_000,
        weight: 100,
        lookback_millis: 3_600_000,
        source: Some([9; 16]),
        found: None,
    }
}
fn found(request: &mut AcceptedWork) {
    request.found = Some(FoundWork {
        header: request.header,
        body_hex: "00".into(),
        height: 10,
        window_end: 100,
        network_difficulty: 1000,
        window_work: 1000,
        fee_bps: 100,
    });
}
fn memory() -> Connection {
    let c = Connection::open_in_memory().unwrap();
    c.execute_batch(db::SCHEMA).unwrap();
    c
}

#[test]
fn one_canonical_proof_cannot_move_credit_to_another_login_mode_or_source() {
    let mut c = memory();
    let first = {
        let tx = c.transaction().unwrap();
        let mut work = request(1, Mode::Pplns, "first");
        found(&mut work);
        let receipt = apply(&tx, work).unwrap();
        tx.commit().unwrap();
        receipt
    };
    let duplicate = {
        let tx = c.transaction().unwrap();
        let mut work = request(1, Mode::Solo, "thief");
        work.source = Some([99; 16]);
        let receipt = apply(&tx, work).unwrap();
        tx.commit().unwrap();
        receipt
    };
    assert!(first.fresh);
    assert!(!duplicate.fresh);
    assert_eq!(
        (
            first.id,
            first.miner,
            first.worker,
            first.mode,
            first.block.clone()
        ),
        (
            duplicate.id,
            duplicate.miner,
            duplicate.worker,
            duplicate.mode,
            duplicate.block
        )
    );
    let counts:(u64,u64,u64,u64,u64)=c.query_row(
        "SELECT (SELECT COUNT(*) FROM miners),(SELECT COUNT(*) FROM workers),(SELECT COUNT(*) FROM shares),
         (SELECT COUNT(*) FROM blocks),(SELECT COUNT(*) FROM found_payload)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).unwrap();
    assert_eq!(counts, (1, 1, 1, 1, 1));
    assert!(
        read::replay(&c, Hash384([7; 48]), first_nonce(1), "first")
            .unwrap()
            .is_some()
    );
    assert!(
        read::replay(&c, Hash384([7; 48]), first_nonce(1), "thief")
            .unwrap()
            .is_none()
    );
    let accepted: u64 = c
        .query_row("SELECT accepted FROM workers", [], |r| r.get(0))
        .unwrap();
    assert_eq!(accepted, 1);
    let source: Vec<u8> = c
        .query_row("SELECT source FROM share_sources", [], |r| r.get(0))
        .unwrap();
    assert_eq!(source, [9; 16]);
}
fn first_nonce(number: u64) -> [u8; 32] {
    let mut nonce = [0; 32];
    nonce[..8].copy_from_slice(&number.to_le_bytes());
    nonce
}

#[test]
fn checked_identifiers_fail_before_creating_a_partial_share_or_registry_entry() {
    let mut c = memory();
    db::put_setting(&c, "accepted_work_sequence", &i64::MAX.to_string()).unwrap();
    {
        let tx = c.transaction().unwrap();
        assert!(apply(&tx, request(1, Mode::Pplns, "owner")).is_err());
    }
    let n: u64 = c
        .query_row("SELECT COUNT(*) FROM miners", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0);
    db::put_setting(&c, "accepted_work_sequence", "0").unwrap();
    c.execute(
        "INSERT INTO miners(id,address,first_seen) VALUES(?1,'maximum',0)",
        [u32::MAX],
    )
    .unwrap();
    {
        let tx = c.transaction().unwrap();
        assert!(apply(&tx, request(1, Mode::Pplns, "overflow")).is_err());
    }
    let n: u64 = c
        .query_row("SELECT COUNT(*) FROM accepted_work", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0);
    let tx = c.transaction().unwrap();
    assert!(apply(&tx, request(1, Mode::Pplns, "maximum")).is_ok());
    tx.commit().unwrap();
}

#[test]
fn already_retired_eligible_history_is_not_silently_replaced_by_a_shorter_window() {
    let mut c = memory();
    let tx = c.transaction().unwrap();
    for id in 1..=3 {
        let mut work = request(id, Mode::Pplns, "owner");
        work.time = id * 100;
        apply(&tx, work).unwrap();
    }
    tx.commit().unwrap();
    c.execute("DELETE FROM shares WHERE id=1", []).unwrap();
    db::put_setting(&c, "shares_pruned_through", "100").unwrap();
    assert!(window(&c, 3, 300, 0).is_err());
    assert_eq!(
        window(&c, 3, 200, 0).unwrap().total,
        200,
        "complete required work precedes the missing tail"
    );
    assert_eq!(
        window(&c, 3, 300, 101).unwrap().total,
        200,
        "the retired row lies outside the requested time window"
    );
}

#[test]
fn retry_filters_keep_payloads_live_on_a_halted_head_and_across_clock_rollback() {
    let mut c = memory();
    let mut work = request(1, Mode::Pplns, "owner");
    found(&mut work);
    let tx = c.transaction().unwrap();
    let receipt = apply(&tx, work).unwrap();
    tx.commit().unwrap();
    let block = receipt.block.unwrap();
    c.execute("UPDATE blocks SET status=1 WHERE id=?1", [block.id])
        .unwrap();
    c.execute(
        "UPDATE found_payload SET retry_height=11,retry_at=6000 WHERE block_id=?1",
        [block.id],
    )
    .unwrap();
    assert!(
        read::payload_partition_at(&c, 0, 0, 1, 10, 1001)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        read::payload_partition_at(&c, 0, 0, 1, 11, 1001)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        read::payload_partition_at(&c, 0, 0, 1, 10, 6000)
            .unwrap()
            .len(),
        1,
        "a halted head still retries after five seconds"
    );
    assert_eq!(
        read::payload_partition_at(&c, 0, 0, 1, 10, 0)
            .unwrap()
            .len(),
        1,
        "clock rollback cannot postpone the retry indefinitely"
    );
    assert!(
        read::payload_partition_at(&c, 0, 0, 1, 101, 0)
            .unwrap()
            .is_empty(),
        "the actual registration height determines eligibility"
    );
    assert_eq!(
        read::payloads(&c, 0).unwrap().len(),
        1,
        "no retry predicate deletes the durable payload"
    );
}

#[tokio::test]
async fn a_mempool_ack_retains_recovery_payload_until_one_atomic_reward_credit() {
    let path = temp_path("accepted-settlement").with_extension("sqlite");
    let (writer, reader, thread) = db::open(&path).unwrap();
    let mut work = request(1, Mode::Pplns, "owner");
    found(&mut work);
    let receipt = writer.accept(work).await.unwrap();
    let block = receipt.block.unwrap();
    writer
        .call(|done| Op::BlockOutcome {
            id: block.id,
            retry_height: 11,
            status: db::BlockStatus::Pending,
            reason: None,
            at: 1000,
            done,
        })
        .await
        .unwrap();
    assert_eq!(
        reader.read(|c| read::payloads(c, 0)).await.unwrap().len(),
        1
    );
    writer
        .call(|done| Op::BlockProgress {
            rows: vec![
                (block.hash, crate::blocks::Stage::Executed),
                (block.hash, crate::blocks::Stage::Registered),
            ],
            done,
        })
        .await
        .unwrap();
    let detail = reader
        .read(move |c| db::read::block(c, block.id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        detail.0.progress,
        Some(crate::blocks::Stage::Executed),
        "persisted progress is monotonic and independent of the bounded display cache"
    );
    assert_eq!(detail.1, 100);
    assert_eq!(detail.2, Some(100));
    assert!(
        detail.3.is_some(),
        "extended row columns preserve exact split offsets"
    );
    let make_credit = || {
        vec![db::BlockCredit {
            id: block.id,
            reward: 100,
            fee: 1,
            credits: vec![(receipt.miner, 99)],
        }]
    };
    let first = writer
        .call(|done| Op::Credit {
            cut: 20,
            blocks: make_credit(),
            at: 2000,
            done,
        })
        .await
        .unwrap();
    assert_eq!(first, 1);
    let again = writer
        .call(|done| Op::Credit {
            cut: 21,
            blocks: make_credit(),
            at: 3000,
            done,
        })
        .await
        .unwrap();
    assert_eq!(again, 0);
    assert!(
        reader
            .read(|c| read::payloads(c, 0))
            .await
            .unwrap()
            .is_empty()
    );
    let balance = reader
        .read(move |c| db::read::balance(c, receipt.miner))
        .await
        .unwrap();
    assert_eq!((balance.unpaid, balance.earned), (99, 99));
    writer.shutdown().await.unwrap();
    drop(reader);
    drop(writer);
    tokio::task::spawn_blocking(move || thread.join().unwrap())
        .await
        .unwrap();
    remove(&path);
}

#[test]
fn a_failed_found_intent_rolls_back_share_metadata_source_and_sequence() {
    let mut c = memory();
    let mut work = request(1, Mode::Pplns, "first");
    found(&mut work);
    work.found.as_mut().unwrap().body_hex = "not hex".into();
    {
        let tx = c.transaction().unwrap();
        assert!(apply(&tx, work).is_err());
    }
    for table in [
        "miners",
        "workers",
        "shares",
        "accepted_participants",
        "share_sources",
        "blocks",
        "found_payload",
        "accepted_work",
        "settings",
    ] {
        let count: u64 = c
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "{table}");
    }
    let tx = c.transaction().unwrap();
    assert_eq!(apply(&tx, request(2, Mode::Pplns, "other")).unwrap().id, 1);
    tx.commit().unwrap();
}

#[test]
fn exact_disk_window_includes_accepted_work_evicted_from_optional_cache() {
    let mut c = memory();
    let mut cache = crate::pplns::ShareLog::new(1);
    let tx = c.transaction().unwrap();
    for id in 1..=9000_u64 {
        let work = request(id, Mode::Pplns, if id % 2 == 0 { "even" } else { "odd" });
        let row = apply(&tx, work).unwrap();
        cache
            .record(row.id, row.time, row.miner, row.weight)
            .unwrap();
    }
    tx.commit().unwrap();
    assert_eq!(cache.len(), MAX_SHARE_CACHE);
    assert!(cache.window(9000, 900_000, 0).is_err());
    let exact = window(&c, 9000, 900_000, 0).unwrap();
    assert_eq!(exact.total, 900_000);
    assert_eq!(exact.shares, 9000);
    assert_eq!(exact.weights, vec![(1, 450_000), (2, 450_000)]);
    assert!(window(&c, 9001, 1, 0).is_err());
    let capped = window(&c, 9000, 150, 0).unwrap();
    assert_eq!(capped.total, 150);
    assert_eq!(capped.shares, 2);
}

#[test]
fn active_participant_bound_refuses_new_metadata_without_truncating_existing_work() {
    let mut c = memory();
    let tx = c.transaction().unwrap();
    for id in 1..=MAX_ACTIVE_PARTICIPANTS {
        tx.execute(
            "INSERT INTO miners(id,address,first_seen) VALUES(?1,?2,0)",
            params![id as u64, format!("miner{id}")],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO accepted_participants(miner_id,last_seen) VALUES(?1,1000)",
            [id as u64],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    {
        let tx = c.transaction().unwrap();
        assert!(apply(&tx, request(1, Mode::Pplns, "newcomer")).is_err());
    }
    let count: u64 = c
        .query_row("SELECT COUNT(*) FROM miners", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, MAX_ACTIVE_PARTICIPANTS as u64);
    let tx = c.transaction().unwrap();
    let row = apply(&tx, request(2, Mode::Pplns, "miner1")).unwrap();
    assert_eq!(row.miner, 1);
    tx.commit().unwrap();
    assert_eq!(window(&c, row.id, 100, 0).unwrap().weights, vec![(1, 100)]);
}

#[tokio::test]
async fn full_ack_survives_real_close_reopen_and_sequence_survives_receipt_pruning() {
    let path = temp_path("accepted-reopen").with_extension("sqlite");
    let (writer, reader, thread) = db::open(&path).unwrap();
    let mut work = request(1, Mode::Pplns, "owner");
    found(&mut work);
    let receipt = writer.accept(work).await.unwrap();
    assert!(receipt.fresh);
    let fingerprint = request(1, Mode::Solo, "other").fingerprint;
    assert!(
        reader
            .read(move |c| Ok(super::receipt(c, fingerprint).unwrap().is_some()))
            .await
            .unwrap()
    );
    drop(reader);
    drop(writer);
    tokio::task::spawn_blocking(move || thread.join().unwrap())
        .await
        .unwrap();
    let (writer, reader, thread) = db::open(&path).unwrap();
    let retry = writer
        .accept(request(1, Mode::Solo, "other"))
        .await
        .unwrap();
    assert!(!retry.fresh);
    assert_eq!(retry.miner, receipt.miner);
    let pending = reader.read(|c| read::payloads(c, 0)).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].block.id, receipt.block.unwrap().id);
    // Age alone cannot permit re-credit while a halted chain still accepts that header.
    writer
        .call(|done| Op::PruneAccepted {
            shares_before: 0,
            hashrate_before: 0,
            splits_before: 0,
            sources_before: 0,
            receipts_before: 2000,
            height: 100,
            done,
        })
        .await
        .unwrap();
    assert!(
        !writer
            .accept(request(1, Mode::Pplns, "owner"))
            .await
            .unwrap()
            .fresh
    );
    writer
        .call(|done| Op::PruneAccepted {
            shares_before: 2000,
            hashrate_before: 0,
            splits_before: 0,
            sources_before: 0,
            receipts_before: 2000,
            height: 101,
            done,
        })
        .await
        .unwrap();
    assert_eq!(reader.read(db::read::next_share_id).await.unwrap(), 2);
    let mut next = request(2, Mode::Pplns, "owner");
    next.window_end = 101; // The next proof uses a still-open canonical job window.
    assert_eq!(writer.accept(next).await.unwrap().id, 2);
    drop(reader);
    drop(writer);
    tokio::task::spawn_blocking(move || thread.join().unwrap())
        .await
        .unwrap();
    remove(&path);
}

fn remove(path: &std::path::Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut name = path.as_os_str().to_owned();
        name.push(suffix);
        let _ = std::fs::remove_file(std::path::PathBuf::from(name));
    }
}

#[tokio::test]
async fn a_found_block_on_the_genesis_anchor_commits_before_its_receipt_and_survives_restart() {
    let path = temp_path("accepted-genesis-found").with_extension("sqlite");
    let (writer, reader, thread) = db::open(&path).unwrap();
    let mut work = request(1, Mode::Pplns, "genesis-miner");
    found(&mut work);
    work.found.as_mut().unwrap().height = 0;
    let receipt = writer.accept(work).await.unwrap();
    let block = receipt.block.unwrap();
    assert_eq!(block.height, 0);
    let hash = block.hash;
    assert_eq!(
        reader
            .read(move |c| read::open(c, hash))
            .await
            .unwrap()
            .unwrap()
            .height,
        0
    );
    assert_eq!(
        reader.read(|c| read::payloads(c, 0)).await.unwrap().len(),
        1
    );
    writer.shutdown().await.unwrap();
    drop((writer, reader));
    tokio::task::spawn_blocking(move || thread.join().unwrap())
        .await
        .unwrap();
    let (writer, reader, thread) = db::open(&path).unwrap();
    let mut retry = request(1, Mode::Solo, "another-claimant");
    found(&mut retry);
    retry.found.as_mut().unwrap().height = 0;
    let replay = writer.accept(retry).await.unwrap();
    assert!(!replay.fresh);
    assert_eq!(replay.miner, receipt.miner);
    assert_eq!(replay.mode, Mode::Pplns);
    assert_eq!(replay.block.unwrap().height, 0);
    assert_eq!(
        reader.read(|c| read::payloads(c, 0)).await.unwrap().len(),
        1
    );
    writer.shutdown().await.unwrap();
    drop((writer, reader));
    tokio::task::spawn_blocking(move || thread.join().unwrap())
        .await
        .unwrap();
    for suffix in ["", "-wal", "-shm"] {
        let mut name = path.as_os_str().to_owned();
        name.push(suffix);
        let _ = std::fs::remove_file(std::path::PathBuf::from(name));
    }
}

mod retirement_race;
