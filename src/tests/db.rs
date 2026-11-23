use super::*;
use crate::{
    config::Mode,
    db::writer::{BlockCredit, NewBlock},
    test_support::temp_path,
};

fn open_temp() -> (Writer, Reader, PathBuf) {
    let path = temp_path("db").with_extension("sqlite");
    let (writer, reader, _) = open(&path).unwrap();
    (writer, reader, path)
}

fn remove(path: &Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut name = path.as_os_str().to_owned();
        name.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(name));
    }
}

fn block(hash: u8, mode: Mode) -> NewBlock {
    NewBlock {
        hash: format!("{hash:02x}").repeat(48),
        mode,
        miner: 1,
        worker: 1,
        height: 10,
        window_end: 105,
        found_at: now_millis(),
        network_difficulty: 1_000,
        round_work: 800,
        fee_bps: 100,
        share_id: Some(3),
        window_work: Some(2_000),
        window_shares: Some(4),
        split: Some(crate::pplns::encode_split(&[(1, 1_500), (2, 500)])),
    }
}

async fn setup(writer: &Writer) {
    for (id, address) in [(1, "dtrai1first"), (2, "dtrai1second")] {
        writer.send(Op::Miner {
            id,
            address: address.into(),
            at: 1,
        });
        writer.send(Op::Worker {
            id: u64::from(id),
            miner: id,
            name: "rig".into(),
            at: 1,
        });
    }
    writer.flush().await;
}

async fn found(writer: &Writer, hash: u8) -> i64 {
    writer
        .call(|done| Op::BlockFound {
            block: block(hash, Mode::Pplns),
            done,
        })
        .await
        .unwrap()
}

async fn credit(writer: &Writer, cut: u64, id: i64, credits: Vec<(u32, u64)>) -> u64 {
    writer
        .call(|done| Op::Credit {
            cut,
            blocks: vec![BlockCredit {
                id,
                reward: 1_000,
                fee: 10,
                credits,
            }],
            at: now_millis(),
            done,
        })
        .await
        .unwrap()
}

async fn balance(reader: &Reader, miner: u32) -> read::Balance {
    reader
        .read(move |connection| read::balance(connection, miner))
        .await
        .unwrap()
}

#[tokio::test]
async fn a_block_is_credited_once() {
    let (writer, reader, path) = open_temp();
    setup(&writer).await;
    let id = found(&writer, 1).await;
    assert_eq!(credit(&writer, 10, id, vec![(1, 600), (2, 390)]).await, 1);
    assert_eq!(credit(&writer, 11, id, vec![(1, 600), (2, 390)]).await, 0);
    let first = balance(&reader, 1).await;
    assert_eq!((first.unpaid, first.earned), (600, 600));
    assert_eq!(balance(&reader, 2).await.unpaid, 390);
    let height = reader
        .read(|connection| setting(connection, "cut_height"))
        .await
        .unwrap();
    assert_eq!(height.as_deref(), Some("11"));
    let other = found(&writer, 2).await;
    credit(&writer, 5, other, vec![(1, 1)]).await;
    let height = reader
        .read(|connection| setting(connection, "cut_height"))
        .await
        .unwrap();
    assert_eq!(
        height.as_deref(),
        Some("11"),
        "the resume height never moves back"
    );
    remove(&path);
}

#[tokio::test]
async fn a_closed_block_is_not_reopened_by_a_late_status() {
    let (writer, reader, path) = open_temp();
    setup(&writer).await;
    let id = found(&writer, 1).await;
    credit(&writer, 10, id, vec![(1, 990)]).await;
    writer.send(Op::BlockState {
        id,
        status: BlockStatus::Pending,
        reason: None,
        at: 1,
    });
    writer.flush().await;
    let open = reader.read(read::open_blocks).await.unwrap();
    assert!(open.is_empty());
    remove(&path);
}

#[tokio::test]
async fn payouts_move_balances_and_failed_ones_give_them_back() {
    let (writer, reader, path) = open_temp();
    setup(&writer).await;
    let id = found(&writer, 1).await;
    credit(&writer, 10, id, vec![(1, 1_000)]).await;
    let create = |amount: u64| {
        let writer = writer.clone();
        async move {
            writer
                .call(|done| Op::PayoutCreate {
                    request_prefix: "pool-test-".into(),
                    items: vec![(1, amount)],
                    at: 1,
                    done,
                })
                .await
        }
    };
    let (payout, request_id) = create(700).await.unwrap();
    assert_eq!(request_id, format!("pool-test-{payout}"));
    let moved = balance(&reader, 1).await;
    assert_eq!((moved.unpaid, moved.pending), (300, 700));
    assert!(
        create(700).await.is_err(),
        "a short balance refuses the payout"
    );
    assert_eq!(balance(&reader, 1).await.unpaid, 300);
    let close = |payout: i64, paid: bool| {
        let writer = writer.clone();
        async move {
            writer
                .call(|done| Op::PayoutClosed {
                    id: payout,
                    paid,
                    error: None,
                    at: 2,
                    done,
                })
                .await
                .unwrap();
        }
    };
    close(payout, false).await;
    let returned = balance(&reader, 1).await;
    assert_eq!((returned.unpaid, returned.pending), (1_000, 0));
    let (second, _) = create(400).await.unwrap();
    writer
        .call(|done| Op::PayoutSent {
            id: second,
            transaction: "ab".repeat(48),
            fee: 3,
            valid_until: Some(500),
            fee_shares: vec![(1, 3)],
            at: 2,
            done,
        })
        .await
        .unwrap();
    let open = reader.read(read::open_payouts).await.unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].status, PayoutStatus::Sent);
    assert_eq!(open[0].valid_until, Some(500));
    close(second, true).await;
    close(second, false).await;
    let paid = balance(&reader, 1).await;
    assert_eq!((paid.unpaid, paid.pending, paid.paid), (600, 0, 400));
    let history = reader
        .read(|connection| read::miner_payouts(connection, 1, 10))
        .await
        .unwrap();
    assert_eq!(history[0].fee_share, 3);
    remove(&path);
}

#[tokio::test]
async fn a_restart_reads_open_blocks_shares_and_payouts_back() {
    let path = temp_path("db").with_extension("sqlite");
    let (writer, reader, thread) = open(&path).unwrap();
    setup(&writer).await;
    for (id, time) in [(1_u64, 100_u64), (2, 200), (3, 300)] {
        writer.send(Op::Share {
            id,
            time,
            miner: 1,
            worker: 1,
            weight: 50,
        });
    }
    found(&writer, 1).await;
    writer
        .call(|done| Op::PayoutCreate {
            request_prefix: "p-".into(),
            items: vec![],
            at: 1,
            done,
        })
        .await
        .unwrap();
    // A restart: the writer stops and its thread ends before the next owner opens the file.
    writer.shutdown().await.unwrap();
    drop((writer, reader));
    tokio::task::spawn_blocking(move || thread.join().unwrap())
        .await
        .unwrap();
    let (_, reader, _) = open(&path).unwrap();
    let blocks = reader.read(read::open_blocks).await.unwrap();
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].status, BlockStatus::Found);
    let split = crate::pplns::decode_split(blocks[0].split.as_deref().unwrap()).unwrap();
    assert_eq!(split, vec![(1, 1_500), (2, 500)]);
    let shares = reader
        .read(|connection| read::shares_since(connection, 150))
        .await
        .unwrap();
    assert_eq!(shares, vec![(2, 200, 1, 50), (3, 300, 1, 50)]);
    assert_eq!(reader.read(read::next_share_id).await.unwrap(), 4);
    assert_eq!(reader.read(read::open_payouts).await.unwrap().len(), 1);
    let counts = reader.read(read::block_counts).await.unwrap();
    assert_eq!(counts[0], (1, 1, 0, 0));
    remove(&path);
}

#[tokio::test]
async fn pruning_drops_old_shares_history_and_splits() {
    let (writer, reader, path) = open_temp();
    setup(&writer).await;
    writer.send(Op::Share {
        id: 1,
        time: 10,
        miner: 1,
        worker: 1,
        weight: 5,
    });
    writer.hashrate(3, vec![(1, Mode::Pplns, 50), (0, Mode::Pplns, 50)]);
    let id = found(&writer, 1).await;
    credit(&writer, 10, id, vec![(1, 990)]).await;
    writer.send(Op::Prune {
        shares_before: 11,
        hashrate_before: 4,
        splits_before: u64::MAX,
        sources_before: 6,
    });
    writer.flush().await;
    let shares = reader
        .read(|connection| read::shares_since(connection, 0))
        .await
        .unwrap();
    assert!(shares.is_empty());
    let history = reader
        .read(|connection| read::hashrate(connection, 1, 0))
        .await
        .unwrap();
    assert!(history.is_empty());
    let hash = format!("{:02x}", 1).repeat(48);
    let detail = reader.block(&hash).await.unwrap().unwrap();
    assert!(detail.3.is_none());
    remove(&path);
}

#[tokio::test]
async fn closed_blocks_leave_after_their_retention_and_open_ones_stay() {
    let (writer, reader, path) = open_temp();
    setup(&writer).await;
    let credited = found(&writer, 1).await;
    credit(&writer, 10, credited, vec![(1, 990)]).await;
    found(&writer, 2).await;
    let deleted = writer
        .call(|done| Op::TrimBlocks {
            before: Some(u64::MAX),
            keep: None,
            done,
        })
        .await
        .unwrap();
    assert_eq!(deleted, 1);
    for (marker, kept) in [(1_u8, false), (2, true)] {
        let hash = format!("{marker:02x}").repeat(48);
        let detail = reader.block(&hash).await.unwrap();
        assert_eq!(detail.is_some(), kept, "block {marker}");
    }
    remove(&path);
}

#[tokio::test]
async fn only_the_newest_closed_blocks_stay_and_open_ones_wait_for_their_reward() {
    let (writer, reader, path) = open_temp();
    setup(&writer).await;
    let mut ids = Vec::new();
    for marker in 1..=5 {
        ids.push(found(&writer, marker).await);
    }
    for (cut, id) in ids.iter().enumerate().skip(1) {
        credit(&writer, 10 + cut as u64, *id, vec![(1, 990)]).await;
    }
    let deleted = writer
        .call(|done| Op::TrimBlocks {
            before: None,
            keep: Some(2),
            done,
        })
        .await
        .unwrap();
    assert_eq!(deleted, 2);
    for (marker, kept) in [(1_u8, true), (2, false), (3, false), (4, true), (5, true)] {
        let hash = format!("{marker:02x}").repeat(48);
        let detail = reader.block(&hash).await.unwrap();
        assert_eq!(detail.is_some(), kept, "block {marker}");
    }
    remove(&path);
}

#[test]
fn sqlite_runtime_is_patched_and_unsigned_money_and_heights_do_not_wrap() {
    assert!(rusqlite::version_number() >= 3_054_000);
    let path = temp_path("patched_sqlite").with_extension("sqlite");
    let connection = crate::db::connect(&path, false).unwrap();
    let maximum = i64::MAX as u64;
    let restored: u64 = connection
        .query_row("SELECT ?1", [maximum], |row| row.get(0))
        .unwrap();
    assert_eq!(restored, maximum);
    let too_large: rusqlite::Result<u64> =
        connection.query_row("SELECT ?1", [u64::MAX], |row| row.get(0));
    assert!(too_large.is_err());
    let negative: rusqlite::Result<u64> = connection.query_row("SELECT -1", [], |row| row.get(0));
    assert!(negative.is_err());
    drop(connection);
    remove(&path);
}
