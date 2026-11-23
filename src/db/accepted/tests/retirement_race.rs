use super::*;
use crate::db::{Reader, Writer};

fn pruning(done: tokio::sync::oneshot::Sender<Result<bool, String>>, height: u64) -> Op {
    Op::PruneAccepted {
        shares_before: 0,
        hashrate_before: 0,
        splits_before: 0,
        sources_before: 0,
        receipts_before: 2_000,
        height,
        done,
    }
}
async fn closed(reader: &Reader) -> u64 {
    reader
        .read(|connection| {
            closed_windows_before(connection)
                .map_err(|error| rusqlite::Error::InvalidParameterName(error.to_string()))
        })
        .await
        .unwrap()
}
async fn close(writer: Writer, reader: Reader, physical: std::thread::JoinHandle<()>) {
    writer.shutdown().await.unwrap();
    drop(reader);
    drop(writer);
    tokio::task::spawn_blocking(move || physical.join().unwrap())
        .await
        .unwrap();
}

/// A trusted-intake SQLite/actor regression. PoW verification finished before this queued race.
#[tokio::test]
async fn a_preverified_duplicate_queued_behind_receipt_gc_cannot_become_fresh_in_another_job_or_login()
 {
    let path = temp_path("pool-receipt-gc-race").with_extension("sqlite");
    let (writer, reader, physical) = db::open(&path).unwrap();
    let original = writer
        .accept(request(1, Mode::Pplns, "owner"))
        .await
        .unwrap();
    let mut preverified = request(1, Mode::Solo, "another-address");
    preverified.job_id = Hash384([99; 48]);
    preverified.key.name = "another-worker".into();
    preverified.time = 5_000;
    let retired = writer.submit(|done| pruning(done, 101)).await.unwrap();
    let outcome = writer.accept(preverified).await;
    retired.await.unwrap().unwrap();
    assert!(
        matches!(outcome,Err(AcceptanceError::Unknown(ref error))if error.contains("durably closed"))
    );
    assert_eq!(closed(&reader).await, 101);
    let counts=reader.read(|connection|connection.query_row(
        "SELECT (SELECT COUNT(*) FROM accepted_work),(SELECT COUNT(*) FROM shares),(SELECT COUNT(*) FROM miners),(SELECT COUNT(*) FROM workers)",
        [],|row|Ok((row.get::<_,u64>(0)?,row.get::<_,u64>(1)?,row.get::<_,u64>(2)?,row.get::<_,u64>(3)?)))).await.unwrap();
    assert_eq!(
        counts,
        (0, 1, 1, 1),
        "no second weight, claimant, worker or receipt is created"
    );
    assert_eq!(original.id, 1);
    close(writer, reader, physical).await;

    let (writer, reader, physical) = db::open(&path).unwrap();
    assert_eq!(closed(&reader).await, 101);
    assert!(
        writer
            .accept(request(1, Mode::Pplns, "owner"))
            .await
            .is_err(),
        "a transport restart cannot resurrect the forgotten canonical header"
    );
    let mut fresh = request(2, Mode::Pplns, "owner");
    fresh.window_end = 101;
    let fresh = writer.accept(fresh).await.unwrap();
    assert_eq!(
        fresh.id, 2,
        "sequence IDs remain monotonic across retirement"
    );
    assert!(
        fresh.fresh,
        "the inclusive still-open registration boundary is accepted"
    );
    close(writer, reader, physical).await;
    remove(&path);
}

#[tokio::test]
async fn an_existing_receipt_still_replays_its_original_ack_after_the_window_floor_advances() {
    let path = temp_path("pool-receipt-existing-ack").with_extension("sqlite");
    let (writer, reader, physical) = db::open(&path).unwrap();
    let mut young = request(1, Mode::Pplns, "owner");
    young.time = 5_000;
    let original = writer.accept(young).await.unwrap();
    writer.call(|done| pruning(done, 101)).await.unwrap();
    assert_eq!(closed(&reader).await, 101);
    let retry = writer
        .accept(request(1, Mode::Solo, "other"))
        .await
        .unwrap();
    assert!(!retry.fresh);
    assert_eq!(
        (retry.id, retry.miner, retry.worker, retry.mode),
        (original.id, original.miner, original.worker, original.mode)
    );
    let shares = reader
        .read(|connection| {
            connection.query_row("SELECT COUNT(*) FROM shares", [], |row| {
                row.get::<_, u64>(0)
            })
        })
        .await
        .unwrap();
    assert_eq!(shares, 1);
    close(writer, reader, physical).await;
    remove(&path);
}

#[tokio::test]
async fn zero_or_lower_retention_requests_cannot_reopen_closed_registration_windows() {
    let path = temp_path("pool-receipt-closed-monotonic").with_extension("sqlite");
    let (writer, reader, physical) = db::open(&path).unwrap();
    writer.call(|done| pruning(done, 101)).await.unwrap();
    writer.call(|done| pruning(done, 0)).await.unwrap();
    writer.call(|done| pruning(done, 50)).await.unwrap();
    assert_eq!(closed(&reader).await, 101);
    assert!(
        writer
            .accept(request(1, Mode::Pplns, "owner"))
            .await
            .is_err()
    );
    let mut next = request(2, Mode::Pplns, "owner");
    next.window_end = 101;
    assert_eq!(
        writer.accept(next).await.unwrap().id,
        1,
        "refused closed windows never consume an accepted-work sequence"
    );
    close(writer, reader, physical).await;
    remove(&path);
}

#[tokio::test]
async fn a_failed_real_retirement_rolls_back_the_floor_and_deletions_in_the_same_transaction() {
    let path = temp_path("pool-receipt-gc-rollback").with_extension("sqlite");
    let (writer, reader, physical) = db::open(&path).unwrap();
    writer
        .accept(request(1, Mode::Pplns, "owner"))
        .await
        .unwrap();
    let injection = db::connect(&path, false).unwrap();
    injection
        .execute_batch(
            "CREATE TRIGGER fail_retirement BEFORE DELETE ON accepted_work
        BEGIN SELECT RAISE(ABORT,'injected receipt retirement failure'); END;",
        )
        .unwrap();
    assert!(
        writer
            .call(|done| Op::PruneAccepted {
                shares_before: 2_000,
                hashrate_before: 0,
                splits_before: 0,
                sources_before: 0,
                receipts_before: 2_000,
                height: 101,
                done
            })
            .await
            .is_err()
    );
    assert_eq!(closed(&reader).await, 0);
    let counts = reader
        .read(|connection| {
            connection.query_row(
                "SELECT (SELECT COUNT(*) FROM accepted_work),(SELECT COUNT(*) FROM shares)",
                [],
                |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)),
            )
        })
        .await
        .unwrap();
    assert_eq!(
        counts,
        (1, 1),
        "the preexisting durable proof and its weight survive failed GC"
    );
    injection
        .execute_batch("DROP TRIGGER fail_retirement")
        .unwrap();
    drop(injection);
    writer.call(|done| pruning(done, 101)).await.unwrap();
    assert_eq!(closed(&reader).await, 101);
    assert!(
        writer
            .accept(request(1, Mode::Solo, "other"))
            .await
            .is_err()
    );
    close(writer, reader, physical).await;
    remove(&path);
}
