use super::*;
use crate::{
    db::{
        self,
        accepted::{MAX_ACCEPTING, tests::request},
    },
    test_support::temp_path,
};
use std::time::Duration;
use tokio::sync::oneshot;

#[test]
fn telemetry_charges_owned_capacity_not_just_visible_rows_and_releases_failed_admission() {
    let budget = Budget::default();
    let mut rows = Vec::with_capacity(MAX_BYTES / size_of::<WorkerCounts>() + 1);
    rows.push((1, 0, 1, 0, 1));
    assert!(budget.take(Change::WorkerCounts(rows)).is_none());
    assert_eq!(budget.bytes.available_permits(), MAX_BYTES);
    assert_eq!(budget.records.available_permits(), MAX_RECORDS);
    let zero_bytes = Budget {
        bytes: Arc::new(Semaphore::new(0)),
        records: Arc::new(Semaphore::new(1)),
    };
    assert!(
        zero_bytes
            .take(Change::WorkerCounts(vec![(1, 0, 1, 0, 1)]))
            .is_none()
    );
    assert_eq!(
        zero_bytes.records.available_permits(),
        1,
        "failed byte admission returns its record slot"
    );
    assert!(budget.take(Change::WorkerCounts(Vec::new())).is_none());
    assert!(
        budget
            .take(Change::WorkerCounts(vec![(1, 0, 1, 0, 1); CHUNK_ROWS + 1]))
            .is_none()
    );
}

#[test]
fn periodic_chart_settings_share_the_same_capacity_budget_but_startup_identity_does_not() {
    let budget = Budget::default();
    let mut value = String::with_capacity(MAX_BYTES);
    value.push('x');
    assert!(
        budget
            .take(Change::Setting {
                key: "blocks",
                value
            })
            .is_none()
    );
    assert_eq!(budget.bytes.available_permits(), MAX_BYTES);
    let record = budget
        .take(Change::Setting {
            key: "round_work",
            value: "1,2".into(),
        })
        .unwrap();
    assert!(!Op::Telemetry(record).durable());
    assert_eq!(budget.records.available_permits(), MAX_RECORDS);
    assert!(
        !Op::Setting {
            key: "chain_id",
            value: "unchanged".into()
        }
        .urgent()
    );
}

#[test]
fn physical_batch_ownership_survives_the_payload_and_stops_at_the_exact_slot_bound() {
    let budget = Budget::default();
    let mut physical = Vec::new();
    for _ in 0..MAX_RECORDS {
        let record = budget
            .take(Change::WorkerCounts(vec![(1, 0, 1, 0, 1)]))
            .unwrap();
        let lease = record.keep_alive();
        drop(record); // apply may consume its allocation before the transaction commits.
        physical.push(lease);
    }
    assert_eq!(budget.records.available_permits(), 0);
    assert!(
        budget
            .take(Change::Hashrate {
                bucket: 1,
                rows: vec![(1, Mode::Pplns, 1)]
            })
            .is_none()
    );
    drop(physical);
    assert_eq!(budget.records.available_permits(), MAX_RECORDS);
    assert_eq!(budget.bytes.available_permits(), MAX_BYTES);
}

#[test]
fn failed_actual_transaction_rolls_back_telemetry_and_releases_its_physical_budget() {
    let budget = Budget::default();
    let mut connection = rusqlite::Connection::open_in_memory().unwrap();
    db::migrate(&mut connection).unwrap();
    let ids = db::BlockIds::load(&connection).unwrap();
    let record = budget
        .take(Change::Hashrate {
            bucket: 1,
            rows: vec![(1, Mode::Pplns, 5)],
        })
        .unwrap();
    let bad = budget
        .take(Change::WorkerCounts(vec![(1, 0, 1, 0, 1)]))
        .unwrap();
    connection.execute_batch("DROP TABLE workers").unwrap();
    super::super::commit(
        &mut connection,
        vec![Op::Telemetry(record), Op::Telemetry(bad)],
        &ids,
    );
    let count: u64 = connection
        .query_row("SELECT COUNT(*) FROM hashrate", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        count, 0,
        "a prior telemetry insert is rolled back with its failing batch"
    );
    assert_eq!(budget.records.available_permits(), MAX_RECORDS);
    assert_eq!(budget.bytes.available_permits(), MAX_BYTES);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_physical_writer_drops_only_excess_diagnostics_and_keeps_cancelled_money() {
    let path = temp_path("telemetry-byte-budget").with_extension("sqlite");
    let (writer, reader, worker) = db::open(&path).unwrap();
    let lock = db::connect(&path, false).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    writer.worker_counts(vec![(1, 0, 1, 0, 1); CHUNK_ROWS]);
    let (done, mut answer) = oneshot::channel();
    writer
        .sender
        .try_send(Op::Flush(done))
        .unwrap_or_else(|_| panic!("empty physical queue"));
    tokio::time::timeout(Duration::from_secs(3), async {
        while writer.queued() != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        answer.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    for _ in 1..MAX_RECORDS {
        writer.worker_counts(vec![(1, 0, 1, 0, 1); CHUNK_ROWS]);
    }
    assert_eq!(writer.telemetry.records.available_permits(), 0);
    let bytes_held = MAX_BYTES - writer.telemetry.bytes.available_permits();
    assert!(bytes_held <= MAX_BYTES);
    assert!(writer.queued() <= MAX_RECORDS);
    // These are intentionally lost; their payload cannot grow the queue or steal a money slot.
    for _ in 0..100 {
        writer.hashrate(7, vec![(1, Mode::Pplns, 5); CHUNK_ROWS]);
    }
    assert_eq!(
        MAX_BYTES - writer.telemetry.bytes.available_permits(),
        bytes_held
    );
    let submitter = writer.clone();
    let caller =
        tokio::spawn(async move { submitter.accept(request(1, Mode::Pplns, "owner")).await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while writer.accepting.available_permits() == MAX_ACCEPTING {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    caller.abort();
    let _ = caller.await;
    assert_eq!(writer.accepting.available_permits(), MAX_ACCEPTING - 1);
    assert_eq!(
        writer.telemetry.records.available_permits(),
        0,
        "future cancellation cannot free physical telemetry"
    );
    lock.execute_batch("COMMIT").unwrap();
    drop(lock);
    answer.await.unwrap();
    writer.flush().await;
    idle(&writer).await;
    assert_eq!(writer.telemetry.records.available_permits(), MAX_RECORDS);
    assert_eq!(writer.telemetry.bytes.available_permits(), MAX_BYTES);
    assert_eq!(writer.accepting.available_permits(), MAX_ACCEPTING);
    let receipt = writer
        .accept(request(1, Mode::Solo, "copied-proof"))
        .await
        .unwrap();
    assert!(!receipt.fresh);
    assert_eq!(receipt.mode, Mode::Pplns);
    assert_eq!(
        reader
            .read(
                |c| c.query_row("SELECT COUNT(*) FROM accepted_work", [], |r| r
                    .get::<_, u64>(0))
            )
            .await
            .unwrap(),
        1
    );
    writer.shutdown().await.unwrap();
    drop(reader);
    drop(writer);
    tokio::task::spawn_blocking(move || worker.join().unwrap())
        .await
        .unwrap();
    let (writer, reader, worker) = db::open(&path).unwrap();
    let again = writer
        .accept(request(1, Mode::Solo, "another-login"))
        .await
        .unwrap();
    assert!(!again.fresh);
    assert_eq!(again.id, receipt.id);
    writer.shutdown().await.unwrap();
    drop(reader);
    drop(writer);
    tokio::task::spawn_blocking(move || worker.join().unwrap())
        .await
        .unwrap();
    remove(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunking_retains_every_admitted_row_in_order_and_no_empty_operation() {
    let path = temp_path("telemetry-chunks").with_extension("sqlite");
    let (writer, reader, worker) = db::open(&path).unwrap();
    let rows: Vec<_> = (1..=2 * CHUNK_ROWS + 1)
        .map(|miner| (miner as u32, Mode::Pplns, miner as u64))
        .collect();
    writer.hashrate(99, rows);
    writer.worker_counts(Vec::new());
    writer.hashrate(99, Vec::new());
    writer.flush().await;
    let (count, work): (u64, u64) = reader
        .read(|c| {
            c.query_row(
                "SELECT COUNT(*),SUM(work) FROM hashrate WHERE bucket=99",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
        })
        .await
        .unwrap();
    let n = (2 * CHUNK_ROWS + 1) as u64;
    assert_eq!(count, n);
    assert_eq!(work, n * (n + 1) / 2);
    assert_eq!(writer.telemetry.records.available_permits(), MAX_RECORDS);
    writer.shutdown().await.unwrap();
    drop(reader);
    drop(writer);
    tokio::task::spawn_blocking(move || worker.join().unwrap())
        .await
        .unwrap();
    remove(&path);
}

async fn idle(writer: &Writer) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while writer.telemetry.records.available_permits() != MAX_RECORDS
            || writer.telemetry.bytes.available_permits() != MAX_BYTES
            || writer.accepting.available_permits() != MAX_ACCEPTING
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

fn remove(path: &std::path::Path) {
    for suffix in ["", "-wal", "-shm", ".pool-owner.lock"] {
        let mut name = path.as_os_str().to_owned();
        name.push(suffix);
        let _ = std::fs::remove_file(std::path::PathBuf::from(name));
    }
}
