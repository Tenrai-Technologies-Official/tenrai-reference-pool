use super::*;
use crate::{
    config::Mode,
    db::{
        self,
        accepted::{
            ReplayReceipt,
            pending::{MAX_PENDING_WORK, Refused},
            tests::request,
        },
    },
    test_support::temp_path,
};
use std::sync::{Arc, Condvar, Mutex};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_queued_acceptance_is_fenced_before_a_reconnect_can_read_absent() {
    let path = temp_path("replay-fence").with_extension("sqlite");
    let (writer, reader, thread) = db::open(&path).unwrap();
    let lock = db::connect(&path, false).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    let work = request(1, Mode::Pplns, "owner");
    let job = work.job_id;
    let nonce = work.header[248..280].try_into().unwrap();
    let lease = writer.claim_work(job, nonce).unwrap();
    let permit = Arc::new(Arc::clone(&writer.accepting).try_acquire_owned().unwrap());
    let (done, answer) = oneshot::channel();
    // Synchronous enqueue fixes the exact FIFO ordering; dropping the caller cannot undo it.
    writer
        .sender
        .try_send(Op::AcceptWork {
            request: Box::new(work),
            permit,
            pending: Some(lease),
            done,
        })
        .unwrap_or_else(|_| panic!("empty writer queue"));
    drop(answer);
    let replay_writer = writer.clone();
    let replay =
        tokio::spawn(async move { replay_writer.replay_work(job, nonce, "owner".into()).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while writer.replaying_work() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(!replay.is_finished());
    assert_eq!(writer.pending_work(), 1);
    lock.execute_batch("COMMIT").unwrap();
    drop(lock);
    let ReplayReceipt::Committed(receipt) = replay.await.unwrap().unwrap() else {
        panic!("FIFO replay missed the committed cancelled write");
    };
    assert_eq!(
        (receipt.id, receipt.miner, receipt.mode),
        (1, 1, Mode::Pplns)
    );
    assert!(!receipt.fresh);
    writer.flush().await;
    assert_eq!(writer.pending_work(), 0);
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
    tokio::task::spawn_blocking(move || thread.join().unwrap())
        .await
        .unwrap();
    remove(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_physical_verification_remains_unknown_until_its_real_owner_finishes() {
    let path = temp_path("replay-verifier").with_extension("sqlite");
    let (writer, reader, thread) = db::open(&path).unwrap();
    let job = Hash384([7; 48]);
    let nonce = [1; 32];
    let lease = writer.claim_work(job, nonce).unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let physical_gate = Arc::clone(&gate);
    let (started, ready) = oneshot::channel();
    let physical = tokio::task::spawn_blocking(move || {
        let _lease = lease;
        let _ = started.send(());
        let (lock, wake) = &*physical_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = wake.wait(released).unwrap();
        }
    });
    ready.await.unwrap();
    assert_eq!(
        writer
            .replay_work(job, nonce, "owner".into())
            .await
            .unwrap(),
        ReplayReceipt::UnknownPending
    );
    assert_eq!(writer.pending_work(), 1);
    {
        let (lock, wake) = &*gate;
        *lock.lock().unwrap() = true;
        wake.notify_all();
    }
    physical.await.unwrap();
    assert_eq!(
        writer
            .replay_work(job, nonce, "owner".into())
            .await
            .unwrap(),
        ReplayReceipt::Absent
    );
    assert_eq!(writer.pending_work(), 0);
    writer.shutdown().await.unwrap();
    drop(reader);
    drop(writer);
    tokio::task::spawn_blocking(move || thread.join().unwrap())
        .await
        .unwrap();
    remove(&path);
}

#[tokio::test]
async fn intake_identity_capacity_and_duplicate_claims_are_bounded_without_evicting_work() {
    let path = temp_path("replay-cap").with_extension("sqlite");
    let (writer, reader, thread) = db::open(&path).unwrap();
    let job = Hash384([7; 48]);
    let mut leases = Vec::new();
    for n in 0..MAX_PENDING_WORK {
        let mut nonce = [0; 32];
        nonce[..8].copy_from_slice(&(n as u64).to_le_bytes());
        leases.push(writer.claim_work(job, nonce).unwrap());
    }
    assert_eq!(writer.pending_work(), MAX_PENDING_WORK);
    assert!(matches!(
        writer.claim_work(job, [0; 32]),
        Err(Refused::Pending)
    ));
    assert!(matches!(
        writer.claim_work(job, [255; 32]),
        Err(Refused::Busy)
    ));
    drop(leases);
    assert_eq!(writer.pending_work(), 0);
    writer.shutdown().await.unwrap();
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
