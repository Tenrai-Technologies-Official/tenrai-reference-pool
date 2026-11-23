use super::*;
use crate::{
    config::Mode,
    db::{
        self,
        accepted::{AcceptanceError, MAX_ACCEPTING, tests::request},
    },
    test_support::temp_path,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caller_cancellation_cannot_release_physical_write_slots_or_lose_committed_work() {
    let path = temp_path("accepted-cancel").with_extension("sqlite");
    let (writer, reader, thread) = db::open(&path).unwrap();
    let lock = db::connect(&path, false).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    let mut callers = Vec::new();
    for n in 1..=MAX_ACCEPTING {
        let writer = writer.clone();
        callers.push(tokio::spawn(async move {
            writer.accept(request(n as u64, Mode::Pplns, "owner")).await
        }));
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while writer.accepting.available_permits() != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    for caller in callers {
        caller.abort();
        let _ = caller.await;
    }
    assert_eq!(
        writer.accepting.available_permits(),
        0,
        "the physical writer still owns every cancelled request"
    );
    assert!(matches!(
        writer.accept(request(9999, Mode::Pplns, "other")).await,
        Err(AcceptanceError::Busy)
    ));
    lock.execute_batch("COMMIT").unwrap();
    drop(lock);
    writer.flush().await;
    assert_eq!(writer.accepting.available_permits(), MAX_ACCEPTING);
    let count: u64 = reader
        .read(|c| c.query_row("SELECT COUNT(*) FROM accepted_work", [], |r| r.get(0)))
        .await
        .unwrap();
    assert_eq!(count, MAX_ACCEPTING as u64);
    let again = writer
        .accept(request(1, Mode::Solo, "other"))
        .await
        .unwrap();
    assert!(!again.fresh);
    assert_eq!(again.mode, Mode::Pplns);
    drop(reader);
    drop(writer);
    tokio::task::spawn_blocking(move || thread.join().unwrap())
        .await
        .unwrap();
    remove(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bad_intent_in_one_batch_does_not_rollback_other_miners_accepted_work() {
    let path = temp_path("accepted-savepoint").with_extension("sqlite");
    let (writer, reader, thread) = db::open(&path).unwrap();
    let mut bad = request(1, Mode::Pplns, "bad");
    bad.weight = 0;
    let (a, b, c) = tokio::join!(
        writer.accept(request(2, Mode::Pplns, "first")),
        writer.accept(bad),
        writer.accept(request(3, Mode::Pplns, "second"))
    );
    assert!(a.unwrap().fresh);
    assert!(matches!(b, Err(AcceptanceError::Unknown(_))));
    assert!(c.unwrap().fresh);
    let (shares, miners): (u64, u64) = reader
        .read(|c| {
            c.query_row(
                "SELECT (SELECT COUNT(*) FROM shares),(SELECT COUNT(*) FROM miners)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
        })
        .await
        .unwrap();
    assert_eq!((shares, miners), (2, 2));
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
