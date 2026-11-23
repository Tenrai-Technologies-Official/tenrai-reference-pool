use super::*;
use crate::db::{self, Op, READERS};

#[test]
fn checkpoint_worker_and_connection_are_physically_drained_by_the_last_owner() {
    let connection = Connection::open_in_memory().unwrap();
    let owner = Owner::start(connection, None).unwrap();
    let finished = Arc::downgrade(&owner.stop);
    let held = Arc::clone(&owner);
    drop(owner);
    assert!(finished.upgrade().is_some());
    drop(held); // Joins the actual worker; this is not a logical stopped flag.
    assert!(finished.upgrade().is_none());
}

#[tokio::test]
async fn repeated_database_open_drop_reopen_releases_each_checkpoint_and_keeps_committed_rows() {
    let path = crate::test_support::temp_path("checkpoint-lifetime").with_extension("sqlite");
    for round in 0..8 {
        let (writer, reader, worker) = db::open(&path).unwrap();
        let finished = Arc::downgrade(&reader.inner._checkpoint.stop);
        writer.send(Op::Setting {
            key: "resource-round",
            value: round.to_string(),
        });
        writer.flush().await;
        assert_eq!(
            reader
                .read(|connection| db::setting(connection, "resource-round"))
                .await
                .unwrap(),
            Some(round.to_string())
        );
        drop(writer);
        drop(reader);
        worker.join().unwrap();
        assert!(
            finished.upgrade().is_none(),
            "no checkpoint thread may retain the database after close"
        );
    }
    // No owned writer/read/checkpoint connection remains. Exercise the Windows-relevant file lifetime too.
    let renamed = path.with_extension("closed.sqlite");
    std::fs::rename(&path, &renamed).unwrap();
    let connection = Connection::open(&renamed).unwrap();
    assert_eq!(
        db::setting(&connection, "resource-round").unwrap(),
        Some("7".into())
    );
    drop(connection);
    std::fs::remove_file(renamed).unwrap();
}

#[tokio::test]
async fn cancelled_async_readers_keep_the_physical_query_budget_until_work_really_finishes() {
    let path = crate::test_support::temp_path("reader-physical-permits").with_extension("sqlite");
    let (writer, reader, worker) = db::open(&path).unwrap();
    let finished = Arc::downgrade(&reader.inner._checkpoint.stop);
    let capacity = Arc::clone(&reader.inner.permits);
    let mut releases = Vec::new();
    let mut callers = Vec::new();
    for _ in 0..READERS {
        let reading = reader.clone();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        callers.push(tokio::spawn(async move {
            reading
                .read(move |connection| {
                    let _ = entered.send(());
                    blocked.recv().map_err(|_| rusqlite::Error::InvalidQuery)?;
                    connection.query_row("SELECT 1", [], |row| row.get::<_, i64>(0))
                })
                .await
        }));
        tokio::time::timeout(Duration::from_secs(10), started)
            .await
            .unwrap()
            .unwrap();
        releases.push(release);
    }
    assert_eq!(capacity.available_permits(), 0);
    for caller in callers {
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
    }
    assert_eq!(
        capacity.available_permits(),
        0,
        "aborting the HTTP/async caller cannot create another physical query slot"
    );
    let reading = reader.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let next = tokio::spawn(async move {
        reading
            .read(move |connection| {
                let _ = entered.send(());
                connection.query_row("SELECT 2", [], |row| row.get::<_, i64>(0))
            })
            .await
    });
    assert!(finished.upgrade().is_some());
    releases.remove(0).send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), started)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.await.unwrap().unwrap(), 2);
    for release in releases {
        release.send(()).unwrap();
    }
    let all = tokio::time::timeout(
        Duration::from_secs(10),
        Arc::clone(&capacity).acquire_many_owned(READERS as u32),
    )
    .await
    .unwrap()
    .unwrap();
    drop(all);
    assert_eq!(capacity.available_permits(), READERS);
    assert!(reader.inner.idle.lock().unwrap().len() <= READERS);
    drop(writer);
    drop(reader);
    worker.join().unwrap();
    assert!(finished.upgrade().is_none());
}
