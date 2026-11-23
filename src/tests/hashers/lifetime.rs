use super::*;
use crate::{db, test_support::temp_path};
use std::process::Command;

async fn stop_db(opened: (db::Writer, db::Reader, std::thread::JoinHandle<()>)) {
    let (writer, reader, thread) = opened;
    writer.shutdown().await.unwrap();
    drop((writer, reader));
    tokio::task::spawn_blocking(move || thread.join().unwrap())
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
async fn pause(
    hashers: &Hashers,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    std::sync::mpsc::Sender<()>,
) {
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    hashers.lifetime.pause_next(entered, blocked);
    (started, release)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborting_real_tenq_setup_retains_the_os_owner_until_its_actual_result_is_released() {
    let path = temp_path("pool-physical-tenq-owner").with_extension("sqlite");
    let opened = db::open(&path).unwrap();
    let hashers = Arc::new(Hashers::new(
        profile_for(Network::Development),
        CHAIN,
        TenQMode::Light,
        1,
        false,
        opened.0.process_lease().unwrap(),
    ));
    let (started, release) = pause(&hashers).await;
    let caller = {
        let hashers = Arc::clone(&hashers);
        tokio::spawn(async move { hashers.ensure(0).await })
    };
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .unwrap()
        .unwrap();
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    drop(hashers);
    stop_db(opened).await;
    // Only the physical setup owns this database now. An actual second process must fail.
    let probe_path = path.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "db::process_owner::tests::process_owner_child_probe",
                "--nocapture",
            ])
            .env("TENRAI_POOL_PROCESS_OWNER_PROBE_PATH", probe_path)
            .env("TENRAI_POOL_PROCESS_OWNER_PROBE_MODE", "busy")
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "second process crossed physical setup: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(db::open(&path).is_err());
    release.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let reopened = loop {
        match db::open(&path) {
            Ok(opened) => break opened,
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(error) => panic!("physical setup did not release process owner: {error:#}"),
        }
    };
    stop_db(reopened).await;
    remove(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_a_setup_caller_does_not_admit_a_second_physical_build() {
    let hashers = hashers(TenQMode::Light, || None);
    let (first_started, release_first) = pause(&hashers).await;
    let first = {
        let hashers = Arc::clone(&hashers);
        tokio::spawn(async move { hashers.ensure(0).await })
    };
    first_started.await.unwrap();
    first.abort();
    assert!(matches!(first.await, Err(error) if error.is_cancelled()));
    let (mut second_started, release_second) = pause(&hashers).await;
    let second = {
        let hashers = Arc::clone(&hashers);
        tokio::spawn(async move { hashers.ensure(1).await })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while hashers.lifetime.waiters() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        matches!(
            second_started.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "a cancelled caller released physical capacity early"
    );
    release_first.send(()).unwrap();
    second_started.await.unwrap();
    release_second.send(()).unwrap();
    assert_eq!(second.await.unwrap().unwrap().epoch(), 1);
    hashers.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_drains_prefetch_and_rejects_new_setup_instead_of_detaching_it() {
    let hashers = hashers(TenQMode::Light, || None);
    hashers.ensure(0).await.unwrap();
    let (started, release) = pause(&hashers).await;
    hashers.advance(epoch_start(1) - 2);
    started.await.unwrap();
    let closing = {
        let hashers = Arc::clone(&hashers);
        tokio::spawn(async move { hashers.close().await })
    };
    while !hashers.lifetime.closed() {
        tokio::task::yield_now().await;
    }
    assert!(!closing.is_finished());
    hashers.advance(epoch_start(2) - 2);
    assert!(hashers.ensure(9).await.is_err());
    release.send(()).unwrap();
    closing.await.unwrap().unwrap();
    assert!(!hashers.lifetime.prefetch_running());
    assert!(hashers.epochs().is_empty());
}

#[tokio::test]
async fn closing_a_source_drains_its_task_and_all_clones_refuse_new_work() {
    let hashers = hashers(TenQMode::Light, || None);
    let url = url::Url::parse("http://127.0.0.1:9/").unwrap();
    let node = NodeClient::new(url.clone(), url).unwrap();
    let (heights, _) = watch::channel(0);
    let stats = Arc::new(Stats::new(
        Default::default(),
        [0, 0],
        crate::day::Day::default(),
    ));
    let weak = Arc::downgrade(&stats);
    let source = WorkSource::start(
        node,
        (
            crate::test_support::address(1),
            crate::test_support::lock(1),
        ),
        CHAIN,
        Arc::clone(&hashers),
        heights,
        50,
        stats,
    );
    let cloned = source.clone();
    source.close().await.unwrap();
    assert!(source.request(None).is_none());
    assert!(cloned.request(None).is_none());
    assert!(source.filler().is_none());
    assert!(weak.upgrade().is_none());
    assert!(hashers.ensure(0).await.is_err());
    drop((source, cloned));
    assert_eq!(Arc::strong_count(&hashers), 1);
}

#[tokio::test]
async fn a_resident_epoch_cache_keeps_its_process_owner_after_setup_and_source_close() {
    let path = temp_path("pool-resident-tenq-owner").with_extension("sqlite");
    let opened = db::open(&path).unwrap();
    let hashers = Arc::new(Hashers::new(
        profile_for(Network::Development),
        CHAIN,
        TenQMode::Light,
        1,
        false,
        opened.0.process_lease().unwrap(),
    ));
    let resident = hashers.ensure(0).await.unwrap();
    hashers.close().await.unwrap();
    drop(hashers);
    stop_db(opened).await;
    assert!(
        db::open(&path).is_err(),
        "resident cache bytes must keep the originating OS owner"
    );
    drop(resident);
    stop_db(db::open(&path).unwrap()).await;
    remove(&path);
}
