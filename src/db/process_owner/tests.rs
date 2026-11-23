use super::*;
use crate::db::{self, Reader, Writer};
use std::{
    io::{BufRead, Read, Write},
    process::{Command, Stdio},
    time::Duration,
};

type OpenDatabase = (Writer, Reader, std::thread::JoinHandle<()>);
const CHILD_PATH: &str = "TENRAI_POOL_PROCESS_OWNER_PROBE_PATH";
const CHILD_MODE: &str = "TENRAI_POOL_PROCESS_OWNER_PROBE_MODE";
const CHILD_TEST: &str = "db::process_owner::tests::process_owner_child_probe";

fn busy(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
    })
}

async fn close((writer, reader, thread): OpenDatabase) {
    writer.shutdown().await.unwrap();
    drop(reader);
    drop(writer);
    tokio::task::spawn_blocking(move || thread.join().unwrap())
        .await
        .unwrap();
}

async fn reopen(path: &Path) -> OpenDatabase {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match db::open(path) {
            Ok(opened) => return opened,
            Err(error) if busy(&error) && tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(error) => panic!("physical database owner did not drain: {error:#}"),
        }
    }
}

fn remove(path: &Path) {
    for suffix in ["", "-wal", "-shm", ".pool-owner.lock"] {
        let mut target = path.as_os_str().to_owned();
        target.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(target));
    }
}

#[tokio::test]
async fn a_second_pool_cannot_open_or_migrate_the_same_database_through_a_path_alias() {
    let path = crate::test_support::temp_path("pool-one-process").with_extension("sqlite");
    let first = db::open(&path).unwrap();
    let alias = path
        .parent()
        .unwrap()
        .join(".")
        .join(path.file_name().unwrap());
    assert!(busy(
        &db::open(&alias).err().expect("duplicate owner must fail")
    ));
    let version = first
        .1
        .read(|connection| {
            connection.pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
        })
        .await
        .unwrap();
    assert_eq!(version, db::SCHEMA_VERSION);
    close(first).await;
    close(db::open(&alias).unwrap()).await;
    remove(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_reader_keeps_process_ownership_until_its_physical_sqlite_connection_drains() {
    let path = crate::test_support::temp_path("pool-owner-physical-read").with_extension("sqlite");
    let (writer, reader, thread) = db::open(&path).unwrap();
    let reading = reader.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let caller = tokio::spawn(async move {
        reading
            .read(move |connection| {
                let _ = entered.send(());
                blocked.recv().map_err(|_| rusqlite::Error::InvalidQuery)?;
                connection.query_row("SELECT 1", [], |row| row.get::<_, i64>(0))
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .unwrap()
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    writer.shutdown().await.unwrap();
    drop(writer);
    drop(reader);
    thread.join().unwrap();
    assert!(busy(
        &db::open(&path)
            .err()
            .expect("the physical query still owns the database")
    ));
    release.send(()).unwrap();
    close(reopen(&path).await).await;
    remove(&path);
}

#[tokio::test]
async fn another_actual_process_is_refused_and_an_abrupt_owner_exit_releases_the_os_lease() {
    let path = crate::test_support::temp_path("pool-owner-child").with_extension("sqlite");
    let opened = db::open(&path).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", CHILD_TEST, "--nocapture"])
        .env(CHILD_PATH, &path)
        .env(CHILD_MODE, "busy")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child process owner check failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    close(opened).await;
    let mut holder = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", CHILD_TEST, "--nocapture"])
        .env(CHILD_PATH, &path)
        .env(CHILD_MODE, "hold")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut output = std::io::BufReader::new(holder.stdout.take().unwrap());
    loop {
        let mut line = String::new();
        assert_ne!(
            output.read_line(&mut line).unwrap(),
            0,
            "child did not acquire its lease"
        );
        if line.contains("POOL_PROCESS_OWNER_READY") {
            break;
        }
    }
    assert!(busy(
        &db::open(&path)
            .err()
            .expect("the other process must exclude us")
    ));
    holder.kill().unwrap();
    holder.wait().unwrap();
    close(reopen(&path).await).await;
    remove(&path);
}

#[test]
fn process_owner_child_probe() {
    let Some(path) = std::env::var_os(CHILD_PATH) else {
        return;
    };
    let path = PathBuf::from(path);
    match std::env::var(CHILD_MODE).unwrap().as_str() {
        "busy" => assert!(busy(
            &db::open(&path).err().expect("parent owns this database")
        )),
        "hold" => {
            let _opened = db::open(&path).unwrap();
            println!("POOL_PROCESS_OWNER_READY");
            std::io::stdout().flush().unwrap();
            let _ = std::io::stdin().read(&mut [0_u8; 1]);
        }
        _ => panic!("invalid process-owner probe mode"),
    }
}
