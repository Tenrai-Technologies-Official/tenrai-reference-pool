use super::*;
use crate::{db, test_support::temp_path};

#[tokio::test]
async fn bounded_cache_eviction_uses_indexed_persistent_identity_without_reassigning_ids() {
    let path = temp_path("registry-cache").with_extension("sqlite");
    let (writer, reader, thread) = db::open(&path).unwrap();
    {
        let mut connection = db::connect(&path, false).unwrap();
        let tx = connection.transaction().unwrap();
        let mut insert = tx
            .prepare("INSERT INTO miners(id,address,first_seen) VALUES(?1,?2,0)")
            .unwrap();
        for n in 1..=MAX_REGISTRY_CACHE + 2 {
            insert
                .execute(rusqlite::params![n as u32, format!("miner{n}")])
                .unwrap();
        }
        drop(insert);
        tx.commit().unwrap();
    }
    let registry = Registry::load(reader.clone(), writer.clone())
        .await
        .unwrap();
    assert_eq!(
        registry.cached(),
        0,
        "startup reads the scalar high-water ID, not every registry row"
    );
    for n in 1..=MAX_REGISTRY_CACHE + 2 {
        registry.remember(&format!("miner{n}"), n as u32);
    }
    assert_eq!(registry.cached(), MAX_REGISTRY_CACHE);
    assert_eq!(registry.find("miner1").await.unwrap(), Some(1));
    assert_eq!(
        registry.address(2).await.unwrap().as_deref(),
        Some("miner2")
    );
    assert_eq!(registry.miner("miner1").await.unwrap(), 1);
    let count: u64 = reader
        .read(|c| c.query_row("SELECT COUNT(*) FROM miners", [], |r| r.get(0)))
        .await
        .unwrap();
    assert_eq!(
        count,
        MAX_REGISTRY_CACHE as u64 + 2,
        "read-through never creates a second identity"
    );
    assert!(registry.cached() <= MAX_REGISTRY_CACHE);
    drop(registry);
    writer.shutdown().await.unwrap();
    reader.drain().await.unwrap();
    drop(reader);
    drop(writer);
    tokio::task::spawn_blocking(move || thread.join().unwrap())
        .await
        .unwrap();
    for suffix in ["", "-wal", "-shm"] {
        let mut name = path.as_os_str().to_owned();
        name.push(suffix);
        let _ = std::fs::remove_file(std::path::PathBuf::from(name));
    }
}
