use super::*;
use crate::{db, test_support::temp_path};
#[tokio::test]
async fn optional_stage_cache_is_bounded_and_monotonic_without_controlling_money() {
    let path = temp_path("block-stage").with_extension("sqlite");
    let (writer, reader, thread) = db::open(&path).unwrap();
    let blocks = Blocks::new(reader.clone());
    let id = Hash384([1; 48]);
    blocks.advance(id, Stage::Registered);
    blocks.advance(id, Stage::Executed);
    blocks.advance(id, Stage::Registered);
    assert_eq!(blocks.stage(&id), Some(Stage::Executed));
    for n in 0..10_000_u64 {
        let mut hash = [0; 48];
        hash[..8].copy_from_slice(&n.to_le_bytes());
        blocks.advance(Hash384(hash), Stage::Registered);
    }
    assert!(blocks.stages.lock().unwrap().len() <= 4096);
    // A display-cache row never invents an unpaid block or a reward.
    assert!(blocks.get(id).await.unwrap().is_none());
    blocks.close(&id);
    assert_eq!(blocks.stage(&id), None);
    drop(blocks);
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
