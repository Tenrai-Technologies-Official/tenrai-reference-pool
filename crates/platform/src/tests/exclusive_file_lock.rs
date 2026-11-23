use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "tenrai-pool-process-lock-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

#[test]
fn another_handle_cannot_own_the_same_process_lease_until_the_actual_file_drops() {
    let path = path();
    let first = lock_exclusive_file(&path).unwrap();
    let error = lock_exclusive_file(&path)
        .err()
        .expect("a second owner must be refused");
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    drop(first);
    let second = lock_exclusive_file(&path).unwrap();
    drop(second);
    std::fs::remove_file(path).unwrap();
}

#[cfg(unix)]
#[test]
fn a_lock_symlink_is_rejected_without_touching_its_target() {
    let target = path();
    let link = path();
    std::fs::write(&target, b"unchanged").unwrap();
    std::os::unix::fs::symlink(&target, &link).unwrap();
    assert_eq!(
        lock_exclusive_file(&link).err().unwrap().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"unchanged");
    std::fs::remove_file(link).unwrap();
    std::fs::remove_file(target).unwrap();
}
