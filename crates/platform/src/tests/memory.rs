use super::*;

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    windows
))]
#[test]
fn free_memory_is_queryable() {
    assert!(available_memory_bytes().unwrap() > 0);
}
