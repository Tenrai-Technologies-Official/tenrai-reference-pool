//! Available memory per operating system.

use std::io;

#[cfg(any(target_os = "linux", target_os = "android", test))]
#[cfg_attr(not(any(target_os = "linux", target_os = "android")), allow(dead_code))]
mod linux_memory;
#[cfg(target_os = "macos")]
mod macos_memory;
#[cfg(windows)]
mod windows_memory;

/// Returns the bytes of memory available without swapping, within the cgroup limits on Linux.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn available_memory_bytes() -> io::Result<u64> {
    linux_memory::available_memory_bytes()
}

/// Returns the bytes of memory available without swapping.
#[cfg(target_os = "macos")]
pub fn available_memory_bytes() -> io::Result<u64> {
    macos_memory::available_memory_bytes()
}

/// Returns the bytes of physical memory available.
#[cfg(windows)]
pub fn available_memory_bytes() -> io::Result<u64> {
    windows_memory::available_memory_bytes()
}

/// Returns the bytes of memory available without swapping.
#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    windows
)))]
pub fn available_memory_bytes() -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "free-memory queries are not implemented for this platform",
    ))
}

#[cfg(test)]
#[path = "tests/memory.rs"]
mod tests;

mod exclusive_file_lock;
pub use exclusive_file_lock::{ExclusiveFileLock, lock_exclusive_file};
