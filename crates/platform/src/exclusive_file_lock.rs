use std::{
    fs::{File, OpenOptions},
    io,
    path::Path,
};

/// A nonblocking process lease; the last real file owner releases the OS lock.
pub struct ExclusiveFileLock {
    _file: File,
}

/// Takes an exclusive process lock without using a PID file or a time-based expiry.
pub fn lock_exclusive_file(path: &Path) -> io::Result<ExclusiveFileLock> {
    match path.symlink_metadata() {
        Ok(metadata) if metadata.file_type().is_symlink() || reparse_point(&metadata) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "lock path must not be a link",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "lock path must be a regular file",
        ));
    }
    file.try_lock().map_err(io::Error::from)?;
    Ok(ExclusiveFileLock { _file: file })
}

#[cfg(windows)]
fn reparse_point(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn reparse_point(_: &std::fs::Metadata) -> bool {
    false
}

#[cfg(test)]
#[path = "tests/exclusive_file_lock.rs"]
mod tests;
