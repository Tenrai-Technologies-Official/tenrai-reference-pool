use anyhow::{Context, Result, bail};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tenrai_platform::{ExclusiveFileLock, lock_exclusive_file};

/// Share the database's OS lease across writer, reader and checkpoint handles.
/// The lease also covers proof verification and payout scheduling.
pub(super) struct ProcessOwner {
    _lease: ExclusiveFileLock,
}

impl ProcessOwner {
    pub(super) fn acquire(path: &Path) -> Result<(PathBuf, Arc<Self>)> {
        let canonical = match path.symlink_metadata() {
            Ok(_) => {
                let canonical = std::fs::canonicalize(path)
                    .with_context(|| format!("cannot resolve pool database {}", path.display()))?;
                if !canonical.metadata()?.is_file() {
                    bail!("the pool database must be a regular file");
                }
                canonical
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let parent = path
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."));
                let parent = std::fs::canonicalize(parent).with_context(|| {
                    format!(
                        "cannot resolve pool database directory {}",
                        parent.display()
                    )
                })?;
                let name = path
                    .file_name()
                    .context("the pool database needs a file name")?;
                parent.join(name)
            }
            Err(error) => return Err(error.into()),
        };
        let mut lock = canonical.as_os_str().to_owned();
        lock.push(".pool-owner.lock");
        let lock = PathBuf::from(lock);
        let lease = lock_exclusive_file(&lock).with_context(|| {
            format!(
                "cannot exclusively own {}; another pool may still be using it",
                canonical.display()
            )
        })?;
        Ok((canonical, Arc::new(Self { _lease: lease })))
    }
}

#[cfg(test)]
mod tests;
