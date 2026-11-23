pub mod accepted;
mod checkpoint;
mod money;
mod process_owner;
mod worker_lifetime;
pub use worker_lifetime::WorkerId;
pub mod read;
pub mod writer;

use crate::chain::Hash384;
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub use writer::{BlockCredit, NewBlock, Op, Writer};

/// Hold the OS lease across physical work, even after async cancellation.
#[derive(Clone)]
pub(crate) struct ProcessLease {
    _owner: Arc<process_owner::ProcessOwner>,
}

const SCHEMA: &str = include_str!("schema.sql");
const SCHEMA_VERSION: i64 = 4;
const READERS: usize = 4;
/// Checkpoint on a separate connection to keep writes moving.
const CHECKPOINT_EVERY: Duration = Duration::from_secs(30);
const WAL_LIMIT_BYTES: i64 = 64 << 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockStatus {
    Found,
    Pending,
    Confirmed,
    Expired,
    Rejected,
}

impl BlockStatus {
    pub const fn code(self) -> i64 {
        match self {
            Self::Found => 0,
            Self::Pending => 1,
            Self::Confirmed => 2,
            Self::Expired => 3,
            Self::Rejected => 4,
        }
    }

    pub const fn from_code(code: i64) -> Self {
        match code {
            0 => Self::Found,
            1 => Self::Pending,
            2 => Self::Confirmed,
            3 => Self::Expired,
            _ => Self::Rejected,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Found => "found",
            Self::Pending => "pending",
            Self::Confirmed => "confirmed",
            Self::Expired => "expired",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayoutStatus {
    Prepared,
    Sent,
    Paid,
    Failed,
}

impl PayoutStatus {
    pub const fn code(self) -> i64 {
        match self {
            Self::Prepared => 0,
            Self::Sent => 1,
            Self::Paid => 2,
            Self::Failed => 3,
        }
    }

    pub const fn from_code(code: i64) -> Self {
        match code {
            0 => Self::Prepared,
            1 => Self::Sent,
            2 => Self::Paid,
            _ => Self::Failed,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Sent => "sent",
            Self::Paid => "paid",
            Self::Failed => "failed",
        }
    }
}

pub fn open(path: &Path) -> Result<(Writer, Reader, std::thread::JoinHandle<()>)> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("cannot create {}", parent.display()))?;
    }
    let (path, process_owner) = process_owner::ProcessOwner::acquire(path)?;
    let path = path.as_path();
    let mut connection = connect(path, false)?;
    migrate(&mut connection)?;
    let ids = BlockIds::load(&connection)?;
    let checkpoints = checkpoint::Owner::start(connect(path, false)?, Some(process_owner))?;
    let reader = Reader::new(path, ids.clone(), Arc::clone(&checkpoints))?;
    let (writer, thread) = Writer::start(connection, ids, checkpoints)?;
    Ok((writer, reader, thread))
}

pub fn connect(path: &Path, read_only: bool) -> Result<Connection> {
    anyhow::ensure!(
        rusqlite::version_number() >= 3_054_000,
        "the pool database requires SQLite 3.54.0 or later"
    );
    let flags = if read_only {
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX
    } else {
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
    };
    let connection = Connection::open_with_flags(path, flags)
        .with_context(|| format!("cannot open {}", path.display()))?;
    connection.busy_timeout(Duration::from_secs(10))?;
    if !read_only {
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "wal_autocheckpoint", 0)?;
        connection.pragma_update(None, "journal_size_limit", WAL_LIMIT_BYTES)?;
    }
    connection.pragma_update(None, "temp_store", "FILE")?;
    connection.pragma_update(None, "cache_size", -16_384)?;
    Ok(connection)
}

fn migrate(connection: &mut Connection) -> Result<()> {
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version != 0 && version != SCHEMA_VERSION {
        bail!(
            "the database has schema version {version}; this pool reads version {SCHEMA_VERSION}"
        );
    }
    if version == 0 {
        let nonempty: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%')",
            [],
            |row| row.get(0),
        )?;
        anyhow::ensure!(!nonempty, "an unversioned pool database must be empty");
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(SCHEMA)?;
    money::validate(&transaction)?;
    worker_lifetime::validate(&transaction)?;
    // Validate the work layout before updating the schema version.
    transaction.prepare("SELECT progress FROM blocks LIMIT 0")?;
    transaction.prepare("SELECT job_id,nonce FROM accepted_work LIMIT 0")?;
    transaction.prepare("SELECT retry_height,retry_at FROM found_payload LIMIT 0")?;
    transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

pub fn setting(connection: &Connection, key: &str) -> rusqlite::Result<Option<String>> {
    connection
        .query_row("SELECT value FROM settings WHERE key = ?1", [key], |row| {
            row.get(0)
        })
        .optional()
}

pub fn put_setting(connection: &Connection, key: &str, value: &str) -> rusqlite::Result<()> {
    connection.execute(
        "INSERT INTO settings (key, value) VALUES (?1, ?2)
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        params![key, value],
    )?;
    Ok(())
}

/// Cache block IDs to avoid maintaining a random-hash index on every write.
#[derive(Clone, Default)]
pub struct BlockIds(Arc<Mutex<HashMap<Hash384, i64>>>);

impl BlockIds {
    fn load(connection: &Connection) -> Result<Self> {
        let mut statement =
            connection.prepare("SELECT id, hash FROM blocks ORDER BY id DESC LIMIT 4096")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut ids = HashMap::new();
        for row in rows {
            let (id, hash) = row?;
            if let Some(hash) = Hash384::from_hex(&hash) {
                ids.insert(hash, id);
            }
        }
        Ok(Self(Arc::new(Mutex::new(ids))))
    }

    fn get(&self, hash: Hash384) -> Option<i64> {
        self.0.lock().ok()?.get(&hash).copied()
    }

    fn change(&self, added: Vec<(Hash384, i64)>, removed: Vec<Hash384>) {
        if let Ok(mut ids) = self.0.lock() {
            for hash in removed {
                ids.remove(&hash);
            }
            for (hash, id) in added {
                if ids.len() >= 4096 && !ids.contains_key(&hash) {
                    ids.clear();
                }
                ids.insert(hash, id);
            }
        }
    }
}

/// Read-only connections use WAL snapshots while the writer commits.
#[derive(Clone)]
pub struct Reader {
    inner: Arc<ReaderPool>,
    ids: BlockIds,
}

struct ReaderPool {
    path: PathBuf,
    idle: Mutex<Vec<Connection>>,
    permits: Arc<Semaphore>,
    _checkpoint: Arc<checkpoint::Owner>,
}

impl Reader {
    fn new(path: &Path, ids: BlockIds, checkpoint: Arc<checkpoint::Owner>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(ReaderPool {
                path: path.to_path_buf(),
                idle: Mutex::new(Vec::new()),
                permits: Arc::new(Semaphore::new(READERS)),
                _checkpoint: checkpoint,
            }),
            ids,
        })
    }

    /// Wait for outstanding physical reads.
    pub async fn drain(&self) -> Result<()> {
        let _all = Arc::clone(&self.inner.permits)
            .acquire_many_owned(READERS as u32)
            .await?;
        Ok(())
    }
    pub async fn block(&self, hash: &str) -> Result<Option<read::BlockDetail>> {
        let Some(hash) = Hash384::from_hex(hash) else {
            return Ok(None);
        };
        let cached = self.ids.get(hash);
        self.read(move |connection| {
            let id = match cached {
                Some(id) => Some(id),
                None => connection
                    .query_row(
                        "SELECT id FROM blocks WHERE hash=?1",
                        [hash.to_hex()],
                        |row| row.get(0),
                    )
                    .optional()?,
            };
            id.map(|id| read::block(connection, id))
                .transpose()
                .map(|row| row.flatten().filter(|row| row.0.hash == hash.to_hex()))
        })
        .await
    }

    /// Reject new reads when all physical slots are in use.
    pub async fn try_read<T, F>(&self, query: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
    {
        let permit = Arc::clone(&self.inner.permits).try_acquire_owned()?;
        self.read_with_permit(permit, query).await
    }

    pub async fn read<T, F>(&self, query: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
    {
        let permit = Arc::clone(&self.inner.permits).acquire_owned().await?;
        self.read_with_permit(permit, query).await
    }

    async fn read_with_permit<T, F>(&self, permit: OwnedSemaphorePermit, query: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
    {
        let query_pool = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            // Drop the pool reference before returning the slot.
            let _permit = permit;
            let pool = query_pool;
            let connection = match pool.idle.lock().ok().and_then(|mut idle| idle.pop()) {
                Some(connection) => connection,
                None => connect(&pool.path, true)?,
            };
            let result = query(&connection);
            if let Ok(mut idle) = pool.idle.lock() {
                idle.push(connection);
            }
            Ok(result?)
        })
        .await?
    }
}

pub fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Convert pool atoms to SQLite's signed integer type.
pub fn int(value: u128) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub fn uint(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

#[cfg(test)]
#[path = "../tests/db.rs"]
mod tests;
