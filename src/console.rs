use std::{
    fs::{self, File, OpenOptions},
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};
use tracing_subscriber::{
    EnvFilter, fmt, fmt::time::ChronoLocal, fmt::writer::BoxMakeWriter, layer::SubscriberExt,
    util::SubscriberInitExt,
};

pub const TALLY_EVERY: Duration = Duration::from_secs(5);
const LOG_FILE_BYTES: u64 = 64 << 20;

pub struct LogOptions<'a> {
    pub debug: bool,
    pub color: bool,
    pub file: Option<&'a Path>,
}

/// `RUST_LOG` overrides the CLI log level.
pub fn init(options: LogOptions<'_>) -> io::Result<()> {
    let level = if options.debug { "debug" } else { "info" };
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("warn,tenrai_pool={level}")));
    let color = options.color && io::stdout().is_terminal() && enable_ansi();
    let console = fmt::layer()
        .with_target(options.debug)
        .with_timer(timer())
        .with_ansi(color)
        .with_writer(BoxMakeWriter::new(io::stdout));
    let file = match options.file {
        Some(path) => Some(
            fmt::layer()
                .with_ansi(false)
                .with_timer(timer())
                .with_writer(Mutex::new(RotatingFile::open(path, LOG_FILE_BYTES)?)),
        ),
        None => None,
    };
    tracing_subscriber::registry()
        .with(filter)
        .with(console)
        .with(file)
        .try_init()
        .map_err(io::Error::other)
}

/// Batch block submission logs.
#[derive(Default)]
pub struct Tally {
    counts: Mutex<Counts>,
}

#[derive(Default)]
struct Counts {
    accepted: u64,
    rejected: u64,
    stale: u64,
    heights: Option<(u64, u64)>,
    reason: Option<String>,
}

impl Tally {
    pub fn accepted(&self, height: u64) {
        self.update(|counts| {
            counts.accepted += 1;
            let (low, high) = counts.heights.unwrap_or((height, height));
            counts.heights = Some((low.min(height), high.max(height)));
        });
    }

    pub fn rejected(&self, reason: String) {
        self.update(|counts| {
            counts.rejected += 1;
            counts.reason = Some(reason);
        });
    }

    pub fn stale(&self) {
        self.update(|counts| counts.stale += 1);
    }

    pub fn log(&self) {
        let Some(counts) = self
            .counts
            .lock()
            .ok()
            .map(|mut counts| std::mem::take(&mut *counts))
        else {
            return;
        };
        let found = counts.accepted + counts.rejected + counts.stale;
        if found == 0 {
            return;
        }
        let heights = match counts.heights {
            Some((low, high)) if low == high => format!(" at height {low}"),
            Some((low, high)) => format!(" at heights {low}-{high}"),
            None => String::new(),
        };
        let mut parts = vec![format!("{} accepted{heights}", counts.accepted)];
        if counts.rejected > 0 {
            let reason = counts.reason.unwrap_or_default();
            parts.push(format!("{} rejected ({reason})", counts.rejected));
        }
        if counts.stale > 0 {
            parts.push(format!("{} stale", counts.stale));
        }
        let noun = if found == 1 { "block" } else { "blocks" };
        let line = format!(
            "{found} {noun} in the last {}s: {}",
            TALLY_EVERY.as_secs(),
            parts.join(", ")
        );
        if counts.rejected > 0 {
            tracing::warn!("{line}");
        } else {
            tracing::info!("{line}");
        }
    }

    fn update(&self, change: impl FnOnce(&mut Counts)) {
        if let Ok(mut counts) = self.counts.lock() {
            change(&mut counts);
        }
    }
}

struct RotatingFile {
    path: PathBuf,
    file: Option<File>,
    written: u64,
    limit: u64,
}

impl RotatingFile {
    fn open(path: &Path, limit: u64) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let written = file.metadata()?.len();
        Ok(Self {
            path: path.to_path_buf(),
            file: Some(file),
            written,
            limit,
        })
    }

    fn rotate(&mut self) -> io::Result<()> {
        drop(self.file.take());
        let mut old = self.path.clone().into_os_string();
        old.push(".old");
        let _ = fs::remove_file(&old);
        fs::rename(&self.path, &old)?;
        self.file = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?,
        );
        self.written = 0;
        Ok(())
    }
}

impl Write for RotatingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.written.saturating_add(buf.len() as u64) > self.limit
            && self.written > 0
            && let Err(error) = self.rotate()
        {
            eprintln!("cannot rotate {}: {error}", self.path.display());
        }
        if self.file.is_none() {
            self.file = Some(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)?,
            );
        }
        let file = self.file.as_mut().expect("opened above");
        let written = file.write(buf)?;
        self.written = self.written.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.as_mut().map_or(Ok(()), File::flush)
    }
}

fn timer() -> ChronoLocal {
    ChronoLocal::new("%Y-%m-%d %H:%M:%S".into())
}

#[cfg(windows)]
fn enable_ansi() -> bool {
    nu_ansi_term::enable_ansi_support().is_ok()
}

#[cfg(not(windows))]
fn enable_ansi() -> bool {
    true
}
