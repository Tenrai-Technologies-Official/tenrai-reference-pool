//! Bounded local aliases. Financial records retain their original, never-reused worker ID.
use anyhow::{Result, anyhow, ensure};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

pub type WorkerId = u64;
/// SQLite stores this exactly, as can consumers that expose the local ID in JSON.
pub(super) const MAX_WORKER_ID: WorkerId = (1_u64 << 53) - 1;
const MAX_ALIASES: u64 = 262_144;
const MAX_MINER_ALIASES: u64 = 1_024;

#[derive(Clone, Copy)]
struct Catalogue {
    live: u64,
    last: WorkerId,
    after: WorkerId,
    through: WorkerId,
}

fn catalogue(connection: &Connection) -> Result<Catalogue> {
    let row = connection.query_row(
        "SELECT live_count,last_id,gc_after,gc_through FROM worker_catalogue WHERE id=1",
        [],
        |row| {
            Ok(Catalogue {
                live: row.get(0)?,
                last: row.get(1)?,
                after: row.get(2)?,
                through: row.get(3)?,
            })
        },
    )?;
    ensure!(
        row.live <= MAX_ALIASES
            && row.last <= MAX_WORKER_ID
            && row.after <= row.through
            && row.through <= row.last,
        "the worker catalogue is malformed"
    );
    Ok(row)
}

/// One bounded catalogue scan at startup detects inconsistent persisted accounting.
pub(super) fn validate(connection: &Connection) -> Result<()> {
    let known = catalogue(connection)?;
    let (count, maximum): (u64, WorkerId) = connection.query_row(
        "SELECT COUNT(*),COALESCE(MAX(id),0) FROM workers",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    ensure!(
        count == known.live && maximum <= known.last,
        "the worker catalogue does not match the stored aliases"
    );
    let largest: u64 = connection.query_row(
        "SELECT COALESCE(MAX(n),0) FROM (SELECT COUNT(*) AS n FROM workers GROUP BY miner_id)",
        [],
        |row| row.get(0),
    )?;
    ensure!(
        largest <= MAX_MINER_ALIASES,
        "a miner exceeds the worker alias capacity"
    );
    Ok(())
}

pub(super) fn worker(
    transaction: &Transaction<'_>,
    miner: u32,
    name: &str,
    at: u64,
) -> Result<WorkerId> {
    ensure!(
        miner > 0 && !name.is_empty() && name.len() <= 128,
        "invalid accepted-work worker name"
    );
    if let Some(id) = transaction
        .query_row(
            "SELECT id FROM workers WHERE miner_id=?1 AND name=?2",
            params![miner, name],
            |row| row.get::<_, WorkerId>(0),
        )
        .optional()?
    {
        ensure!(id > 0 && id <= MAX_WORKER_ID, "invalid stored worker ID");
        return Ok(id);
    }
    let known = catalogue(transaction)?;
    ensure!(
        known.live < MAX_ALIASES,
        "the pool's worker alias capacity is full; reuse an existing name"
    );
    let count: u64 = transaction.query_row(
        "SELECT COUNT(*) FROM workers WHERE miner_id=?1",
        [miner],
        |row| row.get(0),
    )?;
    ensure!(
        count < MAX_MINER_ALIASES,
        "this miner's worker alias capacity is full; reuse an existing name"
    );
    let id = known
        .last
        .checked_add(1)
        .filter(|id| *id <= MAX_WORKER_ID)
        .ok_or_else(|| anyhow!("worker ID exhausted"))?;
    transaction.execute(
        "INSERT INTO workers(id,miner_id,name,first_seen,last_seen) VALUES(?1,?2,?3,?4,?4)",
        params![id, miner, name, i64::try_from(at)?],
    )?;
    let changed = transaction.execute(
        "UPDATE worker_catalogue SET live_count=live_count+1,last_id=?1 WHERE id=1 AND last_id=?2 AND live_count=?3",
        params![id, known.last, known.live],
    )?;
    ensure!(
        changed == 1,
        "the worker catalogue changed during admission"
    );
    Ok(id)
}

/// Scan at most one writer retirement batch, including rows that cannot be reclaimed.
/// A frozen upper ID makes a complete pass possible while new aliases are arriving.
pub(super) fn retire(transaction: &Transaction<'_>, before: u64) -> Result<bool> {
    let mut known = catalogue(transaction)?;
    if known.through == 0 {
        known.through = known.last;
        known.after = 0;
    }
    if known.through == 0 {
        return Ok(false);
    }
    let mut select = transaction
        .prepare_cached("SELECT id FROM workers WHERE id>?1 AND id<=?2 ORDER BY id LIMIT ?3")?;
    let rows = select
        .query_map(
            params![known.after, known.through, super::writer::TRIM_ROWS as i64],
            |row| row.get::<_, WorkerId>(0),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut remove = transaction.prepare_cached(
        "DELETE FROM workers WHERE id=?1 AND last_seen<?2
         AND NOT EXISTS(SELECT 1 FROM shares WHERE worker_id=?1)
         AND NOT EXISTS(SELECT 1 FROM accepted_work WHERE worker_id=?1)
         AND NOT EXISTS(SELECT 1 FROM blocks WHERE worker_id=?1)",
    )?;
    let mut removed = 0_u64;
    for id in &rows {
        removed += u64::try_from(remove.execute(params![id, i64::try_from(before)?])?)?;
    }
    known.live = known
        .live
        .checked_sub(removed)
        .ok_or_else(|| anyhow!("worker catalogue count underflow"))?;
    known.after = rows.last().copied().unwrap_or(known.through);
    let more = rows.len() == super::writer::TRIM_ROWS && known.after < known.through;
    if !more {
        known.after = 0;
        known.through = 0;
    }
    transaction.execute(
        "UPDATE worker_catalogue SET live_count=?1,gc_after=?2,gc_through=?3 WHERE id=1",
        params![known.live, known.after, known.through],
    )?;
    Ok(more)
}

#[cfg(test)]
pub(super) fn note_import(
    transaction: &Transaction<'_>,
    id: WorkerId,
    inserted: usize,
) -> Result<()> {
    ensure!(
        id > 0 && id <= MAX_WORKER_ID && inserted <= 1,
        "invalid test worker import"
    );
    let known = catalogue(transaction)?;
    let live = known
        .live
        .checked_add(inserted as u64)
        .filter(|count| *count <= MAX_ALIASES)
        .ok_or_else(|| anyhow!("worker capacity exhausted"))?;
    transaction.execute(
        "UPDATE worker_catalogue SET live_count=?1,last_id=MAX(last_id,?2) WHERE id=1",
        params![live, id],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests;
