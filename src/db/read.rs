use super::{BlockStatus, PayoutStatus, uint};
use crate::{config::Mode, day::MinuteRow};
use rusqlite::{Connection, OptionalExtension, Row, params};
use std::collections::BTreeMap;

pub struct WorkerRow {
    pub id: super::WorkerId,
    pub miner: u32,
    pub name: String,
    pub last_seen: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub stale: u64,
}

pub struct OpenBlock {
    pub id: i64,
    pub hash: String,
    pub mode: Mode,
    pub status: BlockStatus,
    pub miner: u32,
    pub worker: super::WorkerId,
    pub height: u64,
    pub window_end: u64,
    pub found_at: u64,
    pub fee_bps: u32,
    pub split: Option<Vec<u8>>,
}

pub struct OpenPayout {
    pub id: i64,
    pub request_id: String,
    pub status: PayoutStatus,
    pub transaction_id: Option<String>,
    pub valid_until: Option<u64>,
    pub created_at: u64,
    pub items: Vec<(u32, String, u64)>,
}

#[derive(Clone, Debug)]
pub struct BlockRow {
    pub id: i64,
    pub hash: String,
    pub mode: Mode,
    pub status: BlockStatus,
    pub progress: Option<crate::blocks::Stage>,
    pub address: String,
    pub worker: String,
    pub height: u64,
    pub found_at: u64,
    pub network_difficulty: u64,
    pub round_work: u64,
    pub reward: Option<u64>,
    pub fee: Option<u64>,
    pub cut_height: Option<u64>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PayoutRow {
    pub id: i64,
    pub status: PayoutStatus,
    pub created_at: u64,
    pub done_at: Option<u64>,
    pub total: u64,
    pub fee: Option<u64>,
    pub recipients: u32,
    pub transaction_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct MinerPayout {
    pub id: i64,
    pub status: PayoutStatus,
    pub created_at: u64,
    pub amount: u64,
    pub fee_share: u64,
    pub transaction_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Balance {
    pub unpaid: u64,
    pub pending: u64,
    pub paid: u128,
    pub earned: u128,
}

#[derive(Clone, Debug, Default)]
pub struct Totals {
    pub last_found: [Option<u64>; 2],
    pub unpaid: u64,
    pub paid: u128,
    pub payable: u64,
    pub open_payouts: u64,
    pub payouts: u64,
}

const BLOCK_COLUMNS: &str = "b.id, b.hash, b.mode, b.status, m.address, w.name, b.height,
    b.found_at, b.network_difficulty, b.round_work, b.reward, b.fee, b.cut_height, b.reason, b.progress";

pub fn miners(connection: &Connection) -> rusqlite::Result<Vec<(u32, String)>> {
    let mut statement = connection.prepare("SELECT id, address FROM miners")?;
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect()
}

pub fn workers(connection: &Connection) -> rusqlite::Result<Vec<WorkerRow>> {
    let mut statement = connection
        .prepare("SELECT id, miner_id, name, last_seen, accepted, rejected, stale FROM workers")?;
    statement
        .query_map([], |row| {
            Ok(WorkerRow {
                id: row.get(0)?,
                miner: row.get(1)?,
                name: row.get(2)?,
                last_seen: uint(row.get(3)?),
                accepted: uint(row.get(4)?),
                rejected: uint(row.get(5)?),
                stale: uint(row.get(6)?),
            })
        })?
        .collect()
}

/// Name, last share, accepted, rejected and stale shares of a stored worker.
pub type StoredWorker = (String, u64, u64, u64, u64);

pub fn workers_of(
    connection: &Connection,
    miner: u32,
    since: u64,
) -> rusqlite::Result<Vec<StoredWorker>> {
    let mut statement = connection.prepare_cached(
        "SELECT name, last_seen, accepted, rejected, stale FROM workers
         WHERE miner_id = ?1 AND last_seen >= ?2 ORDER BY last_seen DESC LIMIT 100",
    )?;
    statement
        .query_map(params![miner, super::int(since.into())], |row| {
            Ok((
                row.get(0)?,
                uint(row.get(1)?),
                uint(row.get(2)?),
                uint(row.get(3)?),
                uint(row.get(4)?),
            ))
        })?
        .collect()
}

pub fn open_blocks(connection: &Connection) -> rusqlite::Result<Vec<OpenBlock>> {
    let mut statement = connection.prepare(
        "SELECT id, hash, mode, status, miner_id, worker_id, height, window_end, found_at, fee_bps,
         split FROM blocks WHERE status < 2 ORDER BY id",
    )?;
    statement
        .query_map([], |row| {
            Ok(OpenBlock {
                id: row.get(0)?,
                hash: row.get(1)?,
                mode: Mode::from_code(row.get(2)?),
                status: BlockStatus::from_code(row.get(3)?),
                miner: row.get(4)?,
                worker: row.get(5)?,
                height: uint(row.get(6)?),
                window_end: uint(row.get(7)?),
                found_at: uint(row.get(8)?),
                fee_bps: row.get(9)?,
                split: row.get(10)?,
            })
        })?
        .collect()
}

/// PPLNS shares newer than `since`, oldest first, as (id, time, miner, weight).
pub fn shares_since(
    connection: &Connection,
    since: u64,
) -> rusqlite::Result<Vec<(u64, u64, u32, u64)>> {
    let mut statement = connection
        .prepare("SELECT id, time, miner_id, weight FROM shares WHERE time >= ?1 ORDER BY id")?;
    statement
        .query_map([super::int(since.into())], |row| {
            Ok((
                uint(row.get(0)?),
                uint(row.get(1)?),
                row.get(2)?,
                uint(row.get(3)?),
            ))
        })?
        .collect()
}

pub fn next_share_id(connection: &Connection) -> rusqlite::Result<u64> {
    let last = super::setting(connection, "accepted_work_sequence")?
        .map(|text| {
            text.parse::<u64>().map_err(|_| {
                rusqlite::Error::InvalidParameterName("invalid accepted sequence".into())
            })
        })
        .transpose()?
        .unwrap_or(0);
    let stored: u64 =
        connection.query_row("SELECT COALESCE(MAX(id),0) FROM shares", [], |row| {
            row.get(0)
        })?;
    last.max(stored)
        .checked_add(1)
        .filter(|next| *next <= i64::MAX as u64)
        .ok_or_else(|| rusqlite::Error::InvalidParameterName("accepted sequence exhausted".into()))
}

/// At most this many active payout records are materialized in one settlement pass.
pub const MAX_OPEN_PAYOUT_CHECKS: usize = 64;

pub fn maximum_open_payout_id(connection: &Connection) -> rusqlite::Result<i64> {
    connection.query_row(
        "SELECT id FROM payouts INDEXED BY payouts_open_id WHERE status < 2 ORDER BY id DESC LIMIT 1",
        [], |row| row.get(0),
    ).optional().map(|id| id.unwrap_or(0))
}

pub fn has_prepared_payouts(connection: &Connection) -> rusqlite::Result<bool> {
    connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM payouts INDEXED BY payouts_open WHERE status < 2 AND status = ?1)",
        [PayoutStatus::Prepared.code()], |row| row.get(0),
    )
}

pub fn open_payout_page(
    connection: &Connection,
    after: i64,
    through: i64,
    limit: usize,
) -> rusqlite::Result<Vec<OpenPayout>> {
    if after < 0 || through < after || limit == 0 || limit > MAX_OPEN_PAYOUT_CHECKS {
        return Err(rusqlite::Error::InvalidParameterName(
            "invalid payout page bounds".into(),
        ));
    }
    let mut statement = connection.prepare(
        "SELECT id, request_id, status, transaction_id, valid_until, created_at
         FROM payouts INDEXED BY payouts_open_id
         WHERE status < 2 AND id > ?1 AND id <= ?2 ORDER BY id LIMIT ?3",
    )?;
    let mut payouts: Vec<OpenPayout> = statement
        .query_map(params![after, through, limit as i64], |row| {
            Ok(OpenPayout {
                id: row.get(0)?,
                request_id: row.get(1)?,
                status: PayoutStatus::from_code(row.get(2)?),
                transaction_id: row.get(3)?,
                valid_until: row.get::<_, Option<i64>>(4)?.map(uint),
                created_at: uint(row.get(5)?),
                items: Vec::new(),
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    attach_payout_items(connection, &mut payouts)?;
    Ok(payouts)
}

pub fn open_payouts(connection: &Connection) -> rusqlite::Result<Vec<OpenPayout>> {
    let mut statement = connection.prepare(
        "SELECT id, request_id, status, transaction_id, valid_until, created_at FROM payouts
         WHERE status < 2 ORDER BY id",
    )?;
    let mut payouts: Vec<OpenPayout> = statement
        .query_map([], |row| {
            Ok(OpenPayout {
                id: row.get(0)?,
                request_id: row.get(1)?,
                status: PayoutStatus::from_code(row.get(2)?),
                transaction_id: row.get(3)?,
                valid_until: row.get::<_, Option<i64>>(4)?.map(uint),
                created_at: uint(row.get(5)?),
                items: Vec::new(),
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    attach_payout_items(connection, &mut payouts)?;
    Ok(payouts)
}

fn attach_payout_items(
    connection: &Connection,
    payouts: &mut [OpenPayout],
) -> rusqlite::Result<()> {
    let mut items = connection.prepare(
        "SELECT i.miner_id, m.address, i.amount FROM payout_items i
         JOIN miners m ON m.id = i.miner_id WHERE i.payout_id = ?1 ORDER BY i.miner_id",
    )?;
    for payout in payouts {
        payout.items = items
            .query_map([payout.id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
    }
    Ok(())
}

/// Balances of at least `minimum`, largest first.
pub fn payable(
    connection: &Connection,
    minimum: u64,
    limit: usize,
) -> rusqlite::Result<Vec<(u32, String, u64)>> {
    let mut statement = connection.prepare(
        "SELECT b.miner_id, m.address, b.unpaid FROM balances b JOIN miners m ON m.id = b.miner_id
         WHERE b.unpaid >= ?1 ORDER BY b.unpaid DESC, b.miner_id LIMIT ?2",
    )?;
    statement
        .query_map(
            params![
                i64::try_from(minimum).map_err(|_| rusqlite::Error::InvalidParameterName(
                    "active payout minimum exceeds current balance storage".into()
                ))?,
                limit as i64
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?
        .collect()
}

pub fn blocks(
    connection: &Connection,
    mode: Option<Mode>,
    before: Option<i64>,
    limit: usize,
) -> rusqlite::Result<Vec<BlockRow>> {
    let sql = format!(
        "SELECT {BLOCK_COLUMNS} FROM blocks b JOIN miners m ON m.id = b.miner_id
         JOIN workers w ON w.id = b.worker_id
         WHERE (?1 IS NULL OR b.mode = ?1) AND b.id < ?2 ORDER BY b.id DESC LIMIT ?3"
    );
    let mut statement = connection.prepare_cached(&sql)?;
    statement
        .query_map(
            params![
                mode.map(Mode::code),
                before.unwrap_or(i64::MAX),
                limit as i64
            ],
            block_row,
        )?
        .collect()
}

/// A page of blocks for the public API, newest first.
pub fn blocks_page(
    connection: &Connection,
    mode: Option<Mode>,
    offset: usize,
    limit: usize,
) -> rusqlite::Result<Vec<BlockRow>> {
    let sql = format!(
        "SELECT {BLOCK_COLUMNS} FROM blocks b JOIN miners m ON m.id = b.miner_id
         JOIN workers w ON w.id = b.worker_id
         WHERE (?1 IS NULL OR b.mode = ?1) ORDER BY b.id DESC LIMIT ?2 OFFSET ?3"
    );
    let mut statement = connection.prepare_cached(&sql)?;
    statement
        .query_map(
            params![mode.map(Mode::code), limit as i64, offset as i64],
            block_row,
        )?
        .collect()
}

pub fn miner_blocks(
    connection: &Connection,
    miner: u32,
    limit: usize,
) -> rusqlite::Result<Vec<BlockRow>> {
    let sql = format!(
        "SELECT {BLOCK_COLUMNS} FROM blocks b JOIN miners m ON m.id = b.miner_id
         JOIN workers w ON w.id = b.worker_id WHERE b.miner_id = ?1 ORDER BY b.id DESC LIMIT ?2"
    );
    let mut statement = connection.prepare_cached(&sql)?;
    statement
        .query_map(params![miner, limit as i64], block_row)?
        .collect()
}

/// A block with its fee in basis points, window work and stored PPLNS split.
pub type BlockDetail = (BlockRow, u32, Option<u64>, Option<Vec<u8>>);

pub fn block(connection: &Connection, id: i64) -> rusqlite::Result<Option<BlockDetail>> {
    let sql = format!(
        "SELECT {BLOCK_COLUMNS}, b.fee_bps, b.window_work, b.split FROM blocks b
         JOIN miners m ON m.id = b.miner_id JOIN workers w ON w.id = b.worker_id WHERE b.id = ?1"
    );
    connection
        .query_row(&sql, [id], |row| {
            Ok((
                block_row(row)?,
                row.get(15)?,
                row.get::<_, Option<i64>>(16)?.map(uint),
                row.get(17)?,
            ))
        })
        .optional()
}

fn block_row(row: &Row<'_>) -> rusqlite::Result<BlockRow> {
    Ok(BlockRow {
        id: row.get(0)?,
        hash: row.get(1)?,
        mode: Mode::from_code(row.get(2)?),
        status: BlockStatus::from_code(row.get(3)?),
        progress: match row.get::<_, u8>(14)? {
            0 => None,
            1 => Some(crate::blocks::Stage::Registered),
            2 => Some(crate::blocks::Stage::Executed),
            _ => {
                return Err(rusqlite::Error::InvalidParameterName(
                    "invalid persisted block stage".into(),
                ));
            }
        },
        address: row.get(4)?,
        worker: row.get(5)?,
        height: uint(row.get(6)?),
        found_at: uint(row.get(7)?),
        network_difficulty: uint(row.get(8)?),
        round_work: uint(row.get(9)?),
        reward: row.get(10)?,
        fee: row.get(11)?,
        cut_height: row.get::<_, Option<i64>>(12)?.map(uint),
        reason: row.get(13)?,
    })
}

pub fn payouts(
    connection: &Connection,
    before: Option<i64>,
    limit: usize,
) -> rusqlite::Result<Vec<PayoutRow>> {
    let mut statement = connection.prepare_cached(
        "SELECT id, status, created_at, done_at, total, fee, recipients, transaction_id
         FROM payouts WHERE id < ?1 ORDER BY id DESC LIMIT ?2",
    )?;
    statement
        .query_map(params![before.unwrap_or(i64::MAX), limit as i64], |row| {
            Ok(PayoutRow {
                id: row.get(0)?,
                status: PayoutStatus::from_code(row.get(1)?),
                created_at: uint(row.get(2)?),
                done_at: row.get::<_, Option<i64>>(3)?.map(uint),
                total: row.get(4)?,
                fee: row.get(5)?,
                recipients: row.get(6)?,
                transaction_id: row.get(7)?,
            })
        })?
        .collect()
}

pub fn miner_payouts(
    connection: &Connection,
    miner: u32,
    limit: usize,
) -> rusqlite::Result<Vec<MinerPayout>> {
    let mut statement = connection.prepare_cached(
        "SELECT p.id, p.status, p.created_at, i.amount, i.fee_share, p.transaction_id
         FROM payout_items i JOIN payouts p ON p.id = i.payout_id
         WHERE i.miner_id = ?1 ORDER BY i.payout_id DESC LIMIT ?2",
    )?;
    statement
        .query_map(params![miner, limit as i64], |row| {
            let amount: u64 = row.get(3)?;
            let fee_share: u64 = row.get(4)?;
            if fee_share > amount {
                return Err(rusqlite::Error::InvalidParameterName(
                    "payout fee exceeds its amount".into(),
                ));
            }
            Ok(MinerPayout {
                id: row.get(0)?,
                status: PayoutStatus::from_code(row.get(1)?),
                created_at: uint(row.get(2)?),
                amount,
                fee_share,
                transaction_id: row.get(5)?,
            })
        })?
        .collect()
}

pub fn balance(connection: &Connection, miner: u32) -> rusqlite::Result<Balance> {
    connection
        .query_row(
            "SELECT unpaid, pending, paid, earned FROM balances WHERE miner_id = ?1",
            [miner],
            |row| {
                Ok(Balance {
                    unpaid: row.get(0)?,
                    pending: row.get(1)?,
                    paid: super::money::counter(row, 2)?,
                    earned: super::money::counter(row, 3)?,
                })
            },
        )
        .optional()
        .map(Option::unwrap_or_default)
}

/// Daily earnings since `since_day` (days since the epoch), oldest first.
pub fn earnings(
    connection: &Connection,
    miner: u32,
    since_day: u64,
) -> rusqlite::Result<Vec<(u64, u128, u64)>> {
    let mut statement = connection.prepare_cached(
        "SELECT day, amount, blocks FROM earnings WHERE miner_id = ?1 AND day >= ?2 ORDER BY day",
    )?;
    statement
        .query_map(params![miner, super::int(since_day.into())], |row| {
            Ok((
                uint(row.get(0)?),
                super::money::counter(row, 1)?,
                uint(row.get(2)?),
            ))
        })?
        .collect()
}

/// Ten-minute work buckets since `since`, summed over both modes.
pub fn hashrate(
    connection: &Connection,
    miner: u32,
    since: u64,
) -> rusqlite::Result<Vec<(u64, u64)>> {
    let mut statement = connection.prepare_cached(
        "SELECT bucket, SUM(work) FROM hashrate WHERE miner_id = ?1 AND bucket >= ?2
         GROUP BY bucket ORDER BY bucket",
    )?;
    statement
        .query_map(params![miner, super::int(since.into())], |row| {
            Ok((uint(row.get(0)?), uint(row.get(1)?)))
        })?
        .collect()
}

/// Block counts per mode, read once at startup.
pub fn block_counts(connection: &Connection) -> rusqlite::Result<[(u64, u64, u64, u64); 2]> {
    let mut counts = [(0, 0, 0, 0); 2];
    let mut statement =
        connection.prepare("SELECT mode, status, COUNT(*) FROM blocks GROUP BY mode, status")?;
    let rows = statement.query_map([], |row| {
        Ok((
            Mode::from_code(row.get(0)?),
            BlockStatus::from_code(row.get(1)?),
            uint(row.get(2)?),
        ))
    })?;
    for row in rows {
        let (mode, status, count) = row?;
        let entry = &mut counts[mode_index(mode)];
        entry.0 += count;
        match status {
            BlockStatus::Found | BlockStatus::Pending => entry.1 += count,
            BlockStatus::Confirmed => entry.2 += count,
            BlockStatus::Expired | BlockStatus::Rejected => entry.3 += count,
        }
    }
    Ok(counts)
}

/// Blocks and rewards per minute since `since`, used when no saved figures exist.
pub fn day(connection: &Connection, since: u64) -> rusqlite::Result<Vec<MinuteRow>> {
    let mut statement = connection.prepare(
        "SELECT found_at / ?2, mode, reward FROM blocks WHERE found_at >= ?1 ORDER BY id",
    )?;
    let rows = statement.query_map(
        params![
            super::int(since.into()),
            super::int(crate::day::MINUTE.into())
        ],
        |row| {
            Ok((
                row.get::<_, u64>(0)?,
                Mode::from_code(row.get(1)?),
                row.get::<_, Option<u64>>(2)?,
            ))
        },
    )?;
    let mut minutes = BTreeMap::<u64, ([u64; 2], u128)>::new();
    for row in rows {
        let (minute, mode, reward) = row?;
        let entry = minutes.entry(minute).or_default();
        let count = &mut entry.0[mode_index(mode)];
        *count = count.checked_add(1).ok_or_else(|| {
            rusqlite::Error::InvalidParameterName("minute block count exceeds u64".into())
        })?;
        entry.1 = entry
            .1
            .checked_add(u128::from(reward.unwrap_or(0)))
            .ok_or_else(|| {
                rusqlite::Error::InvalidParameterName("minute rewards exceed u128".into())
            })?;
    }
    Ok(minutes
        .into_iter()
        .map(|(minute, (found, rewards))| (minute, found, rewards))
        .collect())
}

/// Figures for the overview: newest block per mode, balances, payouts.
pub fn totals(connection: &Connection, minimum: u64) -> rusqlite::Result<Totals> {
    let mut totals = Totals::default();
    let mut last = connection
        .prepare_cached("SELECT found_at FROM blocks WHERE mode = ?1 ORDER BY id DESC LIMIT 1")?;
    for mode in Mode::ALL {
        totals.last_found[mode_index(mode)] = last
            .query_row([mode.code()], |row| row.get::<_, i64>(0))
            .optional()?
            .map(uint);
    }
    let mut balances = connection.prepare("SELECT unpaid, pending, paid FROM balances")?;
    let rows = balances.query_map([], |row| {
        Ok((
            row.get::<_, u64>(0)?,
            row.get::<_, u64>(1)?,
            super::money::counter(row, 2)?,
        ))
    })?;
    for row in rows {
        let (unpaid, pending, paid) = row?;
        totals.unpaid = totals
            .unpaid
            .checked_add(unpaid)
            .and_then(|value| value.checked_add(pending))
            .ok_or_else(|| {
                rusqlite::Error::InvalidParameterName(
                    "pool current balance total exceeds u64".into(),
                )
            })?;
        totals.paid = totals.paid.checked_add(paid).ok_or_else(|| {
            rusqlite::Error::InvalidParameterName("pool paid total exceeds u128".into())
        })?;
    }
    totals.payable = uint(connection.query_row(
        "SELECT COUNT(*) FROM balances WHERE unpaid >= ?1",
        [i64::try_from(minimum).map_err(|_| {
            rusqlite::Error::InvalidParameterName(
                "active payout minimum exceeds current balance storage".into(),
            )
        })?],
        |row| row.get(0),
    )?);
    totals.open_payouts = uint(connection.query_row(
        "SELECT COUNT(*) FROM payouts WHERE status < 2",
        [],
        |row| row.get(0),
    )?);
    totals.payouts = uint(connection.query_row(
        "SELECT COUNT(*) FROM payouts WHERE status = ?1",
        [PayoutStatus::Paid.code()],
        |row| row.get(0),
    )?);
    Ok(totals)
}

pub const fn mode_index(mode: Mode) -> usize {
    match mode {
        Mode::Pplns => 0,
        Mode::Solo => 1,
    }
}

#[cfg(test)]
#[path = "read_money_tests.rs"]
mod money_tests;
