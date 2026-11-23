use super::{AcceptedReceipt, CommittedBlock};
use crate::{blocks::OpenBlock, chain::Hash384};
use rusqlite::{Connection, OptionalExtension, params};

pub const OPEN_PAGE: usize = 256;
pub const PAYLOAD_PAGE: usize = 16;

pub fn open_page(
    connection: &Connection,
    after: i64,
) -> rusqlite::Result<Vec<(Hash384, OpenBlock)>> {
    let mut query = connection.prepare(
        "SELECT id,hash,mode,miner_id,height,window_end,found_at,fee_bps FROM blocks
         INDEXED BY blocks_open_id WHERE status<2 AND id>?1 ORDER BY id LIMIT ?2",
    )?;
    query
        .query_map(params![after, OPEN_PAGE as i64], |row| {
            let text: String = row.get(1)?;
            let hash =
                Hash384::from_hex(&text).ok_or_else(|| invalid("invalid open block hash"))?;
            let mode: i64 = row.get(2)?;
            if !matches!(mode, 0 | 1) {
                return Err(invalid("invalid open block mode"));
            }
            Ok((
                hash,
                OpenBlock {
                    id: row.get(0)?,
                    mode: crate::config::Mode::from_code(mode),
                    miner: row.get(3)?,
                    height: row.get(4)?,
                    window_end: row.get(5)?,
                    found_at: row.get(6)?,
                    fee_bps: row.get(7)?,
                },
            ))
        })?
        .collect()
}

pub fn open(connection: &Connection, hash: Hash384) -> rusqlite::Result<Option<OpenBlock>> {
    connection
        .query_row(
            "SELECT id,mode,miner_id,height,window_end,found_at,fee_bps FROM blocks
         WHERE hash=?1 AND status<2",
            [hash.to_hex()],
            |row| {
                let mode: i64 = row.get(1)?;
                if !matches!(mode, 0 | 1) {
                    return Err(invalid("invalid open block mode"));
                }
                Ok(OpenBlock {
                    id: row.get(0)?,
                    mode: crate::config::Mode::from_code(mode),
                    miner: row.get(2)?,
                    height: row.get(3)?,
                    window_end: row.get(4)?,
                    found_at: row.get(5)?,
                    fee_bps: row.get(6)?,
                })
            },
        )
        .optional()
}

pub fn split(connection: &Connection, id: i64) -> rusqlite::Result<Option<Vec<u8>>> {
    connection
        .query_row(
            "SELECT split FROM blocks WHERE id=?1 AND status<2",
            [id],
            |row| row.get(0),
        )
        .optional()
        .map(Option::flatten)
}

pub struct Payload {
    pub block: CommittedBlock,
    pub header: [u8; 312],
    pub body_hex: String,
}

/// At most sixteen bounded payloads are read. The cursor rotates independently of settlement.
pub fn payloads(connection: &Connection, after: i64) -> rusqlite::Result<Vec<Payload>> {
    payload_partition(connection, after, 0, 1, 0)
}

pub fn payload_partition(
    connection: &Connection,
    after: i64,
    slot: usize,
    slots: usize,
    height: u64,
) -> rusqlite::Result<Vec<Payload>> {
    payload_partition_at(
        connection,
        after,
        slot,
        slots,
        height,
        crate::db::now_millis(),
    )
}

pub(super) fn payload_partition_at(
    connection: &Connection,
    after: i64,
    slot: usize,
    slots: usize,
    height: u64,
    now: u64,
) -> rusqlite::Result<Vec<Payload>> {
    if slots == 0 || slots > 64 || slot >= slots {
        return Err(invalid("invalid outbox partition"));
    }
    let mut query=connection.prepare(
        "SELECT p.block_id,p.header,p.body_hex FROM found_payload p JOIN blocks b ON b.id=p.block_id
         WHERE b.status<2 AND p.block_id>?1 AND p.block_id % ?3 = ?4
          AND (?5=0 OR (b.window_end>=?5 AND (p.retry_height<=?5 OR p.retry_at<=?6 OR p.retry_at>?7))) ORDER BY p.block_id LIMIT ?2")?;
    query
        .query_map(
            params![
                after,
                PAYLOAD_PAGE as i64,
                slots as i64,
                slot as i64,
                i64::try_from(height).map_err(|_| invalid("invalid registration height"))?,
                crate::db::int(u128::from(now)),
                crate::db::int(u128::from(now.saturating_add(5000)))
            ],
            |row| {
                let id: i64 = row.get(0)?;
                let header: Vec<u8> = row.get(1)?;
                let body_hex: String = row.get(2)?;
                if body_hex.len() > super::MAX_BODY_HEX {
                    return Err(invalid("oversized durable work body"));
                }
                Ok(Payload {
                    block: super::read_block(connection, id).map_err(|e| invalid(e.to_string()))?,
                    header: header
                        .try_into()
                        .map_err(|_| invalid("malformed durable work header"))?,
                    body_hex,
                })
            },
        )?
        .collect()
}

/// Reconnect lookup only acknowledges the original authenticated address. It never creates work.
pub fn replay(
    connection: &Connection,
    job: Hash384,
    nonce: [u8; 32],
    address: &str,
) -> rusqlite::Result<Option<AcceptedReceipt>> {
    let fingerprint: Option<Vec<u8>> = connection
        .query_row(
            "SELECT a.fingerprint FROM accepted_work a JOIN miners m ON m.id=a.miner_id
         WHERE a.job_id=?1 AND a.nonce=?2 AND m.address=?3 LIMIT 1",
            params![job.0.as_slice(), nonce.as_slice(), address],
            |row| row.get(0),
        )
        .optional()?;
    fingerprint
        .map(|bytes| {
            let hash = Hash384(
                bytes
                    .try_into()
                    .map_err(|_| invalid("invalid accepted proof fingerprint"))?,
            );
            super::receipt(connection, hash)
                .map_err(|e| invalid(e.to_string()))?
                .ok_or_else(|| invalid("missing accepted-work receipt"))
        })
        .transpose()
}

pub(super) fn invalid(message: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::InvalidParameterName(message.into())
}
