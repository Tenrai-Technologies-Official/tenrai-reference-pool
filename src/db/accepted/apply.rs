use super::*;
use crate::{
    chain::domain_hash,
    db::{put_setting, setting},
    pow::template::block_id,
    pplns::encode_split,
};

pub(in crate::db) fn apply(
    transaction: &Transaction<'_>,
    request: AcceptedWork,
) -> Result<AcceptedReceipt> {
    ensure!(
        domain_hash(b"TRAI_POOL_ACCEPTED_WORK_V1", &[&request.header]) == request.fingerprint,
        "accepted-work fingerprint differs from its canonical proof"
    );
    if let Some(receipt) = super::receipt(transaction, request.fingerprint)? {
        return Ok(receipt);
    }
    ensure!(
        request.window_end > 0 && request.window_end <= i64::MAX as u64,
        "invalid accepted work window"
    );
    // Retirement can commit while verification is in flight.
    // Replay existing receipts, then enforce the durable registration floor.
    ensure!(
        request.window_end >= super::closed_windows_before(transaction)?,
        "the accepted-work registration window is durably closed"
    );
    ensure!(
        request.weight > 0 && request.weight <= i64::MAX as u64,
        "accepted share weight exceeds exact SQLite capacity"
    );
    let previous = setting(transaction, "accepted_work_sequence")?
        .map(|text| text.parse::<u64>())
        .transpose()?
        .unwrap_or(0);
    let id = previous
        .checked_add(1)
        .filter(|id| *id <= i64::MAX as u64)
        .ok_or_else(|| anyhow!("accepted-work sequence exhausted"))?;
    let last_time = setting(transaction, "accepted_work_time")?
        .map(|text| text.parse::<u64>())
        .transpose()?
        .unwrap_or(0);
    let at = request.time.max(last_time);
    i64::try_from(at)?;
    let miner = super::miner(transaction, &request.key.address, at)?;
    let since = at.saturating_sub(request.lookback_millis.max(ACTIVE_KEPT_MILLIS));
    let active = transaction
        .query_row(
            "SELECT last_seen FROM accepted_participants WHERE miner_id=?1",
            [miner],
            |row| row.get::<_, u64>(0),
        )
        .optional()?;
    if active.is_none_or(|seen| seen < since) {
        transaction.execute(
            "DELETE FROM accepted_participants WHERE last_seen<?1",
            [i64::try_from(since)?],
        )?;
        let count: u64 =
            transaction.query_row("SELECT COUNT(*) FROM accepted_participants", [], |row| {
                row.get(0)
            })?;
        ensure!(
            count < MAX_ACTIVE_PARTICIPANTS as u64,
            "the pool's active participant capacity is full"
        );
    }
    let worker = super::worker(transaction, miner, &request.key.name, at)?;
    let accepted: u64 = transaction.query_row(
        "SELECT accepted FROM workers WHERE id=?1",
        [worker],
        |row| row.get(0),
    )?;
    let accepted = accepted
        .checked_add(1)
        .filter(|n| *n <= i64::MAX as u64)
        .ok_or_else(|| anyhow!("worker accepted counter exhausted"))?;
    transaction.execute(
        "UPDATE workers SET accepted=?2,last_seen=MAX(last_seen,?3) WHERE id=?1",
        params![worker, accepted, i64::try_from(at)?],
    )?;
    transaction.execute(
        "INSERT INTO accepted_participants(miner_id,last_seen) VALUES(?1,?2)
        ON CONFLICT(miner_id) DO UPDATE SET last_seen=MAX(last_seen,excluded.last_seen)",
        params![miner, i64::try_from(at)?],
    )?;
    if let Some(source) = request.source {
        transaction.execute(
            "INSERT INTO share_sources(miner_id,source,last_seen) VALUES(?1,?2,?3)
            ON CONFLICT(miner_id,source) DO UPDATE SET last_seen=MAX(last_seen,excluded.last_seen)",
            params![miner, source.as_slice(), i64::try_from(at)?],
        )?;
        transaction.execute("DELETE FROM share_sources WHERE miner_id=?1 AND source NOT IN (
            SELECT source FROM share_sources WHERE miner_id=?1 ORDER BY last_seen DESC,source DESC LIMIT 16)", [miner])?;
    }
    if request.key.mode == Mode::Pplns {
        transaction.execute(
            "INSERT INTO shares(id,time,miner_id,worker_id,weight) VALUES(?1,?2,?3,?4,?5)",
            params![id, i64::try_from(at)?, miner, worker, request.weight],
        )?;
    }
    let round_key = match request.key.mode {
        Mode::Pplns => "accepted_round_pplns",
        Mode::Solo => "accepted_round_solo",
    };
    let round = setting(transaction, round_key)?
        .map(|text| text.parse::<u128>())
        .transpose()?
        .unwrap_or(0)
        .checked_add(u128::from(request.weight))
        .ok_or_else(|| anyhow!("accepted round work exhausted"))?;
    let found = request
        .found
        .map(|found| -> Result<CommittedBlock> {
            ensure!(
                found.body_hex.len() <= MAX_BODY_HEX
                    && found.body_hex.len().is_multiple_of(2)
                    && hex::decode(&found.body_hex).is_ok()
                    && found.window_end >= found.height,
                "invalid bounded found-block intent"
            );
            ensure!(
                found.header == request.header && found.window_end == request.window_end,
                "found-block intent differs from accepted proof"
            );
            let open: u64 =
                transaction.query_row("SELECT COUNT(*) FROM blocks WHERE status<2", [], |row| {
                    row.get(0)
                })?;
            ensure!(
                open < MAX_OPEN_BLOCKS as u64,
                "the durable found-block backlog is full"
            );
            let hash = block_id(&found.header);
            let window = if request.key.mode == Mode::Pplns {
                Some(super::window(
                    transaction,
                    id,
                    found.window_work,
                    at.saturating_sub(request.lookback_millis),
                )?)
            } else {
                None
            };
            transaction.execute(
                "INSERT INTO blocks(hash,mode,status,miner_id,worker_id,height,window_end,
            found_at,network_difficulty,round_work,fee_bps,share_id,window_work,window_shares,split)
            VALUES(?1,?2,0,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
                params![
                    hash.to_hex(),
                    request.key.mode.code(),
                    miner,
                    worker,
                    i64::try_from(found.height)?,
                    i64::try_from(found.window_end)?,
                    i64::try_from(at)?,
                    super::super::int(found.network_difficulty),
                    super::super::int(round),
                    found.fee_bps,
                    (request.key.mode == Mode::Pplns).then_some(id),
                    window.as_ref().map(|w| super::super::int(w.total)),
                    window.as_ref().map(|w| w.shares),
                    window.as_ref().map(|w| encode_split(&w.weights))
                ],
            )?;
            let block_id = transaction.last_insert_rowid();
            transaction.execute(
                "INSERT INTO found_payload(block_id,header,body_hex) VALUES(?1,?2,?3)",
                params![block_id, found.header.as_slice(), found.body_hex],
            )?;
            Ok(CommittedBlock {
                id: block_id,
                hash,
                mode: request.key.mode,
                miner,
                worker,
                height: found.height,
                window_end: found.window_end,
                found_at: at,
                fee_bps: found.fee_bps,
            })
        })
        .transpose()?;
    transaction.execute("INSERT INTO accepted_work(fingerprint,id,time,miner_id,worker_id,mode,weight,window_end,block_id,job_id,nonce)
        VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",params![request.fingerprint.0.as_slice(),id,i64::try_from(at)?,
            miner,worker,request.key.mode.code(),request.weight,i64::try_from(request.window_end)?,
            found.as_ref().map(|block|block.id),request.job_id.0.as_slice(), &request.header[248..280]])?;
    put_setting(transaction, "accepted_work_sequence", &id.to_string())?;
    put_setting(transaction, "accepted_work_time", &at.to_string())?;
    put_setting(
        transaction,
        round_key,
        &if found.is_some() { 0 } else { round }.to_string(),
    )?;
    Ok(AcceptedReceipt {
        id,
        time: at,
        miner,
        worker,
        mode: request.key.mode,
        weight: request.weight,
        fresh: true,
        block: found,
    })
}
