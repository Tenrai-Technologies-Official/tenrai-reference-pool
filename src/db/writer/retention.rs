use super::*;

/// Small transactions keep disk retirement from monopolizing the monetary writer.
pub(super) fn prune(
    transaction: &Transaction<'_>,
    shares: u64,
    hashrate: u64,
    splits: u64,
    sources: u64,
    receipts: u64,
    height: u64,
) -> Result<bool> {
    let mut more = false;
    let retired: Option<u64> = transaction.query_row(
        "SELECT MAX(time) FROM (SELECT time FROM shares WHERE time<?1 ORDER BY id LIMIT ?2)",
        params![int(shares.into()), TRIM_ROWS as i64],
        |row| row.get(0),
    )?;
    let deleted = transaction.execute(
        "DELETE FROM shares WHERE id IN (SELECT id FROM shares WHERE time<?1 ORDER BY id LIMIT ?2)",
        params![int(shares.into()), TRIM_ROWS as i64],
    )?;
    more |= deleted == TRIM_ROWS;
    if let Some(retired) = retired {
        let known = crate::db::setting(transaction, "shares_pruned_through")?
            .map(|v| v.parse::<u64>())
            .transpose()?
            .unwrap_or(0);
        crate::db::put_setting(
            transaction,
            "shares_pruned_through",
            &retired.max(known).to_string(),
        )?;
    }
    for (sql, cutoff) in [
        (
            "DELETE FROM share_sources WHERE (miner_id,source) IN (SELECT miner_id,source FROM share_sources WHERE last_seen<?1 LIMIT ?2)",
            sources,
        ),
        (
            "DELETE FROM hashrate WHERE (miner_id,bucket,mode) IN (SELECT miner_id,bucket,mode FROM hashrate WHERE bucket<?1 LIMIT ?2)",
            hashrate,
        ),
        (
            "UPDATE blocks SET split=NULL WHERE id IN (SELECT id FROM blocks WHERE status>=2 AND split IS NOT NULL AND COALESCE(done_at,found_at)<?1 LIMIT ?2)",
            splits,
        ),
    ] {
        more |=
            transaction.execute(sql, params![int(cutoff.into()), TRIM_ROWS as i64])? == TRIM_ROWS;
    }
    // Age alone is insufficient: if the node stops advancing, the same proof remains valid.
    // Never retire a receipt before its actual registration window has closed.
    // Publish closure in the same transaction as receipt deletion. Async verification or
    // a preverified writer queue item must not resurrect a just-forgotten canonical proof.
    super::super::accepted::close_windows_before(transaction, height)?;
    more |= transaction.execute(
        "DELETE FROM accepted_work WHERE fingerprint IN (SELECT fingerprint FROM accepted_work
         WHERE time<?1 AND window_end<?2 LIMIT ?3)",
        params![int(receipts.into()), int(height.into()), TRIM_ROWS as i64],
    )? == TRIM_ROWS;
    more |= super::super::worker_lifetime::retire(transaction, shares.min(receipts))?;
    Ok(more)
}
