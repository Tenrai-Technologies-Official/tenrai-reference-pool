use super::{BlockCredit, BlockStatus, PayoutStatus, int};
use anyhow::{Result, anyhow, bail};
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct MonetaryBalance {
    pub(super) unpaid: u64,
    pub(super) pending: u64,
    pub(super) paid: u128,
    pub(super) earned: u128,
}

pub(super) fn monetary_balance(
    transaction: &Transaction<'_>,
    miner: u32,
) -> Result<Option<MonetaryBalance>> {
    Ok(transaction
        .query_row(
            "SELECT unpaid,pending,paid,earned FROM balances WHERE miner_id=?1",
            [miner],
            |row| {
                Ok(MonetaryBalance {
                    unpaid: row.get(0)?,
                    pending: row.get(1)?,
                    paid: crate::db::money::counter(row, 2)?,
                    earned: crate::db::money::counter(row, 3)?,
                })
            },
        )
        .optional()?)
}

pub(super) fn store_balance(
    transaction: &Transaction<'_>,
    miner: u32,
    balance: &MonetaryBalance,
) -> Result<()> {
    let changed = transaction.execute(
        "INSERT INTO balances(miner_id,unpaid,pending,paid,earned) VALUES(?1,?2,?3,?4,?5)
         ON CONFLICT(miner_id) DO UPDATE SET unpaid=excluded.unpaid,pending=excluded.pending,
         paid=excluded.paid,earned=excluded.earned",
        params![
            miner,
            crate::db::money::amount(balance.unpaid)?,
            crate::db::money::amount(balance.pending)?,
            balance.paid.to_string(),
            balance.earned.to_string()
        ],
    )?;
    if changed != 1 {
        bail!("a monetary balance was not stored exactly once");
    }
    Ok(())
}

pub(super) fn store_earning(
    transaction: &Transaction<'_>,
    miner: u32,
    day: u64,
    amount: u64,
    blocks: u64,
) -> Result<()> {
    let day = i64::try_from(day).map_err(|_| anyhow!("earning day exceeds SQLite capacity"))?;
    let previous: Option<(u128, u64)> = transaction
        .query_row(
            "SELECT amount,blocks FROM earnings WHERE miner_id=?1 AND day=?2",
            params![miner, day],
            |row| Ok((crate::db::money::counter(row, 0)?, row.get(1)?)),
        )
        .optional()?;
    let (earned, known_blocks) = previous.unwrap_or_default();
    let earned = earned
        .checked_add(u128::from(amount))
        .ok_or_else(|| anyhow!("daily monetary counter capacity is exhausted"))?;
    let blocks = known_blocks
        .checked_add(blocks)
        .ok_or_else(|| anyhow!("daily block counter capacity is exhausted"))?;
    let changed = transaction.execute(
        "INSERT INTO earnings(miner_id,day,amount,blocks) VALUES(?1,?2,?3,?4)
         ON CONFLICT(miner_id,day) DO UPDATE SET amount=excluded.amount,blocks=excluded.blocks",
        params![
            miner,
            day,
            earned.to_string(),
            crate::db::money::amount(blocks)?
        ],
    )?;
    if changed != 1 {
        bail!("daily monetary earnings were not stored exactly once");
    }
    Ok(())
}

pub(super) fn credit(
    transaction: &Transaction<'_>,
    cut: u64,
    blocks: Vec<BlockCredit>,
    at: u64,
) -> Result<u64> {
    let day = at / 86_400_000;
    let mut totals: BTreeMap<u32, (u64, u64)> = BTreeMap::new();
    let mut credited = 0_u64;
    let mut confirm = transaction.prepare_cached(
        "UPDATE blocks SET status = ?2, reward = ?3, fee = ?4, cut_height = ?5, done_at = ?6
         WHERE id = ?1 AND status < 2",
    )?;
    for block in blocks {
        let changed = confirm.execute(params![
            block.id,
            BlockStatus::Confirmed.code(),
            crate::db::money::amount(block.reward)?,
            crate::db::money::amount(block.fee)?,
            int(cut.into()),
            int(at.into())
        ])?;
        if changed == 0 {
            continue;
        }
        transaction.execute("DELETE FROM found_payload WHERE block_id=?1", [block.id])?;
        credited = credited
            .checked_add(1)
            .ok_or_else(|| anyhow!("credited block count overflow"))?;
        for (miner, amount) in block.credits {
            let entry = totals.entry(miner).or_default();
            entry.0 = entry
                .0
                .checked_add(amount)
                .ok_or_else(|| anyhow!("credit overflow"))?;
            entry.1 = entry
                .1
                .checked_add(1)
                .ok_or_else(|| anyhow!("credited miner block count overflow"))?;
        }
    }
    for (miner, (amount, blocks)) in totals {
        let mut balance = monetary_balance(transaction, miner)?.unwrap_or_default();
        balance.unpaid = balance
            .unpaid
            .checked_add(amount)
            .ok_or_else(|| anyhow!("unpaid balance overflow"))?;
        balance.earned = balance
            .earned
            .checked_add(u128::from(amount))
            .ok_or_else(|| anyhow!("lifetime earned counter capacity is exhausted"))?;
        store_balance(transaction, miner, &balance)?;
        store_earning(transaction, miner, day, amount, blocks)?;
    }
    Ok(credited)
}

/// Moves the amounts from unpaid to pending. Fails if any balance is short.
pub(super) fn create_payout(
    transaction: &Transaction<'_>,
    request_prefix: &str,
    items: &[(u32, u64)],
    at: u64,
) -> Result<(i64, String)> {
    let total = items
        .iter()
        .try_fold(0_u64, |sum, (_, amount)| sum.checked_add(*amount))
        .ok_or_else(|| anyhow!("payout total overflows"))?;
    transaction.execute_batch("SAVEPOINT payout")?;
    let created = insert_payout(transaction, request_prefix, items, total, at);
    if created.is_ok() {
        transaction.execute_batch("RELEASE payout")?;
    } else {
        transaction.execute_batch("ROLLBACK TO payout; RELEASE payout")?;
    }
    created
}

/// Durable intent precedes the first POST. Every later attempt is recovery-only.
type AttemptRow = (bool, Option<String>, Option<u32>, Option<String>);

pub(super) fn payout_attempt(
    transaction: &Transaction<'_>,
    id: i64,
    request_id: &str,
    scope: &crate::wallet::RequestScope,
) -> Result<bool> {
    let payout: Option<(String, i64)> = transaction
        .query_row(
            "SELECT request_id,status FROM payouts WHERE id=?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if payout.as_ref().is_none_or(|(request, status)| {
        request != request_id || *status != PayoutStatus::Prepared.code()
    }) {
        bail!("the prepared payout changed before its attempt");
    }
    let known: Option<AttemptRow> = transaction
        .query_row(
            "SELECT attempted,archive_id,account,chain_id FROM payout_attempts WHERE payout_id=?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((attempted, archive_id, account, chain_id)) = known else {
        bail!(
            "this earlier prepared payout has no durable attempt binding; retain its ID and debit for review"
        );
    };
    if attempted {
        if archive_id.as_deref() != Some(scope.archive_id.as_str())
            || account != Some(scope.account)
            || chain_id.as_deref() != Some(scope.chain_id.as_str())
        {
            bail!("the payout belongs to another daemon request archive, account or chain");
        }
        return Ok(true);
    }
    let changed = transaction.execute(
        "UPDATE payout_attempts SET attempted=1,archive_id=?2,account=?3,chain_id=?4
         WHERE payout_id=?1 AND attempted=0",
        params![id, scope.archive_id, scope.account, scope.chain_id],
    )?;
    if changed != 1 {
        bail!("the payout attempt intent was not recorded exactly once");
    }
    Ok(false)
}

pub(super) fn insert_payout(
    transaction: &Transaction<'_>,
    request_prefix: &str,
    items: &[(u32, u64)],
    total: u64,
    at: u64,
) -> Result<(i64, String)> {
    transaction.execute(
        "INSERT INTO payouts (request_id, status, created_at, total, recipients)
         VALUES ('', ?1, ?2, ?3, ?4)",
        params![
            PayoutStatus::Prepared.code(),
            int(at.into()),
            crate::db::money::amount(total)?,
            items.len() as i64
        ],
    )?;
    let id = transaction.last_insert_rowid();
    let request_id = format!("{request_prefix}{id}");
    transaction.execute("INSERT INTO payout_attempts(payout_id) VALUES(?1)", [id])?;
    transaction.execute(
        "UPDATE payouts SET request_id = ?2 WHERE id = ?1",
        params![id, request_id],
    )?;
    for (miner, amount) in items {
        let mut balance = monetary_balance(transaction, *miner)?
            .ok_or_else(|| anyhow!("the balance of miner {miner} is missing"))?;
        balance.unpaid = balance
            .unpaid
            .checked_sub(*amount)
            .ok_or_else(|| anyhow!("the balance of miner {miner} no longer covers the payout"))?;
        balance.pending = balance
            .pending
            .checked_add(*amount)
            .ok_or_else(|| anyhow!("pending balance overflow"))?;
        store_balance(transaction, *miner, &balance)?;
        transaction.execute(
            "INSERT INTO payout_items (payout_id, miner_id, amount) VALUES (?1, ?2, ?3)",
            params![id, miner, crate::db::money::amount(*amount)?],
        )?;
    }
    Ok((id, request_id))
}

/// Moves pending amounts to paid, or back to unpaid for a failed payout.
pub(super) fn close_payout(
    transaction: &Transaction<'_>,
    id: i64,
    paid: bool,
    error: Option<String>,
    at: u64,
) -> Result<()> {
    let status = if paid {
        PayoutStatus::Paid
    } else {
        PayoutStatus::Failed
    };
    let changed = transaction.execute(
        "UPDATE payouts SET status = ?2, error = ?3, done_at = ?4 WHERE id = ?1 AND status < 2",
        params![id, status.code(), error, int(at.into())],
    )?;
    if changed == 0 {
        return Ok(());
    }
    let mut statement = transaction
        .prepare("SELECT miner_id,amount FROM payout_items WHERE payout_id=?1 ORDER BY miner_id")?;
    let mut items = statement.query([id])?;
    while let Some(item) = items.next()? {
        let miner: u32 = item.get(0)?;
        let amount: u64 = item.get(1)?;
        let mut balance = monetary_balance(transaction, miner)?
            .ok_or_else(|| anyhow!("a payout balance is missing"))?;
        balance.pending = balance
            .pending
            .checked_sub(amount)
            .ok_or_else(|| anyhow!("a pending balance no longer covers this payout"))?;
        if paid {
            balance.paid = balance
                .paid
                .checked_add(u128::from(amount))
                .ok_or_else(|| anyhow!("lifetime paid counter capacity is exhausted"))?;
        } else {
            balance.unpaid = balance
                .unpaid
                .checked_add(amount)
                .ok_or_else(|| anyhow!("returned unpaid balance overflow"))?;
        }
        store_balance(transaction, miner, &balance)?;
    }
    Ok(())
}

type AttemptedPayoutRow = (
    String,
    i64,
    Option<String>,
    bool,
    Option<String>,
    Option<u32>,
    Option<String>,
);

/// Only the exact attempted, still-prepared row can consume owned never-signed authority.
pub(super) fn close_never_signed(
    transaction: &Transaction<'_>,
    id: i64,
    request: &str,
    scope: &crate::wallet::RequestScope,
    at: u64,
) -> Result<()> {
    let row: AttemptedPayoutRow =
        transaction.query_row("SELECT p.request_id,p.status,p.error,a.attempted,a.archive_id,a.account,a.chain_id FROM payouts p JOIN payout_attempts a ON a.payout_id=p.id WHERE p.id=?1",
            [id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?)))?;
    if row.0 != request
        || !row.3
        || row.4.as_deref() != Some(scope.archive_id.as_str())
        || row.5 != Some(scope.account)
        || row.6.as_deref() != Some(scope.chain_id.as_str())
    {
        bail!("never-signed receipt belongs to another payout attempt");
    }
    const REASON: &str = "never_signed: fee_limit_exceeded";
    if row.1 == PayoutStatus::Failed.code() && row.2.as_deref() == Some(REASON) {
        return Ok(());
    }
    if row.1 != PayoutStatus::Prepared.code() {
        bail!("a sent or closed payout cannot become never-signed");
    }
    close_payout(transaction, id, false, Some(REASON.into()), at)
}
