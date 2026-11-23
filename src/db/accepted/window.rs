use super::*;
use crate::pplns::Window;
use std::collections::BTreeMap;

/// The SQLite cursor streams exact eligible shares. A RAM cache miss is never an empty window.
pub fn window(connection: &Connection, end: u64, work: u128, since: u64) -> Result<Window> {
    ensure!(
        end > 0 && work > 0,
        "a PPLNS window needs an accepted end and positive work"
    );
    let mut statement = connection
        .prepare("SELECT id,time,miner_id,weight FROM shares WHERE id<=?1 ORDER BY id DESC")?;
    let mut rows = statement.query([i64::try_from(end)?])?;
    let mut window = Window::default();
    let mut weights = BTreeMap::<u32, u128>::new();
    let mut first = true;
    while let Some(row) = rows.next()? {
        let (id, time, miner, weight): (u64, u64, u32, u64) =
            (row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?);
        ensure!(
            id > 0 && miner > 0 && weight > 0,
            "malformed accepted PPLNS share"
        );
        if first {
            ensure!(id == end, "the accepted PPLNS window end is missing");
            first = false;
        }
        if time < since {
            break;
        }
        if !weights.contains_key(&miner) {
            ensure!(
                weights.len() < MAX_ACTIVE_PARTICIPANTS,
                "the PPLNS participant admission bound was exceeded"
            );
        }
        let take = u128::from(weight).min(
            work.checked_sub(window.total)
                .ok_or_else(|| anyhow!("window work underflow"))?,
        );
        let amount = weights.entry(miner).or_default();
        *amount = amount
            .checked_add(take)
            .ok_or_else(|| anyhow!("miner window weight overflow"))?;
        window.total = window
            .total
            .checked_add(take)
            .ok_or_else(|| anyhow!("window work overflow"))?;
        window.shares = window
            .shares
            .checked_add(1)
            .ok_or_else(|| anyhow!("window share counter exhausted"))?;
        window.first = id;
        if window.total == work {
            window.complete = true;
            break;
        }
    }
    ensure!(
        !first && window.total > 0,
        "the accepted PPLNS window is not retained"
    );
    let retired = crate::db::setting(connection, "shares_pruned_through")?
        .map(|value| value.parse::<u64>())
        .transpose()?;
    ensure!(
        window.complete || retired.is_none_or(|retired| since > retired),
        "the requested PPLNS horizon includes already retired accepted work"
    );
    window.weights = weights.into_iter().collect();
    Ok(window)
}
