use super::*;
use crate::payouts::REVIEW_LOG_IDS;
use rusqlite::params;

/// Fixture-created, independently reserved payouts; every row keeps its own durable ID.
fn append_payouts(path: &Path, count: usize, prepared: bool) {
    let mut connection = db::connect(path, false).unwrap();
    let transaction = connection.transaction().unwrap();
    let last: i64 = transaction
        .query_row("SELECT COALESCE(MAX(id), 0) FROM payouts", [], |row| {
            row.get(0)
        })
        .unwrap();
    for offset in 1..=count {
        let id = last + i64::try_from(offset).unwrap();
        let status = if prepared { 0 } else { 1 };
        let tx = (!prepared).then(|| format!("{id:096x}"));
        transaction.execute(
            "INSERT INTO payouts(id,request_id,status,created_at,total,recipients,transaction_id,valid_until)
             VALUES(?1,?2,?3,1,1,1,?4,100)",
            params![id, format!("public-test-bounded-{id}"), status, tx],
        ).unwrap();
        transaction
            .execute(
                "INSERT INTO payout_items(payout_id,miner_id,amount) VALUES(?1,1,1)",
                [id],
            )
            .unwrap();
    }
    transaction
        .execute(
            "UPDATE balances SET pending=pending+?1,earned=earned+?1 WHERE miner_id=1",
            [i64::try_from(count).unwrap()],
        )
        .unwrap();
    transaction.commit().unwrap();
}

fn status_requests(backend: &Backend) -> Vec<String> {
    backend
        .state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|path| path.starts_with("/v1/transactions/") && path.ends_with("/status"))
        .cloned()
        .collect()
}

async fn assert_all_reserved(payouts: &Payouts, rows: usize) {
    let (count, active, unique) = payouts
        .reader
        .read(|connection| {
            connection.query_row(
                "SELECT COUNT(*),SUM(status < 2),COUNT(DISTINCT request_id) FROM payouts",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
        })
        .await
        .unwrap();
    assert_eq!(
        (count, active, unique),
        (rows as i64, rows as i64, rows as i64)
    );
    let balance = payouts
        .reader
        .read(|connection| read::balance(connection, 1))
        .await
        .unwrap();
    let total = RESERVED + u64::try_from(rows - 1).unwrap();
    assert_eq!(
        (
            balance.unpaid,
            balance.pending,
            balance.paid,
            balance.earned
        ),
        (0, total, 0, u128::from(total))
    );
}

#[tokio::test]
async fn review_log_cache_is_bounded_without_deleting_request_ids_or_reservations() {
    let (backend, base, server) = serving().await;
    let path = test_support::temp_path("payout-review-cache").with_extension("sqlite");
    let (payouts, writer) = sent_service(&path, base.clone(), true, false, &backend).await;
    let original = payouts.reader.read(read::open_payouts).await.unwrap();
    let id = original[0].id;
    let request_id = original[0].request_id.clone();
    for next in id..id + i64::try_from(REVIEW_LOG_IDS * 3).unwrap() {
        payouts.review(next, &"ab".repeat(48));
        let state = payouts.unresolved.lock().unwrap();
        assert!(state.seen.len() <= REVIEW_LOG_IDS);
        assert_eq!(state.order.len(), state.seen.len());
    }
    {
        let mut state = payouts.unresolved.lock().unwrap();
        assert_eq!(state.seen.len(), REVIEW_LOG_IDS);
        assert!(!state.seen.contains(&id));
        let newest = *state.order.back().unwrap();
        assert!(
            !state.insert(newest),
            "an active cache entry is still deduplicated"
        );
        assert!(state.insert(id), "an evicted log ID may be reported again");
        assert_eq!(state.order.back(), Some(&id));
        assert_eq!(state.seen.len(), REVIEW_LOG_IDS);
        state.remove(&id);
        assert!(!state.order.contains(&id));
    }
    still_reserved(&payouts, &request_id).await;
    drop(payouts);
    writer.join().unwrap();
    let (payouts, writer) = sent_service(&path, base, false, false, &backend).await;
    still_reserved(&payouts, &request_id).await;
    assert!(payouts.unresolved.lock().unwrap().is_empty());
    drop(payouts);
    writer.join().unwrap();
    server.abort();
}

#[tokio::test]
async fn bounded_settlement_revisits_old_payouts_despite_continuous_new_arrivals() {
    let (backend, base, server) = serving().await;
    scenario(&backend, Scenario::UnknownExpired);
    let path = test_support::temp_path("payout-fair-page").with_extension("sqlite");
    let (payouts, writer) = sent_service(&path, base.clone(), true, false, &backend).await;
    let original_path = format!("/v1/transactions/{}/status", "ab".repeat(48));
    let initial = read::MAX_OPEN_PAYOUT_CHECKS + 1;
    append_payouts(&path, initial - 1, false);
    let mut rows = initial;
    let mut revisited = false;
    // No timing assumption: even a slow machine processing one completed row per pass
    // must revisit the first row within a full fixed cycle plus one pass.
    for _ in 0..=initial + 1 {
        let before = status_requests(&backend).len();
        assert_eq!(payouts.settle().await.unwrap(), 0);
        let requests = status_requests(&backend);
        let checked = requests.len() - before;
        assert!(checked > 0 && checked <= read::MAX_OPEN_PAYOUT_CHECKS);
        assert_all_reserved(&payouts, rows).await;
        assert!(backend.state.lock().unwrap().sends.is_empty());
        if requests
            .iter()
            .filter(|path| *path == &original_path)
            .count()
            >= 2
        {
            revisited = true;
            break;
        }
        append_payouts(&path, read::MAX_OPEN_PAYOUT_CHECKS + 1, false);
        rows += read::MAX_OPEN_PAYOUT_CHECKS + 1;
    }
    assert!(
        revisited,
        "new arrivals must not postpone old unresolved payouts indefinitely"
    );
    drop(payouts);
    writer.join().unwrap();
    let (payouts, writer) = sent_service(&path, base, false, false, &backend).await;
    assert_all_reserved(&payouts, rows).await;
    drop(payouts);
    writer.join().unwrap();
    server.abort();
}

#[tokio::test]
async fn a_prepared_payout_outside_the_current_page_still_blocks_a_new_round() {
    let (backend, base, server) = serving().await;
    scenario(&backend, Scenario::UnknownExpired);
    let path = test_support::temp_path("payout-prepared-outside-page").with_extension("sqlite");
    let (payouts, writer) = sent_service(&path, base, true, false, &backend).await;
    let initial = read::MAX_OPEN_PAYOUT_CHECKS * 2 + 1;
    append_payouts(&path, initial - 1, false);
    assert_eq!(payouts.settle().await.unwrap(), 0);
    let cycle = payouts.unresolved.lock().unwrap().through;
    assert_eq!(cycle, initial as i64);
    append_payouts(&path, 1, true);
    let before = status_requests(&backend).len();
    assert_eq!(
        payouts.settle().await.unwrap(),
        1,
        "global prepared detection must not depend on the current bounded page"
    );
    assert!(status_requests(&backend).len() - before <= read::MAX_OPEN_PAYOUT_CHECKS);
    assert_eq!(payouts.unresolved.lock().unwrap().through, cycle);
    assert!(
        backend.state.lock().unwrap().sends.is_empty(),
        "the prepared row lies beyond this cycle and was not posted"
    );
    assert_all_reserved(&payouts, initial + 1).await;
    drop(payouts);
    writer.join().unwrap();
    server.abort();
}

#[tokio::test]
async fn payout_page_bounds_reject_invalid_limits_and_use_the_active_id_index() {
    let (backend, base, server) = serving().await;
    let path = test_support::temp_path("payout-page-query").with_extension("sqlite");
    let (payouts, writer) = sent_service(&path, base, true, false, &backend).await;
    append_payouts(&path, read::MAX_OPEN_PAYOUT_CHECKS + 1, false);
    payouts
        .reader
        .read(|connection| {
            for (after, through, limit) in [
                (-1, 1, 1),
                (2, 1, 1),
                (0, 1, 0),
                (0, 1, read::MAX_OPEN_PAYOUT_CHECKS + 1),
            ] {
                assert!(read::open_payout_page(connection, after, through, limit).is_err());
            }
            let through = read::maximum_open_payout_id(connection)?;
            let first =
                read::open_payout_page(connection, 0, through, read::MAX_OPEN_PAYOUT_CHECKS)?;
            assert_eq!(first.len(), read::MAX_OPEN_PAYOUT_CHECKS);
            let rest = read::open_payout_page(
                connection,
                first.last().unwrap().id,
                through,
                read::MAX_OPEN_PAYOUT_CHECKS,
            )?;
            assert_eq!(rest.len(), 2);
            assert!(first.iter().all(|row| row.id < rest[0].id));
            assert!(first.iter().chain(&rest).all(|row| row.items.len() == 1));
            let mut query = connection.prepare(
                "EXPLAIN QUERY PLAN SELECT id FROM payouts INDEXED BY payouts_open_id
             WHERE status < 2 AND id > ?1 AND id <= ?2 ORDER BY id LIMIT ?3",
            )?;
            let plans: Vec<String> = query
                .query_map(params![0, through, 64], |row| row.get(3))?
                .collect::<rusqlite::Result<_>>()?;
            assert!(
                plans
                    .iter()
                    .any(|detail| detail.contains("payouts_open_id"))
            );
            assert!(!plans.iter().any(|detail| detail.contains("TEMP B-TREE")));
            Ok(())
        })
        .await
        .unwrap();
    drop(payouts);
    writer.join().unwrap();
    server.abort();
}
