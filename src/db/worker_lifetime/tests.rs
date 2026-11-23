use super::*;
use crate::{
    config::Mode,
    db::{accepted, accepted::tests::request},
};

fn memory() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    connection.execute_batch(crate::db::SCHEMA).unwrap();
    connection
}
fn add(connection: &mut Connection, miner: u32, name: &str, seen: u64) -> WorkerId {
    let tx = connection.transaction().unwrap();
    let id = worker(&tx, miner, name, seen).unwrap();
    tx.commit().unwrap();
    id
}
fn prune(connection: &mut Connection, before: u64) -> bool {
    let tx = connection.transaction().unwrap();
    let more = retire(&tx, before).unwrap();
    tx.commit().unwrap();
    validate(connection).unwrap();
    more
}
fn count(connection: &Connection) -> u64 {
    connection
        .query_row("SELECT COUNT(*) FROM workers", [], |row| row.get(0))
        .unwrap()
}

#[test]
fn per_miner_saturation_refuses_only_new_names_without_dropping_known_shares() {
    let mut connection = memory();
    {
        let tx = connection.transaction().unwrap();
        let miner = accepted::miner(&tx, "owner", 1).unwrap();
        for index in 0..MAX_MINER_ALIASES {
            worker(&tx, miner, &format!("rig{index}"), 1).unwrap();
        }
        tx.commit().unwrap();
    }
    validate(&connection).unwrap();
    {
        let tx = connection.transaction().unwrap();
        let mut work = request(1, Mode::Pplns, "owner");
        work.key.name = "new-label".into();
        assert!(
            accepted::apply(&tx, work)
                .unwrap_err()
                .to_string()
                .contains("alias capacity")
        );
        tx.rollback().unwrap();
    }
    assert_eq!(count(&connection), MAX_MINER_ALIASES);
    let receipt = {
        let tx = connection.transaction().unwrap();
        let mut work = request(1, Mode::Pplns, "owner");
        work.key.name = "rig0".into();
        let receipt = accepted::apply(&tx, work).unwrap();
        tx.commit().unwrap();
        receipt
    };
    assert!(receipt.fresh);
    assert_eq!(receipt.worker, 1);
    assert_eq!(
        accepted::receipt(&connection, request(1, Mode::Pplns, "owner").fingerprint)
            .unwrap()
            .unwrap()
            .id,
        receipt.id
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM shares", [], |row| row
                .get::<_, u64>(0))
            .unwrap(),
        1
    );
    validate(&connection).unwrap();
}

#[test]
fn the_actual_global_cap_keeps_existing_names_usable_and_refuses_new_ones() {
    let mut connection = memory();
    connection
        .execute_batch(
            "WITH RECURSIVE n(id) AS (VALUES(1) UNION ALL SELECT id+1 FROM n WHERE id<262144)
         INSERT INTO workers(id,miner_id,name,first_seen,last_seen)
         SELECT id,((id-1)/1024)+1,CAST(id AS TEXT),1,1 FROM n;
         UPDATE worker_catalogue SET live_count=262144,last_id=262144;",
        )
        .unwrap();
    validate(&connection).unwrap();
    assert_eq!(count(&connection), MAX_ALIASES);
    {
        let tx = connection.transaction().unwrap();
        assert_eq!(worker(&tx, 1, "1", 2).unwrap(), 1);
        assert!(
            worker(&tx, 257, "new", 2)
                .unwrap_err()
                .to_string()
                .contains("alias capacity")
        );
        tx.rollback().unwrap();
    }
    assert_eq!(count(&connection), MAX_ALIASES);
    assert_eq!(catalogue(&connection).unwrap().last, MAX_ALIASES);
}

#[test]
fn every_financial_reference_and_recent_worker_survives_alias_retirement() {
    let mut connection = memory();
    for id in 1..=5 {
        assert_eq!(
            add(
                &mut connection,
                1,
                &format!("rig{id}"),
                if id == 4 { 20 } else { 1 }
            ),
            id
        );
    }
    connection.execute_batch(
        "INSERT INTO shares VALUES(1,1,1,1,100);
         INSERT INTO accepted_work(job_id,nonce,fingerprint,id,time,miner_id,worker_id,mode,weight,window_end)
         VALUES(zeroblob(48),zeroblob(32),zeroblob(48),1,1,1,2,0,100,10);
         INSERT INTO blocks(id,hash,mode,status,miner_id,worker_id,height,window_end,found_at,network_difficulty,round_work,fee_bps)
         VALUES(1,'original-found-work',0,2,1,3,0,10,1,100,100,0);",
    ).unwrap();
    assert!(!prune(&mut connection, 10));
    let ids: Vec<WorkerId> = connection
        .prepare("SELECT id FROM workers ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(ids, vec![1, 2, 3, 4]);
    assert_eq!(catalogue(&connection).unwrap().last, 5);
    connection
        .execute_batch("DELETE FROM shares; DELETE FROM accepted_work; DELETE FROM blocks;")
        .unwrap();
    assert!(!prune(&mut connection, 10));
    assert_eq!(count(&connection), 1);
    assert_eq!(
        add(&mut connection, 1, "rig5", 30),
        6,
        "a retired alias never reuses its old ID"
    );
}

#[test]
fn bounded_scans_make_progress_past_pinned_workers_and_ignore_new_arrivals_until_next_pass() {
    let mut connection = memory();
    {
        let tx = connection.transaction().unwrap();
        for id in 1..=1_100_u64 {
            let miner = u32::try_from((id - 1) / MAX_MINER_ALIASES + 1).unwrap();
            worker(&tx, miner, &id.to_string(), if id <= 500 { 20 } else { 1 }).unwrap();
        }
        tx.commit().unwrap();
    }
    assert!(prune(&mut connection, 10));
    assert_eq!(
        count(&connection),
        1_100,
        "only 500 fresh candidates were scanned"
    );
    let new = add(&mut connection, 2, "arrived-during-pass", 1);
    assert_eq!(new, 1_101);
    assert!(prune(&mut connection, 10));
    assert_eq!(count(&connection), 601);
    assert!(!prune(&mut connection, 10));
    assert_eq!(
        count(&connection),
        501,
        "the finite pass excludes its new arrival"
    );
    assert!(prune(&mut connection, 10));
    assert!(!prune(&mut connection, 10));
    assert_eq!(count(&connection), 500);
    assert_eq!(catalogue(&connection).unwrap().last, 1_101);
}

#[test]
fn wide_worker_ids_and_gc_cursor_survive_restart_without_reusing_removed_ids() {
    let path = crate::test_support::temp_path("worker-alias-restart").with_extension("sqlite");
    let mut connection = Connection::open(&path).unwrap();
    crate::db::migrate(&mut connection).unwrap();
    connection
        .execute(
            "UPDATE worker_catalogue SET last_id=?1",
            [u64::from(u32::MAX)],
        )
        .unwrap();
    let wide = add(&mut connection, 1, "wide", 1);
    assert_eq!(wide, u64::from(u32::MAX) + 1);
    validate(&connection).unwrap();
    drop(connection);
    let mut connection = Connection::open(&path).unwrap();
    crate::db::migrate(&mut connection).unwrap();
    assert_eq!(add(&mut connection, 1, "wide", 1), wide);
    assert!(!prune(&mut connection, 10));
    drop(connection);
    let mut connection = Connection::open(&path).unwrap();
    crate::db::migrate(&mut connection).unwrap();
    assert_eq!(add(&mut connection, 1, "wide", 20), wide + 1);
    connection
        .execute("UPDATE worker_catalogue SET last_id=?1", [MAX_WORKER_ID])
        .unwrap();
    {
        let tx = connection.transaction().unwrap();
        assert!(
            worker(&tx, 1, "exhausted", 20)
                .unwrap_err()
                .to_string()
                .contains("exhausted")
        );
        assert_eq!(worker(&tx, 1, "wide", 20).unwrap(), wide + 1);
        tx.rollback().unwrap();
    }
    assert_eq!(count(&connection), 1);
    drop(connection);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn stale_telemetry_cannot_rebind_a_retired_worker_and_bad_accounting_refuses_restart() {
    let mut connection = memory();
    let old = add(&mut connection, 1, "rig", 1);
    assert!(!prune(&mut connection, 10));
    let new = add(&mut connection, 1, "rig", 20);
    assert_ne!(old, new);
    assert_eq!(
        connection
            .execute("UPDATE workers SET rejected=123 WHERE id=?1", [old])
            .unwrap(),
        0
    );
    assert_eq!(
        connection
            .query_row("SELECT rejected FROM workers WHERE id=?1", [new], |row| row
                .get::<_, u64>(0))
            .unwrap(),
        0
    );
    connection
        .execute("UPDATE worker_catalogue SET live_count=0", [])
        .unwrap();
    assert!(validate(&connection).is_err());
    connection
        .execute("UPDATE worker_catalogue SET live_count=1,last_id=0", [])
        .unwrap();
    assert!(validate(&connection).is_err());
}

#[test]
fn alias_gc_rollback_preserves_rows_counts_and_restart_cursor() {
    let mut connection = memory();
    add(&mut connection, 1, "retirable", 1);
    {
        let tx = connection.transaction().unwrap();
        assert!(!retire(&tx, 10).unwrap());
        assert_eq!(count(&tx), 0);
        tx.rollback().unwrap();
    }
    assert_eq!(count(&connection), 1);
    validate(&connection).unwrap();
    assert_eq!(
        (
            catalogue(&connection).unwrap().after,
            catalogue(&connection).unwrap().through
        ),
        (0, 0)
    );
    assert!(!prune(&mut connection, 10));
    assert_eq!(add(&mut connection, 1, "retirable", 20), 2);
}

#[test]
fn an_incomplete_scan_resumes_at_its_durable_cursor_after_reopen() {
    let path = crate::test_support::temp_path("worker-gc-cursor").with_extension("sqlite");
    let mut connection = Connection::open(&path).unwrap();
    crate::db::migrate(&mut connection).unwrap();
    {
        let tx = connection.transaction().unwrap();
        for id in 1..=600 {
            worker(&tx, 1, &id.to_string(), if id <= 500 { 20 } else { 1 }).unwrap();
        }
        tx.commit().unwrap();
    }
    assert!(prune(&mut connection, 10));
    assert_eq!(
        (
            catalogue(&connection).unwrap().after,
            catalogue(&connection).unwrap().through
        ),
        (500, 600)
    );
    drop(connection);
    let mut connection = Connection::open(&path).unwrap();
    crate::db::migrate(&mut connection).unwrap();
    assert!(!prune(&mut connection, 10));
    assert_eq!(
        count(&connection),
        500,
        "restart must not repeatedly rescan the first pinned batch"
    );
    assert_eq!(catalogue(&connection).unwrap().last, 600);
    assert_eq!(add(&mut connection, 1, "after-restart", 20), 601);
    drop(connection);
    std::fs::remove_file(path).unwrap();
}
