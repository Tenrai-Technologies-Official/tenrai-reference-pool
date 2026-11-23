use super::*;

fn database() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    connection.execute_batch("CREATE TABLE balances(miner_id INTEGER PRIMARY KEY,unpaid INTEGER,pending INTEGER,paid TEXT,earned TEXT); CREATE TABLE earnings(miner_id INTEGER,day INTEGER,amount TEXT,blocks INTEGER); CREATE TABLE blocks(id INTEGER PRIMARY KEY,mode INTEGER,found_at INTEGER,reward INTEGER); CREATE TABLE payouts(status INTEGER); CREATE TABLE miners(id INTEGER PRIMARY KEY,address TEXT); CREATE TABLE payout_thresholds(miner_id INTEGER,atoms INTEGER);").unwrap();
    connection
}
fn figures(connection: &Connection) -> rusqlite::Result<Totals> {
    totals(connection, 1)
}

#[test]
fn exact_lifetime_readers_and_totals_cross_both_signed_and_unsigned_64_bit_boundaries() {
    let connection = database();
    let wide = u128::from(u64::MAX) + 123;
    connection
        .execute(
            "INSERT INTO balances VALUES(1,?1,1,?2,?3)",
            params![i64::MAX, wide.to_string(), (wide + 10).to_string()],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO balances VALUES(2,?1,0,?2,?2)",
            params![i64::MAX, wide.to_string()],
        )
        .unwrap();
    let first = balance(&connection, 1).unwrap();
    assert_eq!(first.unpaid, i64::MAX as u64);
    assert_eq!(first.pending, 1);
    assert_eq!(first.paid, wide);
    assert_eq!(first.earned, wide + 10);
    let all = figures(&connection).unwrap();
    assert_eq!(all.unpaid, u64::MAX);
    assert_eq!(all.paid, 2 * wide);
    connection
        .execute("INSERT INTO earnings VALUES(1,12,?1,3)", [wide.to_string()])
        .unwrap();
    assert_eq!(earnings(&connection, 1, 0).unwrap(), vec![(12, wide, 3)]);
}

#[test]
fn malformed_current_and_historical_cells_are_errors_instead_of_zero_or_rounding() {
    for value in ["-1", "1.5"] {
        let connection = database();
        connection
            .execute(
                &format!("INSERT INTO balances VALUES(1,{value},0,'0','0')"),
                [],
            )
            .unwrap();
        assert!(balance(&connection, 1).is_err());
        assert!(figures(&connection).is_err());
    }
    for value in ["01", "-1", "1.5", "340282366920938463463374607431768211456"] {
        let connection = database();
        connection
            .execute("INSERT INTO balances VALUES(1,0,0,?1,'0')", [value])
            .unwrap();
        assert!(balance(&connection, 1).is_err());
        assert!(figures(&connection).is_err());
    }
}

#[test]
fn exact_total_exhaustion_is_rejected_without_a_saturated_or_wrapped_result() {
    let connection = database();
    connection
        .execute(
            "INSERT INTO balances VALUES(1,?1,1,'0','0'),(2,?1,1,'0','0')",
            [i64::MAX],
        )
        .unwrap();
    assert!(figures(&connection).is_err());
    connection.execute("DELETE FROM balances", []).unwrap();
    connection
        .execute(
            "INSERT INTO balances VALUES(1,0,0,?1,'0'),(2,0,0,'1','0')",
            [u128::MAX.to_string()],
        )
        .unwrap();
    assert!(figures(&connection).is_err());
}

#[test]
fn minute_rewards_are_streamed_exactly_above_u64_without_sql_sum() {
    let connection = database();
    connection.execute("INSERT INTO blocks VALUES(1,0,60000,?1),(2,1,60000,?1),(3,0,60000,?1),(4,1,60000,NULL)", [i64::MAX]).unwrap();
    assert_eq!(
        day(&connection, 0).unwrap(),
        vec![(1, [2, 2], 3 * i64::MAX as u128)]
    );
    connection
        .execute("UPDATE blocks SET reward=-1 WHERE id=1", [])
        .unwrap();
    assert!(day(&connection, 0).is_err());
}

#[test]
fn oversized_active_minimum_is_an_error_instead_of_a_clamped_payout_threshold() {
    let connection = database();
    connection
        .execute("INSERT INTO miners VALUES(1,'functional')", [])
        .unwrap();
    connection
        .execute("INSERT INTO balances VALUES(1,?1,0,'0','0')", [i64::MAX])
        .unwrap();
    assert_eq!(payable(&connection, 1, 1).unwrap().len(), 1);
    assert!(payable(&connection, i64::MAX as u64 + 1, 1).is_err());
    assert!(totals(&connection, i64::MAX as u64 + 1).is_err());
    assert!(figures(&connection).is_ok());
}
