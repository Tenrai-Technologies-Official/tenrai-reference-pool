use super::*;
use rusqlite::{params, types::Value};

fn fund(connection: &Connection) {
    connection
        .execute(
            "INSERT INTO balances VALUES(1,100,7,?1,?2)",
            params![(u128::MAX - 107).to_string(), u128::MAX.to_string()],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO earnings VALUES(1,1,?1,1)",
            [u128::MAX.to_string()],
        )
        .unwrap();
    connection.execute("INSERT INTO payouts(id,request_id,status,created_at,total,recipients) VALUES(1,'kept-request',0,1,7,1)", []).unwrap();
    connection
        .execute(
            "INSERT INTO payout_items(payout_id,miner_id,amount) VALUES(1,1,7)",
            [],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO payout_attempts VALUES(1,1,?1,0,?2)",
            params!["ab".repeat(32), "cd".repeat(48)],
        )
        .unwrap();
    connection.execute("INSERT INTO blocks(id,hash,mode,status,miner_id,worker_id,height,window_end,found_at,network_difficulty,round_work,fee_bps,reward,fee)
        VALUES(1,'retained-block',0,2,1,1,0,10,1,100,100,0,7,0)", []).unwrap();
}
fn initialized() -> Connection {
    let mut connection = Connection::open_in_memory().unwrap();
    crate::db::migrate(&mut connection).unwrap();
    fund(&connection);
    connection
}
fn reserved(connection: &Connection) {
    let held: (String,i64,i64,bool) = connection.query_row(
        "SELECT p.request_id,p.total,i.amount,a.attempted FROM payouts p JOIN payout_items i ON i.payout_id=p.id JOIN payout_attempts a ON a.payout_id=p.id WHERE p.id=1",
        [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).unwrap();
    assert_eq!(held, ("kept-request".into(), 7, 7, true));
}

#[test]
fn current_exact_money_and_reservations_survive_reopen_without_table_replacement() {
    let path = crate::test_support::temp_path("money-current-reopen").with_extension("sqlite");
    let mut connection = Connection::open(&path).unwrap();
    crate::db::migrate(&mut connection).unwrap();
    fund(&connection);
    let before: String = connection
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE name='balances'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    drop(connection);
    let mut connection = Connection::open(&path).unwrap();
    crate::db::migrate(&mut connection).unwrap();
    reserved(&connection);
    let amounts = connection
        .query_row(
            "SELECT unpaid,pending,paid,earned FROM balances",
            [],
            |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, u64>(1)?,
                    counter(row, 2)?,
                    counter(row, 3)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(amounts, (100, 7, u128::MAX - 107, u128::MAX));
    assert_eq!(
        u128::from(amounts.0) + u128::from(amounts.1) + amounts.2,
        amounts.3
    );
    assert_eq!(
        connection
            .query_row("SELECT amount FROM earnings", [], |row| counter(row, 0))
            .unwrap(),
        u128::MAX
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE name='balances'",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        before
    );
    assert_eq!(
        connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        crate::db::SCHEMA_VERSION
    );
    drop(connection);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn restart_refuses_inexact_negative_noncanonical_and_overflowing_money_without_rewriting() {
    for (table, column, value) in [
        ("balances", "paid", Value::Text("00".into())),
        ("balances", "earned", Value::Text("1e30".into())),
        ("balances", "paid", Value::Real(1.5)),
        (
            "balances",
            "paid",
            Value::Text("340282366920938463463374607431768211456".into()),
        ),
        ("balances", "unpaid", Value::Integer(-1)),
        ("balances", "pending", Value::Real(0.5)),
        ("earnings", "amount", Value::Text("not-an-integer".into())),
        ("earnings", "blocks", Value::Integer(-1)),
        ("payouts", "total", Value::Integer(-1)),
        ("payouts", "fee", Value::Real(0.25)),
        ("payout_items", "amount", Value::Real(0.25)),
        ("payout_items", "fee_share", Value::Integer(-1)),
        ("blocks", "reward", Value::Real(0.25)),
        ("blocks", "fee", Value::Integer(-1)),
    ] {
        let mut connection = initialized();
        connection
            .execute_batch("PRAGMA ignore_check_constraints=ON")
            .unwrap();
        connection
            .execute(&format!("UPDATE {table} SET {column}=?1"), [value])
            .unwrap();
        connection
            .execute_batch("PRAGMA ignore_check_constraints=OFF")
            .unwrap();
        let quoted = format!("SELECT quote({column}) FROM {table}");
        let before: String = connection.query_row(&quoted, [], |row| row.get(0)).unwrap();
        assert!(
            crate::db::migrate(&mut connection).is_err(),
            "{table}.{column}"
        );
        assert_eq!(
            connection
                .query_row(&quoted, [], |row| row.get::<_, String>(0))
                .unwrap(),
            before
        );
        assert_eq!(
            column_type(&connection, "balances", "paid").unwrap(),
            "TEXT"
        );
        assert_eq!(
            connection
                .query_row("SELECT request_id FROM payouts", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "kept-request"
        );
    }
}

#[test]
fn failed_current_validation_rolls_back_schema_changes_and_preserves_prepared_money() {
    let mut connection = initialized();
    connection
        .execute_batch(
            "DROP INDEX balances_unpaid; PRAGMA ignore_check_constraints=ON;
        UPDATE balances SET paid='00'; PRAGMA ignore_check_constraints=OFF;",
        )
        .unwrap();
    assert!(crate::db::migrate(&mut connection).is_err());
    assert!(
        !connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='balances_unpaid')",
                [],
                |row| row.get::<_, bool>(0)
            )
            .unwrap()
    );
    reserved(&connection);
    assert_eq!(
        connection
            .query_row("SELECT paid FROM balances", [], |row| row
                .get::<_, String>(0))
            .unwrap(),
        "00"
    );
}

#[test]
fn only_empty_genesis_or_current_format_can_open_and_other_versions_are_untouched() {
    for version in [1, 2, 3, crate::db::SCHEMA_VERSION + 1] {
        let mut connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE owner_data(value TEXT); INSERT INTO owner_data VALUES('preserve');",
            )
            .unwrap();
        connection
            .pragma_update(None, "user_version", version)
            .unwrap();
        assert!(crate::db::migrate(&mut connection).is_err());
        assert_eq!(
            connection
                .query_row("SELECT value FROM owner_data", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "preserve"
        );
        assert_eq!(
            connection
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            version
        );
        assert!(
            !connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='payouts')",
                    [],
                    |row| row.get::<_, bool>(0)
                )
                .unwrap()
        );
    }
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE owner_data(value TEXT); INSERT INTO owner_data VALUES('unversioned');",
        )
        .unwrap();
    assert!(crate::db::migrate(&mut connection).is_err());
    assert_eq!(
        connection
            .query_row("SELECT value FROM owner_data", [], |row| row
                .get::<_, String>(0))
            .unwrap(),
        "unversioned"
    );
}

#[test]
fn current_format_refuses_an_integer_turnover_column_instead_of_migrating_it() {
    let mut connection = Connection::open_in_memory().unwrap();
    crate::db::migrate(&mut connection).unwrap();
    connection.execute_batch("DROP TABLE balances; CREATE TABLE balances(miner_id INTEGER PRIMARY KEY,unpaid INTEGER,pending INTEGER,paid INTEGER,earned INTEGER);
        INSERT INTO balances VALUES(1,3,4,5,12);").unwrap();
    assert!(crate::db::migrate(&mut connection).is_err());
    assert_eq!(
        column_type(&connection, "balances", "paid").unwrap(),
        "INTEGER"
    );
    assert_eq!(
        connection
            .query_row("SELECT paid FROM balances", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        5
    );
}

#[test]
fn counters_require_canonical_bounded_text_and_refuse_explicit_exhaustion() {
    let connection = Connection::open_in_memory().unwrap();
    for atoms in [0, 1, i64::MAX as u128, u64::MAX as u128, u128::MAX] {
        assert_eq!(
            connection
                .query_row("SELECT ?1", [atoms.to_string()], |row| counter(row, 0))
                .unwrap(),
            atoms
        );
    }
    for value in [
        "",
        "00",
        "01",
        "-1",
        "+1",
        "1.0",
        "1e3",
        "\u{ff11}\u{ff12}\u{ff13}",
        "340282366920938463463374607431768211456",
        "9999999999999999999999999999999999999999",
    ] {
        assert!(
            connection
                .query_row("SELECT ?1", [value], |row| counter(row, 0))
                .is_err()
        );
    }
    for query in ["SELECT 1", "SELECT 1.5", "SELECT NULL"] {
        assert!(
            connection
                .query_row(query, [], |row| counter(row, 0))
                .is_err()
        );
    }
    assert!(amount(u64::MAX).is_err());
    assert_eq!(amount(i64::MAX as u64).unwrap(), i64::MAX);
}
