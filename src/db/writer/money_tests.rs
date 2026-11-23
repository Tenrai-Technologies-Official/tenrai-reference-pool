use super::*;

fn database(path: &std::path::Path) -> Connection {
    let mut connection = Connection::open(path).unwrap();
    crate::db::migrate(&mut connection).unwrap();
    connection
        .execute(
            "INSERT OR IGNORE INTO miners VALUES(1,'test-only-address',1)",
            [],
        )
        .unwrap();
    connection
}

fn found(connection: &Connection) {
    connection.execute("INSERT INTO blocks(id,hash,mode,status,miner_id,worker_id,height,window_end,found_at,network_difficulty,round_work,fee_bps)
        VALUES(1,?1,0,0,1,1,1,96,1,1,1,0)",["ab".repeat(48)]).unwrap();
}

fn prepare(transaction: &Transaction<'_>, amount: u64) -> Result<(i64, String)> {
    create_payout(transaction, "money-", &[(1, amount)], 1)
}

#[test]
fn lifetime_and_daily_turnover_cross_signed_and_unsigned_64bit_limits_exactly() {
    let path = crate::test_support::temp_path("wide-money-writer").with_extension("sqlite");
    let mut connection = database(&path);
    let initial_paid = u128::from(u64::MAX) - 3;
    {
        let transaction = connection.transaction().unwrap();
        store_balance(
            &transaction,
            1,
            &MonetaryBalance {
                unpaid: 1000,
                pending: 0,
                paid: initial_paid,
                earned: initial_paid + 1000,
            },
        )
        .unwrap();
        transaction
            .execute(
                "INSERT INTO earnings VALUES(1,1,?1,1)",
                [(i64::MAX as u128).to_string()],
            )
            .unwrap();
        transaction.commit().unwrap();
    }
    found(&connection);
    let transaction = connection.transaction().unwrap();
    assert_eq!(
        credit(
            &transaction,
            2,
            vec![BlockCredit {
                id: 1,
                reward: 5,
                fee: 0,
                credits: vec![(1, 5)]
            }],
            86_400_000
        )
        .unwrap(),
        1
    );
    assert_eq!(
        credit(
            &transaction,
            3,
            vec![BlockCredit {
                id: 1,
                reward: 5,
                fee: 0,
                credits: vec![(1, 5)]
            }],
            86_400_000
        )
        .unwrap(),
        0
    );
    let (payout, request_id) = prepare(&transaction, 7).unwrap();
    transaction.commit().unwrap();
    let transaction = connection.transaction().unwrap();
    close_payout(&transaction, payout, true, None, 4).unwrap();
    close_payout(&transaction, payout, false, Some("late retry".into()), 5).unwrap();
    transaction.commit().unwrap();
    let balance = monetary_balance(&connection.unchecked_transaction().unwrap(), 1)
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            balance.unpaid,
            balance.pending,
            balance.paid,
            balance.earned
        ),
        (998, 0, initial_paid + 7, initial_paid + 1005)
    );
    assert_eq!(
        u128::from(balance.unpaid) + u128::from(balance.pending) + balance.paid,
        balance.earned
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT amount FROM earnings WHERE miner_id=1 AND day=1",
                [],
                |row| super::super::money::counter(row, 0)
            )
            .unwrap(),
        i64::MAX as u128 + 5
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT request_id FROM payouts WHERE id=?1",
                [payout],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        request_id
    );
    drop(connection);
    let mut connection = Connection::open(&path).unwrap();
    crate::db::migrate(&mut connection).unwrap();
    let transaction = connection.transaction().unwrap();
    let balance = monetary_balance(&transaction, 1).unwrap().unwrap();
    assert_eq!(balance.paid, initial_paid + 7);
    assert_eq!(balance.earned, initial_paid + 1005);
    assert_eq!(
        u128::from(balance.unpaid) + u128::from(balance.pending) + balance.paid,
        balance.earned
    );
    close_payout(&transaction, payout, true, None, 6).unwrap();
    transaction.commit().unwrap();
    drop(connection);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn monetary_capacity_failures_roll_back_block_credit_and_keep_prepared_reservations() {
    for failure in ["earned", "daily", "unpaid", "pending", "paid"] {
        let mut connection = Connection::open_in_memory().unwrap();
        crate::db::migrate(&mut connection).unwrap();
        found(&connection);
        let transaction = connection.transaction().unwrap();
        let mut balance = MonetaryBalance {
            unpaid: 1,
            pending: 0,
            paid: 0,
            earned: 1,
        };
        match failure {
            "earned" => balance.earned = u128::MAX,
            "unpaid" => balance.unpaid = i64::MAX as u64,
            "pending" => balance.pending = i64::MAX as u64,
            "paid" => balance.paid = u128::MAX,
            _ => {}
        }
        store_balance(&transaction, 1, &balance).unwrap();
        if failure == "daily" {
            transaction
                .execute(
                    "INSERT INTO earnings VALUES(1,1,?1,1)",
                    [u128::MAX.to_string()],
                )
                .unwrap();
        }
        transaction.commit().unwrap();
        if matches!(failure, "earned" | "daily" | "unpaid") {
            let transaction = connection.transaction().unwrap();
            assert!(
                credit(
                    &transaction,
                    2,
                    vec![BlockCredit {
                        id: 1,
                        reward: 1,
                        fee: 0,
                        credits: vec![(1, 1)]
                    }],
                    86_400_000
                )
                .is_err(),
                "{failure}"
            );
            drop(transaction);
            let status: i64 = connection
                .query_row("SELECT status FROM blocks WHERE id=1", [], |row| row.get(0))
                .unwrap();
            assert_eq!(status, BlockStatus::Found.code());
            let transaction = connection.transaction().unwrap();
            let actual = monetary_balance(&transaction, 1).unwrap().unwrap();
            assert_eq!(
                (actual.unpaid, actual.pending, actual.paid, actual.earned),
                (
                    balance.unpaid,
                    balance.pending,
                    balance.paid,
                    balance.earned
                )
            );
        } else if failure == "pending" {
            let transaction = connection.transaction().unwrap();
            assert!(prepare(&transaction, 1).is_err());
            drop(transaction);
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM payouts", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                0
            );
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM payout_attempts", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                0
            );
        } else {
            let transaction = connection.transaction().unwrap();
            let (id, request) = prepare(&transaction, 1).unwrap();
            transaction.commit().unwrap();
            let transaction = connection.transaction().unwrap();
            assert!(close_payout(&transaction, id, true, None, 2).is_err());
            drop(transaction);
            let (status, rid): (i64, String) = connection
                .query_row(
                    "SELECT status,request_id FROM payouts WHERE id=?1",
                    [id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!((status, rid), (PayoutStatus::Prepared.code(), request));
            let transaction = connection.transaction().unwrap();
            let actual = monetary_balance(&transaction, 1).unwrap().unwrap();
            assert_eq!(
                (actual.unpaid, actual.pending, actual.paid),
                (0, 1, u128::MAX)
            );
        }
    }
}

#[test]
fn above_sqlite_current_amounts_are_rejected_without_clamping_or_creating_a_payout() {
    let mut connection = Connection::open_in_memory().unwrap();
    crate::db::migrate(&mut connection).unwrap();
    let transaction = connection.transaction().unwrap();
    store_balance(
        &transaction,
        1,
        &MonetaryBalance {
            unpaid: i64::MAX as u64,
            pending: 0,
            paid: 0,
            earned: i64::MAX as u128,
        },
    )
    .unwrap();
    transaction.commit().unwrap();
    let transaction = connection.transaction().unwrap();
    assert!(prepare(&transaction, u64::MAX).is_err());
    drop(transaction);
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM payouts", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    let transaction = connection.transaction().unwrap();
    let balance = monetary_balance(&transaction, 1).unwrap().unwrap();
    assert_eq!((balance.unpaid, balance.pending), (i64::MAX as u64, 0));
}

#[test]
fn never_signed_release_is_scoped_prepared_and_idempotent_without_touching_other_attempts() {
    let path = crate::test_support::temp_path("budget-never-signed").with_extension("sqlite");
    let mut connection = database(&path);
    let scope = crate::wallet::RequestScope {
        archive_id: "69".repeat(32),
        account: 0,
        chain_id: crate::test_support::CHAIN.to_hex(),
    };
    let (id, rid) = {
        let tx = connection.transaction().unwrap();
        store_balance(
            &tx,
            1,
            &MonetaryBalance {
                unpaid: 100,
                pending: 0,
                paid: 0,
                earned: 100,
            },
        )
        .unwrap();
        let result = prepare(&tx, 40).unwrap();
        assert!(!payout_attempt(&tx, result.0, &result.1, &scope).unwrap());
        tx.commit().unwrap();
        result
    };
    {
        let tx = connection.transaction().unwrap();
        let mut wrong = scope.clone();
        wrong.account = 1;
        assert!(accounting::close_never_signed(&tx, id, &rid, &wrong, 2).is_err());
        assert!(accounting::close_never_signed(&tx, id, "other-request", &scope, 2).is_err());
        let b = monetary_balance(&tx, 1).unwrap().unwrap();
        assert_eq!((b.unpaid, b.pending), (60, 40));
        accounting::close_never_signed(&tx, id, &rid, &scope, 2).unwrap();
        accounting::close_never_signed(&tx, id, &rid, &scope, 2).unwrap();
        let b = monetary_balance(&tx, 1).unwrap().unwrap();
        assert_eq!((b.unpaid, b.pending, b.paid), (100, 0, 0));
        let (other, other_rid) = prepare(&tx, 30).unwrap();
        payout_attempt(&tx, other, &other_rid, &scope).unwrap();
        tx.execute(
            "UPDATE payouts SET status=?2 WHERE id=?1",
            params![other, PayoutStatus::Sent.code()],
        )
        .unwrap();
        assert!(accounting::close_never_signed(&tx, other, &other_rid, &scope, 3).is_err());
        let b = monetary_balance(&tx, 1).unwrap().unwrap();
        assert_eq!((b.unpaid, b.pending), (70, 30));
        tx.commit().unwrap();
    }
    drop(connection);
    let mut connection = database(&path);
    let tx = connection.transaction().unwrap();
    accounting::close_never_signed(&tx, id, &rid, &scope, 4).unwrap();
    let b = monetary_balance(&tx, 1).unwrap().unwrap();
    assert_eq!((b.unpaid, b.pending), (70, 30));
}
