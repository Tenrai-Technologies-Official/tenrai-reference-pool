//! Exact bounded monetary storage; turnover counters never use SQLite arithmetic.
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{
    Connection, Row,
    types::{Type, ValueRef},
};

pub(super) fn amount(value: u64) -> Result<i64> {
    i64::try_from(value).context("monetary amount exceeds exact SQLite integer capacity")
}

fn parse_counter(bytes: &[u8]) -> Result<u128> {
    ensure!(
        !bytes.is_empty()
            && bytes.len() <= 39
            && (bytes.len() == 1 || bytes[0] != b'0')
            && bytes.iter().all(u8::is_ascii_digit),
        "monetary counter is not canonical decimal text"
    );
    // The ASCII check makes this conversion infallible, without an unbounded allocation.
    std::str::from_utf8(bytes)?
        .parse::<u128>()
        .context("monetary counter capacity is exhausted")
}

pub(super) fn counter(row: &Row<'_>, index: usize) -> rusqlite::Result<u128> {
    let value = row.get_ref(index)?;
    let ValueRef::Text(bytes) = value else {
        return Err(rusqlite::Error::InvalidColumnType(
            index,
            "monetary counter".into(),
            value.data_type(),
        ));
    };
    parse_counter(bytes).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error.to_string(),
            )),
        )
    })
}

fn column_type(connection: &Connection, table: &str, column: &str) -> Result<String> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        if row.get::<_, String>(1)? == column {
            return Ok(row.get::<_, String>(2)?.to_ascii_uppercase());
        }
    }
    bail!("the monetary table {table} is missing column {column}")
}

fn current(value: ValueRef<'_>) -> Result<()> {
    ensure!(
        matches!(value, ValueRef::Integer(value) if value >= 0),
        "current monetary storage must be a nonnegative INTEGER, without rounding"
    );
    Ok(())
}

fn stored_counter(value: ValueRef<'_>) -> Result<()> {
    let ValueRef::Text(bytes) = value else {
        bail!("monetary counters must be canonical TEXT");
    };
    parse_counter(bytes)?;
    Ok(())
}

fn validate_amounts(connection: &Connection, table: &str, columns: &[&str]) -> Result<()> {
    for column in columns {
        ensure!(
            column_type(connection, table, column)? == "INTEGER",
            "the monetary column {table}.{column} has the wrong storage type"
        );
    }
    let mut statement =
        connection.prepare(&format!("SELECT {} FROM {table}", columns.join(",")))?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        for (index, column) in columns.iter().enumerate() {
            let value = row.get_ref(index)?;
            let optional = *column == "fee" || (table == "blocks" && *column == "reward");
            if !matches!(value, ValueRef::Null) || !optional {
                current(value)?;
            }
        }
    }
    Ok(())
}

/// Checks the only persisted Mainnet format. No table is rebuilt or rounded on restart.
pub(super) fn validate(connection: &Connection) -> Result<()> {
    for (table, column) in [
        ("balances", "paid"),
        ("balances", "earned"),
        ("earnings", "amount"),
    ] {
        ensure!(
            column_type(connection, table, column)? == "TEXT",
            "the monetary column {table}.{column} must use canonical TEXT"
        );
    }
    let mut balances = connection.prepare("SELECT unpaid,pending,paid,earned FROM balances")?;
    let mut rows = balances.query([])?;
    while let Some(row) = rows.next()? {
        current(row.get_ref(0)?)?;
        current(row.get_ref(1)?)?;
        stored_counter(row.get_ref(2)?)?;
        stored_counter(row.get_ref(3)?)?;
    }
    let mut earnings = connection.prepare("SELECT amount,blocks FROM earnings")?;
    let mut rows = earnings.query([])?;
    while let Some(row) = rows.next()? {
        stored_counter(row.get_ref(0)?)?;
        current(row.get_ref(1)?)?;
    }
    validate_amounts(connection, "payouts", &["total", "fee"])?;
    validate_amounts(connection, "payout_items", &["amount", "fee_share"])?;
    validate_amounts(connection, "blocks", &["reward", "fee"])?;
    Ok(())
}

#[cfg(test)]
mod tests;
