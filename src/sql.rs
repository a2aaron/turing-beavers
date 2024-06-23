use std::str::FromStr;

use sqlx::{
    database::HasValueRef,
    sqlite::{SqliteConnectOptions, SqliteQueryResult},
    ConnectOptions, Database, Decode, Encode, Sqlite, SqliteConnection,
};

use crate::{
    seed::{DecidedNode, MachineDecision, STARTING_MACHINE},
    turing::Table,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Halting = 0,
    NonHalting = 1,
    UndecidedStepLimit = 2,
    UndecidedSpaceLimit = 3,
    EmptyTransition = 4,
}

impl From<&MachineDecision> for Decision {
    fn from(value: &MachineDecision) -> Self {
        match value {
            MachineDecision::Halting => Decision::Halting,
            MachineDecision::NonHalting => Decision::NonHalting,
            MachineDecision::UndecidedStepLimit => Decision::UndecidedStepLimit,
            MachineDecision::UndecidedSpaceLimit => Decision::UndecidedSpaceLimit,
            MachineDecision::EmptyTransition(_) => Decision::EmptyTransition,
        }
    }
}

impl sqlx::Type<Sqlite> for Decision {
    fn type_info() -> <Sqlite as Database>::TypeInfo {
        u8::type_info()
    }
}

impl<'r> Encode<'r, Sqlite> for Decision {
    fn encode_by_ref(
        &self,
        buf: &mut <Sqlite as sqlx::database::HasArguments<'r>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        (*self as u8).encode(buf)
    }
}

impl<'r> Decode<'r, Sqlite> for Decision {
    fn decode(
        value: <Sqlite as HasValueRef<'r>>::ValueRef,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let value = <u8 as Decode<Sqlite>>::decode(value)?;
        let value = match value {
            0 => Ok(Decision::Halting),
            1 => Ok(Decision::NonHalting),
            2 => Ok(Decision::UndecidedSpaceLimit),
            3 => Ok(Decision::UndecidedSpaceLimit),
            4 => Ok(Decision::EmptyTransition),
            _ => Err(format!(
                "Decision out of range (expected 0 to 4, got {value}"
            )),
        };
        Ok(value?)
    }
}

impl<'r> Encode<'r, Sqlite> for Table {
    fn encode_by_ref(
        &self,
        buf: &mut <Sqlite as sqlx::database::HasArguments<'r>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        let table = PackedTable::from(*self).to_vec();
        let foo = table.encode(buf);
        foo
    }
}

impl<'r> Decode<'r, Sqlite> for Table {
    fn decode(
        value: <Sqlite as HasValueRef<'r>>::ValueRef,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let value = <&[u8] as Decode<Sqlite>>::decode(value)?;
        let value = Table::try_from(value);
        Ok(value?)
    }
}

impl sqlx::Type<Sqlite> for Table {
    fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
        <&[u8]>::type_info()
    }
}

pub type RowID = u32;

#[derive(sqlx::FromRow, Clone, Copy, PartialEq, Eq)]
pub struct ResultRow {
    pub id: RowID,
    pub machine: Table,
    pub decision: Option<Decision>,
}

#[derive(sqlx::FromRow, Clone, Copy, PartialEq, Eq)]
pub struct StatsRow {
    pub id: RowID,
    pub steps: u32,
    pub space: u32,
}

type PackedTable = [u8; 7];

pub async fn submit_result(
    conn: &mut SqliteConnection,
    node: &DecidedNode,
) -> Result<usize, sqlx::Error> {
    let decision = &node.decision;

    let mut rows_processed = 1;
    update_row(conn, node).await;
    match decision {
        MachineDecision::EmptyTransition(new_nodes) => {
            rows_processed += new_nodes.len();
            for node in new_nodes {
                insert_row(conn, node.table).await;
            }
        }
        _ => (),
    }

    Ok(rows_processed)
}

pub async fn insert_row(conn: &mut SqliteConnection, table: Table) {
    let table: PackedTable = table.into();
    let query = sqlx::query("INSERT INTO results (machine, decision) VALUES($1, NULL)")
        .bind(&table[..])
        .execute(conn);
    let result = query.await.unwrap();
    assert_eq!(result.rows_affected(), 1);
}

pub async fn update_row(conn: &mut SqliteConnection, node: &DecidedNode) {
    let decision = Decision::from(&node.decision) as u8;
    let table: PackedTable = node.table.into();
    // Update decision row
    let result = sqlx::query("UPDATE results SET decision = $2 WHERE machine = $1")
        .bind(&table[..])
        .bind(decision)
        .execute(&mut *conn)
        .await
        .unwrap();
    assert_eq!(result.rows_affected(), 1);

    // Insert stats--first grab the id
    let id: i64 = sqlx::query_scalar("SELECT id FROM results WHERE machine = $1")
        .bind(&table[..])
        .fetch_one(&mut *conn)
        .await
        .unwrap();

    // Now actually insert the stats
    let result = sqlx::query("INSERT INTO stats (id, steps, space) VALUES($1, $2, $3)")
        .bind(id)
        .bind(node.stats.get_total_steps() as i64)
        .bind(node.stats.space_used() as i64)
        .execute(conn)
        .await
        .unwrap();
    assert_eq!(result.rows_affected(), 1);
}

pub async fn create_tables(conn: &mut SqliteConnection) {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS results (
        id       INTEGER NOT NULL PRIMARY KEY,
        machine  BLOB    NOT NULL UNIQUE,
        decision INTEGER     NULL)",
    )
    .execute(&mut *conn)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS stats (
            id    INTEGER NOT NULL REFERENCES results(id),
            steps INTEGER NOT NULL,
            space INTEGER NOT NULL)",
    )
    .execute(&mut *conn)
    .await
    .unwrap();

    // PRAGMAs set here are from https://phiresky.github.io/blog/2020/sqlite-performance-tuning/
    // This is to allow faster writes to the database.
    run_command(&mut *conn, "PRAGMA synchronous = normal").await;
    run_command(&mut *conn, "PRAGMA journal_mode = WAL;").await;
}

pub async fn insert_initial_row(conn: &mut SqliteConnection) {
    // Try to insert the initial row. OR IGNORE is used here to not do the insert if we have already
    // decided the row.
    let starting_table = Table::from_str(STARTING_MACHINE).unwrap();
    let array: PackedTable = starting_table.into();
    sqlx::query("INSERT OR IGNORE INTO results (machine, decision) VALUES($1, NULL)")
        .bind(&array[..])
        .execute(conn)
        .await
        .unwrap();
}

pub async fn get_queue(conn: &mut SqliteConnection) -> Vec<Table> {
    let tables = sqlx::query_scalar("SELECT machine FROM results WHERE decision IS NULL")
        .fetch_all(conn)
        .await
        .unwrap();

    tables
        .into_iter()
        .map(|t: Vec<u8>| Table::try_from(t.as_slice()).unwrap())
        .collect()
}

pub async fn run_command(conn: &mut SqliteConnection, sql: &str) -> SqliteQueryResult {
    sqlx::query(sql).execute(conn).await.unwrap()
}

pub async fn get_connection(file: &str) -> SqliteConnection {
    SqliteConnectOptions::from_str(file)
        .unwrap()
        .create_if_missing(true)
        .connect()
        .await
        .unwrap()
}
