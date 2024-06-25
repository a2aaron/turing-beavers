use std::{path::Path, str::FromStr};

use smol::stream::{Stream, StreamExt};
use sqlx::{
    database::HasValueRef,
    prelude::FromRow,
    sqlite::{SqliteConnectOptions, SqliteQueryResult},
    ConnectOptions, Database, Decode, Encode, Sqlite, SqliteConnection,
};

use crate::{
    seed::{DecidedNode, MachineDecision, STARTING_MACHINE},
    turing::MachineTable,
};

/// The type of "results_id" column
pub type RowID = u32;

/// The type of the "machine" column in the results table
pub type PackedTable = [u8; 7];
#[derive(Debug, Clone, Copy, PartialEq, Eq)]

/// The type of the "decision" column in the results table. This is effectively a discriminant-only
/// version of [MachineDecision]
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
            2 => Ok(Decision::UndecidedStepLimit),
            3 => Ok(Decision::UndecidedSpaceLimit),
            4 => Ok(Decision::EmptyTransition),
            _ => Err(format!(
                "Decision out of range (expected 0 to 4, got {value})"
            )),
        };
        Ok(value?)
    }
}

// Impls for Encoding/Decoding a MachineTable
impl<'r> Encode<'r, Sqlite> for MachineTable {
    fn encode_by_ref(
        &self,
        buf: &mut <Sqlite as sqlx::database::HasArguments<'r>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        let table = PackedTable::from(*self).to_vec();
        let foo = table.encode(buf);
        foo
    }
}

impl<'r> Decode<'r, Sqlite> for MachineTable {
    fn decode(
        value: <Sqlite as HasValueRef<'r>>::ValueRef,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let value = <&[u8] as Decode<Sqlite>>::decode(value)?;
        let value = MachineTable::try_from(value);
        Ok(value?)
    }
}

impl sqlx::Type<Sqlite> for MachineTable {
    fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
        <&[u8]>::type_info()
    }
}

/// Represents a single row in the results table. This differs from [ResultObject] in that this
/// struct does not contain any extra data from the stats table.
#[derive(sqlx::FromRow, Clone, Copy, PartialEq, Eq)]
pub struct ResultRow {
    pub results_id: RowID,
    pub machine: MachineTable,
    /// The result of deciding the machine. If None, then this machine is still pending.
    pub decision: Option<Decision>,
}

/// Represents a single row in the stats table
#[derive(sqlx::FromRow, Clone, Copy, PartialEq, Eq)]
pub struct StatsRow {
    pub results_id: RowID,
    /// Number of steps taken before the machine was decided
    pub steps: u32,
    /// Number of cells used by the machine before the machine was decided
    pub space: u32,
}

/// Represents a single row in the results table, along with any additional information in other
/// tables. This differs from [ResultRow] in that this struct also contains information from the
/// stats table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultObject {
    pub results_id: RowID,
    pub machine: MachineTable,
    /// The result of deciding the machine along with any relevant statistics. If None, then
    /// this machine is still pending
    pub decision: Option<DecisionWithStats>,
}

impl ResultObject {
    /// Insert the entire ResultObject into the database. This will insert into the result
    /// as well as the stats table if decision is present.
    pub async fn insert(&self, conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
        if let Some(decision) = self.decision {
            let result = sqlx::query(
                "INSERT INTO results (results_id, machine, decision) VALUES($1, $2, $3)",
            )
            .bind(self.results_id)
            .bind(self.machine)
            .bind(decision.decision)
            .execute(&mut *conn)
            .await?;
            assert_eq!(result.rows_affected(), 1);

            let result =
                sqlx::query("INSERT INTO stats (results_id, steps, space) VALUES($1, $2, $3)")
                    .bind(self.results_id)
                    .bind(decision.steps)
                    .bind(decision.space)
                    .execute(&mut *conn)
                    .await?;
            assert_eq!(result.rows_affected(), 1);
        } else {
            let result = sqlx::query(
                "INSERT INTO results (results_id, machine, decision) VALUES($1, $2, NULL)",
            )
            .bind(self.results_id)
            .bind(self.machine)
            .execute(&mut *conn)
            .await?;
            assert_eq!(result.rows_affected(), 1);
        }
        Ok(())
    }

    /// Retrieve all results from the database
    pub async fn get_rows(
        conn: &mut SqliteConnection,
    ) -> impl Stream<Item = Result<ResultObject, sqlx::Error>> + '_ {
        #[derive(FromRow)]
        struct Row {
            results_id: RowID,
            machine: MachineTable,
            decision: Option<Decision>,
            steps: Option<u32>,
            space: Option<u32>,
        }

        let result_rows = sqlx::query_as::<_, Row>(
            "SELECT results_id, machine, decision, steps, space FROM results
                 LEFT JOIN stats USING (results_id)",
        )
        .fetch(&mut (*conn));

        result_rows.map(|result| {
            let row = result?;
            let decision = if let Some(decision) = row.decision {
                Some(DecisionWithStats {
                    decision,
                    // These unwraps are safe because the stats row is present if and only if there is a decision
                    steps: row.steps.unwrap(),
                    space: row.space.unwrap(),
                })
            } else {
                None
            };

            Ok(ResultObject {
                results_id: row.results_id,
                machine: row.machine,
                decision,
            })
        })
    }
}

/// A decision along with any relevant stats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecisionWithStats {
    pub decision: Decision,
    pub steps: u32,
    pub space: u32,
}

/// Submit results for a decided node. This method assumes that the node has not ever been submitted
/// before. If has, an error will occur.
pub async fn submit_result(
    conn: &mut SqliteConnection,
    node: &DecidedNode,
) -> Result<usize, sqlx::Error> {
    let decision = &node.decision;

    let mut rows_processed = 1;
    update_pending_row(conn, node).await;
    match decision {
        MachineDecision::EmptyTransition(new_nodes) => {
            rows_processed += new_nodes.len();
            for node in new_nodes {
                insert_pending_row(conn, node.table).await;
            }
        }
        _ => (),
    }
    Ok(rows_processed)
}

/// Insert a [MachineTable] as a pending result row
async fn insert_pending_row(conn: &mut SqliteConnection, table: MachineTable) {
    let table: PackedTable = table.into();
    let query = sqlx::query("INSERT INTO results (machine, decision) VALUES($1, NULL)")
        .bind(&table[..])
        .execute(conn);
    let result = query.await.unwrap();
    assert_eq!(result.rows_affected(), 1);
}

/// Update a pending result row into a decided row, including stats
async fn update_pending_row(conn: &mut SqliteConnection, node: &DecidedNode) {
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

    // Insert stats--first grab the results_id
    let results_id: RowID = sqlx::query_scalar("SELECT results_id FROM results WHERE machine = $1")
        .bind(&table[..])
        .fetch_one(&mut *conn)
        .await
        .unwrap();

    // Now actually insert the stats
    let result = sqlx::query("INSERT INTO stats (results_id, steps, space) VALUES($1, $2, $3)")
        .bind(results_id)
        .bind(node.stats.get_total_steps() as u32)
        .bind(node.stats.space_used() as u32)
        .execute(conn)
        .await
        .unwrap();
    assert_eq!(result.rows_affected(), 1);
}

/// Create the tables for the database if they do not already exist.
pub async fn create_tables(conn: &mut SqliteConnection) {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS results (
        results_id       INTEGER NOT NULL PRIMARY KEY,
        machine  BLOB    NOT NULL UNIQUE,
        decision INTEGER     NULL)",
    )
    .execute(&mut *conn)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS stats (
            results_id    INTEGER NOT NULL REFERENCES results(results_id),
            steps INTEGER NOT NULL,
            space INTEGER NOT NULL)",
    )
    .execute(&mut *conn)
    .await
    .unwrap();
}

/// Insert the initial row, if it does not already exist. This is used to set up the database for the first time.
pub async fn insert_initial_row(conn: &mut SqliteConnection) {
    // Try to insert the initial row. OR IGNORE is used here to not do the insert if we have already
    // decided the row.
    let starting_table = MachineTable::from_str(STARTING_MACHINE).unwrap();
    let array: PackedTable = starting_table.into();
    sqlx::query("INSERT OR IGNORE INTO results (machine, decision) VALUES($1, NULL)")
        .bind(&array[..])
        .execute(conn)
        .await
        .unwrap();
}

/// Retrieve all of the [MachineTable]s which are pending
pub async fn get_pending_queue(conn: &mut SqliteConnection) -> Vec<MachineTable> {
    let tables = sqlx::query_scalar("SELECT machine FROM results WHERE decision IS NULL")
        .fetch_all(conn)
        .await
        .unwrap();

    tables
        .into_iter()
        .map(|t: Vec<u8>| MachineTable::try_from(t.as_slice()).unwrap())
        .collect()
}

/// Convience function to run a command.
pub async fn run_command(conn: &mut SqliteConnection, sql: &str) -> SqliteQueryResult {
    sqlx::query(sql).execute(conn).await.unwrap()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionMode {
    /// Open for write. Does create new file if missing.
    Write,
    /// Open as read-only. Does not create new file if missing.
    ReadOnly,
}
/// Initialize a connection.
/// This also sets the synchronous and journal_mode PRAGMAs to make writes faster.
pub async fn get_connection(file: impl AsRef<Path>, mode: ConnectionMode) -> SqliteConnection {
    let mut conn = SqliteConnectOptions::new()
        .filename(file)
        .create_if_missing(mode == ConnectionMode::Write)
        .read_only(mode == ConnectionMode::ReadOnly)
        .connect()
        .await
        .unwrap();

    // PRAGMAs set here are from https://phiresky.github.io/blog/2020/sqlite-performance-tuning/
    // This is to allow faster writes to the database.
    run_command(&mut conn, "PRAGMA synchronous = normal").await;
    run_command(&mut conn, "PRAGMA journal_mode = WAL;").await;
    conn
}
