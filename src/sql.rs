use std::{path::Path, str::FromStr};

use smol::stream::{Stream, StreamExt};
use sqlx::{
    database::HasValueRef, prelude::FromRow, sqlite::SqliteConnectOptions, Acquire, ConnectOptions,
    Database, Decode, Encode, Sqlite, SqliteConnection,
};

use crate::{
    seed::{MachineDecision, RunStats, STARTING_MACHINE},
    turing::MachineTable,
};

/// Convience type
pub type SqlResult<T> = Result<T, sqlx::Error>;
pub type SqlQueryResult = SqlResult<sqlx::sqlite::SqliteQueryResult>;

/// The type of "results_id" column
pub type ResultRowID = i64;

/// The type of the "machine" column in the results table
pub type PackedMachine = [u8; 7];

/// The type of the "decision" column in the results table. This is effectively a discriminant-only
/// version of [MachineDecision]
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
        let machine = PackedMachine::from(*self).to_vec();
        machine.encode(buf)
    }
}

impl<'r> Decode<'r, Sqlite> for MachineTable {
    fn decode(
        value: <Sqlite as HasValueRef<'r>>::ValueRef,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let bytes = <&[u8] as Decode<Sqlite>>::decode(value)?;
        let machine = MachineTable::try_from(bytes)?;
        Ok(machine)
    }
}

impl sqlx::Type<Sqlite> for MachineTable {
    fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
        <&[u8]>::type_info()
    }
}

/// Represents a single row in the results table. This differs from [RowObject] in that this
/// struct does not contain any extra data from the stats table.
#[derive(sqlx::FromRow, Clone, Copy, PartialEq, Eq)]
pub struct ResultRow {
    pub results_id: ResultRowID,
    pub machine: MachineTable,
    /// The result of deciding the machine. If None, then this machine is still pending.
    pub decision: Option<Decision>,
}

/// Represents a single row in the stats table
#[derive(sqlx::FromRow, Clone, Copy, PartialEq, Eq)]
pub struct StatsRow {
    pub results_id: ResultRowID,
    /// Number of steps taken before the machine was decided
    pub steps: u32,
    /// Number of cells used by the machine before the machine was decided
    pub space: u32,
}

impl StatsRow {
    async fn insert(&self, conn: &mut SqliteConnection) -> SqlResult<()> {
        let result = sqlx::query("INSERT INTO stats (results_id, steps, space) VALUES($1, $2, $3)")
            .bind(self.results_id)
            .bind(self.steps)
            .bind(self.space)
            .execute(conn)
            .await?;
        assert_eq!(result.rows_affected(), 1);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// An undecided row that does not yet exist in the database.
pub struct UninsertedPendingRow {
    pub machine: MachineTable,
}

impl UninsertedPendingRow {
    /// Insert a [MachineTable] as a pending result row
    pub async fn insert_pending_row(
        self,
        conn: &mut SqliteConnection,
    ) -> SqlResult<InsertedPendingRow> {
        let packed = PackedMachine::from(self.machine);
        let result = sqlx::query("INSERT INTO results (machine, decision) VALUES($1, NULL)")
            .bind(&packed[..])
            .execute(conn)
            .await?;
        assert_eq!(result.rows_affected(), 1);
        Ok(InsertedPendingRow {
            id: result.last_insert_rowid(),
            machine: self.machine,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// An undecided row that exists in the database
pub struct InsertedPendingRow {
    id: ResultRowID,
    pub machine: MachineTable,
}

impl InsertedPendingRow {
    /// Insert this row. It is assumed that there is not already a row with the same [ResultRowID]
    /// in the database.
    pub async fn insert(&self, conn: &mut SqliteConnection) -> SqlResult<()> {
        let result =
            sqlx::query("INSERT INTO results (results_id, machine, decision) VALUES($1, $2, NULL)")
                .bind(self.id)
                .bind(self.machine)
                .execute(&mut *conn)
                .await?;
        assert_eq!(result.rows_affected(), 1);
        Ok(())
    }

    /// Update a pending result row into a decided row, including stats
    pub async fn update(
        self,
        conn: &mut SqliteConnection,
        decision: Decision,
        stats: RunStats,
    ) -> SqlResult<InsertedDecidedRow> {
        let mut txn = conn.begin().await?;

        // Update decision row
        let machine = PackedMachine::from(self.machine);
        let result = sqlx::query("UPDATE results SET decision = $2 WHERE machine = $1")
            .bind(&machine[..])
            .bind(decision)
            .execute(&mut *txn)
            .await?;
        assert_eq!(result.rows_affected(), 1);

        // Now actually insert the stats
        let stats_row = StatsRow {
            steps: stats.get_total_steps() as u32,
            space: stats.space_used() as u32,
            results_id: self.id,
        };
        stats_row.insert(&mut txn).await?;

        txn.commit().await?;

        Ok(InsertedDecidedRow {
            id: self.id,
            machine: self.machine,
            decision,
            steps: stats.get_total_steps() as u32,
            space: stats.space_used() as u32,
        })
    }

    /// Retrieve all of the [InsertedPendingRow]s in the database.
    pub async fn get_pending_rows(
        conn: &mut SqliteConnection,
    ) -> SqlResult<Vec<InsertedPendingRow>> {
        #[derive(FromRow)]
        struct Row {
            results_id: ResultRowID,
            machine: Vec<u8>,
        }
        let rows = sqlx::query_as::<_, Row>(
            "SELECT results_id, machine FROM results WHERE decision IS NULL",
        )
        .fetch_all(conn)
        .await?;
        let rows = rows
            .into_iter()
            .map(|row| Self {
                id: row.results_id,
                machine: MachineTable::try_from(row.machine.as_slice()).unwrap(),
            })
            .collect();
        Ok(rows)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// A decided row that exists in the database
pub struct InsertedDecidedRow {
    pub id: ResultRowID,
    pub machine: MachineTable,
    pub decision: Decision,
    pub steps: u32,
    pub space: u32,
}

impl InsertedDecidedRow {
    /// Insert this row into the database. It is assumed there is not already a row with the same [ResultRowID] in the database.
    pub async fn insert(&self, conn: &mut SqliteConnection) -> SqlResult<()> {
        let mut txn = conn.begin().await?;

        let result =
            sqlx::query("INSERT INTO results (results_id, machine, decision) VALUES($1, $2, $3)")
                .bind(self.id)
                .bind(self.machine)
                .bind(self.decision)
                .execute(&mut *txn)
                .await?;
        assert_eq!(result.rows_affected(), 1);

        let stats_row = StatsRow {
            results_id: self.id,
            steps: self.steps,
            space: self.space,
        };
        stats_row.insert(&mut txn).await?;

        txn.commit().await
    }

    /// Retrieve all of the [InsertedDecidedRow]s in the database.
    pub async fn get_decided_rows(
        conn: &mut SqliteConnection,
    ) -> SqlResult<Vec<InsertedDecidedRow>> {
        #[derive(FromRow)]
        struct Row {
            results_id: ResultRowID,
            machine: Vec<u8>,
            decision: Decision,
            steps: u32,
            space: u32,
        }
        let rows = sqlx::query_as::<_, Row>(
            "SELECT results_id, machine, decision, steps, space FROM results
                 INNER JOIN stats USING (results_id)",
        )
        .fetch_all(conn)
        .await?;
        let rows = rows
            .into_iter()
            .map(|row| Self {
                id: row.results_id,
                machine: MachineTable::try_from(row.machine.as_slice()).unwrap(),
                decision: row.decision,
                steps: row.steps,
                space: row.space,
            })
            .collect();
        Ok(rows)
    }
}

/// Represents a single row in the results table, along with any additional information in other
/// tables. This differs from [ResultRow] in that this struct also contains information from the
/// stats table.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum InsertedRow {
    Pending(InsertedPendingRow),
    Decided(InsertedDecidedRow),
}

impl InsertedRow {
    /// Retrieve all rows from the database
    pub async fn get_all_rows(
        conn: &mut SqliteConnection,
    ) -> impl Stream<Item = SqlResult<InsertedRow>> + '_ {
        #[derive(FromRow)]
        struct Row {
            results_id: ResultRowID,
            machine: MachineTable,
            decision: Option<Decision>,
            steps: Option<u32>,
            space: Option<u32>,
        }

        let result_rows = sqlx::query_as::<_, Row>(
            "SELECT results_id, machine, decision, steps, space FROM results
                 LEFT JOIN stats USING (results_id)",
        )
        .fetch(&mut *conn);

        result_rows.map(|row| {
            let row = row?;
            let id = row.results_id;
            let machine = row.machine;
            let row_object = if let Some(decision) = row.decision {
                // These unwraps are safe because the stats row is present if and only if there is a decision
                let steps = row.steps.unwrap();
                let space = row.space.unwrap();
                InsertedRow::Decided(InsertedDecidedRow {
                    id,
                    machine,
                    decision,
                    steps,
                    space,
                })
            } else {
                InsertedRow::Pending(InsertedPendingRow { id, machine })
            };
            Ok(row_object)
        })
    }

    pub fn machine(&self) -> MachineTable {
        match self {
            InsertedRow::Pending(pending) => pending.machine,
            InsertedRow::Decided(decided) => decided.machine,
        }
    }
}

/// A decision along with any relevant stats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecisionWithStats {
    pub decision: Decision,
    pub steps: u32,
    pub space: u32,
}

#[derive(FromRow)]
pub struct RowCounts {
    pub total: u32,
    pub pending: u32,
    pub decided: u32,
    pub halt: u32,
    pub nonhalt: u32,
    pub step: u32,
    pub space: u32,
    pub empty: u32,
}
impl RowCounts {
    pub async fn get_counts(conn: &mut SqliteConnection) -> RowCounts {
        sqlx::query_as(
            "SELECT
                    COUNT(*) AS total,
                    SUM(decision IS NULL) AS pending,
                    SUM(decision IS NOT NULL) AS decided,
                    SUM(decision = ?) AS halt,
                    SUM(decision = ?) AS nonhalt,
                    SUM(decision = ?) AS step,
                    SUM(decision = ?) AS space,
                    SUM(decision = ?) AS empty
                FROM results;",
        )
        .bind(Decision::Halting)
        .bind(Decision::NonHalting)
        .bind(Decision::UndecidedStepLimit)
        .bind(Decision::UndecidedSpaceLimit)
        .bind(Decision::EmptyTransition)
        .fetch_one(conn)
        .await
        .unwrap()
    }
}

/// Create the tables for the database if they do not already exist.
pub async fn create_tables(conn: &mut SqliteConnection) -> SqlResult<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS results (
        results_id       INTEGER NOT NULL PRIMARY KEY,
        machine  BLOB    NOT NULL UNIQUE,
        decision INTEGER     NULL)",
    )
    .execute(&mut *conn)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS stats (
            results_id    INTEGER NOT NULL REFERENCES results(results_id),
            steps INTEGER NOT NULL,
            space INTEGER NOT NULL)",
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Insert the initial row, if it does not already exist. This is used to set up the database for the first time.
pub async fn insert_initial_row(conn: &mut SqliteConnection) -> SqlResult<InsertedPendingRow> {
    // Try to insert the initial row. OR IGNORE is used here to not do the insert if we have already
    // decided the row.
    let machine = MachineTable::from_str(STARTING_MACHINE).unwrap();
    let array: PackedMachine = machine.into();
    let result = sqlx::query("INSERT OR IGNORE INTO results (machine, decision) VALUES($1, NULL)")
        .bind(&array[..])
        .execute(conn)
        .await?;

    Ok(InsertedPendingRow {
        id: result.last_insert_rowid(),
        machine,
    })
}

/// Convience function to run a command.
pub async fn run_command(conn: &mut SqliteConnection, sql: &str) -> SqlQueryResult {
    sqlx::query(sql).execute(conn).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionMode {
    /// Open for write. Does create new file if missing.
    WriteNew,
    /// Open for write. Does not create new file if missing.
    WriteExisting,
    /// Open as read-only. Does not create new file if missing.
    ReadOnly,
}
/// Initialize a connection.
/// This also sets the synchronous and journal_mode PRAGMAs to make writes faster.
pub async fn get_connection(
    file: impl AsRef<Path>,
    mode: ConnectionMode,
) -> SqlResult<SqliteConnection> {
    // Prevent overwriting existing file if using WriteNew
    if file.as_ref().exists() && mode == ConnectionMode::WriteNew {
        return Err(sqlx::Error::Io(std::io::ErrorKind::AlreadyExists.into()));
    }

    let mut conn = SqliteConnectOptions::new()
        .filename(file)
        .create_if_missing(mode == ConnectionMode::WriteNew)
        .read_only(mode == ConnectionMode::ReadOnly)
        .connect()
        .await?;

    // PRAGMAs set here are from https://phiresky.github.io/blog/2020/sqlite-performance-tuning/
    // This is to allow faster writes to the database.
    run_command(&mut conn, "PRAGMA synchronous = normal").await?;
    run_command(&mut conn, "PRAGMA journal_mode = WAL;").await?;
    Ok(conn)
}
