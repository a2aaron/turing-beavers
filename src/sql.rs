use std::{collections::HashMap, path::Path};

use smol::stream::{Stream, StreamExt};
use sqlx::{
    database::HasValueRef, prelude::FromRow, sqlite::SqliteConnectOptions, Acquire, ConnectOptions,
    Database, Decode, Encode, Sqlite, SqliteConnection,
};

use crate::{
    seed::{Decision, RunStats},
    turing::MachineTable,
};

/// Convience type
pub type SqlResult<T> = Result<T, sqlx::Error>;
pub type SqlQueryResult = SqlResult<sqlx::sqlite::SqliteQueryResult>;

/// The type of "results_id" column
pub type ResultRowID = i64;
pub type RowsAffected = u64;

/// The type of the "machine" column in the results table
pub type PackedMachine = [u8; 7];

/// The type of the "decision" column in the results table. This is effectively a discriminant-only
/// version of [Decision]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionKind {
    Halting = 0,
    NonHalting = 1,
    UndecidedStepLimit = 2,
    UndecidedSpaceLimit = 3,
    EmptyTransition = 4,
}

impl From<&Decision> for DecisionKind {
    fn from(value: &Decision) -> Self {
        match value {
            Decision::Halting => DecisionKind::Halting,
            Decision::NonHalting => DecisionKind::NonHalting,
            Decision::UndecidedStepLimit => DecisionKind::UndecidedStepLimit,
            Decision::UndecidedSpaceLimit => DecisionKind::UndecidedSpaceLimit,
            Decision::EmptyTransition(_) => DecisionKind::EmptyTransition,
        }
    }
}

impl sqlx::Type<Sqlite> for DecisionKind {
    fn type_info() -> <Sqlite as Database>::TypeInfo {
        u8::type_info()
    }
}

impl<'r> Encode<'r, Sqlite> for DecisionKind {
    fn encode_by_ref(
        &self,
        buf: &mut <Sqlite as sqlx::database::HasArguments<'r>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        (*self as u8).encode(buf)
    }
}

impl<'r> Decode<'r, Sqlite> for DecisionKind {
    fn decode(
        value: <Sqlite as HasValueRef<'r>>::ValueRef,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let value = <u8 as Decode<Sqlite>>::decode(value)?;
        let value = match value {
            0 => Ok(DecisionKind::Halting),
            1 => Ok(DecisionKind::NonHalting),
            2 => Ok(DecisionKind::UndecidedStepLimit),
            3 => Ok(DecisionKind::UndecidedSpaceLimit),
            4 => Ok(DecisionKind::EmptyTransition),
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

/// Represents a single row in the soft_stats table
#[derive(Debug, sqlx::FromRow, Clone, PartialEq)]
pub struct SoftStatsRow {
    pub results_id: ResultRowID,
    pub seconds: f64,
    pub session_id: String,
    pub start_time: String,
}

impl SoftStatsRow {
    pub async fn get_rows(conn: &mut SqliteConnection) -> SqlResult<Vec<SoftStatsRow>> {
        sqlx::query_as::<_, SoftStatsRow>(
            "SELECT results_id, seconds, session_id, start_time FROM soft_stats",
        )
        .fetch_all(&mut *conn)
        .await
    }

    pub async fn get_rows_by_id(
        conn: &mut SqliteConnection,
        results_id: ResultRowID,
    ) -> SqlResult<Vec<SoftStatsRow>> {
        sqlx::query_as::<_, SoftStatsRow>(
            "SELECT results_id, seconds, session_id, start_time FROM soft_stats WHERE results_id = ?",
        ).bind(results_id)
        .fetch_all(&mut *conn)
        .await
    }

    pub async fn insert(&self, conn: &mut SqliteConnection) -> SqlQueryResult {
        let result = sqlx::query(
            "INSERT INTO soft_stats(results_id, seconds, session_id, start_time) VALUES($1, $2, $3, $4)",
        )
        .bind(self.results_id)
        .bind(self.seconds)
        .bind(&self.session_id)
        .bind(&self.start_time)
        .execute(conn)
        .await?;
        assert_eq!(result.rows_affected(), 1);
        Ok(result)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HardStats {
    /// Number of steps taken before the machine was decided
    pub steps: u32,
    /// Number of cells used by the machine before the machine was decided
    pub space: u32,
}

/// Represents a single row in the stats table
#[derive(sqlx::FromRow, Clone, Copy, PartialEq, Eq)]
struct StatsRow {
    results_id: ResultRowID,
    stats: HardStats,
}

impl StatsRow {
    async fn insert(&self, conn: &mut SqliteConnection) -> SqlQueryResult {
        let result = sqlx::query("INSERT INTO stats (results_id, steps, space) VALUES($1, $2, $3)")
            .bind(self.results_id)
            .bind(self.stats.steps)
            .bind(self.stats.space)
            .execute(conn)
            .await?;
        assert_eq!(result.rows_affected(), 1);
        Ok(result)
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
    pub id: ResultRowID,
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
        decision: DecisionKind,
        stats: RunStats,
    ) -> SqlResult<(InsertedDecidedRow, RowsAffected)> {
        let mut rows_affected = 0;
        let mut txn = conn.begin().await?;

        // Update decision row
        let machine = PackedMachine::from(self.machine);
        let result = sqlx::query("UPDATE results SET decision = $2 WHERE machine = $1")
            .bind(&machine[..])
            .bind(decision)
            .execute(&mut *txn)
            .await?;
        assert_eq!(result.rows_affected(), 1);
        rows_affected += result.rows_affected();

        // Now actually insert the stats
        let stats_row = StatsRow {
            stats: HardStats {
                steps: stats.get_total_steps() as u32,
                space: stats.space_used() as u32,
            },
            results_id: self.id,
        };
        let result = stats_row.insert(&mut txn).await?;
        rows_affected += result.rows_affected();

        txn.commit().await?;

        let inserted_decided_row = InsertedDecidedRow {
            id: self.id,
            machine: self.machine,
            decision,
            stats: HardStats {
                steps: stats.get_total_steps() as u32,
                space: stats.space_used() as u32,
            },
        };
        Ok((inserted_decided_row, rows_affected))
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

#[derive(Debug, Clone, Copy)]
pub struct UninsertedDecidedRow {
    pub machine: MachineTable,
    pub decision: DecisionKind,
    pub stats: HardStats,
}
impl UninsertedDecidedRow {
    /// Insert this row into the database. It is assumed there is not already a row with the same [ResultRowID] in the database.
    pub async fn insert(&self, conn: &mut SqliteConnection) -> SqlResult<InsertedDecidedRow> {
        let mut txn = conn.begin().await?;

        let result = sqlx::query("INSERT INTO results (machine, decision) VALUES($1, $2)")
            .bind(self.machine)
            .bind(self.decision)
            .execute(&mut *txn)
            .await?;
        assert_eq!(result.rows_affected(), 1);
        let row_id = result.last_insert_rowid();

        let stats_row = StatsRow {
            results_id: row_id,
            stats: self.stats,
        };
        stats_row.insert(&mut txn).await?;

        txn.commit().await?;

        Ok(InsertedDecidedRow {
            id: row_id,
            machine: self.machine,
            decision: self.decision,
            stats: self.stats,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// A decided row that exists in the database
pub struct InsertedDecidedRow {
    pub id: ResultRowID,
    pub machine: MachineTable,
    pub decision: DecisionKind,
    pub stats: HardStats,
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
            stats: self.stats,
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
            decision: DecisionKind,
            steps: u32,
            space: u32,
        }

        async fn fetch_kind(
            conn: &mut SqliteConnection,
            kind: DecisionKind,
        ) -> SqlResult<Vec<Row>> {
            let rows_step = sqlx::query_as::<_, Row>(
                "SELECT results_id, machine, decision, steps, space FROM results
                     INNER JOIN stats USING (results_id) WHERE decision = ?",
            )
            .bind(kind)
            .fetch_all(conn)
            .await?;
            Ok(rows_step)
        }

        let rows1 = fetch_kind(conn, DecisionKind::Halting).await?;
        let rows2 = fetch_kind(conn, DecisionKind::NonHalting).await?;
        let rows3 = fetch_kind(conn, DecisionKind::EmptyTransition).await?;
        let rows4 = fetch_kind(conn, DecisionKind::UndecidedSpaceLimit).await?;
        let rows5 = fetch_kind(conn, DecisionKind::UndecidedStepLimit).await?;

        let rows = [rows1, rows2, rows3, rows4, rows5].into_iter().flatten();

        let rows = rows
            .into_iter()
            .map(|row| Self {
                id: row.results_id,
                machine: MachineTable::try_from(row.machine.as_slice()).unwrap(),
                decision: row.decision,
                stats: HardStats {
                    steps: row.steps,
                    space: row.space,
                },
            })
            .collect();
        Ok(rows)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertedRow {
    pub kind: InsertedRowKind,
    pub soft_stats: Vec<SoftStatsRow>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum InsertedRowKind {
    Pending(InsertedPendingRow),
    Decided(InsertedDecidedRow),
}

pub trait RowStream = Stream<Item = SqlResult<InsertedRow>>;

impl InsertedRow {
    /// Retrieve all rows from the database
    pub async fn get_all_rows(conn: &mut SqliteConnection) -> SqlResult<impl RowStream + '_> {
        #[derive(FromRow)]
        struct Row {
            results_id: ResultRowID,
            machine: MachineTable,
            decision: Option<DecisionKind>,
            steps: Option<u32>,
            space: Option<u32>,
        }

        fn map_row(
            row: Row,
            soft_stats: &HashMap<ResultRowID, Vec<SoftStatsRow>>,
        ) -> SqlResult<InsertedRow> {
            let id = row.results_id;
            let machine = row.machine;
            let soft_stats = soft_stats.get(&id).cloned().unwrap_or(vec![]);
            let row = if let Some(decision) = row.decision {
                // It is technically legal for there to be no soft_stats even when having a decided row
                // This is because it might be an old row from before these stats were collected.
                // These unwraps are safe because the stats row is present if and only if there is a decision
                let steps = row.steps.unwrap();
                let space = row.space.unwrap();
                InsertedRowKind::Decided(InsertedDecidedRow {
                    id,
                    machine,
                    decision,
                    stats: HardStats { steps, space },
                })
            } else {
                // There shouldn't be any recorded soft_stats if this row is still pending.
                assert!(soft_stats.is_empty());
                InsertedRowKind::Pending(InsertedPendingRow { id, machine })
            };

            Ok(InsertedRow {
                kind: row,
                soft_stats,
            })
        }

        let soft_stats = SoftStatsRow::get_rows(&mut *conn).await.map(|stats| {
            let mut hashmap = HashMap::new();
            for stat in stats {
                hashmap
                    .entry(stat.results_id)
                    .or_insert_with(|| Vec::with_capacity(1))
                    .push(stat);
            }
            hashmap
        })?;

        let result_rows = sqlx::query_as::<_, Row>(
            "SELECT results_id, machine, decision, steps, space FROM results
                 LEFT JOIN stats USING (results_id)",
        )
        .fetch(&mut *conn);

        let result_rows = result_rows.map(move |row| map_row(row?, &soft_stats));
        Ok(result_rows)
    }
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
    pub async fn get_counts(conn: &mut SqliteConnection) -> SqlResult<RowCounts> {
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
        .bind(DecisionKind::Halting)
        .bind(DecisionKind::NonHalting)
        .bind(DecisionKind::UndecidedStepLimit)
        .bind(DecisionKind::UndecidedSpaceLimit)
        .bind(DecisionKind::EmptyTransition)
        .fetch_one(conn)
        .await
    }
}

/// Create the tables for the database if they do not already exist.
pub async fn create_tables(conn: &mut SqliteConnection) -> SqlResult<()> {
    sqlx::query(
        "CREATE TABLE results (
        results_id       INTEGER NOT NULL PRIMARY KEY,
        machine          BLOB    NOT NULL UNIQUE,
        decision         INTEGER     NULL)",
    )
    .execute(&mut *conn)
    .await?;

    sqlx::query(
        "CREATE TABLE stats (
            results_id INTEGER NOT NULL REFERENCES results(results_id),
            steps      INTEGER NOT NULL,
            space      INTEGER NOT NULL)",
    )
    .execute(&mut *conn)
    .await?;

    sqlx::query("CREATE UNIQUE INDEX stats_are_unique ON stats(results_id)")
        .execute(&mut *conn)
        .await?;

    sqlx::query(
        "CREATE TABLE soft_stats (
            results_id INTEGER NOT NULL REFERENCES results(results_id),
            start_time TEXT    NOT NULL,
            seconds    REAL    NOT NULL,
            session_id TEXT        NULL)",
    )
    .execute(&mut *conn)
    .await?;

    Ok(())
}

/// Convience function to run a command.
async fn run_command(conn: &mut SqliteConnection, sql: &str) -> SqlQueryResult {
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

    if mode != ConnectionMode::ReadOnly {
        // PRAGMAs set here are from https://phiresky.github.io/blog/2020/sqlite-performance-tuning/
        // This is to allow faster writes to the database.
        run_command(&mut conn, "PRAGMA synchronous = normal").await?;
        run_command(&mut conn, "PRAGMA journal_mode = WAL;").await?;
    }
    Ok(conn)
}
