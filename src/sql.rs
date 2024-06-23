use std::str::FromStr;

use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteQueryResult},
    ConnectOptions, SqliteConnection,
};

use crate::{
    seed::{DecidedNode, MachineDecision, STARTING_MACHINE},
    turing::Table,
};

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
    let decision = match node.decision {
        MachineDecision::Halting => 0,
        MachineDecision::NonHalting => 1,
        MachineDecision::UndecidedStepLimit => 2,
        MachineDecision::UndecidedSpaceLimit => 3,
        MachineDecision::EmptyTransition(_) => 4,
    };
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
    .execute(conn)
    .await
    .unwrap();
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
