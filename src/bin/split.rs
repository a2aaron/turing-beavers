use std::{
    path::{Path, PathBuf},
    time::Instant,
};

use clap::Parser;
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, Connection, FromRow, SqliteConnection};

use smol::{
    block_on,
    stream::{self, Stream, StreamExt},
};
use turing_beavers::{
    sql::{create_tables, Decision, RowID},
    turing::Table,
};
#[derive(Parser, Debug)]
struct Args {
    /// Database file to split
    #[arg(short = 'i', long = "in")]
    in_path: PathBuf,
    /// Output folder to write to
    #[arg(short = 'o', long = "out")]
    out_path: PathBuf,
    /// Number of files to split into
    #[arg(short = 'n', long)]
    num_split: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RowObject {
    results_id: RowID,
    machine: Table,
    decision: Option<DecisionWithStats>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DecisionWithStats {
    decision: Decision,
    steps: u32,
    space: u32,
}

async fn create_output_file(path: impl AsRef<Path>) -> SqliteConnection {
    println!("Creating {:?}", path.as_ref());
    let mut conn = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .connect()
        .await
        .unwrap();
    create_tables(&mut conn).await;
    conn
}

async fn insert_row(row: &RowObject, conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    if let Some(decision) = row.decision {
        let result =
            sqlx::query("INSERT INTO results (results_id, machine, decision) VALUES($1, $2, $3)")
                .bind(row.results_id)
                .bind(row.machine)
                .bind(decision.decision)
                .execute(&mut *conn)
                .await?;
        assert_eq!(result.rows_affected(), 1);

        let result = sqlx::query("INSERT INTO stats (results_id, steps, space) VALUES($1, $2, $3)")
            .bind(row.results_id)
            .bind(decision.steps)
            .bind(decision.space)
            .execute(&mut *conn)
            .await?;
        assert_eq!(result.rows_affected(), 1);
    } else {
        let result =
            sqlx::query("INSERT INTO results (results_id, machine, decision) VALUES($1, $2, NULL)")
                .bind(row.results_id)
                .bind(row.machine)
                .execute(&mut *conn)
                .await?;
        assert_eq!(result.rows_affected(), 1);
    }
    Ok(())
}

async fn get_row_count(conn: &mut SqliteConnection) -> u32 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM results
             LEFT JOIN stats USING (results_id)",
    )
    .fetch_one(&mut *conn)
    .await
    .unwrap()
}

async fn get_rows(
    conn: &mut SqliteConnection,
) -> impl Stream<Item = Result<RowObject, sqlx::Error>> + '_ {
    #[derive(FromRow)]
    struct Row {
        results_id: RowID,
        machine: Table,
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

        Ok(RowObject {
            results_id: row.results_id,
            machine: row.machine,
            decision,
        })
    })
}

async fn run(args: Args) -> Result<(), sqlx::Error> {
    println!("Getting rows...");
    let mut input_conn = SqliteConnectOptions::new()
        .filename(args.in_path.clone())
        .create_if_missing(false)
        .read_only(true)
        .connect()
        .await
        .unwrap();
    let row_count = get_row_count(&mut input_conn).await;
    println!("Splitting {:?} rows...", row_count);
    let mut rows = get_rows(&mut input_conn).await;

    let mut decided_conn = create_output_file(&args.out_path.join("decided.sqlite")).await;
    let mut undecided_conns: Vec<SqliteConnection> = stream::iter(0..args.num_split)
        .then(|i| {
            let path = args.out_path.join(format!("undecided_{}.sqlite", i));
            create_output_file(path)
        })
        .collect()
        .await;

    let mut total_i = 0;
    let mut undecided_i = 0;
    let mut now = Instant::now();
    while let Some(row) = rows.try_next().await? {
        if now.elapsed().as_secs() >= 1 {
            println!(
                "{}/{} ({:.2}%)",
                total_i,
                row_count,
                100.0 * total_i as f32 / row_count as f32
            );
            now = Instant::now();
        }

        if row.decision.is_some() {
            insert_row(&row, &mut decided_conn).await?;
        } else {
            insert_row(&row, &mut undecided_conns[undecided_i]).await?;
            undecided_i = (undecided_i + 1) % undecided_conns.len();
        }
        total_i += 1;
    }

    // Close the connections explicitly so that sqlite will clean up the wal and shm files.
    decided_conn.close().await.unwrap();
    for conn in undecided_conns {
        conn.close().await.unwrap();
    }
    println!("{}/{} (100%)", row_count, row_count,);
    Ok(())
}

fn main() -> Result<(), sqlx::Error> {
    let args = Args::parse();
    assert!(args.in_path.is_file(), "Input path must be a file");
    assert!(args.out_path.is_dir(), "Output path must be a directory");
    block_on(run(args))?;
    Ok(())
}
