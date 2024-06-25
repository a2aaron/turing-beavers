use std::{
    path::{Path, PathBuf},
    time::Instant,
};

use clap::Parser;
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, Connection, SqliteConnection};

use smol::{
    block_on,
    stream::{self, StreamExt},
};
use turing_beavers::sql::{create_tables, get_connection, ConnectionMode, ResultObject};
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

async fn create_output_file(path: impl AsRef<Path>) -> SqliteConnection {
    println!("Creating {:?}", path.as_ref());
    let mut conn = get_connection(path, ConnectionMode::Write).await;
    create_tables(&mut conn).await;
    conn
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

async fn run(args: Args) -> Result<(), sqlx::Error> {
    println!("Getting rows...");
    let mut input_conn = get_connection(args.in_path, ConnectionMode::ReadOnly).await;
    let row_count = get_row_count(&mut input_conn).await;
    println!("Splitting {:?} rows...", row_count);
    let mut rows = ResultObject::get_rows(&mut input_conn).await;

    let mut decided_conn = create_output_file(&args.out_path.join("decided.sqlite")).await;
    let mut pending_conns: Vec<SqliteConnection> = stream::iter(0..args.num_split)
        .then(|i| {
            let path = args.out_path.join(format!("pending_{}.sqlite", i));
            create_output_file(path)
        })
        .collect()
        .await;

    let mut total_i = 0;
    let mut pending_i = 0;
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
            row.insert(&mut decided_conn).await?;
        } else {
            row.insert(&mut pending_conns[pending_i]).await?;
            pending_i = (pending_i + 1) % pending_conns.len();
        }
        total_i += 1;
    }

    // Close the connections explicitly so that sqlite will clean up the wal and shm files.
    decided_conn.close().await.unwrap();
    for conn in pending_conns {
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
