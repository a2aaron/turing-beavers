use std::path::{Path, PathBuf};

use clap::Parser;
use sqlx::{Connection, SqliteConnection};

use smol::{
    block_on,
    stream::{self, StreamExt},
};
use turing_beavers::{
    sql::{create_tables, get_connection, ConnectionMode, InsertedRow, InsertedRowKind, RowCounts},
    util::ProgressIndicator,
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

async fn create_output_file(path: impl AsRef<Path>) -> SqliteConnection {
    println!("Creating {:?}", path.as_ref());
    let mut conn = get_connection(path, ConnectionMode::WriteNew)
        .await
        .unwrap();
    create_tables(&mut conn).await.unwrap();
    conn
}

async fn run(args: Args) -> Result<(), sqlx::Error> {
    println!("Getting rows...");
    let mut input_conn = get_connection(args.in_path, ConnectionMode::ReadOnly)
        .await
        .unwrap();

    let mut decided_conn = create_output_file(&args.out_path.join("decided.sqlite")).await;
    let mut pending_conns: Vec<SqliteConnection> = stream::iter(0..args.num_split)
        .then(|i| {
            let path = args.out_path.join(format!("pending_{}.sqlite", i));
            create_output_file(path)
        })
        .collect()
        .await;

    insert_main_rows(&mut input_conn, &mut pending_conns, &mut decided_conn).await?;

    // Close the connections explicitly so that sqlite will clean up the wal and shm files.
    decided_conn.close().await.unwrap();
    for conn in pending_conns {
        conn.close().await.unwrap();
    }
    Ok(())
}

async fn insert_main_rows(
    input_conn: &mut SqliteConnection,
    pending_conns: &mut [SqliteConnection],
    decided_conn: &mut SqliteConnection,
) -> Result<(), sqlx::Error> {
    let RowCounts {
        total,
        decided,
        pending,
        ..
    } = RowCounts::get_counts(input_conn).await?;
    println!(
        "Splitting {:?} rows (decided: {}, pending: {} -> {} per split db)...",
        total,
        decided,
        pending,
        pending as usize / pending_conns.len() as usize,
    );

    let mut rows = InsertedRow::get_all_rows(input_conn).await?;

    let mut pending_i = 0;
    let mut progress_indicator = ProgressIndicator::new(total as usize);
    while let Some(row) = rows.try_next().await? {
        match row.kind {
            InsertedRowKind::Pending(pending) => {
                let pending_conn = &mut pending_conns[pending_i];
                pending.insert(pending_conn).await?;
                pending_i = (pending_i + 1) % pending_conns.len();
            }
            InsertedRowKind::Decided(decided) => {
                decided.insert(decided_conn).await?;
                for soft_stat in row.soft_stats {
                    assert_eq!(soft_stat.results_id, decided.id);
                    soft_stat.insert(decided_conn).await?;
                }
            }
        }

        progress_indicator.tick();
    }
    progress_indicator.print();
    Ok(())
}

fn main() -> Result<(), sqlx::Error> {
    let args = Args::parse();
    assert!(args.in_path.is_file(), "Input path must be a file");
    assert!(args.out_path.is_dir(), "Output path must be a directory");
    block_on(run(args))?;
    Ok(())
}
