use std::path::{Path, PathBuf};

use clap::Parser;
use smol::{block_on, stream::StreamExt};
use sqlx::{Connection, SqliteConnection};
use turing_beavers::{
    sql::{
        create_tables, get_connection, ConnectionMode, InsertedRow, InsertedRowKind, RowCounts,
        SoftStatsRow, SqlResult, UninsertedDecidedRow, UninsertedPendingRow,
    },
    util::ProgressIndicator,
};

#[derive(Parser, Debug)]
struct Args {
    /// Directory of database files to merge
    #[arg(short = 'i', long = "in")]
    in_path: PathBuf,
    /// Output file to write to. Must not be a file that already exists.
    #[arg(short = 'o', long = "out")]
    out_path: PathBuf,
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
    let mut input_conns = vec![];
    for file in args.in_path.read_dir()? {
        let file = file?.path();
        let is_sqlite_file = file
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("sqlite"));
        if !is_sqlite_file {
            continue;
        }
        let mut input_conn = get_connection(&file, ConnectionMode::ReadOnly).await?;
        let RowCounts {
            total,
            decided,
            pending,
            ..
        } = RowCounts::get_counts(&mut input_conn).await?;
        println!(
            "Retrieved {} rows from {} ({} decided, {} pending)",
            total,
            file.to_str().unwrap_or("?"),
            decided,
            pending
        );
        input_conns.push((input_conn, total));
    }

    input_conns.reverse();

    let mut output_conn = create_output_file(&args.out_path).await;

    for (input_conn, total) in &mut input_conns {
        merge(input_conn, &mut output_conn, *total).await?;
    }

    // Close the connections explicitly so that sqlite will clean up the wal and shm files.
    output_conn.close().await.unwrap();
    for (conn, _) in input_conns {
        conn.close().await.unwrap();
    }
    Ok(())
}

async fn merge<'a>(
    input_conn: &'a mut SqliteConnection,
    output_conn: &'a mut SqliteConnection,
    input_total: u32,
) -> SqlResult<()> {
    let mut rows = InsertedRow::get_all_rows(input_conn).await?;
    let mut progress_indicator = ProgressIndicator::new(input_total as usize);
    println!("Inserting {} rows", input_total);
    while let Some(row) = rows.try_next().await? {
        // TODO: is this right? should this check for merge conflicts?
        // Convert the inserted rows to an uninserted row so that it gets a new rowid.
        match row.kind {
            InsertedRowKind::Pending(pending) => {
                let pending = UninsertedPendingRow {
                    machine: pending.machine,
                };
                pending.insert_pending_row(output_conn).await?;
            }
            InsertedRowKind::Decided(decided) => {
                let decided = UninsertedDecidedRow {
                    machine: decided.machine,
                    decision: decided.decision,
                    stats: decided.stats,
                };
                let result = decided.insert(output_conn).await;
                let decided = match &result {
                    Ok(decided) => decided,
                    Err(err) => panic!("{}, {:?}", err, decided),
                };
                for soft_stat in row.soft_stats {
                    let soft_stat = SoftStatsRow {
                        results_id: decided.id,
                        ..soft_stat
                    };
                    soft_stat.insert(output_conn).await?;
                }
            }
        }
        progress_indicator.tick();
    }
    progress_indicator.print();
    Ok(())
}

fn main() -> SqlResult<()> {
    let args = Args::parse();
    assert!(args.in_path.is_dir(), "Input path must be a directory");
    assert!(!args.out_path.is_dir(), "Output path must be a file");
    assert!(
        !args.out_path.exists(),
        "Output path must not already exist (won't overwrite existing database)"
    );
    block_on(run(args))?;
    Ok(())
}
