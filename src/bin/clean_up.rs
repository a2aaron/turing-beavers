use std::path::PathBuf;

use clap::Parser;
use smol::block_on;
use sqlx::{prelude::*, sqlite::SqliteConnectOptions, SqliteConnection};
use turing_beavers::sql::Decision;

#[derive(Parser, Debug)]

struct Args {
    #[arg(short, long)]
    file: PathBuf,
}

#[derive(FromRow)]
struct Counts {
    total: u32,
    pending: u32,
    decided: u32,
    halt: u32,
    nonhalt: u32,
    step: u32,
    space: u32,
    empty: u32,
}
async fn get_count(conn: &mut SqliteConnection) -> Counts {
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

async fn run(args: Args) {
    let mut conn = SqliteConnectOptions::new()
        .filename(args.file)
        .connect()
        .await
        .unwrap();

    sqlx::query("VACUUM").execute(&mut conn).await.unwrap();

    let Counts {
        total,
        pending,
        decided,
        halt,
        nonhalt,
        space,
        step,
        empty,
    } = get_count(&mut conn).await;
    println!("Total: {total}, Pending: {pending}, Decided: {decided} (Halt: {halt}, Non-Halt: {nonhalt}, Step: {step}, Space: {space}, Empty: {empty})");
    conn.close().await.unwrap();
}

fn main() {
    let args = Args::parse();
    block_on(run(args));
}
