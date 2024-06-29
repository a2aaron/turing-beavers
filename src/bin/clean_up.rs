use std::path::PathBuf;

use clap::Parser;
use smol::block_on;
use sqlx::{prelude::*, sqlite::SqliteConnectOptions};
use turing_beavers::sql::RowCounts;

#[derive(Parser, Debug)]

struct Args {
    #[arg(short, long)]
    file: PathBuf,
}

async fn run(args: Args) {
    let mut conn = SqliteConnectOptions::new()
        .filename(args.file)
        .connect()
        .await
        .unwrap();

    sqlx::query("VACUUM").execute(&mut conn).await.unwrap();

    let RowCounts {
        total,
        pending,
        decided,
        halt,
        nonhalt,
        space,
        step,
        empty,
    } = RowCounts::get_counts(&mut conn).await;
    println!("Total: {total}, Pending: {pending}, Decided: {decided} (Halt: {halt}, Non-Halt: {nonhalt}, Step: {step}, Space: {space}, Empty: {empty})");
    conn.close().await.unwrap();
}

fn main() {
    let args = Args::parse();
    block_on(run(args));
}
