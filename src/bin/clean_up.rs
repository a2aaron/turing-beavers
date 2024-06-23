use std::path::PathBuf;

use clap::Parser;
use smol::block_on;
use sqlx::prelude::*;
use sqlx::sqlite::SqliteConnectOptions;

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
    conn.close().await.unwrap();
}

fn main() {
    let args = Args::parse();
    block_on(run(args));
}
