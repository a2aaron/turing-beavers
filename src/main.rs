#![feature(let_chains)]

use std::{str::FromStr, time::Instant};

use crossbeam::channel::{Receiver, Sender};
use smol::block_on;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteQueryResult},
    ConnectOptions, SqliteConnection,
};
use turing_beavers::{
    seed::{add_work_to_queue, new_queue, DecidedNode, MachineDecision, UndecidedNode},
    turing::Table,
};

#[derive(Debug, Clone, Copy)]
struct Stats {
    start: Instant,
    total_steps: usize,
    total_space: usize,
    unprocessed: usize,
    remaining: usize,
    halt: usize,
    nonhalt: usize,
    undecided_step: usize,
    undecided_space: usize,
}

impl Stats {
    fn new() -> Stats {
        Stats {
            start: Instant::now(),
            remaining: 0,
            halt: 0,
            nonhalt: 0,
            undecided_step: 0,
            undecided_space: 0,
            total_steps: 0,
            total_space: 0,
            unprocessed: 0,
        }
    }

    fn print(&mut self, prev: Stats, this_elapsed: f32) {
        let decided = self.decided();

        let delta_total_steps = self.total_steps - prev.total_steps;
        let total_elapsed = self.start.elapsed().as_secs_f32();

        let this_step_rate = delta_total_steps as f32 / this_elapsed;
        let rate = decided as f32 / total_elapsed;
        let total_step_rate = self.total_steps as f32 / total_elapsed;

        let status = format!("remain: {: >6} | unproc: {: >5} | halt: {: >4} | nonhalt: {: >4} | undec step: {: >4} | undec space: {: >4}",
            self.remaining,
            self.unprocessed,
            self.halt - prev.halt,
            self.nonhalt - prev.nonhalt,
            self.undecided_step - prev.undecided_step,
            self.undecided_space - prev.undecided_space,
        );

        println!(
            "{} | steps/s: {this_step_rate: >9.0} (decided {} in {:.1}s, total {:} at {:.0}/s, {:.0} steps/s)",
            status,
            self.decided() - prev.decided(),
            this_elapsed,
            decided,
            rate,
            total_step_rate,
        );
    }

    fn decided(&self) -> usize {
        self.halt + self.nonhalt + self.undecided_space + self.undecided_step
    }
}

struct TableResults {
    results: Vec<ResultRow>,
    last_submit_time: Instant,
}

impl TableResults {
    fn new() -> TableResults {
        TableResults {
            results: Vec::with_capacity(1024),
            last_submit_time: Instant::now(),
        }
    }

    fn push(&mut self, node: &DecidedNode) {
        let row = ResultRow::from(node.table, &node.decision);
        self.results.push(row)
    }

    fn len(&self) -> usize {
        self.results.len()
    }

    fn should_submit(&self) -> bool {
        self.len() >= 1 || self.last_submit_time.elapsed().as_secs() > 5
    }

    async fn submit(&mut self, conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
        for result in self.results.drain(..) {
            let _result = insert_row(conn, result).await;
        }
        self.clear();
        Ok(())
    }

    fn clear(&mut self) {
        self.last_submit_time = Instant::now();
        self.results.clear();
    }
}

fn run_manager(
    config: Config,
    mut db_connection: SqliteConnection,
    send_undecided: Sender<UndecidedNode>,
    recv_decided: Receiver<DecidedNode>,
) {
    let mut results = TableResults::new();
    let mut stats = Stats::new();
    let mut prev_stats = Stats::new();
    let mut last_printed_at = Instant::now();

    while let Ok(node) = recv_decided.recv() {
        match node.decision {
            MachineDecision::EmptyTransition(nodes) => add_work_to_queue(&send_undecided, nodes),
            MachineDecision::Halting => {
                results.push(&node);
                stats.halt += 1
            }
            MachineDecision::NonHalting => {
                results.push(&node);
                stats.nonhalt += 1
            }
            MachineDecision::UndecidedStepLimit => {
                results.push(&node);
                stats.undecided_step += 1
            }
            MachineDecision::UndecidedSpaceLimit => {
                results.push(&node);
                stats.undecided_space += 1
            }
        }
        stats.unprocessed = recv_decided.len();
        stats.remaining = send_undecided.len();
        stats.total_steps += node.stats.get_delta_steps();
        stats.total_space += node.stats.space_used();

        if last_printed_at.elapsed().as_secs() >= 1 {
            stats.print(prev_stats, last_printed_at.elapsed().as_secs_f32());
            prev_stats = stats;
            last_printed_at = Instant::now();
        }

        if results.should_submit() {
            // let len = results.len();
            // let now = Instant::now();
            // println!("Submitting batch of size {}", len);
            block_on(results.submit(&mut db_connection)).unwrap();
            // let time = now.elapsed().as_secs_f32();
            // println!(
            //     "Submitted batch of size {} in {} seconds ({}/s)",
            //     len,
            //     time,
            //     len as f32 / time
            // )
        }

        if config.should_exit() {
            println!("Manager exiting -- configured timeout expired");
            break;
        }
    }
}

fn run_decider_worker(
    thread_id: usize,
    send_decided: Sender<DecidedNode>,
    recv_undecided: Receiver<UndecidedNode>,
) {
    while let Ok(mut node) = recv_undecided.recv() {
        let result = node.decide();
        match send_decided.send(result) {
            Ok(()) => continue,
            Err(_) => {
                println!("Worker {} exiting -- send_decided was closed", thread_id);
                return;
            }
        }
    }
    println!(
        "Worker {} exiting -- machines_to_check was closed",
        thread_id
    );
}

struct Config {
    max_run_time: Option<u64>,
    now: Instant,
}

impl Config {
    fn new(max_run_time: Option<u64>) -> Config {
        Config {
            max_run_time,
            now: Instant::now(),
        }
    }

    fn should_exit(&self) -> bool {
        if let Some(max_run_time) = self.max_run_time {
            self.now.elapsed().as_secs() > max_run_time
        } else {
            false
        }
    }
}

async fn get_connection() -> SqliteConnection {
    SqliteConnectOptions::from_str("/Users/aaron/dev/Rust/turing-beavers/results.sqlite")
        .unwrap()
        .create_if_missing(true)
        .connect()
        .await
        .unwrap()
}

struct ResultRow {
    machine: String,
    decision: Option<String>,
}

impl ResultRow {
    fn from(table: Table, decision: &MachineDecision) -> ResultRow {
        let decision = match decision {
            MachineDecision::Halting => Some("Halting".to_string()),
            MachineDecision::NonHalting => Some("Non-Halting".to_string()),
            MachineDecision::UndecidedStepLimit => Some("Undecided (Step)".to_string()),
            MachineDecision::UndecidedSpaceLimit => Some("Undecided (Space)".to_string()),
            MachineDecision::EmptyTransition(_) => None,
        };
        ResultRow {
            machine: table.to_string(),
            decision,
        }
    }
}

async fn insert_row(conn: &mut SqliteConnection, row: ResultRow) -> SqliteQueryResult {
    let insert = sqlx::query(
        "INSERT INTO results (machine, decision) VALUES($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(row.machine)
    .bind(row.decision)
    .execute(conn);
    insert.await.unwrap()
}

async fn create_tables(conn: &mut SqliteConnection) -> SqliteQueryResult {
    let query = sqlx::query(
        "CREATE TABLE IF NOT EXISTS results (
        id       INTEGER PRIMARY KEY NOT NULL,
        machine  TEXT    UNIQUE      NOT NULL,
        decision TEXT              NULL)",
    )
    .execute(conn);
    query.await.unwrap()
}
fn main() {
    let num_threads = 1;
    let config = Config::new(Some(300));

    let (send_decided, recv_decided) = crossbeam::channel::unbounded();
    let (recv_undecided, send_undecided) = new_queue();

    let mut db_connection: SqliteConnection = block_on(get_connection());
    let result = block_on(create_tables(&mut db_connection));
    println!("{:?}", result);
    std::thread::spawn(|| run_manager(config, db_connection, send_undecided, recv_decided));

    let mut workers = vec![];
    for i in 0..num_threads {
        let send_decided = send_decided.clone();
        let recv_undecided = recv_undecided.clone();
        workers.push(std::thread::spawn(move || {
            run_decider_worker(i, send_decided, recv_undecided)
        }));
    }

    for thread in workers {
        thread.join().unwrap()
    }
}
