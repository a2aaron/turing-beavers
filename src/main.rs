#![feature(let_chains)]

use std::time::Instant;

use crossbeam::channel::{Receiver, Sender};
use smol::block_on;
use sqlx::SqliteConnection;
use turing_beavers::{
    seed::{add_work_to_queue, with_starting_queue, DecidedNode, MachineDecision, UndecidedNode},
    sql::{
        create_tables, get_connection, get_queue, insert_initial_row, run_command, submit_result,
    },
    turing::TableArray,
};

#[derive(Debug, Clone, Copy)]
struct Stats {
    start: Instant,
    total_steps: usize,
    total_space: usize,
    unprocessed: usize,
    processed: usize,
    empty: usize,
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
            empty: 0,
            halt: 0,
            nonhalt: 0,
            undecided_step: 0,
            undecided_space: 0,
            total_steps: 0,
            total_space: 0,
            unprocessed: 0,
            processed: 0,
        }
    }

    fn print(&mut self, prev: Stats, this_elapsed: f32) {
        let decided = self.decided();

        let delta_total_steps = self.total_steps - prev.total_steps;
        let total_elapsed = self.start.elapsed().as_secs_f32();

        let this_step_rate = delta_total_steps as f32 / this_elapsed;
        let rate = decided as f32 / total_elapsed;
        let total_step_rate = self.total_steps as f32 / total_elapsed;

        let status = format!("remain: {: >6} | proc: {: >5} | unproc: {: >4} | empty: {: >3} | halt: {: >2} | nonhalt: {: >4} | undec step: {: >3} | undec space: {: >3}",
            self.remaining,
            self.processed - prev.processed,
            self.unprocessed,
            self.empty - prev.empty,
            self.halt - prev.halt,
            self.nonhalt - prev.nonhalt,
            self.undecided_step - prev.undecided_step,
            self.undecided_space - prev.undecided_space,
        );

        let delta_processed = self.processed - prev.processed;
        let processed_rate = delta_processed as f32 / this_elapsed;

        let rate_status = format!(
            "steps/s: {this_step_rate: >9.0} | decided {} in {:.1}s, total {:} at {:.0}/s, {:.0} steps/s | processed {} in {:.1}s ({:}/s)",
            self.decided() - prev.decided(),
            this_elapsed,
            decided,
            rate,
            total_step_rate,
            delta_processed,
            this_elapsed,
            processed_rate,
            );
        println!("{} {}", status, rate_status);
    }

    fn decided(&self) -> usize {
        self.halt + self.nonhalt + self.undecided_space + self.undecided_step
    }
}

fn run_manager(
    config: Config,
    mut db_connection: SqliteConnection,
    send_undecided: Sender<UndecidedNode>,
    recv_decided: Receiver<DecidedNode>,
) {
    let mut stats = Stats::new();
    let mut prev_stats = Stats::new();
    let mut last_printed_at = Instant::now();

    while let Ok(node) = recv_decided.recv() {
        let submitted = block_on(submit_result(&mut db_connection, &node)).unwrap();
        stats.processed += submitted;
        match node.decision {
            MachineDecision::EmptyTransition(nodes) => {
                add_work_to_queue(&send_undecided, nodes);
                stats.empty += 1
            }
            MachineDecision::Halting => stats.halt += 1,
            MachineDecision::NonHalting => stats.nonhalt += 1,
            MachineDecision::UndecidedStepLimit => stats.undecided_step += 1,
            MachineDecision::UndecidedSpaceLimit => stats.undecided_space += 1,
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

fn init_connection(file: &str) -> (SqliteConnection, Vec<TableArray>) {
    let mut conn: SqliteConnection = block_on(get_connection(file));
    block_on(create_tables(&mut conn));

    // PRAGMAs set here are from https://phiresky.github.io/blog/2020/sqlite-performance-tuning/
    // This is to allow faster writes to the database.
    block_on(run_command(&mut conn, "PRAGMA synchronous = normal"));
    block_on(run_command(&mut conn, "PRAGMA journal_mode = WAL;"));

    // set up initial queue
    block_on(insert_initial_row(&mut conn));

    let starting_queue = block_on(get_queue(&mut conn));
    (conn, starting_queue)
}

fn main() {
    let num_threads = 10;
    let config = Config::new(None);

    let (conn, starting_queue) =
        init_connection("/Users/aaron/dev/Rust/turing-beavers/results.sqlite");
    println!("Starting queue size: {}", starting_queue.len());

    let (send_decided, recv_decided) = crossbeam::channel::unbounded();

    let (recv_undecided, send_undecided) = with_starting_queue(starting_queue);

    std::thread::spawn(|| run_manager(config, conn, send_undecided, recv_decided));

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
