#![feature(let_chains)]

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::Instant,
};

use crossbeam::{
    channel::{Receiver, Sender},
    select,
};
use smol::block_on;
use sqlx::SqliteConnection;
use turing_beavers::{
    seed::{
        add_work_to_queue, with_starting_queue, DecidedNode, MachineDecision, RunStats,
        UndecidedNode,
    },
    sql::{
        create_tables, get_connection, get_queue, insert_initial_row, run_command, submit_result,
    },
    turing::TableArray,
};

#[derive(Debug, Clone, Copy)]
struct WorkerStats {
    total_steps: usize,
    total_space: usize,
    empty: usize,
    halt: usize,
    nonhalt: usize,
    undecided_step: usize,
    undecided_space: usize,
}

impl WorkerStats {
    fn new() -> WorkerStats {
        WorkerStats {
            empty: 0,
            halt: 0,
            nonhalt: 0,
            undecided_step: 0,
            undecided_space: 0,
            total_steps: 0,
            total_space: 0,
        }
    }

    fn add(&mut self, (decision, stats): (MachineDecision, RunStats)) {
        match decision {
            MachineDecision::EmptyTransition(_) => self.empty += 1,
            MachineDecision::Halting => self.halt += 1,
            MachineDecision::NonHalting => self.nonhalt += 1,
            MachineDecision::UndecidedStepLimit => self.undecided_step += 1,
            MachineDecision::UndecidedSpaceLimit => self.undecided_space += 1,
        }
        self.total_steps += stats.get_delta_steps();
        self.total_space += stats.space_used();
    }
}

#[derive(Debug, Clone, Copy)]
struct ProcessorStats {
    // The number of unprocessed decided nodes
    unprocessed: usize,
    rows_written: usize,
    // The number of undecided nodes
    remaining: usize,
}

impl ProcessorStats {
    fn new() -> ProcessorStats {
        ProcessorStats {
            unprocessed: 0,
            rows_written: 0,
            remaining: 0,
        }
    }

    fn add(&mut self, stats: ProcessorStats) {
        self.rows_written += stats.rows_written;
        self.unprocessed = stats.unprocessed;
        self.remaining = stats.remaining;
    }
}

#[derive(Debug, Clone, Copy)]
struct Stats {
    start: Instant,
    worker: WorkerStats,
    processor: ProcessorStats,
}

impl Stats {
    fn new() -> Stats {
        Stats {
            start: Instant::now(),
            worker: WorkerStats::new(),
            processor: ProcessorStats::new(),
        }
    }

    fn print(&self, prev: Stats, this_elapsed: f32) {
        let decided = self.decided();

        let delta_total_steps = self.worker.total_steps - prev.worker.total_steps;
        let total_elapsed = self.start.elapsed().as_secs_f32();

        let this_step_rate = delta_total_steps as f32 / this_elapsed;
        let rate = decided as f32 / total_elapsed;
        let total_step_rate = self.worker.total_steps as f32 / total_elapsed;

        let worker_status = format!("new empty: {: >5} | halt: {: >5} | nonhalt: {: >5} | undec step: {: >5} | undec space: {: >5}",
            self.worker.empty - prev.worker.empty,
            self.worker.halt - prev.worker.halt,
            self.worker.nonhalt - prev.worker.nonhalt,
            self.worker.undecided_step - prev.worker.undecided_step,
            self.worker.undecided_space - prev.worker.undecided_space,
        );

        let worker_rate_status = format!(
            "steps/s: {: >9.0} | decided {} in {:.1}s, total {:} at {:.0}/s, {:.0} total steps/s",
            this_step_rate,
            self.decided() - prev.decided(),
            this_elapsed,
            decided,
            rate,
            total_step_rate,
        );

        let delta_rows_written = self.processor.rows_written - prev.processor.rows_written;
        let processed_rate = delta_rows_written as f32 / this_elapsed;

        let processor_status = format!(
            "remain: {: >6} | rows written: {: >6} | unprocessed: {: >6}",
            self.processor.remaining,
            self.processor.rows_written - prev.processor.rows_written,
            self.processor.unprocessed,
        );

        let processor_rate_status = format!(
            "wrote {} rows in {:.1}s ({:}/s)",
            delta_rows_written, this_elapsed, processed_rate,
        );
        println!("{} | {}", worker_status, worker_rate_status);
        println!("{} | {}", processor_status, processor_rate_status);
    }

    fn decided(&self) -> usize {
        self.worker.halt
            + self.worker.nonhalt
            + self.worker.undecided_space
            + self.worker.undecided_step
    }
}

fn run_stats_printer(
    recv_processor: Receiver<ProcessorStats>,
    recv_worker: Receiver<(MachineDecision, RunStats)>,
) {
    let mut last_printed_at = Instant::now();
    let mut stats = Stats::new();
    let mut prev_stats = Stats::new();
    loop {
        select! {
            recv(recv_processor) -> msg => if let Ok(processor_stats) = msg { stats.processor.add(processor_stats) },
            recv(recv_worker) -> msg => if let Ok(worker_stats) = msg{ stats.worker.add(worker_stats) },
        };

        if last_printed_at.elapsed().as_secs() >= 1 {
            stats.print(prev_stats, last_printed_at.elapsed().as_secs_f32());
            prev_stats = stats;
            last_printed_at = Instant::now();
        }
    }
}

fn run_processor(
    mut conn: SqliteConnection,
    send_stats: Sender<ProcessorStats>,
    send_undecided: Sender<UndecidedNode>,
    recv_decided: Receiver<DecidedNode>,
) {
    let mut sender_closed = false;
    while let Ok(node) = recv_decided.recv() {
        let rows_written = block_on(submit_result(&mut conn, &node)).unwrap();
        match node.decision {
            MachineDecision::EmptyTransition(nodes) => {
                if !sender_closed && let Err(_) = add_work_to_queue(&send_undecided, nodes) {
                    println!("Manager -- send_undecided closed, no longer adding work to undecided queue");
                    sender_closed = true;
                };
            }
            MachineDecision::Halting => (),
            MachineDecision::NonHalting => (),
            MachineDecision::UndecidedStepLimit => (),
            MachineDecision::UndecidedSpaceLimit => (),
        }
        let unprocessed = recv_decided.len();
        let remaining = send_undecided.len();
        send_stats
            .send(ProcessorStats {
                unprocessed,
                rows_written,
                remaining,
            })
            .unwrap();
    }
    println!(
        "Manager -- exiting with {} queued machines written to database",
        block_on(get_queue(&mut conn)).len()
    )
}

fn run_decider_worker(
    thread_id: usize,
    state: Arc<SharedThreadState>,
    send_stats: Sender<(MachineDecision, RunStats)>,
    send_decided: Sender<DecidedNode>,
    recv_undecided: Receiver<UndecidedNode>,
) {
    while let Ok(mut node) = recv_undecided.recv() {
        let result = node.decide();
        send_stats
            .send((result.decision.clone(), result.stats))
            .unwrap();
        match send_decided.send(result) {
            Ok(()) => (),
            Err(_) => {
                println!("Worker {} exiting -- send_decided was closed", thread_id);
                return;
            }
        }

        if state.should_exit() {
            println!("Worker {} exiting -- graceful shutdown", thread_id);
            return;
        }
    }
}

struct SharedThreadState {
    manually_shutdown: AtomicBool,
}

impl SharedThreadState {
    fn new() -> SharedThreadState {
        SharedThreadState {
            manually_shutdown: AtomicBool::from(false),
        }
    }

    fn should_exit(&self) -> bool {
        self.manually_shutdown.load(Ordering::Relaxed)
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

fn install_ctrlc_handler(state: Arc<SharedThreadState>) {
    ctrlc::set_handler(move || {
        println!("Control-C caught! Shutting down gracefully");
        state.manually_shutdown.store(true, Ordering::Relaxed);
    })
    .expect("Could not set Ctrl-C handler");
}

fn start_threads(
    starting_queue: Vec<TableArray>,
    conn: SqliteConnection,
    num_threads: usize,
    state: Arc<SharedThreadState>,
) -> (JoinHandle<()>, Vec<JoinHandle<()>>, JoinHandle<()>) {
    let (send_stats_processor, recv_stats_processor) = crossbeam::channel::unbounded();
    let (send_stats_worker, recv_stats_worker) = crossbeam::channel::unbounded();

    let (send_decided, recv_decided) = crossbeam::channel::unbounded();
    let (recv_undecided, send_undecided) = with_starting_queue(starting_queue);

    let manager = std::thread::spawn(|| {
        run_processor(conn, send_stats_processor, send_undecided, recv_decided)
    });

    let mut workers = vec![];
    for i in 0..num_threads {
        let send_decided = send_decided.clone();
        let recv_undecided = recv_undecided.clone();
        let send_stats_worker = send_stats_worker.clone();
        let state = state.clone();

        workers.push(std::thread::spawn(move || {
            run_decider_worker(i, state, send_stats_worker, send_decided, recv_undecided)
        }));
    }

    let stats_printer =
        std::thread::spawn(|| run_stats_printer(recv_stats_processor, recv_stats_worker));
    (manager, workers, stats_printer)
}

fn main() {
    let num_threads = 1;

    let (conn, starting_queue) =
        init_connection("/Users/aaron/dev/Rust/turing-beavers/results.sqlite");
    println!("Starting queue size: {}", starting_queue.len());

    let state = Arc::new(SharedThreadState::new());
    install_ctrlc_handler(state.clone());
    let (manager, workers, _stats_printer) =
        start_threads(starting_queue, conn, num_threads, state);

    for thread in workers {
        thread.join().unwrap()
    }
    manager.join().unwrap();
}
