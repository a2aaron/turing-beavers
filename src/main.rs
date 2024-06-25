#![feature(let_chains)]

use std::{
    path::Path,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use clap::{arg, Parser};
use crossbeam::channel::{Receiver, Sender};
use smol::block_on;
use sqlx::{Connection, SqliteConnection};
use turing_beavers::{
    seed::{
        add_work_to_queue, with_starting_queue, DecidedNode, MachineDecision, PendingNode, RunStats,
    },
    sql::{
        create_tables, get_connection, get_pending_queue, insert_initial_row, submit_result,
        ConnectionMode,
    },
    turing::{MachineTable, MachineTableArray},
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
    // The number of pending nodes
    pending: Option<usize>,
}

impl ProcessorStats {
    fn new() -> ProcessorStats {
        ProcessorStats {
            unprocessed: 0,
            rows_written: 0,
            pending: Some(0),
        }
    }

    fn add(&mut self, stats: ProcessorStats) {
        self.rows_written += stats.rows_written;
        self.unprocessed = stats.unprocessed;
        self.pending = stats.pending;
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
            "steps/s: {: >9.0} | decided {}, total {:} at {:.0}/s + {:.0} total steps/s",
            this_step_rate,
            self.decided() - prev.decided(),
            decided,
            rate,
            total_step_rate,
        );

        let delta_rows_written = self.processor.rows_written - prev.processor.rows_written;

        let processor_status = format!(
            "total pending: {} | total unprocessed: {: >6} | rows written: {: >6}",
            if let Some(pending) = self.processor.pending {
                format!("{: >6}", pending)
            } else {
                "N/A".to_string()
            },
            self.processor.unprocessed,
            delta_rows_written,
        );

        println!("{} | {}", worker_status, worker_rate_status);
        println!("{}", processor_status);
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
        while let Ok(processor_stats) = recv_processor.try_recv() {
            stats.processor.add(processor_stats)
        }
        while let Ok(worker_stats) = recv_worker.try_recv() {
            stats.worker.add(worker_stats)
        }

        stats.print(prev_stats, last_printed_at.elapsed().as_secs_f32());
        prev_stats = stats;
        last_printed_at = Instant::now();
        std::thread::sleep(Duration::from_secs(1));
    }
}

async fn run_processor(
    mut conn: SqliteConnection,
    state: Arc<SharedThreadState>,
    send_stats: Sender<ProcessorStats>,
    send_pending: Sender<PendingNode>,
    recv_decided: Receiver<DecidedNode>,
) {
    let mut sender_closed = false;
    while let Ok(node) = recv_decided.recv() {
        let rows_written = submit_result(&mut conn, &node).await.unwrap();
        match node.decision {
            MachineDecision::EmptyTransition(nodes) => {
                if !sender_closed && let Err(_) = add_work_to_queue(&send_pending, nodes) {
                    println!("Processor -- send_pending closed, no longer adding work to queue");
                    sender_closed = true;
                };
            }
            MachineDecision::Halting => (),
            MachineDecision::NonHalting => (),
            MachineDecision::UndecidedStepLimit => (),
            MachineDecision::UndecidedSpaceLimit => (),
        }
        let unprocessed = recv_decided.len();
        let pending = if sender_closed {
            None
        } else {
            Some(send_pending.len())
        };
        send_stats
            .send(ProcessorStats {
                unprocessed,
                rows_written,
                pending,
            })
            .unwrap();

        if state.should_processor_force_exit() {
            println!(
                "Processor -- Abandoning {} unprocessed results in queue",
                unprocessed
            );
            break;
        }
    }
    let remaining_queue_length = get_pending_queue(&mut conn).await.unwrap().len();
    println!(
        "Processor -- exiting with {} queued machines written to database",
        remaining_queue_length
    );
    conn.close()
        .await
        .expect("Processor -- Could not close database connection!");
}

fn run_decider_worker(
    thread_id: usize,
    state: Arc<SharedThreadState>,
    send_stats: Sender<(MachineDecision, RunStats)>,
    send_decided: Sender<DecidedNode>,
    recv_pending: Receiver<PendingNode>,
) {
    while let Ok(mut node) = recv_pending.recv() {
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

        if state.should_workers_exit() {
            println!("Worker {} exiting -- graceful shutdown", thread_id);
            return;
        }
    }
}

struct SharedThreadState {
    manually_shutdown: AtomicU8,
}

impl SharedThreadState {
    fn new() -> SharedThreadState {
        SharedThreadState {
            manually_shutdown: AtomicU8::from(0),
        }
    }

    fn should_workers_exit(&self) -> bool {
        let state = self.manually_shutdown.load(Ordering::Relaxed);
        state > 0
    }

    fn should_processor_force_exit(&self) -> bool {
        let state = self.manually_shutdown.load(Ordering::Relaxed);
        state > 1
    }
}

async fn init_connection(file: impl AsRef<Path>) -> (SqliteConnection, Vec<MachineTable>) {
    let mut conn: SqliteConnection = get_connection(file, ConnectionMode::Write).await.unwrap();
    create_tables(&mut conn).await.unwrap();

    // set up initial queue
    insert_initial_row(&mut conn).await.unwrap();

    let starting_queue = get_pending_queue(&mut conn).await.unwrap();
    (conn, starting_queue)
}

fn install_ctrlc_handler(state: Arc<SharedThreadState>) {
    ctrlc::set_handler(move || {
        state.manually_shutdown.fetch_add(1, Ordering::Relaxed);
    })
    .expect("Could not set Ctrl-C handler");
}

fn start_threads(
    starting_queue: Vec<MachineTableArray>,
    conn: SqliteConnection,
    num_workers: usize,
    state: Arc<SharedThreadState>,
) -> (JoinHandle<()>, Vec<JoinHandle<()>>, JoinHandle<()>) {
    let (send_stats_processor, recv_stats_processor) = crossbeam::channel::unbounded();
    let (send_stats_worker, recv_stats_worker) = crossbeam::channel::unbounded();

    let (send_decided, recv_decided) = crossbeam::channel::unbounded();
    let (recv_pending, send_pending) = with_starting_queue(starting_queue);

    let processor = thread::Builder::new()
        .name("processor".to_string())
        .spawn({
            let state = state.clone();
            move || {
                block_on(run_processor(
                    conn,
                    state.clone(),
                    send_stats_processor,
                    send_pending,
                    recv_decided,
                ))
            }
        })
        .unwrap();

    let mut workers = vec![];
    for i in 0..num_workers {
        let send_decided = send_decided.clone();
        let recv_pending = recv_pending.clone();
        let send_stats_worker = send_stats_worker.clone();
        let state = state.clone();

        let worker = thread::Builder::new()
            .name(format!("worker_{i}"))
            .spawn(move || {
                run_decider_worker(i, state, send_stats_worker, send_decided, recv_pending)
            })
            .unwrap();
        workers.push(worker);
    }

    let stats_printer = thread::Builder::new()
        .name("stats".to_string())
        .spawn(|| run_stats_printer(recv_stats_processor, recv_stats_worker))
        .unwrap();
    (processor, workers, stats_printer)
}

#[derive(Parser, Debug)]
struct Args {
    /// Number of worker threads to run with.
    #[arg(short, long)]
    workers: usize,
    /// If present, sort so that tables with the Halted state are processed first
    #[arg(long, action)]
    sort_halted_first: bool,
}

fn main() {
    let args = Args::parse();

    let file = "/Users/aaron/dev/Rust/turing-beavers/results.sqlite";
    let (conn, starting_queue) = block_on(init_connection(file));
    println!("Starting queue size: {}", starting_queue.len());

    let starting_queue = if args.sort_halted_first {
        let (mut halts, mut not_halts): (Vec<MachineTable>, Vec<MachineTable>) = starting_queue
            .iter()
            .partition(|table| table.contains_halt_transition());
        halts.append(&mut not_halts);
        halts
    } else {
        starting_queue
    };

    let state = Arc::new(SharedThreadState::new());
    install_ctrlc_handler(state.clone());
    let (manager, workers, _stats_printer) =
        start_threads(starting_queue, conn, args.workers, state);

    for thread in workers {
        thread.join().unwrap()
    }
    manager.join().unwrap();
}
