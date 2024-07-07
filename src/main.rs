#![feature(let_chains)]

use std::{
    path::{Path, PathBuf},
    str::FromStr,
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
    seed::{Decision, PendingNode, RunStats, STARTING_MACHINE},
    sql::{
        create_tables, get_connection, ConnectionMode, InsertedDecidedRow, InsertedPendingRow,
        RowCounts, RowsAffected, SqlResult, UninsertedPendingRow,
    },
    turing::MachineTable,
    worker::{
        add_work_to_queue, with_starting_queue, ReceiverProcessorQueue, ReceiverWorkerQueue,
        SenderProcessorQueue, SenderWorkerQueue, SoftStats, WorkUnit, WorkerResult,
    },
};

type SenderProcessorStatsQueue = Sender<ProcessorStats>;
type ReceiverProcessorStatsQueue = Receiver<ProcessorStats>;

type SenderWorkerStatsQueue = Sender<(Decision, RunStats)>;
type ReceiverWorkerStatsQueue = Receiver<(Decision, RunStats)>;

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

    fn add(&mut self, (decision, stats): (Decision, RunStats)) {
        match decision {
            Decision::EmptyTransition(_) => self.empty += 1,
            Decision::Halting => self.halt += 1,
            Decision::NonHalting => self.nonhalt += 1,
            Decision::UndecidedStepLimit => self.undecided_step += 1,
            Decision::UndecidedSpaceLimit => self.undecided_space += 1,
        }
        self.total_steps += stats.get_total_steps();
        self.total_space += stats.space_used();
    }
}

#[derive(Debug, Clone, Copy)]
struct ProcessorStats {
    // The number of unprocessed decided rows
    unprocessed: usize,
    rows_written: RowsAffected,
    // The number of pending rows
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

        let worker_status = format!("empty: {: >5} | halt: {: >5} | nonhalt: {: >5} | undec step: {: >5} | undec space: {: >5}",
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
    recv_processor: ReceiverProcessorStatsQueue,
    recv_worker: ReceiverWorkerStatsQueue,
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
    send_stats: SenderProcessorStatsQueue,
    send_pending: SenderWorkerQueue,
    recv_decided: ReceiverProcessorQueue,
) {
    let mut sender_closed = false;
    while let Ok(worker_result) = recv_decided.recv() {
        let (rows_written, _decided_row, new_work_units) =
            worker_result.submit(&mut conn).await.unwrap();

        if !sender_closed && let Err(_) = add_work_to_queue(&send_pending, new_work_units) {
            println!("Processor -- send_pending closed, no longer adding work to queue");
            sender_closed = true;
        };

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
    let remaining_pending = RowCounts::get_counts(&mut conn).await.pending;
    println!(
        "Processor -- exiting with {} pending machines written to database",
        remaining_pending
    );
    conn.close()
        .await
        .expect("Processor -- Could not close database connection!");
}

fn run_decider_worker(
    session_id: String,
    thread_id: usize,
    state: Arc<SharedThreadState>,
    send_stats: SenderWorkerStatsQueue,
    send_decided: SenderProcessorQueue,
    recv_pending: ReceiverWorkerQueue,
) {
    while let Ok(work_unit) = recv_pending.recv() {
        let now = Instant::now();
        let result = PendingNode::new(work_unit.machine()).decide();
        let duration = now.elapsed();

        send_stats
            .send((result.decision.clone(), result.stats))
            .unwrap();

        let soft_stats = SoftStats {
            duration,
            session_id: session_id.clone(),
        };

        let result = WorkerResult::new(work_unit, &result, soft_stats);
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

async fn init_connection(
    file: impl AsRef<Path>,
    is_new_database: bool,
) -> SqlResult<SqliteConnection> {
    if is_new_database {
        println!("Creating initial database file at {:?}", file.as_ref());
        let mut conn: SqliteConnection = get_connection(file, ConnectionMode::WriteNew).await?;
        create_tables(&mut conn).await?;

        // set up initial queue
        let machine = MachineTable::from_str(STARTING_MACHINE).unwrap();
        let row = UninsertedPendingRow { machine };
        row.insert_pending_row(&mut conn).await?;
        Ok(conn)
    } else {
        println!("Using existing database file at {:?}", file.as_ref());
        get_connection(file, ConnectionMode::WriteExisting).await
    }
}

async fn get_starting_queue(conn: &mut SqliteConnection, reprocess: bool) -> Vec<WorkUnit> {
    if !reprocess {
        println!("Retrieving pending rows...");
        InsertedPendingRow::get_pending_rows(conn)
            .await
            .unwrap()
            .into_iter()
            .map(WorkUnit::Pending)
            .collect()
    } else {
        println!("Retrieving decided rows for reprocessing...");
        InsertedDecidedRow::get_decided_rows(conn)
            .await
            .unwrap()
            .into_iter()
            .map(WorkUnit::Reprocess)
            .collect()
    }
}

fn install_ctrlc_handler(state: Arc<SharedThreadState>) {
    ctrlc::set_handler(move || {
        state.manually_shutdown.fetch_add(1, Ordering::Relaxed);
    })
    .expect("Could not set Ctrl-C handler");
}

fn start_threads(
    session_id: String,
    starting_queue: Vec<WorkUnit>,
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
    for thread_id in 0..num_workers {
        let send_decided = send_decided.clone();
        let recv_pending = recv_pending.clone();
        let send_stats_worker = send_stats_worker.clone();
        let state = state.clone();
        let session_id = session_id.clone();

        let worker = thread::Builder::new()
            .name(format!("worker_{thread_id}"))
            .spawn(move || {
                run_decider_worker(
                    session_id,
                    thread_id,
                    state,
                    send_stats_worker,
                    send_decided,
                    recv_pending,
                )
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
    /// If present, sort so that machines with the Halted state are processed first
    #[arg(long, action)]
    sort_halted_first: bool,
    /// If present, run with initial conditions. Otherwise, assume that we are using an existing database.
    #[arg(long, action)]
    init: bool,
    /// If present, reprocess all decided rows
    #[arg(long, action)]
    reprocess: bool,
    /// Database file to use. If init is set, this writes a new file. Otherwise, this opens an existing database.
    #[arg(short = 'i', long = "in")]
    in_path: PathBuf,
    /// Optional session ID to tag decided rows with.
    #[arg(long = "session")]
    session_id: Option<String>,
}

fn main() {
    let args = Args::parse();

    let mut conn = block_on(init_connection(args.in_path, args.init)).unwrap();

    let starting_queue = block_on(get_starting_queue(&mut conn, args.reprocess));
    let starting_queue = if args.sort_halted_first {
        println!("Sorting rows to work on machines with halt transitions first");
        let (mut halts, mut not_halts): (Vec<WorkUnit>, Vec<WorkUnit>) = starting_queue
            .iter()
            .partition(|row| row.machine().contains_halt_transition());
        halts.append(&mut not_halts);
        halts
    } else {
        starting_queue
    };
    println!("Starting queue size: {}", starting_queue.len());

    let state = Arc::new(SharedThreadState::new());
    install_ctrlc_handler(state.clone());

    let full_session_id = {
        let session_id = args.session_id.unwrap_or("default".to_string());
        format!("{}-workers={}", session_id, args.workers)
    };
    let (manager, workers, _stats_printer) =
        start_threads(full_session_id, starting_queue, conn, args.workers, state);

    for thread in workers {
        thread.join().unwrap()
    }
    manager.join().unwrap();
}
