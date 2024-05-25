#![feature(let_chains)]

use std::{
    sync::{atomic::Ordering, Arc},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use turing_beavers::seed::{Explorer, Stats};

fn spawn_timer(explorer: Arc<Explorer>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut now = Instant::now();

        let mut last = Stats::new();
        let mut last_total_steps = 0;

        loop {
            let stats = explorer.stats();
            let delta = stats - last;

            let decided = stats.decided();

            let total_steps = explorer.total_steps.load(Ordering::Relaxed);

            let delta_total_steps = total_steps - last_total_steps;
            let elapsed = now.elapsed().as_secs_f32();

            let total_step_rate = delta_total_steps as f32 / elapsed;
            let rate = decided as f32 / elapsed;

            let status = format!("remain: {: >6} | halt: {: >6} | nonhalt: {: >6} | undecided step: {: >6} | undecided space: {: >6}", 
                stats.remaining,
                delta.halt,
                delta.nonhalt,
                delta.undecided_step,
                delta.undecided_space
            );

            println!(
                "{} | steps/s: {total_step_rate: >9.0} (decided {} in {:.1}s, total {:} at {:.2}/s)",
                status,
                delta.decided(),
                elapsed,
                decided,
                rate
            );

            now = Instant::now();
            last = stats;
            last_total_steps = total_steps;

            std::thread::sleep(Duration::from_secs(1));
        }
    })
}

fn spawn_thread(mut config: WorkerConfig, explorer: Arc<Explorer>) -> JoinHandle<()> {
    std::thread::spawn(move || loop {
        while !explorer.machines_to_check.is_empty() {
            if config.should_exit() {
                println!("Thread {} exiting", config.id);
                return;
            }

            let result = explorer.step_decide();
            match result {
                Some(_result) => continue,
                None => break,
            }
        }

        println!("Thread {} sleeping -- no work in queue", config.id);
        explorer.wait_for_work.wait();
        if explorer.done() {
            config.set_all_work_done();
        } else {
            println!("Thread {} restarting", config.id);
        }
    })
}

struct WorkerConfig {
    id: usize,
    max_run_time: Option<u64>,
    now: Instant,
    all_work_done: bool,
}

impl WorkerConfig {
    fn should_exit(&self) -> bool {
        let exit_due_to_timer = if let Some(max_run_time) = self.max_run_time {
            self.now.elapsed().as_secs() > max_run_time
        } else {
            false
        };

        exit_due_to_timer || self.all_work_done
    }

    fn set_all_work_done(&mut self) {
        self.all_work_done = true;
    }

    fn new(thread_i: usize, max_run_time: Option<u64>) -> WorkerConfig {
        WorkerConfig {
            id: thread_i,
            max_run_time,
            now: Instant::now(),
            all_work_done: false,
        }
    }
}

fn main() {
    let num_threads = 10;
    let explorer = Arc::new(Explorer::new(num_threads));
    spawn_timer(explorer.clone());

    let mut threads = vec![];
    for i in 0..num_threads {
        let config = WorkerConfig::new(i, Some(5));
        threads.push(spawn_thread(config, explorer.clone()));
    }

    for thread in threads {
        thread.join().unwrap()
    }
}
