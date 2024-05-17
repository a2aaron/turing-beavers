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

fn spawn_thread(thread_i: usize, explorer: Arc<Explorer>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let now = Instant::now();
        loop {
            while !explorer.machines_to_check.is_empty() {
                if now.elapsed().as_secs() > 10 {
                    return;
                }

                let result = explorer.step_decide();
                match result {
                    Some(_result) => continue,
                    None => break,
                }
            }
            println!("Thread {thread_i} sleeping -- no work in queue");
            explorer.cond_var.wait();
            if explorer.done() {
                println!("Thread {thread_i} exiting");
                break;
            } else {
                println!("Thread {thread_i} restarting");
            }
        }
    })
}

fn main() {
    let num_threads = 10;
    let explorer = Arc::new(Explorer::new(num_threads));
    spawn_timer(explorer.clone());

    let mut threads = vec![];
    for i in 0..num_threads {
        threads.push(spawn_thread(i, explorer.clone()));
    }

    for thread in threads {
        thread.join().unwrap()
    }
}
