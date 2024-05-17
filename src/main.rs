use std::{
    sync::{atomic::Ordering, Arc},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use turing_beavers::seed::{Explorer, Stats};

fn spawn_timer(explorer: Arc<Explorer>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let total_time = Instant::now();
        let mut now = Instant::now();

        let mut last = Stats::new();
        let mut last_total_steps = 0;
        let mut last_total_space = 0;

        loop {
            let stats = explorer.stats();
            let delta = stats - last;

            let decided = stats.decided();

            let total_steps = explorer.total_steps.load(Ordering::Relaxed);
            let total_space = explorer.total_space.load(Ordering::Relaxed);

            let delta_total_space = total_space - last_total_space;
            let delta_total_steps = total_steps - last_total_steps;

            // let avg_steps = explorer.total_steps.load(Ordering::Relaxed) as f32 / decided as f32;
            // let avg_spaces = explorer.total_space.load(Ordering::Relaxed) as f32 / decided as f32;
            let rate = decided as f32 / total_time.elapsed().as_secs_f32();

            let status = format!("remain: {: >6} | halt: {: >6} | nonhalt: {: >6} | undecided step: {: >6} | undecided space: {: >6}", 
                delta.remaining,
                delta.halt,
                delta.nonhalt,
                delta.undecided_step,
                delta.undecided_space
            );

            println!(
                "{} | space: {delta_total_space:} | steps: {delta_total_steps:} (decided {} in {:.1}s, total {:} at {:.2}/s)",
                status,
                delta.decided(),
                now.elapsed().as_secs_f32(),
                decided,
                rate
            );

            now = Instant::now();
            last = stats;
            last_total_space = total_space;
            last_total_steps = total_steps;

            std::thread::sleep(Duration::from_secs(1));
        }
    })
}

fn spawn_thread(_thread_i: usize, explorer: Arc<Explorer>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        while !explorer.machines_to_check.is_empty() && explorer.stats().decided() < 70_000 {
            let result = explorer.step_decide();
            match result {
                Some(_result) => continue,
                None => break,
            }
        }
    })
}

fn main() {
    let explorer = Arc::new(Explorer::new());
    spawn_timer(explorer.clone());

    let mut threads = vec![];
    for i in 0..1 {
        threads.push(spawn_thread(i, explorer.clone()));
    }

    for thread in threads {
        thread.join().unwrap()
    }
}
