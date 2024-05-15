use std::{
    sync::Arc,
    thread::JoinHandle,
    time::{Duration, Instant},
};

use turing_beavers::seed::Explorer;

fn spawn_timer(explorer: Arc<Explorer>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut last_count = 0;
        let total_time = Instant::now();
        let mut now = Instant::now();
        loop {
            let count = explorer.total_decided();
            let delta = count - last_count;
            let rate = delta as f32 / total_time.elapsed().as_secs_f32();
            println!(
                "{} ({} in {:?}, total {} at {}/sec)",
                explorer.status(),
                delta,
                now.elapsed(),
                count,
                rate
            );
            now = Instant::now();
            last_count = count;
            std::thread::sleep(Duration::from_secs(1));
        }
    })
}

fn spawn_thread(thread_i: usize, explorer: Arc<Explorer>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        while !explorer.machines_to_check.is_empty() && explorer.total_decided() < 70_000 {
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
    println!("{}", explorer.status());
}
