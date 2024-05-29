use std::time::Instant;
use turing_beavers::seed::Explorer;

fn run(explorer: Explorer) {
    while !explorer.machines_to_check.is_empty() {
        if explorer.stats().decided() > NUM_MACHINES {
            break;
        }

        let result = explorer.step_decide();
        match result {
            Some(_result) => continue,
            None => break,
        }
    }
}

const NUM_MACHINES: usize = 70_000;

fn main() {
    println!("Deciding {} machines", NUM_MACHINES);
    let now = Instant::now();
    run(Explorer::new(1));
    let elapsed = now.elapsed().as_secs_f32();
    println!(
        "Decided {} machines in {:.2} seconds ({:.0}/s)",
        NUM_MACHINES,
        elapsed,
        (NUM_MACHINES as f32 / elapsed)
    );
}
