use std::time::Instant;
use turing_beavers::seed::Explorer;

fn run(explorer: Explorer, num_machines: usize) {
    while !explorer.machines_to_check.is_empty() {
        if explorer.stats().decided() > num_machines {
            break;
        }

        let result = explorer.step_decide();
        match result {
            Some(_result) => continue,
            None => break,
        }
    }
}

fn early_bench() {
    const NUM_MACHINES: usize = 70_000;
    let explorer = Explorer::new(1);

    println!("Deciding {} machines", NUM_MACHINES);
    let now = Instant::now();

    run(explorer, NUM_MACHINES);
    let elapsed = now.elapsed().as_secs_f32();
    println!(
        "Decided {} machines in {:.2} seconds ({:.0}/s)",
        NUM_MACHINES,
        elapsed,
        (NUM_MACHINES as f32 / elapsed)
    );
}

fn main() {
    early_bench();
}
