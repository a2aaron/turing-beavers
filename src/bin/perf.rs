use std::time::Instant;
use turing_beavers::seed::{Explorer, MachineDecision};

fn run(num_machines: usize) {
    let (explorer, send) = Explorer::new();
    let mut num_decided = 0;
    while num_decided < num_machines {
        let result = std::hint::black_box(explorer.machines_to_check.recv().unwrap().decide());
        match result.decision {
            MachineDecision::EmptyTransition(nodes) => Explorer::add_work(&send, nodes),
            _ => num_decided += 1,
        }
    }
}

fn early_bench() {
    const NUM_MACHINES: usize = 70_000;

    println!("Deciding {} machines", NUM_MACHINES);
    let now = Instant::now();

    run(NUM_MACHINES);
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
