use crossbeam::channel::{Receiver, SendError, Sender};
use std::{str::FromStr, time::Instant};
use turing_beavers::{
    seed::{MachineDecision, PendingNode, STARTING_MACHINE},
    turing::MachineTable,
};

pub fn with_starting_queue(
    tables: Vec<MachineTable>,
) -> (Receiver<PendingNode>, Sender<PendingNode>) {
    let (send, recv) = crossbeam::channel::unbounded();
    for table in tables {
        let machine = PendingNode::new(table);
        send.send(machine).unwrap();
    }
    (recv, send)
}

pub fn new_queue() -> (Receiver<PendingNode>, Sender<PendingNode>) {
    let table = MachineTable::from_str(STARTING_MACHINE).unwrap();
    with_starting_queue(vec![table])
}

pub fn add_work_to_queue(
    sender: &Sender<PendingNode>,
    nodes: Vec<PendingNode>,
) -> Result<(), SendError<PendingNode>> {
    for node in nodes {
        sender.send(node)?;
    }
    Ok(())
}

fn run(num_machines: usize) {
    let (recv, send) = new_queue();
    let mut num_decided = 0;
    while num_decided < num_machines {
        let result = std::hint::black_box(recv.recv().unwrap().decide());
        match result.decision {
            MachineDecision::EmptyTransition(nodes) => add_work_to_queue(&send, nodes).unwrap(),
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
