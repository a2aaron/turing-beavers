#![feature(let_chains)]

use std::time::Instant;

use crossbeam::channel::{Receiver, Sender};
use turing_beavers::seed::{
    add_work_to_queue, new_queue, DecidedNode, MachineDecision, UndecidedNode,
};

#[derive(Debug, Clone, Copy)]
struct Stats {
    start: Instant,
    total_steps: usize,
    total_space: usize,
    remaining: usize,
    halt: usize,
    nonhalt: usize,
    undecided_step: usize,
    undecided_space: usize,
}

impl Stats {
    fn new() -> Stats {
        Stats {
            start: Instant::now(),
            remaining: 0,
            halt: 0,
            nonhalt: 0,
            undecided_step: 0,
            undecided_space: 0,
            total_steps: 0,
            total_space: 0,
        }
    }

    fn print(&mut self, prev: Stats, this_elapsed: f32) {
        let decided = self.decided();

        let delta_total_steps = self.total_steps - prev.total_steps;
        let total_elapsed = self.start.elapsed().as_secs_f32();

        let this_step_rate = delta_total_steps as f32 / this_elapsed;
        let rate = decided as f32 / total_elapsed;
        let total_step_rate = self.total_steps as f32 / total_elapsed;

        let status = format!("remain: {: >6} | halt: {: >6} | nonhalt: {: >6} | undecided step: {: >6} | undecided space: {: >6}", 
            self.remaining,
            self.halt - prev.halt,
            self.nonhalt - prev.nonhalt,
            self.undecided_step - prev.undecided_step,
            self.undecided_space - prev.undecided_space,
        );

        println!(
            "{} | steps/s: {this_step_rate: >9.0} (decided {} in {:.1}s, total {:} at {:.0}/s, {:.0} steps/s)",
            status,
            self.decided() - prev.decided(),
            this_elapsed,
            decided,
            rate,
            total_step_rate,
        );
    }

    fn decided(&self) -> usize {
        self.halt + self.nonhalt + self.undecided_space + self.undecided_step
    }
}

fn run_manager(
    config: Config,
    send_undecided: Sender<UndecidedNode>,
    recv_decided: Receiver<DecidedNode>,
) {
    let mut stats = Stats::new();
    let mut prev_stats = Stats::new();
    let mut last_printed_at = Instant::now();

    while let Ok(node) = recv_decided.recv() {
        match node.decision {
            MachineDecision::EmptyTransition(nodes) => add_work_to_queue(&send_undecided, nodes),
            MachineDecision::Halting => stats.halt += 1,
            MachineDecision::NonHalting => stats.nonhalt += 1,
            MachineDecision::UndecidedStepLimit => stats.undecided_step += 1,
            MachineDecision::UndecidedSpaceLimit => stats.undecided_space += 1,
        }
        stats.total_steps += node.stats.get_delta_steps();
        stats.total_space += node.stats.space_used();

        if last_printed_at.elapsed().as_secs() >= 1 {
            stats.print(prev_stats, last_printed_at.elapsed().as_secs_f32());
            prev_stats = stats;
            last_printed_at = Instant::now();
        }

        if config.should_exit() {
            println!("Manager exiting -- configured timeout expired");
            break;
        }
    }
}

fn run_decider_worker(
    thread_id: usize,
    send_decided: Sender<DecidedNode>,
    recv_undecided: Receiver<UndecidedNode>,
) {
    while let Ok(mut node) = recv_undecided.recv() {
        let result = node.decide();
        match send_decided.send(result) {
            Ok(()) => continue,
            Err(_) => {
                println!("Worker {} exiting -- send_decided was closed", thread_id);
                return;
            }
        }
    }
    println!(
        "Worker {} exiting -- machines_to_check was closed",
        thread_id
    );
}

struct Config {
    max_run_time: Option<u64>,
    now: Instant,
}

impl Config {
    fn new(max_run_time: Option<u64>) -> Config {
        Config {
            max_run_time,
            now: Instant::now(),
        }
    }

    fn should_exit(&self) -> bool {
        if let Some(max_run_time) = self.max_run_time {
            self.now.elapsed().as_secs() > max_run_time
        } else {
            false
        }
    }
}

fn main() {
    let num_threads = 10;
    let config = Config::new(Some(5));

    let (send_decided, recv_decided) = crossbeam::channel::unbounded();
    let (recv_undecided, send_undecided) = new_queue();

    std::thread::spawn(|| run_manager(config, send_undecided, recv_decided));

    let mut workers = vec![];
    for i in 0..num_threads {
        let send_decided = send_decided.clone();
        let recv_undecided = recv_undecided.clone();
        workers.push(std::thread::spawn(move || {
            run_decider_worker(i, send_decided, recv_undecided)
        }));
    }

    for thread in workers {
        thread.join().unwrap()
    }
}
