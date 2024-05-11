use crate::turing::{Action, State, Symbol, Table, Tape, Transition};

/// The number of steps that [BB5_CHAMPION] runs for before halting.
const TIME_LIMIT: usize = 47_176_870;
/// The number of unique cells visited by the [BB5_CHAMPION]. Note that this is not how many ones
/// that the champion writes to the tape, rather it's every cell written to (even cells which are
/// written to but do not have their value changed). Hence, this will be larger than the number of
/// ones written)
const SPACE_LIMIT: usize = 12_289;
/// The current contender champion for the 5-State 2-Symbol Busy Beaver.
const BB5_CHAMPION: &str = "1RB1LC_1RC1RB_1RD0LE_1LA1LD_1RZ0LA";
/// The starting machine to use when doing the phase 1 seed generation. This is the machine with
/// all empty transitions, except for the "state A + symbol 0" transition, which is
/// "write 1 + move right + state b".
const STARTING_MACHINE: &str = "1RB---_------_------_------_------";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepResult {
    /// The machine was in the Halt state. The tape remains unchanged.
    Halted,
    /// The machine was not in the Halt state and is not in an empty [Action].
    Continue,
    /// The machine was in an empty [Action]. The tape remains unchanged.
    Empty,
}

/// Simulate the [Tape] for one step according to the [Table]. If the Tape's state is [State::Halt]
/// or if the [Action] supplied by the Table is empty, then nothing happens to the Tape
/// (it is assumed that empty Actions correspond to a halt state). Otherwise, the appropriate Action
/// is executed on the Tape.
pub fn step(tape: &mut Tape, table: &Table) -> StepResult {
    let action = match (tape.state, tape.read()) {
        (State::A, Symbol::Zero) => table.state_a_0,
        (State::A, Symbol::One) => table.state_a_1,
        (State::B, Symbol::Zero) => table.state_b_0,
        (State::B, Symbol::One) => table.state_b_1,
        (State::C, Symbol::Zero) => table.state_c_0,
        (State::C, Symbol::One) => table.state_c_1,
        (State::D, Symbol::Zero) => table.state_d_0,
        (State::D, Symbol::One) => table.state_d_1,
        (State::E, Symbol::Zero) => table.state_e_0,
        (State::E, Symbol::One) => table.state_e_1,
        (State::Halt, Symbol::Zero) => return StepResult::Halted,
        (State::Halt, Symbol::One) => return StepResult::Halted,
    };

    match action {
        Some(Transition(cell, direction, state)) => {
            tape.write(cell);
            tape.shift(direction);
            tape.set_state(state);
            StepResult::Continue
        }
        None => StepResult::Empty,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HaltReason {
    /// Machine halted because it entered the halt state.
    HaltState,
    /// Machine halted because it entered an empty transition
    EmptyTransition,
    /// Machine reached the space limit--that is, it visited more cells than the limit specified by
    /// the maximum space limit.
    ExceededSpaceLimit,
    /// Machine reached the step limit--that is, it was ran for more steps than the limit specified
    /// by the maximum step limit.
    ExceededStepLimit,
}

#[derive(Debug, Clone, Copy)]
struct RunStats {
    steps_ran: usize,
    max_index: isize,
    min_index: isize,
}
impl RunStats {
    fn new() -> RunStats {
        RunStats {
            steps_ran: 0,
            min_index: 0,
            max_index: 0,
        }
    }

    fn space_used(&self) -> usize {
        1 + (self.max_index - self.min_index) as usize
    }
}

struct ExplorerState {
    table: Table,
    tape: Tape,
    stats: RunStats,
}

impl ExplorerState {
    fn new(table: Table) -> ExplorerState {
        ExplorerState {
            table,
            tape: Tape::new(),
            stats: RunStats::new(),
        }
    }

    fn run(
        &mut self,
        max_steps: Option<usize>,
        max_space: Option<usize>,
    ) -> (HaltReason, RunStats) {
        let halt_reason;
        loop {
            match self.step() {
                StepResult::Halted => {
                    halt_reason = HaltReason::HaltState;
                    break;
                }
                StepResult::Empty => {
                    halt_reason = HaltReason::EmptyTransition;
                    break;
                }
                StepResult::Continue => {
                    if let Some(max_steps) = max_steps
                        && self.stats.steps_ran > max_steps
                    {
                        halt_reason = HaltReason::ExceededStepLimit;
                        break;
                    }

                    if let Some(max_space) = max_space
                        && self.stats.space_used() > max_space
                    {
                        halt_reason = HaltReason::ExceededSpaceLimit;
                        break;
                    }
                }
            }
        }
        (halt_reason, self.stats)
    }

    fn step(&mut self) -> StepResult {
        let result = step(&mut self.tape, &self.table);

        if result == StepResult::Continue {
            self.stats.steps_ran += 1;
            self.stats.min_index = self.stats.min_index.min(self.tape.index);
            self.stats.max_index = self.stats.max_index.max(self.tape.index);
        }
        result
    }
}

pub struct Explorer {
    machines_to_check: Vec<ExplorerState>,
}

impl Explorer {
    pub fn new() -> Explorer {
        let table = Table::parse(STARTING_MACHINE).unwrap();
        let machine = ExplorerState::new(table);
        Explorer {
            machines_to_check: vec![machine],
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        seed::{ExplorerState, HaltReason, SPACE_LIMIT, TIME_LIMIT},
        turing::Table,
    };

    use super::BB5_CHAMPION;

    #[test]
    fn test_bb_champion() {
        let table = Table::parse(BB5_CHAMPION).unwrap();

        let (halt_reason, stats) = ExplorerState::new(table).run(None, None);

        assert_eq!(halt_reason, HaltReason::HaltState);
        assert_eq!(stats.steps_ran, TIME_LIMIT);
        assert_eq!(stats.space_used(), SPACE_LIMIT);
    }
}
