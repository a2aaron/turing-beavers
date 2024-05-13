use crate::turing::{Direction, State, Symbol, Table, Tape, Transition};

/// The number of steps that the 4-State 2-Symbol Busy Beaver champion runs for before halting.
/// This is useful because any 5-State machine must access it's 5th state within 107 steps or else
/// is it guaranteed to be non-halting. (If the machine were able to goes for more than 107 steps
/// without accessing it's 5th state but halted anyways, it would contradict the 4-State champion as
/// being the champion.)
pub const BUSY_BEAVER_FOUR_STEPS: usize = 107;

/// The number of steps that [BB5_CHAMPION] runs for before halting.
pub const TIME_LIMIT: usize = 47_176_870;
/// The number of unique cells visited by the [BB5_CHAMPION]. Note that this is not how many ones
/// that the champion writes to the tape, rather it's every cell written to (even cells which are
/// written to but do not have their value changed). Hence, this will be larger than the number of
/// ones written)
pub const SPACE_LIMIT: usize = 12_289;
/// The current contender champion for the 5-State 2-Symbol Busy Beaver.
pub const BB5_CHAMPION: &str = "1RB1LC_1RC1RB_1RD0LE_1LA1LD_1RZ0LA";
/// The starting machine to use when doing the phase 1 seed generation. This is the machine with
/// all empty transitions, except for the "state A + symbol 0" transition, which is
/// "write 1 + move right + state b".
pub const STARTING_MACHINE: &str = "1RB---_------_------_------_------";

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
    if tape.state == State::Halt {
        return StepResult::Halted;
    }

    let action = table.get(tape.state, tape.read());

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunStats {
    pub steps_ran: usize,
    pub max_index: isize,
    pub min_index: isize,
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

#[derive(Debug, Clone)]
pub struct ExplorerState {
    pub table: Table,
    pub tape: Tape,
    pub stats: RunStats,
}

impl ExplorerState {
    pub fn new(table: Table) -> ExplorerState {
        ExplorerState {
            table,
            tape: Tape::new(),
            stats: RunStats::new(),
        }
    }

    fn run(&mut self, max_steps: Option<usize>, max_space: Option<usize>) -> HaltReason {
        loop {
            match self.step() {
                StepResult::Halted => return HaltReason::HaltState,
                StepResult::Empty => return HaltReason::EmptyTransition,
                StepResult::Continue => {
                    if let Some(max_steps) = max_steps
                        && self.stats.steps_ran > max_steps
                    {
                        return HaltReason::ExceededStepLimit;
                    }

                    if let Some(max_space) = max_space
                        && self.stats.space_used() > max_space
                    {
                        return HaltReason::ExceededSpaceLimit;
                    }
                }
            }
        }
    }

    fn step(&mut self) -> StepResult {
        let result = step(&mut self.tape, &self.table);

        if result == StepResult::Continue {
            self.stats.steps_ran += 1;
            self.stats.min_index = self.stats.min_index.min(self.tape.index());
            self.stats.max_index = self.stats.max_index.max(self.tape.index());
        }
        result
    }

    pub fn print(&self) {
        println!(
            "{} | index: {}, state: {} | steps: {}, min: {}, max: {}",
            self.table,
            self.tape.index(),
            self.tape.state,
            self.stats.steps_ran,
            self.stats.min_index,
            self.stats.max_index,
        );
    }
}

pub struct Explorer {
    pub machines_to_check: Vec<ExplorerState>,
    pub nonhalting: Vec<Table>,
    pub halting: Vec<Table>,
    pub undecided: Vec<Table>,
}

impl Explorer {
    pub fn new() -> Explorer {
        let table = Table::parse(STARTING_MACHINE).unwrap();
        let machine = ExplorerState::new(table);
        Explorer {
            machines_to_check: vec![machine],
            nonhalting: vec![],
            halting: vec![],
            undecided: vec![],
        }
    }

    pub fn step(&mut self) -> Option<ExplorerStepResult> {
        if let Some(mut node) = self.machines_to_check.pop() {
            let halt_result = Self::step_node(&mut node);

            let mut nodes_added = None;
            match halt_result {
                ExplorerResult::Halting => self.halting.push(node.table),
                ExplorerResult::NonHalting => self.nonhalting.push(node.table),
                ExplorerResult::UndecidedStepLimit | ExplorerResult::UndecidedSpaceLimit => {
                    self.undecided.push(node.table)
                }
                ExplorerResult::EmptyTransition => {
                    // We halted because we encountered an empty transition. This means that we need
                    // to create a bunch of new machines whose tape is the same, but with the missing
                    // transition defined.
                    let mut nodes = Self::get_nodes(&node);
                    nodes_added = Some(nodes.size_hint().0);
                    self.machines_to_check.extend(&mut nodes);
                }
            }
            Some(ExplorerStepResult {
                node,
                nodes_added,
                halt_result,
            })
        } else {
            None
        }
    }

    fn step_node(node: &mut ExplorerState) -> ExplorerResult {
        // We build a tree of all of the "interesting" machines using the following algorithm:
        // - Simulate a machine step-by-step. One of three things happen:
        //      1. The machine reaches the time or space limit. In this case, we mark the machine as
        //         "Undecided" and remove it from the list of machines to check
        //      2. The machine reaches the BB(4) = 107 step limit. In this case, we mark the machine
        //         "Non-Halting" and remove it from the list of machines to check
        //      3. The machine reaches an empty transition. In this case, for each transition in the
        //         set of unique transitions (described below), we duplicate the machine
        //         and replace the empty transition with a filled transition. These new machines are
        //         added to the list of undecided machines and the original is removed.
        let four_states_or_less = visited_states(&node.table) < 5;
        let halt_reason = if four_states_or_less {
            node.run(Some(BUSY_BEAVER_FOUR_STEPS), Some(SPACE_LIMIT))
        } else {
            node.run(Some(TIME_LIMIT), Some(SPACE_LIMIT))
        };

        match halt_reason {
            // If we exceed the step limit, but visited 4 or less states, then the machine will
            // never halt since there's no way for it break out of those 4 states (if there was,
            // this would contradict the value of BB(4), since it would mean there is a halting
            // 2-symbol 4-state TM that halts later than BB(4) = 107 steps)
            HaltReason::ExceededStepLimit if four_states_or_less => ExplorerResult::NonHalting,
            HaltReason::ExceededStepLimit => ExplorerResult::UndecidedStepLimit,
            HaltReason::ExceededSpaceLimit => ExplorerResult::UndecidedSpaceLimit,
            HaltReason::HaltState => ExplorerResult::Halting,
            HaltReason::EmptyTransition => ExplorerResult::EmptyTransition,
        }
    }

    // Since there are 2 symbols, 2 directions, and 5 + 1 target states, there are
    // 24 possible transitions we could use. However, we can actually cut down how
    // many transitions we consider with the following rules:
    // 1. If we are defining the very first transition, we fix the first transition
    //    to be 0A -> 1RB (this is not done here, but instead is a consequence of
    //    the starting machine used).
    //    This is valid since the only other meaningfully different transitions are
    //    0A -> 0_A and 0A -> 1_A. In both cases, the machine will obviously
    //    never halt, since it will constantly be move left or right and remain in
    //    state A.
    // 2. If we are defining the very last empty transition, we fix the target state
    //    to be the halt state. This is needed because if there's no halt state,
    //    then obviously the machine cannot halt. In fact, we can just fix the
    //    transition to be _ -> 1LZ (or something equivalent) since it won't matter
    //    what symbol or direction we move since we're about to halt anyways.
    // 3. Otherwise, we're defining some empty transition that isn't the last one.
    //    In this case, we have a set of possible target states. The target states
    //    will be all of the visited states (so all of the states which have a
    //    defined transition) and the "lowest" unvisited state (where we consider
    //    state A to be the lowest and state E to be the highest).
    //    For example, if we have already visited states A, B, C (and unvisited states
    //    D and E), then the target states will be A, B, C, and D.
    //    (Sidenote: This also means that "visited" states will always be lower than
    //    unvisited ones, and that we only ever visit new states in the order of
    //    A, B, C, D, E. Another way to say this: The set of target states is just
    //    the states which have a defined transition in the table, plus the next
    //    state which would come after that).
    //    Also: Note that this case deliberately excludes the halt state from
    //    consideration (obviously if we define the target state for the transition
    //    we are about to take as the halt state then the machine will immediately
    //    halt, which is boring)
    //

    fn get_target_states(table: &Table) -> Vec<State> {
        let is_last_transition = defined_transitions(table) == 10 - 1;
        if is_last_transition {
            vec![State::Halt]
        } else {
            match visited_states(table) {
                // Technically not reachable, but included for completeness
                0 => vec![State::A],
                1 => vec![State::A, State::B],
                2 => vec![State::A, State::B, State::C],
                3 => vec![State::A, State::B, State::C, State::D],
                4 => vec![State::A, State::B, State::C, State::D, State::E],
                5 => vec![State::A, State::B, State::C, State::D, State::E],
                _ => unreachable!(),
            }
        }
    }

    fn get_nodes(node: &ExplorerState) -> impl Iterator<Item = ExplorerState> {
        let target_states = Self::get_target_states(&node.table);

        let transitions = target_states.into_iter().flat_map(|target_state| {
            [
                Transition(Symbol::Zero, Direction::Left, target_state),
                Transition(Symbol::Zero, Direction::Right, target_state),
                Transition(Symbol::One, Direction::Left, target_state),
                Transition(Symbol::One, Direction::Right, target_state),
            ]
        });

        let node = node.clone();
        transitions.map(move |transition| {
            let mut new_node = node.clone();
            let action = new_node.table.get_mut(node.tape.state, node.tape.read());
            *action = Some(transition);
            new_node
        })
    }

    pub fn print_status(&self, result: ExplorerStepResult) {
        let node = result.node;
        let table = node.table;
        let stats = node.stats;
        let remaining = self.machines_to_check.len();
        let message = match result.halt_result {
            ExplorerResult::Halting => format!(
                "halted ({} steps, {} cells)",
                stats.steps_ran,
                stats.space_used()
            ),
            ExplorerResult::NonHalting => {
                format!("nonhalting")
            }
            ExplorerResult::UndecidedStepLimit => {
                format!("undecided (step limit)")
            }
            ExplorerResult::UndecidedSpaceLimit => {
                format!("undecided (space limit)")
            }
            ExplorerResult::EmptyTransition => {
                format!(
                    "empty transition (added {} nodes)",
                    result.nodes_added.unwrap()
                )
            }
        };
        let num_halt = self.halting.len();
        let num_nonhalt = self.nonhalting.len();
        let num_undecided = self.undecided.len();
        println!("{table} - remain: {remaining:0>4} | halt: {num_halt:0>8} | nonhalt: {num_nonhalt:0>8} | undecided: {num_undecided:0>8} - {message}");
    }

    pub fn print_machines_to_check(&self) {
        for state in &self.machines_to_check {
            state.print();
        }
    }
}

pub struct ExplorerStepResult {
    pub node: ExplorerState,
    pub nodes_added: Option<usize>,
    pub halt_result: ExplorerResult,
}

/// Returns the number of transitions which are defined on the table.
fn defined_transitions(table: &Table) -> usize {
    table.state_a_0.is_some() as usize
        + table.state_a_1.is_some() as usize
        + table.state_b_0.is_some() as usize
        + table.state_b_1.is_some() as usize
        + table.state_c_0.is_some() as usize
        + table.state_c_1.is_some() as usize
        + table.state_d_0.is_some() as usize
        + table.state_d_1.is_some() as usize
        + table.state_e_0.is_some() as usize
        + table.state_e_1.is_some() as usize
}

/// Returns the number of states which are visited in this table. This will give how many
/// states were visited since we only define a state transition as a machien is about to
/// visit it.
fn visited_states(table: &Table) -> usize {
    let a_visited = table.state_a_0.is_some() || table.state_a_1.is_some();
    let b_visited = table.state_b_0.is_some() || table.state_b_1.is_some();
    let c_visited = table.state_c_0.is_some() || table.state_c_1.is_some();
    let d_visited = table.state_d_0.is_some() || table.state_d_1.is_some();
    let e_visited = table.state_e_0.is_some() || table.state_e_1.is_some();

    a_visited as usize
        + b_visited as usize
        + c_visited as usize
        + d_visited as usize
        + e_visited as usize
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplorerResult {
    Halting,
    NonHalting,
    UndecidedStepLimit,
    UndecidedSpaceLimit,
    EmptyTransition,
}

#[cfg(test)]
mod test {
    use crate::{
        seed::{ExplorerResult, ExplorerState, HaltReason, SPACE_LIMIT, TIME_LIMIT},
        turing::Table,
    };

    use super::{Explorer, BB5_CHAMPION};

    #[test]
    fn test_bb_champion() {
        let table = Table::parse(BB5_CHAMPION).unwrap();

        let explorer_state = &mut ExplorerState::new(table);
        let halt_reason = explorer_state.run(None, None);

        assert_eq!(halt_reason, HaltReason::HaltState);
        assert_eq!(explorer_state.stats.steps_ran, TIME_LIMIT);
        assert_eq!(explorer_state.stats.space_used(), SPACE_LIMIT);
    }

    #[test]
    fn test_explorer() {
        let mut explorer = Explorer::new();

        let result = explorer.step().unwrap();
        assert_eq!(ExplorerResult::EmptyTransition, result.halt_result);
    }
}
