use std::str::FromStr;

use crossbeam::channel::{Receiver, Sender};

use crate::turing::{Direction, State, Symbol, Table, Tape, Transition};

/// The number of steps that the 4-State 2-Symbol Busy Beaver champion runs for before halting.
/// This is useful because any 5-State machine must access it's 5th state within 107 steps or else
/// is it guaranteed to be non-halting. (If the machine were able to goes for more than 107 steps
/// without accessing it's 5th state but halted anyways, it would contradict the 4-State champion as
/// being the champion.)
pub const BUSY_BEAVER_FOUR_STEPS: usize = 107;

/// The number of steps that [BB5_CHAMPION] runs for before halting, plus one.
pub const TIME_LIMIT: usize = BB5_STEPS + 1;
/// The number of unique cells visited by the [BB5_CHAMPION]. Note that this is not how many ones
/// that the champion writes to the tape, rather it's every cell written to (even cells which are
/// written to but do not have their value changed). Hence, this will be larger than the number of
/// ones written)
pub const SPACE_LIMIT: usize = BB5_SPACE + 1;
/// The current contender champion for the 5-State 2-Symbol Busy Beaver.
pub const BB5_CHAMPION: &str = "1RB1LC_1RC1RB_1RD0LE_1LA1LD_1RZ0LA";
pub const BB5_STEPS: usize = 47_176_870;
pub const BB5_SPACE: usize = 12_289;
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
        Some(transition) => {
            // Note: Packaging these four lines into a function produces significantly faster code
            // I am unsure why
            // let (symbol, direction, state) = transition.into_tuple();
            // tape.write(symbol);
            // tape.shift(direction);
            // tape.set_state(state);
            tape.execute(transition);
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
    steps_ran: usize,
    prev_steps: usize,
    pub max_index: isize,
    pub min_index: isize,
}
impl RunStats {
    fn new() -> RunStats {
        RunStats {
            steps_ran: 0,
            prev_steps: 0,
            min_index: 0,
            max_index: 0,
        }
    }
    pub fn get_delta_steps(&self) -> usize {
        self.steps_ran - self.prev_steps
    }

    pub fn space_used(&self) -> usize {
        1 + (self.max_index - self.min_index) as usize
    }

    pub fn get_total_steps(&self) -> usize {
        self.steps_ran
    }
}

#[derive(Debug, Clone)]
pub struct UndecidedNode {
    pub table: Table,
    pub tape: Tape,
    pub stats: RunStats,
}

impl UndecidedNode {
    pub fn new(table: Table) -> UndecidedNode {
        UndecidedNode {
            table,
            tape: Tape::new(),
            stats: RunStats::new(),
        }
    }

    pub fn decide(&mut self) -> DecidedNode {
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
        let four_states_or_less = self.table.visited_states() < 5;
        let halt_reason = if four_states_or_less {
            self.run(Some(BUSY_BEAVER_FOUR_STEPS), Some(SPACE_LIMIT))
        } else {
            self.run(Some(TIME_LIMIT), Some(SPACE_LIMIT))
        };

        let decision = match halt_reason {
            // If we exceed the step limit, but visited 4 or less states, then the machine will
            // never halt since there's no way for it break out of those 4 states (if there was,
            // this would contradict the value of BB(4), since it would mean there is a halting
            // 2-symbol 4-state TM that halts later than BB(4) = 107 steps)
            HaltReason::ExceededStepLimit if four_states_or_less => MachineDecision::NonHalting,
            HaltReason::ExceededStepLimit => MachineDecision::UndecidedStepLimit,
            HaltReason::ExceededSpaceLimit => MachineDecision::UndecidedSpaceLimit,
            HaltReason::HaltState => MachineDecision::Halting,
            HaltReason::EmptyTransition => {
                // We halted because we encountered an empty transition. This means that we need
                // to create a bunch of new machines whose tape is the same, but with the missing
                // transition defined.
                let nodes = get_child_nodes(self).collect();
                MachineDecision::EmptyTransition(nodes)
            }
        };
        DecidedNode {
            table: self.table,
            decision,
            stats: self.stats,
        }
    }

    fn run(&mut self, max_steps: Option<usize>, max_space: Option<usize>) -> HaltReason {
        loop {
            match self.step() {
                StepResult::Halted => return HaltReason::HaltState,
                StepResult::Empty => return HaltReason::EmptyTransition,
                StepResult::Continue => {
                    if let Some(max_steps) = max_steps
                        && self.stats.steps_ran >= max_steps
                    {
                        return HaltReason::ExceededStepLimit;
                    }

                    if let Some(max_space) = max_space
                        && self.stats.space_used() >= max_space
                    {
                        return HaltReason::ExceededSpaceLimit;
                    }
                }
            }
        }
    }

    pub fn step(&mut self) -> StepResult {
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

pub struct DecidedNode {
    pub table: Table,
    pub decision: MachineDecision,
    pub stats: RunStats,
}

pub fn with_starting_queue(tables: Vec<Table>) -> (Receiver<UndecidedNode>, Sender<UndecidedNode>) {
    let (send, recv) = crossbeam::channel::unbounded();
    for table in tables {
        let machine = UndecidedNode::new(table);
        send.send(machine).unwrap();
    }
    (recv, send)
}

pub fn new_queue() -> (Receiver<UndecidedNode>, Sender<UndecidedNode>) {
    let table = Table::from_str(STARTING_MACHINE).unwrap();
    with_starting_queue(vec![table])
}

pub fn add_work_to_queue(sender: &Sender<UndecidedNode>, nodes: Vec<UndecidedNode>) {
    for node in nodes {
        // This unwrap is safe because the worker threads will always have an open receiver
        // (until the sender is dropped)
        sender.send(node).unwrap();
    }
}

#[derive(Debug, Clone)]
pub enum MachineDecision {
    Halting,
    NonHalting,
    UndecidedStepLimit,
    UndecidedSpaceLimit,
    EmptyTransition(Vec<UndecidedNode>),
}

fn get_child_nodes(node: &UndecidedNode) -> impl Iterator<Item = UndecidedNode> {
    let children = get_child_tables_for_transition(node.table, node.tape.state, node.tape.read());

    let node = node.clone();
    children.map(move |table| {
        // TODO: Explore effects of using ExplorerNode::new(table) here instead
        // Using new seems to make this much slower??
        let mut new_node = node.clone();
        new_node.table = table;
        new_node.stats.prev_steps = new_node.stats.steps_ran;
        new_node
    })
}

/// Given a transition [Table], returns a set of Tables whose
/// The input Table is assumed to have the follwoing conditions:
/// 1. The table's visited states (that is, the states for which at least one of the two transitions is defined) should
///    all be lower than the unvisited states (where states are order with A as the lowest and E as the highest)
/// 2. The table has at least one empty [Action]. In particular, the Action returned by table.get(state, symbol) should
///    be empty.
/// 3. `state` is the lowest unvisited state in the table.
fn get_child_tables_for_transition(
    table: Table,
    state: State,
    symbol: Symbol,
) -> impl Iterator<Item = Table> {
    let target_states = get_target_states(&table);
    // Turn each target state into 0LX, 0RX, 1LX, and 1RX
    let transitions = target_states.into_iter().flat_map(|target_state| {
        [
            Transition::from_tuple(Symbol::Zero, Direction::Left, target_state),
            Transition::from_tuple(Symbol::Zero, Direction::Right, target_state),
            Transition::from_tuple(Symbol::One, Direction::Left, target_state),
            Transition::from_tuple(Symbol::One, Direction::Right, target_state),
        ]
    });
    // Replace the undefined transition with the transitions we just created.
    transitions.map(move |transition| {
        let mut table = table.clone();
        let action = table.get_mut(state, symbol);
        *action = Some(transition);
        table
    })
}

/// Returns the target states that an undefined transition can opt to visit. This is the set of
/// already visited states in the Table plus the lowest unvisited state.
fn get_target_states(table: &Table) -> Vec<State> {
    let is_last_transition = table.defined_transitions() == 10 - 1;
    if is_last_transition {
        vec![State::Halt]
    } else {
        match table.visited_states() {
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

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::{
        seed::{
            new_queue, HaltReason, MachineDecision, UndecidedNode, BB5_SPACE, BB5_STEPS,
            SPACE_LIMIT, TIME_LIMIT,
        },
        turing::{State, Symbol, Table},
    };

    use super::{get_child_tables_for_transition, BB5_CHAMPION, STARTING_MACHINE};

    fn assert_contains(tables: &[Table], table: &str) {
        assert!(tables.contains(&Table::from_str(table).unwrap()))
    }

    #[test]
    fn test_bb_champion() {
        let table = Table::from_str(BB5_CHAMPION).unwrap();

        let node = &mut UndecidedNode::new(table);
        let halt_reason = node.run(Some(TIME_LIMIT), Some(SPACE_LIMIT));

        assert_eq!(halt_reason, HaltReason::HaltState);
        assert_eq!(node.stats.steps_ran, BB5_STEPS);
        assert_eq!(node.stats.space_used(), BB5_SPACE);
    }

    #[test]
    fn test_decide() {
        let (recv, _send) = new_queue();
        let mut node = recv.recv().unwrap();
        let result = node.decide();
        assert!(matches!(
            result.decision,
            MachineDecision::EmptyTransition(_)
        ));
    }

    #[test]
    fn test_get_child_tables_1() {
        let table = Table::from_str(STARTING_MACHINE).unwrap();
        let tables: Vec<Table> =
            get_child_tables_for_transition(table, State::B, Symbol::One).collect();

        assert_contains(&tables, "1RB---_---0LA_------_------_------");
        assert_contains(&tables, "1RB---_---0RA_------_------_------");
        assert_contains(&tables, "1RB---_---1LA_------_------_------");
        assert_contains(&tables, "1RB---_---1RA_------_------_------");
        assert_contains(&tables, "1RB---_---0LB_------_------_------");
        assert_contains(&tables, "1RB---_---0RB_------_------_------");
        assert_contains(&tables, "1RB---_---1LB_------_------_------");
        assert_contains(&tables, "1RB---_---1RB_------_------_------");
    }

    #[test]
    fn test_get_child_tables_2() {
        let table = Table::from_str("1RB1LC_1RC1RB_1RD0LE_1LA1LD_---0LA").unwrap();
        let tables: Vec<Table> =
            get_child_tables_for_transition(table, State::E, Symbol::Zero).collect();

        assert_contains(&tables, "1RB1LC_1RC1RB_1RD0LE_1LA1LD_0LZ0LA");
        assert_contains(&tables, "1RB1LC_1RC1RB_1RD0LE_1LA1LD_0RZ0LA");
        assert_contains(&tables, "1RB1LC_1RC1RB_1RD0LE_1LA1LD_1LZ0LA");
        assert_contains(&tables, "1RB1LC_1RC1RB_1RD0LE_1LA1LD_1RZ0LA");
    }

    #[test]
    fn test_time_limit() {
        let table = Table::from_str("1RB0LC_0LA0LD_1LA---_0RE0RD_0LD---").unwrap();
        let mut node = UndecidedNode::new(table);
        let reason = node.run(Some(TIME_LIMIT), None);
        assert_eq!(reason, HaltReason::ExceededStepLimit);
        // runs for one less step than actual for probably silly reasons.
        assert_eq!(node.stats.steps_ran, TIME_LIMIT);
    }

    #[test]
    fn test_space_limit_right() {
        let table = Table::from_str("1RA1RA_------_------_------_------").unwrap();
        let mut node = UndecidedNode::new(table);
        let reason = node.run(None, Some(SPACE_LIMIT));
        assert_eq!(reason, HaltReason::ExceededSpaceLimit);
        // runs for one less step than actual for probably silly reasons.
        assert_eq!(node.stats.space_used(), SPACE_LIMIT);
        assert_eq!(node.stats.steps_ran, SPACE_LIMIT - 1);
    }

    #[test]
    fn test_space_limit_left() {
        let table = Table::from_str("1LA1LA_------_------_------_------").unwrap();
        let mut node = UndecidedNode::new(table);
        let reason = node.run(None, Some(SPACE_LIMIT));
        assert_eq!(reason, HaltReason::ExceededSpaceLimit);
        // runs for one less step than actual for probably silly reasons.
        assert_eq!(node.stats.space_used(), SPACE_LIMIT);
        assert_eq!(node.stats.steps_ran, SPACE_LIMIT - 1);
    }
}
