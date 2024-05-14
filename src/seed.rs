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
pub struct ExplorerNode {
    pub table: Table,
    pub tape: Tape,
    pub stats: RunStats,
}

impl ExplorerNode {
    pub fn new(table: Table) -> ExplorerNode {
        ExplorerNode {
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
    pub machines_to_check: Vec<ExplorerNode>,
    pub nonhalting: Vec<Table>,
    pub halting: Vec<Table>,
    pub undecided: Vec<Table>,
}

impl Explorer {
    pub fn new() -> Explorer {
        let table = Table::parse(STARTING_MACHINE).unwrap();
        let machine = ExplorerNode::new(table);

        let mut machines_to_check = Vec::new();
        machines_to_check.push(machine);
        Explorer {
            machines_to_check,
            nonhalting: vec![],
            halting: vec![],
            undecided: vec![],
        }
    }

    pub fn step(&mut self) -> Option<ExplorerStepInfo> {
        if let Some(mut node) = self.machines_to_check.pop() {
            let halt_result = Self::step_node(&mut node);

            match halt_result.clone() {
                MachineDecision::Halting => self.halting.push(node.table),
                MachineDecision::NonHalting => self.nonhalting.push(node.table),
                MachineDecision::UndecidedStepLimit | MachineDecision::UndecidedSpaceLimit => {
                    self.undecided.push(node.table)
                }
                MachineDecision::EmptyTransition(mut nodes) => {
                    self.machines_to_check.append(&mut nodes)
                }
            }
            Some(ExplorerStepInfo {
                node,
                decision: halt_result,
            })
        } else {
            None
        }
    }

    fn step_node(node: &mut ExplorerNode) -> MachineDecision {
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
            HaltReason::ExceededStepLimit if four_states_or_less => MachineDecision::NonHalting,
            HaltReason::ExceededStepLimit => MachineDecision::UndecidedStepLimit,
            HaltReason::ExceededSpaceLimit => MachineDecision::UndecidedSpaceLimit,
            HaltReason::HaltState => MachineDecision::Halting,
            HaltReason::EmptyTransition => {
                // We halted because we encountered an empty transition. This means that we need
                // to create a bunch of new machines whose tape is the same, but with the missing
                // transition defined.
                let nodes = get_child_nodes(node.clone()).collect();
                MachineDecision::EmptyTransition(nodes)
            }
        }
    }

    pub fn print_status(&self, result: ExplorerStepInfo) {
        let node = result.node;
        let table = node.table;
        let stats = node.stats;
        let remaining = self.machines_to_check.len();
        let message = match result.decision {
            MachineDecision::Halting => format!(
                "halted ({} steps, {} cells)",
                stats.steps_ran,
                stats.space_used()
            ),
            MachineDecision::NonHalting => {
                format!("nonhalting")
            }
            MachineDecision::UndecidedStepLimit => {
                format!("undecided (step limit)")
            }
            MachineDecision::UndecidedSpaceLimit => {
                format!("undecided (space limit)")
            }
            MachineDecision::EmptyTransition(nodes) => {
                format!("empty transition (added {} nodes)", nodes.len())
            }
        };
        let num_halt = self.halting.len();
        let num_nonhalt = self.nonhalting.len();
        let num_undecided = self.undecided.len();
        println!("{table} - remain: {remaining:0>4} | halt: {num_halt:0>8} | nonhalt: {num_nonhalt:0>8} | undecided: {num_undecided:0>8} - {message}");
    }
}

pub struct ExplorerStepInfo {
    pub node: ExplorerNode,
    pub decision: MachineDecision,
}

#[derive(Debug, Clone)]
pub enum MachineDecision {
    Halting,
    NonHalting,
    UndecidedStepLimit,
    UndecidedSpaceLimit,
    EmptyTransition(Vec<ExplorerNode>),
}

fn get_child_nodes(node: ExplorerNode) -> impl Iterator<Item = ExplorerNode> {
    get_child_tables_for_transition(node.table, node.tape.state, node.tape.read()).map(
        move |table| {
            let mut new_node = node.clone();
            new_node.table = table;
            new_node
        },
    )
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
            Transition(Symbol::Zero, Direction::Left, target_state),
            Transition(Symbol::Zero, Direction::Right, target_state),
            Transition(Symbol::One, Direction::Left, target_state),
            Transition(Symbol::One, Direction::Right, target_state),
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

#[cfg(test)]
mod test {
    use crate::{
        seed::{ExplorerNode, HaltReason, MachineDecision, SPACE_LIMIT, TIME_LIMIT},
        turing::{State, Symbol, Table},
    };

    use super::{get_child_tables_for_transition, Explorer, BB5_CHAMPION, STARTING_MACHINE};

    fn assert_contains(tables: &[Table], table: &str) {
        assert!(tables.contains(&Table::parse(table).unwrap()))
    }

    #[test]
    fn test_bb_champion() {
        let table = Table::parse(BB5_CHAMPION).unwrap();

        let explorer_state = &mut ExplorerNode::new(table);
        let halt_reason = explorer_state.run(None, None);

        assert_eq!(halt_reason, HaltReason::HaltState);
        assert_eq!(explorer_state.stats.steps_ran, TIME_LIMIT);
        assert_eq!(explorer_state.stats.space_used(), SPACE_LIMIT);
    }

    #[test]
    fn test_explorer() {
        let mut explorer = Explorer::new();

        let result = explorer.step().unwrap();
        assert!(matches!(
            result.decision,
            MachineDecision::EmptyTransition(_)
        ));
    }

    #[test]
    fn test_get_child_tables_1() {
        let table = Table::parse(STARTING_MACHINE).unwrap();
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
        let table = Table::parse("1RB1LC_1RC1RB_1RD0LE_1LA1LD_---0LA").unwrap();
        let tables: Vec<Table> =
            get_child_tables_for_transition(table, State::E, Symbol::Zero).collect();

        assert_contains(&tables, "1RB1LC_1RC1RB_1RD0LE_1LA1LD_0LZ0LA");
        assert_contains(&tables, "1RB1LC_1RC1RB_1RD0LE_1LA1LD_0RZ0LA");
        assert_contains(&tables, "1RB1LC_1RC1RB_1RD0LE_1LA1LD_1LZ0LA");
        assert_contains(&tables, "1RB1LC_1RC1RB_1RD0LE_1LA1LD_1RZ0LA");
    }
}
