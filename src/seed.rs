use crate::turing::{Direction, MachineTable, State, Symbol, Tape, Transition};

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
pub fn step(tape: &mut Tape, table: &MachineTable) -> StepResult {
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

    pub fn space_used(&self) -> usize {
        1 + (self.max_index - self.min_index) as usize
    }

    pub fn get_total_steps(&self) -> usize {
        self.steps_ran
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingNode {
    pub table: MachineTable,
    pub tape: Tape,
    pub stats: RunStats,
}

impl PendingNode {
    pub fn new(table: MachineTable) -> PendingNode {
        PendingNode {
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
        //         added to the list of pending machines and the original is removed.
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
                let nodes =
                    get_child_tables_for_transition(self.table, self.tape.state, self.tape.read())
                        .collect();
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

#[derive(Debug, PartialEq, Eq)]
pub struct DecidedNode {
    pub table: MachineTable,
    pub decision: MachineDecision,
    pub stats: RunStats,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MachineDecision {
    Halting,
    NonHalting,
    UndecidedStepLimit,
    UndecidedSpaceLimit,
    EmptyTransition(Vec<MachineTable>),
}

/// Given a transition [Table], returns a set of Tables whose
/// The input Table is assumed to have the follwoing conditions:
/// 1. The table's visited states (that is, the states for which at least one of the two transitions is defined) should
///    all be lower than the unvisited states (where states are order with A as the lowest and E as the highest)
/// 2. The table has at least one empty [Action]. In particular, the Action returned by table.get(state, symbol) should
///    be empty.
/// 3. `state` is the lowest unvisited state in the table.
fn get_child_tables_for_transition(
    table: MachineTable,
    state: State,
    symbol: Symbol,
) -> impl Iterator<Item = MachineTable> {
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
fn get_target_states(table: &MachineTable) -> Vec<State> {
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
            HaltReason, MachineDecision, PendingNode, BB5_SPACE, BB5_STEPS, SPACE_LIMIT, TIME_LIMIT,
        },
        turing::{MachineTable, State, Symbol},
    };

    use super::{get_child_tables_for_transition, BB5_CHAMPION, STARTING_MACHINE};

    fn assert_contains(tables: &[MachineTable], table: &str) {
        assert!(tables.contains(&MachineTable::from_str(table).unwrap()))
    }

    #[test]
    fn test_bb_champion() {
        let table = MachineTable::from_str(BB5_CHAMPION).unwrap();

        let node = &mut PendingNode::new(table);
        let halt_reason = node.run(Some(TIME_LIMIT), Some(SPACE_LIMIT));

        assert_eq!(halt_reason, HaltReason::HaltState);
        assert_eq!(node.stats.steps_ran, BB5_STEPS);
        assert_eq!(node.stats.space_used(), BB5_SPACE);
    }

    #[test]
    fn test_get_child_tables_1() {
        let table = MachineTable::from_str(STARTING_MACHINE).unwrap();
        let tables: Vec<MachineTable> =
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
    fn test_decide_node_empty_transition() {
        let table = MachineTable::from_str("1RB---_------_------_------_------").unwrap();
        let mut node = PendingNode::new(table);
        let node = node.decide();
        assert!(matches!(node.decision, MachineDecision::EmptyTransition(_)));
        let machines = if let MachineDecision::EmptyTransition(machines) = node.decision {
            machines
        } else {
            unreachable!()
        };
        println!("{:?}", machines);
        assert_eq!(machines.len(), 8);
        assert_contains(&machines, "1RB---_0LA---_------_------_------");
        assert_contains(&machines, "1RB---_0RA---_------_------_------");
        assert_contains(&machines, "1RB---_1LA---_------_------_------");
        assert_contains(&machines, "1RB---_1RA---_------_------_------");
        assert_contains(&machines, "1RB---_0LB---_------_------_------");
        assert_contains(&machines, "1RB---_0RB---_------_------_------");
        assert_contains(&machines, "1RB---_1LB---_------_------_------");
        assert_contains(&machines, "1RB---_1RB---_------_------_------");
    }

    #[test]
    fn test_decide_node_empty_transition2() {
        let table = MachineTable::from_str("1RB---_1LA---_------_------_------").unwrap();
        let mut node = PendingNode::new(table);
        let node = node.decide();
        assert!(matches!(node.decision, MachineDecision::EmptyTransition(_)));
        let machines = if let MachineDecision::EmptyTransition(machines) = node.decision {
            machines
        } else {
            unreachable!()
        };
        println!("{:?}", machines);
        assert_eq!(machines.len(), 12);
        assert_contains(&machines, "1RB0RA_1LA---_------_------_------");
        assert_contains(&machines, "1RB0LA_1LA---_------_------_------");
        assert_contains(&machines, "1RB1RA_1LA---_------_------_------");
        assert_contains(&machines, "1RB1LA_1LA---_------_------_------");

        assert_contains(&machines, "1RB0RB_1LA---_------_------_------");
        assert_contains(&machines, "1RB0LB_1LA---_------_------_------");
        assert_contains(&machines, "1RB1RB_1LA---_------_------_------");
        assert_contains(&machines, "1RB1LB_1LA---_------_------_------");

        assert_contains(&machines, "1RB0RC_1LA---_------_------_------");
        assert_contains(&machines, "1RB0LC_1LA---_------_------_------");
        assert_contains(&machines, "1RB1RC_1LA---_------_------_------");
        assert_contains(&machines, "1RB1LC_1LA---_------_------_------");
    }

    #[test]
    fn test_decide_node_empty_transition4() {
        let table = MachineTable::from_str("1RB1RB_1LA---_------_------_------").unwrap();
        let mut node = PendingNode::new(table);
        let node = node.decide();
        assert!(matches!(node.decision, MachineDecision::EmptyTransition(_)));
    }

    #[test]
    fn test_decide_node_empty_transition3() {
        let table = MachineTable::from_str("1RB1RB_------_------_------_------").unwrap();
        let mut node = PendingNode::new(table);
        let node = node.decide();
        assert!(matches!(node.decision, MachineDecision::EmptyTransition(_)));
        let machines = if let MachineDecision::EmptyTransition(machines) = node.decision {
            machines
        } else {
            unreachable!()
        };
        println!("{:?}", machines);
        assert_eq!(machines.len(), 8);
        assert_contains(&machines, "1RB1RB_0LA---_------_------_------");
        assert_contains(&machines, "1RB1RB_0RA---_------_------_------");
        assert_contains(&machines, "1RB1RB_1LA---_------_------_------");
        assert_contains(&machines, "1RB1RB_1RA---_------_------_------");

        assert_contains(&machines, "1RB1RB_0LB---_------_------_------");
        assert_contains(&machines, "1RB1RB_0RB---_------_------_------");
        assert_contains(&machines, "1RB1RB_1LB---_------_------_------");
        assert_contains(&machines, "1RB1RB_1RB---_------_------_------");
    }

    #[test]
    fn test_get_child_tables_2() {
        let table = MachineTable::from_str("1RB1LC_1RC1RB_1RD0LE_1LA1LD_---0LA").unwrap();
        let tables: Vec<MachineTable> =
            get_child_tables_for_transition(table, State::E, Symbol::Zero).collect();

        assert_contains(&tables, "1RB1LC_1RC1RB_1RD0LE_1LA1LD_0LZ0LA");
        assert_contains(&tables, "1RB1LC_1RC1RB_1RD0LE_1LA1LD_0RZ0LA");
        assert_contains(&tables, "1RB1LC_1RC1RB_1RD0LE_1LA1LD_1LZ0LA");
        assert_contains(&tables, "1RB1LC_1RC1RB_1RD0LE_1LA1LD_1RZ0LA");
    }

    #[test]
    fn test_time_limit() {
        let table = MachineTable::from_str("1RB0LC_0LA0LD_1LA---_0RE0RD_0LD---").unwrap();
        let mut node = PendingNode::new(table);
        let reason = node.run(Some(TIME_LIMIT), None);
        assert_eq!(reason, HaltReason::ExceededStepLimit);
        // runs for one less step than actual for probably silly reasons.
        assert_eq!(node.stats.steps_ran, TIME_LIMIT);
    }

    #[test]
    fn test_space_limit_right() {
        let table = MachineTable::from_str("1RA1RA_------_------_------_------").unwrap();
        let mut node = PendingNode::new(table);
        let reason = node.run(None, Some(SPACE_LIMIT));
        assert_eq!(reason, HaltReason::ExceededSpaceLimit);
        // runs for one less step than actual for probably silly reasons.
        assert_eq!(node.stats.space_used(), SPACE_LIMIT);
        assert_eq!(node.stats.steps_ran, SPACE_LIMIT - 1);
    }

    #[test]
    fn test_space_limit_left() {
        let table = MachineTable::from_str("1LA1LA_------_------_------_------").unwrap();
        let mut node = PendingNode::new(table);
        let reason = node.run(None, Some(SPACE_LIMIT));
        assert_eq!(reason, HaltReason::ExceededSpaceLimit);
        // runs for one less step than actual for probably silly reasons.
        assert_eq!(node.stats.space_used(), SPACE_LIMIT);
        assert_eq!(node.stats.steps_ran, SPACE_LIMIT - 1);
    }

    #[test]
    fn test_simple_halt() {
        let table = MachineTable::from_str("1RB---_1RC---_1RD---_1RE---_1RZ---").unwrap();
        let mut node = PendingNode::new(table);
        let reason = node.run(None, None);
        assert_eq!(reason, HaltReason::HaltState);
        // Step 0:
        // 0 0 0 0 0 0
        // ^ A

        // Step 1:
        // 1 0 0 0 0 0
        //   ^ B

        // Step 2:
        // 1 1 0 0 0 0
        //     ^ C

        // Step 3:
        // 1 1 1 0 0 0
        //       ^ D

        // Step 4:
        // 1 1 1 1 1 0
        //         ^ E

        // Step 5:
        // 1 1 1 1 1 0
        //           ^ Z
        assert_eq!(node.stats.space_used(), 6);
        assert_eq!(node.stats.steps_ran, 5);
    }

    #[test]
    fn test_decide_empty_transitions() {
        let table = MachineTable::from_str("1RB---_1RC---_1RD---_1RE---_1LD---").unwrap();
        let mut node = PendingNode::new(table);
        // Step 0:
        // 0 0 0 0 0
        // ^ A

        // Step 1:
        // 1 0 0 0 0
        //   ^ B

        // Step 2:
        // 1 1 0 0 0
        //     ^ C

        // Step 3:
        // 1 1 1 0 0
        //       ^ D

        // Step 4:
        // 1 1 1 1 1
        //         ^ E

        // Step 5:
        // 1 1 1 1 1
        //       ^ D (no transition for D1)

        let reason = node.decide();

        match reason.decision {
            MachineDecision::EmptyTransition(machines) => {
                assert!(machines.len() > 0);
            }
            _ => unreachable!(),
        };
        assert_eq!(5, reason.stats.space_used());
        assert_eq!(5, reason.stats.get_total_steps());
    }
}
