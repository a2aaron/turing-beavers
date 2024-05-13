use std::fmt::Display;

use crate::seed::SPACE_LIMIT;

/// The two symbols which can be written to the tape (zeros, or ones)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Symbol {
    Zero,
    One,
}

impl Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Symbol::Zero => write!(f, "0"),
            Symbol::One => write!(f, "1"),
        }
    }
}

/// The direction for the tape head to move on a step
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Left,
    Right,
}

impl Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::Left => write!(f, "L"),
            Direction::Right => write!(f, "R"),
        }
    }
}

/// The current state of the turing machine. Note that "Halt" is not considered to be a part of
/// the normal states (any machine transitioning to Halt instantly Halts)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    A,
    B,
    C,
    D,
    E,
    Halt,
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::A => write!(f, "A"),
            State::B => write!(f, "B"),
            State::C => write!(f, "C"),
            State::D => write!(f, "D"),
            State::E => write!(f, "E"),
            State::Halt => write!(f, "Z"),
        }
    }
}

/// A transition edge in the transition table
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Transition(pub Symbol, pub Direction, pub State);

pub type Action = Option<Transition>;

/// The transition table. Note that this is written to allow for ease with enumerating transition
/// tables. An [Action] which is None is used to represent that a particular transition is unusued or
/// unreachable (and hence could be replaced by any Transition without affecting the behavior of the
/// machine). When simulated on a [Tape], empty Actions are assumed to immediately halt without
/// performing any changes to the Tape.
///
/// Each field in this table is named after the [State] + [Symbol] combination that the transition rule
/// is for. For example, `state_d_1` is the transition rule for when the Turing machine reads a
/// [Symbol::One] in [State::D]
#[derive(Debug, Clone, Copy)]
pub struct Table {
    pub state_a_0: Action,
    pub state_a_1: Action,
    pub state_b_0: Action,
    pub state_b_1: Action,
    pub state_c_0: Action,
    pub state_c_1: Action,
    pub state_d_0: Action,
    pub state_d_1: Action,
    pub state_e_0: Action,
    pub state_e_1: Action,
}

impl Table {
    pub fn get(&self, state: State, symbol: Symbol) -> Action {
        match (state, symbol) {
            (State::A, Symbol::Zero) => self.state_a_0,
            (State::A, Symbol::One) => self.state_a_1,
            (State::B, Symbol::Zero) => self.state_b_0,
            (State::B, Symbol::One) => self.state_b_1,
            (State::C, Symbol::Zero) => self.state_c_0,
            (State::C, Symbol::One) => self.state_c_1,
            (State::D, Symbol::Zero) => self.state_d_0,
            (State::D, Symbol::One) => self.state_d_1,
            (State::E, Symbol::Zero) => self.state_e_0,
            (State::E, Symbol::One) => self.state_e_1,
            (State::Halt, _) => unreachable!("Cannot return Halt-state Action"),
        }
    }

    pub fn get_mut(&mut self, state: State, symbol: Symbol) -> &mut Action {
        match (state, symbol) {
            (State::A, Symbol::Zero) => &mut self.state_a_0,
            (State::A, Symbol::One) => &mut self.state_a_1,
            (State::B, Symbol::Zero) => &mut self.state_b_0,
            (State::B, Symbol::One) => &mut self.state_b_1,
            (State::C, Symbol::Zero) => &mut self.state_c_0,
            (State::C, Symbol::One) => &mut self.state_c_1,
            (State::D, Symbol::Zero) => &mut self.state_d_0,
            (State::D, Symbol::One) => &mut self.state_d_1,
            (State::E, Symbol::Zero) => &mut self.state_e_0,
            (State::E, Symbol::One) => &mut self.state_e_1,
            (State::Halt, _) => unreachable!("Cannot return Halt-state Action"),
        }
    }

    /// Parse a Turing machine string in the following format:
    /// `AAAaaa-BBBbbb-CCCccc-DDDddd-EEEeee`
    /// Each triple of letters correspond to an transition. For instance, "aaa" is the [Action]
    /// for the [State::A] + [Symbol::One] combination. A letter triple can either be `---`,
    /// indicating that this is an empty Action, or has the following three charactes:
    /// - First character: 0 or 1, representing the [Symbol] to write.
    /// - Second character: L or R, representing the [Direction] for the tape head to move
    /// - Third character: A, B, C, D, E, or Z, representing the [State] for the machine to
    /// transition to. "Z" is assumed to be [State::Halt].
    pub fn parse(str: &str) -> Result<Table, ()> {
        fn parse_action(action: &str) -> Result<Action, ()> {
            if action.len() != 3 {
                return Err(());
            }
            if action == "---" {
                return Ok(None);
            }

            let mut action = action.chars();
            let cell = action.next().unwrap();
            let direction = action.next().unwrap();
            let state = action.next().unwrap();

            let cell = match cell {
                '0' => Ok(Symbol::Zero),
                '1' => Ok(Symbol::One),
                _ => Err(()),
            }?;

            let direction = match direction {
                'L' => Ok(Direction::Left),
                'R' => Ok(Direction::Right),
                _ => Err(()),
            }?;

            let state = match state {
                'A' => Ok(State::A),
                'B' => Ok(State::B),
                'C' => Ok(State::C),
                'D' => Ok(State::D),
                'E' => Ok(State::E),
                'Z' => Ok(State::Halt),
                _ => Err(()),
            }?;

            Ok(Some(Transition(cell, direction, state)))
        }

        fn parse_group(group: &str) -> Result<(Action, Action), ()> {
            if group.len() != 6 {
                Err(())
            } else {
                let (zero_action, one_action) = group.split_at(3);
                let zero_action = parse_action(zero_action)?;
                let one_action = parse_action(one_action)?;
                Ok((zero_action, one_action))
            }
        }

        let groups = str.split("_").collect::<Vec<&str>>();
        if groups.len() != 5 {
            Err(())
        } else {
            let (state_a_0, state_a_1) = parse_group(groups[0])?;
            let (state_b_0, state_b_1) = parse_group(groups[1])?;
            let (state_c_0, state_c_1) = parse_group(groups[2])?;
            let (state_d_0, state_d_1) = parse_group(groups[3])?;
            let (state_e_0, state_e_1) = parse_group(groups[4])?;

            Ok(Table {
                state_a_0,
                state_a_1,
                state_b_0,
                state_b_1,
                state_c_0,
                state_c_1,
                state_d_0,
                state_d_1,
                state_e_0,
                state_e_1,
            })
        }
    }
}

impl Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn display_action(action: &Action) -> String {
            if let Some(action) = action {
                let symbol = action.0;
                let direction = action.1;
                let state = action.2;
                format!("{symbol}{direction}{state}")
            } else {
                "---".to_string()
            }
        }
        write!(
            f,
            "{}{}_{}{}_{}{}_{}{}_{}{}",
            display_action(&self.state_a_0),
            display_action(&self.state_a_1),
            display_action(&self.state_b_0),
            display_action(&self.state_b_1),
            display_action(&self.state_c_0),
            display_action(&self.state_c_1),
            display_action(&self.state_d_0),
            display_action(&self.state_d_1),
            display_action(&self.state_e_0),
            display_action(&self.state_e_1),
        )
    }
}

/// The tape, containing information needed to run a Turing machine.
/// The tape consists of an infinitely long one-dimensional tape. The tape head, starts initially
/// at index zero of this tape, and can move left or right. Cells to the left of the starting
/// position have a negative index, while cells to the right have a positive index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tape {
    // The tape, which is always 2 * SPACE_LIMIT + 1 cells large. This is fine because the seeding
    // rules will never simulate a Tape such that it hits more than SPACE_LIMIT cells (so we can just
    // make a Tape which is at least as long in both directions to guarentee we won't go over this.)
    tape: Box<[Symbol]>,
    /// The current state of the machine
    pub state: State,
    // The location of the tape head.
    index: usize,
}

impl Tape {
    /// Construct a new empty tape. The tape will start in [State::A] and it's tape head will
    /// be located at index 0.
    pub fn new() -> Tape {
        Tape {
            tape: [Symbol::Zero; 2 * SPACE_LIMIT + 1].into(),
            index: SPACE_LIMIT,
            state: State::A,
        }
    }

    /// Move the tape head one unit in the specified direction.
    pub fn shift(&mut self, direction: Direction) {
        match direction {
            Direction::Left => self.index -= 1,
            Direction::Right => self.index += 1,
        }
    }

    /// Write the specified [Symbol] to the cell at the tape head.
    pub fn write(&mut self, value: Symbol) {
        self.tape[self.index] = value;
    }

    /// Return the specified [Symbol] on the cell at the tape head.
    pub fn read(&self) -> Symbol {
        self.tape[self.index]
    }

    /// Set the machine's [State]
    pub fn set_state(&mut self, state: State) {
        self.state = state;
    }

    pub fn index(&self) -> isize {
        self.index as isize - SPACE_LIMIT as isize
    }
}
