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
pub enum Transition {
    L0A,
    R0A,
    L1A,
    R1A,
    L0B,
    R0B,
    L1B,
    R1B,
    L0C,
    R0C,
    L1C,
    R1C,
    L0D,
    R0D,
    L1D,
    R1D,
    L0E,
    R0E,
    L1E,
    R1E,
    L0Z,
    R0Z,
    L1Z,
    R1Z,
}

impl From<Transition> for (Symbol, Direction, State) {
    fn from(value: Transition) -> Self {
        match value {
            Transition::L0A => (Symbol::Zero, Direction::Left, State::A),
            Transition::R0A => (Symbol::Zero, Direction::Right, State::A),
            Transition::L1A => (Symbol::One, Direction::Left, State::A),
            Transition::R1A => (Symbol::One, Direction::Right, State::A),
            Transition::L0B => (Symbol::Zero, Direction::Left, State::B),
            Transition::R0B => (Symbol::Zero, Direction::Right, State::B),
            Transition::L1B => (Symbol::One, Direction::Left, State::B),
            Transition::R1B => (Symbol::One, Direction::Right, State::B),
            Transition::L0C => (Symbol::Zero, Direction::Left, State::C),
            Transition::R0C => (Symbol::Zero, Direction::Right, State::C),
            Transition::L1C => (Symbol::One, Direction::Left, State::C),
            Transition::R1C => (Symbol::One, Direction::Right, State::C),
            Transition::L0D => (Symbol::Zero, Direction::Left, State::D),
            Transition::R0D => (Symbol::Zero, Direction::Right, State::D),
            Transition::L1D => (Symbol::One, Direction::Left, State::D),
            Transition::R1D => (Symbol::One, Direction::Right, State::D),
            Transition::L0E => (Symbol::Zero, Direction::Left, State::E),
            Transition::R0E => (Symbol::Zero, Direction::Right, State::E),
            Transition::L1E => (Symbol::One, Direction::Left, State::E),
            Transition::R1E => (Symbol::One, Direction::Right, State::E),
            Transition::L0Z => (Symbol::Zero, Direction::Left, State::Halt),
            Transition::R0Z => (Symbol::Zero, Direction::Right, State::Halt),
            Transition::L1Z => (Symbol::One, Direction::Left, State::Halt),
            Transition::R1Z => (Symbol::One, Direction::Right, State::Halt),
        }
    }
}

impl From<(Symbol, Direction, State)> for Transition {
    fn from((symbol, direction, state): (Symbol, Direction, State)) -> Self {
        match (direction, symbol, state) {
            (Direction::Left, Symbol::Zero, State::A) => Transition::L0A,
            (Direction::Left, Symbol::Zero, State::B) => Transition::L0B,
            (Direction::Left, Symbol::Zero, State::C) => Transition::L0C,
            (Direction::Left, Symbol::Zero, State::D) => Transition::L0D,
            (Direction::Left, Symbol::Zero, State::E) => Transition::L0E,
            (Direction::Left, Symbol::Zero, State::Halt) => Transition::L0Z,
            (Direction::Left, Symbol::One, State::A) => Transition::L1A,
            (Direction::Left, Symbol::One, State::B) => Transition::L1B,
            (Direction::Left, Symbol::One, State::C) => Transition::L1C,
            (Direction::Left, Symbol::One, State::D) => Transition::L1D,
            (Direction::Left, Symbol::One, State::E) => Transition::L1E,
            (Direction::Left, Symbol::One, State::Halt) => Transition::L1Z,
            (Direction::Right, Symbol::Zero, State::A) => Transition::R0A,
            (Direction::Right, Symbol::Zero, State::B) => Transition::R0B,
            (Direction::Right, Symbol::Zero, State::C) => Transition::R0C,
            (Direction::Right, Symbol::Zero, State::D) => Transition::R0D,
            (Direction::Right, Symbol::Zero, State::E) => Transition::R0E,
            (Direction::Right, Symbol::Zero, State::Halt) => Transition::R0Z,
            (Direction::Right, Symbol::One, State::A) => Transition::R1A,
            (Direction::Right, Symbol::One, State::B) => Transition::R1B,
            (Direction::Right, Symbol::One, State::C) => Transition::R1C,
            (Direction::Right, Symbol::One, State::D) => Transition::R1D,
            (Direction::Right, Symbol::One, State::E) => Transition::R1E,
            (Direction::Right, Symbol::One, State::Halt) => Transition::R1Z,
        }
    }
}

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

            Ok(Some((cell, direction, state).into()))
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
                let (symbol, direction, state) = (*action).into();
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

// The lenght of the tape, in the actual cells we want to simulate. In this case, we limit it to double
// the SPACE_LIMIT, so that the machine can go SPACE_LIMIT cells in either direciton (we add one for
// the starting index. Note: I'm not sure if this is required it is?)
const TAPE_LOGICAL_LENGTH: usize = 2 * SPACE_LIMIT;
const TAPE_PHYSICAL_LENGTH: usize = 1 + (TAPE_LOGICAL_LENGTH / (std::mem::size_of::<u8>() * 8));
/// The tape, containing information needed to run a Turing machine.
/// The tape consists of an infinitely long one-dimensional tape. The tape head, starts initially
/// at index zero of this tape, and can move left or right. Cells to the left of the starting
/// position have a negative index, while cells to the right have a positive index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tape {
    // The tape, which is represented as a long bitvec. The tape is set such that it has at least
    // TAPE_LOGICAL_LENGTH bits, and each bit represents a cell on the tape. Since there's only
    // two symbols, we can just have this map to 0 and 1.
    tape: Box<[u8; TAPE_PHYSICAL_LENGTH]>,
    /// The current state of the machine
    pub state: State,
    // The location of the tape head. This is a logical index, not a physical index
    index: usize,
}

impl Tape {
    /// Construct a new empty tape. The tape will start in [State::A] and it's tape head will
    /// be located at index 0.
    pub fn new() -> Tape {
        Tape {
            tape: [0; TAPE_PHYSICAL_LENGTH].into(),
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
        let major_index = self.index / 8;
        let minor_index = self.index % 8;

        let mut cell_8 = self.tape[major_index];
        let mask = 1 << minor_index;
        cell_8 = match value {
            Symbol::Zero => cell_8 & !mask,
            Symbol::One => cell_8 | mask,
        };
        self.tape[major_index] = cell_8;
    }

    /// Return the specified [Symbol] on the cell at the tape head.
    pub fn read(&self) -> Symbol {
        let major_index = self.index / 8;
        let minor_index = self.index % 8;

        let cell_8 = self.tape[major_index];
        let bit = cell_8 & (1 << minor_index);
        if bit == 0 {
            Symbol::Zero
        } else {
            Symbol::One
        }
    }

    /// Set the machine's [State]
    pub fn set_state(&mut self, state: State) {
        self.state = state;
    }

    pub fn index(&self) -> isize {
        self.index as isize - SPACE_LIMIT as isize
    }
}

#[cfg(test)]
mod test {
    use crate::{
        seed::SPACE_LIMIT,
        turing::{Direction, Symbol, Tape},
    };

    use super::{State, TAPE_LOGICAL_LENGTH};

    pub struct SimpleTape {
        tape: [Symbol; TAPE_LOGICAL_LENGTH],
        pub state: State,
        index: usize,
    }

    impl SimpleTape {
        /// Construct a new empty tape. The tape will start in [State::A] and it's tape head will
        /// be located at index 0.
        pub fn new() -> SimpleTape {
            SimpleTape {
                tape: [Symbol::Zero; TAPE_LOGICAL_LENGTH],
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

    #[test]
    fn test_read_and_write() {
        let mut tape = Tape::new();

        assert_eq!(tape.read(), Symbol::Zero);

        tape.write(Symbol::Zero);
        assert_eq!(tape.read(), Symbol::Zero);

        tape.write(Symbol::One);
        assert_eq!(tape.read(), Symbol::One);

        tape.shift(Direction::Left);
        assert_eq!(tape.read(), Symbol::Zero);

        tape.shift(Direction::Left);
        assert_eq!(tape.read(), Symbol::Zero);

        tape.write(Symbol::One);
        assert_eq!(tape.read(), Symbol::One);

        tape.shift(Direction::Right);
        assert_eq!(tape.read(), Symbol::Zero);

        tape.shift(Direction::Right);
        assert_eq!(tape.read(), Symbol::One);
    }

    #[test]
    fn test_fuzz() {
        let mut tape = Tape::new();
        let mut simple_tape = SimpleTape::new();

        for _ in 0..1000 {
            match rand::random::<u8>() % 4 {
                0 => {
                    tape.shift(Direction::Left);
                    simple_tape.shift(Direction::Left);
                }
                1 => {
                    tape.shift(Direction::Right);
                    simple_tape.shift(Direction::Right);
                }
                2 => {
                    tape.write(Symbol::Zero);
                    simple_tape.write(Symbol::Zero);
                }
                3 => {
                    tape.write(Symbol::One);
                    simple_tape.write(Symbol::One);
                }
                _ => unreachable!(),
            }

            assert_eq!(tape.read(), simple_tape.read());
            assert_eq!(tape.state, simple_tape.state);
            assert_eq!(tape.index(), simple_tape.index());
        }
    }
}
