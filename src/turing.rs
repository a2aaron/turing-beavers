use std::{fmt::Display, str::FromStr};

use crate::seed::SPACE_LIMIT;

// for choosing which implementation to use
impl Transition {
    pub fn into_tuple(&self) -> (Symbol, Direction, State) {
        self.into_tuple_2()
    }
}
pub type Table = Table2;

/// The two symbols which can be written to the tape (zeros, or ones)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Symbol {
    Zero = 0,
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
#[repr(u8)]
pub enum State {
    A = 0,
    B = 1,
    C = 2,
    D = 3,
    E = 4,
    Halt = 5,
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
/// Bit layout
/// 000 000 0 0
/// ^^^ ^^^ ^ ^-- direction (0 = Left, 1 = Right)
///  |   |  |
///  |   |  +---- symbol    (0 = Zero, 1 = One)
///  |   +------- state     (0 = A, 1 = B, 2 = C, 3 = D, 4 = E, 5 = Halt)
///  +----------- unused
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Transition {
    L0A = 0b000_000_0_0,
    R0A = 0b000_000_0_1,
    L1A = 0b000_000_1_0,
    R1A = 0b000_000_1_1,
    L0B = 0b000_001_0_0,
    R0B = 0b000_001_0_1,
    L1B = 0b000_001_1_0,
    R1B = 0b000_001_1_1,
    L0C = 0b000_010_0_0,
    R0C = 0b000_010_0_1,
    L1C = 0b000_010_1_0,
    R1C = 0b000_010_1_1,
    L0D = 0b000_011_0_0,
    R0D = 0b000_011_0_1,
    L1D = 0b000_011_1_0,
    R1D = 0b000_011_1_1,
    L0E = 0b000_100_0_0,
    R0E = 0b000_100_0_1,
    L1E = 0b000_100_1_0,
    R1E = 0b000_100_1_1,
    L0Z = 0b000_101_0_0,
    R0Z = 0b000_101_0_1,
    L1Z = 0b000_101_1_0,
    R1Z = 0b000_101_1_1,
}

impl Transition {
    pub fn into_tuple_1(&self) -> (Symbol, Direction, State) {
        match self {
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

    // TODO: This is actually slower than the giant match??
    fn into_tuple_2(&self) -> (Symbol, Direction, State) {
        let value = *self as u8;
        let direction = value & 0b000_000_0_1;
        let symbol = (value & 0b000_000_1_0) >> 1;
        let state = (value & 0b000_111_0_0) >> 2;

        let direction = if direction == 0 {
            Direction::Left
        } else {
            Direction::Right
        };

        let symbol = if symbol == 0 {
            Symbol::Zero
        } else {
            Symbol::One
        };

        let state = match state {
            0 => State::A,
            1 => State::B,
            2 => State::C,
            3 => State::D,
            4 => State::E,
            5 => State::Halt,
            _ => unreachable!("guarenteed by all possible values of the Transition enum"),
        };
        (symbol, direction, state)
    }

    pub fn from_tuple(symbol: Symbol, direction: Direction, state: State) -> Transition {
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
pub struct Table1 {
    state_a_0: Action,
    state_a_1: Action,
    state_b_0: Action,
    state_b_1: Action,
    state_c_0: Action,
    state_c_1: Action,
    state_d_0: Action,
    state_d_1: Action,
    state_e_0: Action,
    state_e_1: Action,
}

impl Table1 {
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

    /// Returns the number of transitions which are defined on the table.
    pub fn defined_transitions(&self) -> usize {
        self.state_a_0.is_some() as usize
            + self.state_a_1.is_some() as usize
            + self.state_b_0.is_some() as usize
            + self.state_b_1.is_some() as usize
            + self.state_c_0.is_some() as usize
            + self.state_c_1.is_some() as usize
            + self.state_d_0.is_some() as usize
            + self.state_d_1.is_some() as usize
            + self.state_e_0.is_some() as usize
            + self.state_e_1.is_some() as usize
    }

    /// Returns the number of states which are visited in this table. This will give how many
    /// states were visited since we only define a state transition as a machien is about to
    /// visit it.
    pub fn visited_states(&self) -> usize {
        let a_visited = self.state_a_0.is_some() || self.state_a_1.is_some();
        let b_visited = self.state_b_0.is_some() || self.state_b_1.is_some();
        let c_visited = self.state_c_0.is_some() || self.state_c_1.is_some();
        let d_visited = self.state_d_0.is_some() || self.state_d_1.is_some();
        let e_visited = self.state_e_0.is_some() || self.state_e_1.is_some();

        a_visited as usize
            + b_visited as usize
            + c_visited as usize
            + d_visited as usize
            + e_visited as usize
    }
}

impl FromStr for Table1 {
    type Err = ();
    /// Parse a Turing machine string in the following format:
    /// `AAAaaa-BBBbbb-CCCccc-DDDddd-EEEeee`
    /// Each triple of letters correspond to an transition. For instance, "aaa" is the [Action]
    /// for the [State::A] + [Symbol::One] combination. A letter triple can either be `---`,
    /// indicating that this is an empty Action, or has the following three charactes:
    /// - First character: 0 or 1, representing the [Symbol] to write.
    /// - Second character: L or R, representing the [Direction] for the tape head to move
    /// - Third character: A, B, C, D, E, or Z, representing the [State] for the machine to
    /// transition to. "Z" is assumed to be [State::Halt].
    fn from_str(str: &str) -> Result<Self, Self::Err> {
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

            Ok(Some(Transition::from_tuple(cell, direction, state)))
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

            Ok(Table1 {
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

impl Display for Table1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn display_action(action: &Action) -> String {
            if let Some(action) = action {
                let (symbol, direction, state) = action.into_tuple();
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

impl From<Table2> for Table1 {
    fn from(table: Table2) -> Self {
        Table1 {
            state_a_0: table.0[0],
            state_a_1: table.0[1],
            state_b_0: table.0[2],
            state_b_1: table.0[3],
            state_c_0: table.0[4],
            state_c_1: table.0[5],
            state_d_0: table.0[6],
            state_d_1: table.0[7],
            state_e_0: table.0[8],
            state_e_1: table.0[9],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Table2([Action; 10]);
impl Table2 {
    pub fn get(&self, state: State, symbol: Symbol) -> Action {
        let index = (state as usize) * 2 + symbol as usize;
        self.0[index]
    }

    pub fn get_mut(&mut self, state: State, symbol: Symbol) -> &mut Action {
        let index = (state as usize) * 2 + symbol as usize;
        &mut self.0[index]
    }

    /// Returns the number of transitions which are defined on the table.
    pub fn defined_transitions(&self) -> usize {
        self.0[0].is_some() as usize
            + self.0[1].is_some() as usize
            + self.0[2].is_some() as usize
            + self.0[3].is_some() as usize
            + self.0[4].is_some() as usize
            + self.0[5].is_some() as usize
            + self.0[6].is_some() as usize
            + self.0[7].is_some() as usize
            + self.0[8].is_some() as usize
            + self.0[9].is_some() as usize
    }

    /// Returns the number of states which are visited in this table. This will give how many
    /// states were visited since we only define a state transition as a machien is about to
    /// visit it.
    pub fn visited_states(&self) -> usize {
        let table = self.0;
        let a_visited = table[0].is_some() || table[1].is_some();
        let b_visited = table[2].is_some() || table[3].is_some();
        let c_visited = table[4].is_some() || table[5].is_some();
        let d_visited = table[6].is_some() || table[7].is_some();
        let e_visited = table[8].is_some() || table[9].is_some();

        a_visited as usize
            + b_visited as usize
            + c_visited as usize
            + d_visited as usize
            + e_visited as usize
    }
}

impl From<Table1> for Table2 {
    fn from(table: Table1) -> Self {
        Table2([
            table.state_a_0,
            table.state_a_1,
            table.state_b_0,
            table.state_b_1,
            table.state_c_0,
            table.state_c_1,
            table.state_d_0,
            table.state_d_1,
            table.state_e_0,
            table.state_e_1,
        ])
    }
}

impl FromStr for Table2 {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Table2::from(Table1::from_str(s)?))
    }
}

impl Display for Table2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Table1::fmt(&Table1::from(*self), f)
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
            // TODO: Explore effect of using vec![] instead of array literal
            // It seems like using vec![] is slightly faster?
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
    use std::str::FromStr;

    use crate::{
        seed::SPACE_LIMIT,
        turing::{Direction, Symbol, Table2, Tape},
    };

    use super::{State, Table1, Transition, TAPE_LOGICAL_LENGTH};

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

    #[test]
    fn test_into_tuple() {
        #[rustfmt::skip]
        let transitions = [
            (Transition::L0A, (Symbol::Zero, Direction::Left, State::A)),
            (Transition::R0A, (Symbol::Zero, Direction::Right, State::A)),
            (Transition::L1A, (Symbol::One, Direction::Left, State::A)),
            (Transition::R1A, (Symbol::One, Direction::Right, State::A)),
            (Transition::L0B, (Symbol::Zero, Direction::Left, State::B)),
            (Transition::R0B, (Symbol::Zero, Direction::Right, State::B)),
            (Transition::L1B, (Symbol::One, Direction::Left, State::B)),
            (Transition::R1B, (Symbol::One, Direction::Right, State::B)),
            (Transition::L0C, (Symbol::Zero, Direction::Left, State::C)),
            (Transition::R0C, (Symbol::Zero, Direction::Right, State::C)),
            (Transition::L1C, (Symbol::One, Direction::Left, State::C)),
            (Transition::R1C, (Symbol::One, Direction::Right, State::C)),
            (Transition::L0D, (Symbol::Zero, Direction::Left, State::D)),
            (Transition::R0D, (Symbol::Zero, Direction::Right, State::D)),
            (Transition::L1D, (Symbol::One, Direction::Left, State::D)),
            (Transition::R1D, (Symbol::One, Direction::Right, State::D)),
            (Transition::L0E, (Symbol::Zero, Direction::Left, State::E)),
            (Transition::R0E, (Symbol::Zero, Direction::Right, State::E)),
            (Transition::L1E, (Symbol::One, Direction::Left, State::E)),
            (Transition::R1E, (Symbol::One, Direction::Right, State::E)),
            (Transition::L0Z, (Symbol::Zero, Direction::Left, State::Halt)),
            (Transition::R0Z, (Symbol::Zero, Direction::Right, State::Halt)),
            (Transition::L1Z, (Symbol::One, Direction::Left, State::Halt)),
            (Transition::R1Z, (Symbol::One, Direction::Right, State::Halt)),
        ];
        for (transition, tuple) in transitions {
            assert_eq!(transition.into_tuple_1(), tuple);
            assert_eq!(transition.into_tuple_2(), tuple);
            assert_eq!(
                Transition::from_tuple(tuple.0, tuple.1, tuple.2),
                transition
            );
        }
    }

    #[test]
    fn test_table() {
        let s = "1RB1LC_1RC1RB_1RD0LE_1LA1LD_1RZ0LA";
        let table = Table1::from_str(s).unwrap();
        let table2 = Table2::from_str(s).unwrap();

        assert_eq!(table, Table1::from(table2));
        assert_eq!(Table2::from(table), table2);

        let state_symbols = [
            (State::A, Symbol::Zero),
            (State::A, Symbol::One),
            (State::B, Symbol::Zero),
            (State::B, Symbol::One),
            (State::C, Symbol::Zero),
            (State::C, Symbol::One),
            (State::D, Symbol::Zero),
            (State::D, Symbol::One),
            (State::E, Symbol::Zero),
            (State::E, Symbol::One),
        ];
        for (state, symbol) in state_symbols {
            assert_eq!(table.get(state, symbol), table2.get(state, symbol))
        }
    }

    #[test]
    fn test_table_2() {
        let s = "1RB---_1RC1RB_---0LE_1LA1LD_------";
        let table = Table1::from_str(s).unwrap();
        let table2 = Table2::from_str(s).unwrap();

        assert_eq!(table.visited_states(), table2.visited_states());
        assert_eq!(table.defined_transitions(), table2.defined_transitions());
    }
}
