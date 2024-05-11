use std::collections::HashSet;

/// The two symbols which can be written to the tape (zeros, or ones)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Symbol {
    Zero,
    One,
}

/// The direction for the tape head to move on a step
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Left,
    Right,
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
#[derive(Debug)]
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

/// The tape, containing information needed to run a Turing machine.
/// The tape consists of an infinitely long one-dimensional tape. The tape head, starts initially
/// at index zero of this tape, and can move left or right. Cells to the left of the starting
/// position have a negative index, while cells to the right have a positive index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tape {
    /// A hashset containing the index of every [Symbol::One] written to the tape.
    pub ones: HashSet<isize>,
    /// The current state of the machine
    pub state: State,
    /// The location of the tape head
    pub index: isize,
}

impl Tape {
    /// Construct a new empty tape. The tape will start in [State::A] and it's tape head will
    /// be located at index 0.
    pub fn new() -> Tape {
        Tape {
            ones: HashSet::with_capacity(1024),
            index: 0,
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
        match value {
            Symbol::Zero => self.ones.remove(&self.index),
            Symbol::One => self.ones.insert(self.index),
        };
    }

    /// Return the specified [Symbol] on the cell at the tape head.
    pub fn read(&self) -> Symbol {
        if self.ones.contains(&self.index) {
            Symbol::One
        } else {
            Symbol::Zero
        }
    }

    /// Set the machine's [State]
    pub fn set_state(&mut self, state: State) {
        self.state = state;
    }
}
