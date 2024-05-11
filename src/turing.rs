use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Symbol {
    Zero,
    One,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Left,
    Right,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    A,
    B,
    C,
    D,
    E,
    Halt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Transition(pub Symbol, pub Direction, pub State);

pub type Action = Option<Transition>;

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

            let states = [
                ('A', State::A),
                ('B', State::B),
                ('C', State::C),
                ('D', State::D),
                ('E', State::E),
                ('Z', State::Halt),
            ];
            let states = HashMap::from(states);

            let cell = if cell == '0' {
                Symbol::Zero
            } else {
                Symbol::One
            };
            let direction = if direction == 'L' {
                Direction::Left
            } else {
                Direction::Right
            };
            let state = *states.get(&state).unwrap();
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tape {
    pub ones: HashSet<isize>,
    pub state: State,
    pub index: isize,
}

impl Tape {
    pub fn new() -> Tape {
        Tape {
            ones: HashSet::with_capacity(1024),
            index: 0,
            state: State::A,
        }
    }

    pub fn shift(&mut self, direction: Direction) {
        match direction {
            Direction::Left => self.index -= 1,
            Direction::Right => self.index += 1,
        }
    }

    pub fn write(&mut self, value: Symbol) {
        match value {
            Symbol::Zero => self.ones.remove(&self.index),
            Symbol::One => self.ones.insert(self.index),
        };
    }

    pub fn read(&self) -> Symbol {
        if self.ones.contains(&self.index) {
            Symbol::One
        } else {
            Symbol::Zero
        }
    }

    pub fn set_state(&mut self, state: State) {
        self.state = state;
    }
}
