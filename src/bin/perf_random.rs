use rand::{rngs::StdRng, Rng, SeedableRng};
use std::{
    fs::File,
    io::{self, BufReader, Read, Seek},
    path::Path,
    time::Instant,
};
use turing_beavers::{
    seed::UndecidedNode,
    turing::{Action, Direction, State, Symbol, Table, TableStruct, Transition},
};

struct Header {
    num_step_limit: u32,
    num_space_limit: u32,
    total_undecided: u32,
    lexiographically_sorted: bool,
}

fn get_machines(path: impl AsRef<Path>, num_machines: usize, seed: u64) -> io::Result<Vec<Table>> {
    fn parse_transition(transition: [u8; 3]) -> io::Result<Action> {
        let (symbol, direction, state) = match transition {
            [0, 0, 0] => return Ok(None),
            [0, 0, 1] => (Symbol::Zero, Direction::Left, State::A),
            [0, 0, 2] => (Symbol::Zero, Direction::Left, State::B),
            [0, 0, 3] => (Symbol::Zero, Direction::Left, State::C),
            [0, 0, 4] => (Symbol::Zero, Direction::Left, State::D),
            [0, 1, 5] => (Symbol::Zero, Direction::Left, State::E),
            [0, 1, 1] => (Symbol::Zero, Direction::Right, State::A),
            [0, 1, 2] => (Symbol::Zero, Direction::Right, State::B),
            [0, 1, 3] => (Symbol::Zero, Direction::Right, State::C),
            [0, 1, 4] => (Symbol::Zero, Direction::Right, State::D),
            [0, 0, 5] => (Symbol::Zero, Direction::Right, State::E),
            [1, 0, 1] => (Symbol::One, Direction::Left, State::A),
            [1, 0, 2] => (Symbol::One, Direction::Left, State::B),
            [1, 0, 3] => (Symbol::One, Direction::Left, State::C),
            [1, 0, 4] => (Symbol::One, Direction::Left, State::D),
            [1, 0, 5] => (Symbol::One, Direction::Left, State::E),
            [1, 1, 1] => (Symbol::One, Direction::Right, State::A),
            [1, 1, 2] => (Symbol::One, Direction::Right, State::B),
            [1, 1, 3] => (Symbol::One, Direction::Right, State::C),
            [1, 1, 4] => (Symbol::One, Direction::Right, State::D),
            [1, 1, 5] => (Symbol::One, Direction::Right, State::E),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("cannot parse transition: {:?}", transition),
                ))
            }
        };

        let transition = Transition::from_tuple(symbol, direction, state);
        let action = Some(transition);
        Ok(action)
    }

    fn parse_table(table: [u8; 30]) -> io::Result<Table> {
        let table = TableStruct {
            state_a_0: parse_transition(table[0..3].try_into().unwrap())?,
            state_a_1: parse_transition(table[3..6].try_into().unwrap())?,
            state_b_0: parse_transition(table[6..9].try_into().unwrap())?,
            state_b_1: parse_transition(table[9..12].try_into().unwrap())?,
            state_c_0: parse_transition(table[12..15].try_into().unwrap())?,
            state_c_1: parse_transition(table[15..18].try_into().unwrap())?,
            state_d_0: parse_transition(table[18..21].try_into().unwrap())?,
            state_d_1: parse_transition(table[21..24].try_into().unwrap())?,
            state_e_0: parse_transition(table[24..27].try_into().unwrap())?,
            state_e_1: parse_transition(table[27..30].try_into().unwrap())?,
        };
        Ok(Table::from(table))
    }

    let undecided = File::open(path)?;
    let mut bytes = BufReader::new(undecided);

    let mut num_step_limit = [0; 4];
    let mut num_space_limit = [0; 4];
    let mut total_undecided = [0; 4];
    let mut lexiographically_sorted = [0; 1];
    bytes.read_exact(&mut num_step_limit)?;
    bytes.read_exact(&mut num_space_limit)?;
    bytes.read_exact(&mut total_undecided)?;
    bytes.read_exact(&mut lexiographically_sorted)?;
    let header = Header {
        num_step_limit: u32::from_be_bytes(num_step_limit),
        num_space_limit: u32::from_be_bytes(num_space_limit),
        total_undecided: u32::from_be_bytes(total_undecided),
        lexiographically_sorted: lexiographically_sorted[0] != 0,
    };

    assert_eq!(header.num_step_limit, 14_322_029);
    assert_eq!(header.num_space_limit, 74_342_035);
    assert_eq!(header.total_undecided, 88_664_064);
    assert!(header.lexiographically_sorted);

    let mut tables = vec![];

    let thread_rng = &mut StdRng::seed_from_u64(seed);
    for _ in 0..num_machines {
        let index: u64 = thread_rng.gen_range(0..header.total_undecided).into();
        bytes.seek(io::SeekFrom::Start(index * 30 + 30))?;

        let mut table = [0; 30];
        bytes.read_exact(&mut table)?;

        let table = parse_table(table)?;
        tables.push(table);
    }

    Ok(tables)
}

fn random_bench() {
    const NUM_MACHINES: usize = 1000;
    const SEED: u64 = 413; // 413 is the most cryptographically secure seed. fully suitable for gambling games
                           // (source: https://rust-random.github.io/book/guide-seeding.html#a-simple-number)

    let tables = get_machines("undecided.bin", NUM_MACHINES, SEED).unwrap();
    let nodes = tables.iter().map(|&table| UndecidedNode::new(table));
    println!("Deciding {} machines", NUM_MACHINES);

    let now = Instant::now();

    for mut node in nodes {
        std::hint::black_box(node.decide());
    }

    let elapsed = now.elapsed().as_secs_f32();
    println!(
        "Decided {} machines in {:.2} seconds ({:.0}/s)",
        NUM_MACHINES,
        elapsed,
        (NUM_MACHINES as f32 / elapsed)
    );
}

fn main() {
    random_bench();
}
