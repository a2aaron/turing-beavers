use rand::{rngs::StdRng, Rng, SeedableRng};
use std::{
    fs::File,
    io::{self, BufReader, Read, Seek},
    path::Path,
    time::Instant,
};
use turing_beavers::{
    seed::PendingNode,
    turing::{Action, Direction, MachineTable, MachineTableStruct, State, Symbol, Transition},
};

struct Header {
    num_step_limit: u32,
    num_space_limit: u32,
    total_undecided: u32,
    lexiographically_sorted: bool,
}

fn get_machines(
    path: impl AsRef<Path>,
    num_machines: usize,
    seed: u64,
) -> io::Result<Vec<MachineTable>> {
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

    fn parse_machine(machine: [u8; 30]) -> io::Result<MachineTable> {
        let machine = MachineTableStruct {
            state_a_0: parse_transition(machine[0..3].try_into().unwrap())?,
            state_a_1: parse_transition(machine[3..6].try_into().unwrap())?,
            state_b_0: parse_transition(machine[6..9].try_into().unwrap())?,
            state_b_1: parse_transition(machine[9..12].try_into().unwrap())?,
            state_c_0: parse_transition(machine[12..15].try_into().unwrap())?,
            state_c_1: parse_transition(machine[15..18].try_into().unwrap())?,
            state_d_0: parse_transition(machine[18..21].try_into().unwrap())?,
            state_d_1: parse_transition(machine[21..24].try_into().unwrap())?,
            state_e_0: parse_transition(machine[24..27].try_into().unwrap())?,
            state_e_1: parse_transition(machine[27..30].try_into().unwrap())?,
        };
        Ok(MachineTable::from(machine))
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

    let mut machines = vec![];

    let thread_rng = &mut StdRng::seed_from_u64(seed);
    for _ in 0..num_machines {
        let index: u64 = thread_rng.gen_range(0..header.total_undecided).into();
        bytes.seek(io::SeekFrom::Start(index * 30 + 30))?;

        let mut machine = [0; 30];
        bytes.read_exact(&mut machine)?;

        let machine = parse_machine(machine)?;
        machines.push(machine);
    }

    Ok(machines)
}

fn random_bench() {
    const NUM_MACHINES: usize = 1000;
    const SEED: u64 = 413; // 413 is the most cryptographically secure seed. fully suitable for gambling games
                           // (source: https://rust-random.github.io/book/guide-seeding.html#a-simple-number)

    let machines = get_machines("undecided.bin", NUM_MACHINES, SEED).unwrap();
    let nodes = machines.iter().map(|&machine| PendingNode::new(machine));
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
