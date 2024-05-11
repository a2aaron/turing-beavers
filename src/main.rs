use turing_beavers::{
    seed::{step, StepResult},
    turing::{Table, Tape},
};

fn main() {
    let table = Table::parse("1LC0RA_---1LE_1RD0LB_1RA1RC_0LC1LD").unwrap();
    let mut tape = Tape::new();
    let mut steps = 0;
    loop {
        let result = step(&mut tape, &table);

        if result != StepResult::Continue {
            break;
        }
        steps += 1;
        if steps % 10_000_000 == 0 {
            println!("{steps}");
        }
    }
    println!("{}", steps);
}
