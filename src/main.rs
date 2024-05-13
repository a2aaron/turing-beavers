use std::time::Instant;

use turing_beavers::seed::Explorer;

fn main() {
    let start = Instant::now();
    let mut explorer = Explorer::new();

    // while let Some(result) = explorer.step() {
    //     explorer.print_status(result);
    // }
    for i in 0..1_000 {
        let result = explorer.step();
        if i % 100 == 0 {
            explorer.print_status(result.unwrap());
        }
    }

    println!("elapsed: {:?}", start.elapsed());
}
