use turing_beavers::seed::Explorer;

fn main() {
    let mut explorer = Explorer::new();

    // while let Some(result) = explorer.step(){
    // explorer.print_status(result);
    // }
    for i in 0..100_000 {
        let result = explorer.step();
        if i % 10_000 == 0 {
            explorer.print_status(result.unwrap());
        }
    }
    // explorer.print();
}
