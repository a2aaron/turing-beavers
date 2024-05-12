use turing_beavers::seed::Explorer;

fn main() {
    let mut explorer = Explorer::new();

    for _ in 0..10 {
        explorer.print();
        explorer.step();
    }
    explorer.print();
}
