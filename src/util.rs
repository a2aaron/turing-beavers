use std::time::Instant;

pub struct ProgressIndicator {
    current: usize,
    total: usize,
    now: Instant,
}

impl ProgressIndicator {
    pub fn new(total: usize) -> ProgressIndicator {
        ProgressIndicator {
            current: 0,
            total,
            now: Instant::now(),
        }
    }

    pub fn tick(&mut self) {
        self.current += 1;
        self.current = self.current.min(self.total);

        if self.now.elapsed().as_secs() >= 5 {
            self.print();
            self.now = Instant::now();
        }
    }

    pub fn print(&self) {
        let percent = 100.0 * self.current as f32 / self.total as f32;
        println!("{}/{} ({:.2}%)", self.current, self.total, percent);
    }
}
