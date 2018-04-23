
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

struct TesterRunner {

}

struct Tester {
    set: OrSet,
    num_elapsed: usize,
}

struct Request {
    start_time: Instant,
    end_time: Instant,
}

enum Req {
    Add(u64),
    Remove(u64),
}

impl Tester {
    pub fn do_run_fix_throughput(
        &mut self,
        requests: Vec<Request>,
        low_throughput: u64, // KHz
        high_throughput: u64, // KHz
        spike_start: u64, // elapsed time secs
        spike_duration: u64, // elapsed time secs
        quit: Arc<AtomicBool>,
    ) -> Vec<Instant> {
        let start_times = Vec::with_capacity(requests.len());
        let low_diff = 1000 / low_throughput;
        let high_diff = 1000 / high_throughput;

        let spike_start = Duration::from_secs(spike_start);
        let spike_end = spike_start + Duration::from_secs(spike_duration);

        let mut start = Instant::now();
        let experiment_start = start;
        let (mut phase, mut num_pending) = (0, 0);

        let mut i = 0;
        while !quit.load(Relaxed) {
            let rq_start = loop {
                let now = Instant::now();
                let elapsed = now - start;

                let diff = match phase {
                    0 | 2 => low_diff,
                    1 => high_diff,
                    _ => unreachable!(),
                };

                if elapsed > Duration::from_millis(diff) {
                    let start_time = now;
                    self.issue_request();
                    num_pending += 1;
                    start = now;
                    let time_since_experiment_start = now - experiment_start;

                    if phase == 0 && time_since_experiment_start >= spike_start {
                        phase = 1
                    } else if phase == 1 && time_since_experiment_start >= spike_end {
                        phase = 2
                    }
                    break start_time
                } else {
                    let completed = self.try_get_pending();
                    num_pending -= completed;
                    self.num_elapsed += completed;
                }
            };
            start_times.push(rq_start);
            i += 1
        }

        while num_pending != 0 {
            self.use_idle_cycles();
            let completed = self.try_get_pending();
            num_pending -= completed;
            self.num_elapsed += completed;
        }

        start_times
    }

    fn issue_request(&mut self, rq: Req) {
        match rq {
            Req::Add()

        }
    }

    fn use_idle_cycles(&mut self) {
        unimplemented!()
    }

    fn try_get_pending(&mut self) -> usize {
        unimplemented!()
    }
}
