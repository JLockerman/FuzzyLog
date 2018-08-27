
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use or_set::OrSet;

use fuzzy_log_client::{
    hash::{IdHashMap, IdSet},
    fuzzy_log::log_handle::Uuid,
};


struct Tester {
    set: OrSet,
    num_elapsed: usize,
    window_sz: usize,
}

pub struct Request {
    pub start_time: Option<Instant>,
    pub end_time: Option<Instant>,
    pub op: Op,
}

pub enum Op {
    Add(u64),
    Rem(u64),
}

impl From<Op> for Request {
    fn from(op: Op) -> Self {
        Request {
            start_time: None,
            end_time: None,
            op,
        }
    }
}

pub(crate) fn do_run_fix_throughput(
    set: &mut OrSet,
    requests: &mut [Request],

    interval: u64,
    duration: u64,

    low_throughput: f64,
    high_throughput: f64,
    spike_start: f64,
    spike_duration: f64,
) -> Vec<f64> {
    let quit = &AtomicBool::new(false);
    let num_elapsed = &AtomicUsize::new(0);
    ::crossbeam_utils::thread::scope(move |scope| {
        scope.spawn(move || run_throughput(
            set,
            requests,
            low_throughput,
            high_throughput,
            spike_start,
            spike_duration,
            num_elapsed,
            quit)
        );

        do_measurement(interval, duration, num_elapsed)
    })
}

fn do_measurement(interval: u64, duration: u64, num_elapsed: &AtomicUsize) -> Vec<f64> {
    let mut samples = Vec::with_capacity((duration / interval) as usize);
    let mut prev = 0;
    let end_time = Instant::now() + Duration::from_secs(duration);
    while Instant::now() < end_time {
        ::std::thread::sleep(Duration::from_secs(interval));
        let cur = num_elapsed.load(Relaxed);
        samples.push((cur - prev) as f64 / interval as f64);
        prev = cur;
    }
    samples
}

fn run_throughput(
    set: &mut OrSet,
    requests: &mut [Request],
    low_throughput: f64,
    high_throughput: f64,
    spike_start: f64,
    spike_duration: f64,
    num_elapsed: &AtomicUsize,
    quit: &AtomicBool,
) -> usize {

    let mut request_map = IdHashMap::default();

    const S_TO_NS: f64 = 1_000_000_000.0;
    let spike_start = Duration::new(
        spike_start.trunc() as u64,
        (spike_start.fract() * S_TO_NS) as u32,
    );
    let spike_end = spike_start + Duration::new(
        spike_duration.trunc() as u64,
        (spike_duration.fract() * S_TO_NS) as u32,
    );

    let low_diff = (1000.0 / low_throughput) as u64;
    let high_diff = (1000.0 / high_throughput) as u64;

    let mut start = Instant::now();
    let experiment_start = start;
    let (mut phase, mut num_pending) = (0, 0);

    let mut i = 0;
    let mut local_num_elapsed = 0;
    while !quit.load(Relaxed) {
        assert!(requests[i].start_time.is_none());
        loop {
            let now = Instant::now();
            let elapsed = now - start;

            let diff = match phase {
                0 | 2 => low_diff,
                1 => high_diff,
                _ => unreachable!("diff must be in {{0,1,2}}"),
            };

            if elapsed > Duration::from_millis(diff) {
                requests[i].start_time = Some(now);
                let id = issue_request(set, &mut requests[i].op);
                request_map.insert(id, i);
                num_pending += 1;
                start = now;
                let time_since_experiment_start = now - experiment_start;

                if phase == 0 && time_since_experiment_start >= spike_start {
                    phase = 1
                } else if phase == 1 && time_since_experiment_start >= spike_end {
                    phase = 2
                }
                break
            } else {
                let completed = try_get_pending_puts(set, requests, &mut request_map);
                num_pending -= completed;
                local_num_elapsed += completed;
                num_elapsed.store(local_num_elapsed, Relaxed);
            }
        };
        i += 1
    }

    assert!(i < requests.len());
    while num_pending != 0 {
        use_idle_cycles();
        let completed = try_get_pending_puts(set, requests, &mut request_map);
        num_pending -= completed;
        local_num_elapsed += completed;
        num_elapsed.store(local_num_elapsed, Relaxed);
    }

    local_num_elapsed
}

fn issue_request(set: &mut OrSet, request: &mut Op) -> Uuid {
    match request {
        Op::Add(val) => set.async_add(*val),
        Op::Rem(val) => set.async_remove(*val),
    }
}

fn try_get_pending_puts(
    set: &mut OrSet,
    requests: &mut [Request],
    request_map: &mut IdHashMap<Uuid, usize>,
) -> usize {
    let done_rqs = wait_put_requests(set, request_map);
    let end_time = Instant::now();
    done_rqs.into_iter().map(|rq| {
        assert!(requests[rq].end_time.is_none());
        requests[rq].end_time = Some(end_time);
    }).count()
}

fn use_idle_cycles() {
    ::std::thread::yield_now()
}

fn wait_put_requests(
    set: &mut OrSet,
    request_map: &mut IdHashMap<Uuid, usize>
) -> impl IntoIterator<Item=usize> {
    let mut done_set = IdSet::default();
    while let Some(id) = set.try_wait_for_op_finished() {
        let req = request_map.remove(&id);
        done_set.insert(req.unwrap());
    }
    done_set
}
