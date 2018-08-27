
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use or_set::OrSet;

pub fn run(set: &mut OrSet, interval: u64, duration: u64) -> (Vec<f64>, Vec<u64>) {
    let quit = &AtomicBool::new(false);
    let num_elapsed = &AtomicUsize::new(0);
    ::crossbeam_utils::thread::scope(move |scope| {
        let gets_per_snapshot = scope.spawn(move || do_run(set, num_elapsed, quit));
        let samples = do_measurement(interval, duration, num_elapsed, quit);
        (samples, gets_per_snapshot.join().unwrap())
    })
}

fn do_run(set: &mut OrSet, num_elapsed: &AtomicUsize, quit: &AtomicBool) -> Vec<u64> {
    let mut gets_per_snapshot = vec![];
    let mut local_num_elapsed = 0;
    while !quit.load(Relaxed) {
        let gets = set.get_remote();
        local_num_elapsed += gets;
        num_elapsed.store(local_num_elapsed, Relaxed);
        gets_per_snapshot.push(gets as u64);
    }
    gets_per_snapshot
}

fn do_measurement(interval: u64, duration: u64, num_elapsed: &AtomicUsize, quit: &AtomicBool)
-> Vec<f64> {
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
