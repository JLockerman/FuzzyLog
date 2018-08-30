#![allow(dead_code)]
#![allow(unused_variables)]

// Try multiple partitions per client
// add a optional return snapshot, which returns what the snapshotted OrderIndex is
// use to know when we can return observations and resnapshot for a partition

pub extern crate bincode;
extern crate crossbeam_utils;
extern crate fuzzy_log_client;
extern crate rand;
pub extern crate serde;

#[macro_use]
extern crate serde_derive;

#[cfg(test)]
extern crate env_logger;

#[cfg(test)]
extern crate fuzzy_log_server;

#[macro_use] extern crate structopt;

use std::fs::File;
use std::io::Write;
use std::time::Duration;

use fuzzy_log_client::fuzzy_log::log_handle::{LogHandle, LogBuilder, order};

use rand::Rng;
use structopt::StructOpt;

use or_set::OrSet;
use tester::Request;

pub mod or_set;
mod getter;
mod tester;

#[derive(StructOpt, Debug)]
struct Args {
    #[structopt(long = "log_addr")]
    log_addr: Vec<::std::net::SocketAddr>,
    #[structopt(long = "log_tails")] //TODO should be optional
    log_tails: Vec<::std::net::SocketAddr>,
    #[structopt(long = "expt_duration")]
    expt_duration: u64,
    #[structopt(long = "expt_range")]
    expt_range: u64,
    #[structopt(long = "server_id")]
    server_id: u64,
    #[structopt(long = "sync_duration")]
    sync_duration: u64,
    #[structopt(long = "window_sz")]
    window_sz: usize,
    #[structopt(long = "num_clients")]
    num_clients: u64,
    #[structopt(long = "clients_num", default_value = "0")]
    clients_num: u64,
    #[structopt(long = "num_rqs")]
    num_rqs: usize,
    #[structopt(long = "sample_interval")]
    sample_interval: u64,
    #[structopt(long = "writer")]
    writer: bool,
    #[structopt(long = "reader")]
    reader: bool,
    #[structopt(long = "low_throughput")]
    low_throughput: f64,
    #[structopt(long = "high_throughput")]
    high_throughput: f64,
    #[structopt(long = "spike_start")]
    spike_start: f64,
    #[structopt(long = "spike_duration")]
    spike_duration: f64,
}

fn main() {
    let args: Args = Args::from_args();
    if !args.log_tails.is_empty() {
        assert_eq!(
            args.log_tails.len(),
            args.log_addr.len(),
            "must have same number of head and tail log-servers"
        );
    }
    if unimplemented!() {
        do_put_experiment(&args)
    } else {
        do_get_experiment(&args)
    }
}

fn do_put_experiment(args: &Args) {
    let (ops, throughput_samples) = run_putter(args);
    validate_puts(args, &ops, &throughput_samples);
    write_put_output(args, &ops, &throughput_samples);
}

fn do_get_experiment(args: &Args) {
    let (throughput_samples, gets_per_snapshot) = run_getter(args);
    write_get_output(args, &throughput_samples, &gets_per_snapshot)
}

fn run_putter(args: &Args) -> (Vec<Request>, Vec<f64>) {
    let handle = args.build_log_handle();
    let mut inputs = args.gen_inputs();
    args.wait_signal();

    let mut set = OrSet::new(handle, args.my_chain());

    println!("Worker {} initialized", args.server_id);

    let throughput_samples = tester::do_run_fix_throughput(
        &mut set,
        &mut inputs,
        args.sample_interval,
        args.expt_duration,
        args.low_throughput,
        args.high_throughput,
        args.spike_start,
        args.spike_duration
    );
    (inputs, throughput_samples)
}

fn run_getter(args: &Args) -> (Vec<f64>, Vec<u64>) {
    let handle = args.build_log_handle();

    //TODO args.wait_signal()?

    let mut set = OrSet::new(handle, args.my_chain());
    getter::run(&mut set, args.sample_interval, args.expt_duration)
}

impl Args {
    fn chains(&self) -> impl Iterator<Item=order> {
        (1..(self.num_clients+1)).map(order::from)
    }

    fn my_chain(&self) -> order {
        (self.clients_num + 1).into()
    }

    fn build_log_handle(&self) -> LogHandle<[u8]> {
        self.log_handle_builder()
            .my_colors_chains(self.chains().collect())
            .reads_my_writes() //TODO
            .build()
    }

    fn log_handle_builder(&self) -> LogBuilder<[u8]> {
        match self.log_tails.len() {
            0 => LogHandle::unreplicated_with_servers(&self.log_addr),
            _ => LogHandle::replicated_with_servers(
                    self.log_addr.iter().cloned().zip(self.log_tails.iter().cloned())),
        }
    }

    fn gen_inputs(&self) -> Vec<tester::Request> {
        use rand::thread_rng;

        let mut seen_keys = vec![];
        thread_rng().gen_iter::<u64>().take(self.num_rqs).map(|i|{
            if i == 0 || thread_rng().gen() {
                let key = thread_rng().gen();
                seen_keys.push(key);
                tester::Op::Add(key)
            } else {
                let idx = thread_rng().gen::<usize>() % seen_keys.len();
                tester::Op::Rem(seen_keys[idx])
            }.into()
        }).collect()
    }

    fn wait_signal(&self) {
        let mut handle = self.log_handle_builder()
            .reads_my_writes()
            .my_colors_chains(Some(self.sig_color()).into_iter().collect())
            .build();
        handle.simpler_causal_append(&[], &mut []);
        let mut num_received = 0;
        while num_received < self.num_clients {
            handle.sync(|_, _, _| num_received += 1).unwrap();
        }
    }

    fn sig_color(&self) -> order {
        (self.num_clients as u64 + 1).into()
    }
}

fn validate_puts(args: &Args, reqs: &[tester::Request], throughput_samples: &[f64]) {
    let input_count = reqs.into_iter().take_while(|r| r.end_time.is_some()).count();
    let throughput_count: f64 = throughput_samples.into_iter().sum();
    println!("Input count: {}", input_count);
    println!("Throughput count: {}", throughput_count);
}

fn write_put_output(args: &Args, reqs: &[tester::Request], throughput_samples: &[f64]) {
    write_throughput(args, throughput_samples);
    write_latency(args, reqs);
}

fn write_get_output(args: &Args, throughput_samples: &[f64], gets_per_snapshot: &[u64]) {
    write_throughput(args, throughput_samples);
    write_gets_per_snapshot(args, gets_per_snapshot);
}

fn write_throughput(args: &Args, throughput_samples: &[f64]) {
    let mut result_file = File::create(format!("{}.txt", args.server_id))
        .unwrap();
    for v in throughput_samples {
        writeln!(&mut result_file, "{}", v).unwrap();
    }
}

fn write_latency(args: &Args, latencies: &[tester::Request]) {
    let mut latency_file = File::create(format!("{}_latency.txt", args.server_id))
        .unwrap();
    for rq in latencies {
        match (rq.start_time, rq.end_time) {
            (Some(start), Some(end)) =>{
                writeln!(&mut latency_file, "{}", millis(end - start)).unwrap();
            }
            _ => break
        }
    }
}

fn write_gets_per_snapshot(args: &Args, gets_per_snapshot: &[u64]) {
    let mut result_file = File::create("gets_per_snapshot.txt")
        .unwrap();
    for &v in gets_per_snapshot {
        writeln!(&mut result_file, "{}", v).unwrap();
    }
}

fn millis(dur: Duration) -> u64 {
    const MILLIS_PER_SEC: u64 = 1_000;
    dur.as_secs() as u64 * MILLIS_PER_SEC + dur.subsec_millis() as u64
}
