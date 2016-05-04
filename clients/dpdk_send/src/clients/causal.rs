
use std::collections::HashMap;
use std::collections::hash_map::Entry::*;

use rand::{self, Rng};

use fuzzy_log::prelude::*;
use types::*;
use async_log::AsyncLogClient;

use self::NextState::*;
use self::Connectivity::*;

pub struct CausalClient {
    map: HashMap<u32, (u64, u32)>,
    local_chain: u32,
    foreign_chain: u32,
    foreign_dep: u32,
    connectivity: Connectivity,
    num_writes: u64,
    num_reads: u64,
    next_state: NextState,
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum Connectivity {
    Connected,
    Partitioned(u64),
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum NextState {
    Write,
    ReadForeign,
    ReadLocal,
}

impl CausalClient {
    pub fn new(local_chain: u32, foreign_chain: u32) -> Self {
        CausalClient {
            map: HashMap::new(),
            local_chain: local_chain,
            foreign_chain: foreign_chain,
            connectivity: Connectivity::Connected,
            num_writes: 0,
            num_reads: 0,
            foreign_dep: 0,
            next_state: Write,
        }
    }

    pub fn under_partition(local_chain: u32, foreign_chain: u32, partitioned_since: u64) -> Self {
        CausalClient {
            map: HashMap::new(),
            local_chain: local_chain,
            foreign_chain: foreign_chain,
            connectivity: Connectivity::Partitioned(partitioned_since),
            num_writes: 0,
            num_reads: 0,
            foreign_dep: 0,
            next_state: ReadLocal,
        }
    }

    pub fn num_writes(&self) -> u64 {
        self.num_writes
    }

    pub fn num_reads(&self) -> u64 {
        self.num_reads
    }

    pub fn reset_write_counter(&mut self) {
        self.num_writes = 0
    }

    pub fn reset_read_counter(&mut self) {
        self.num_reads = 0
    }

    pub fn assume_connected(&mut self) {
        self.connectivity = Connected
    }

    pub fn assume_partitioned(&mut self, partitioned_since: u64) {
        self.connectivity = Partitioned(partitioned_since)
    }
}

impl AsyncLogClient for CausalClient {
    type V = [(u64, u32, u32)];

    fn on_new_entry(&mut self, val: &Self::V, locs: &[OrderIndex], _: &[OrderIndex]) -> bool {
        trace!("new entry");
        self.num_reads += 1;
        for &(o, i) in locs {
            if o == self.foreign_chain.into() && i > self.foreign_dep.into() { self.foreign_dep = i.into() }
            if self.local_chain == o.into() || self.foreign_chain == o.into() {
                for &(t, k, v) in val {
                    match self.map.entry(k) {
                        Vacant(e) => { e.insert((t, v)); } 
                        Occupied(mut o) => {
                            let &mut (ref mut old_t, ref mut old_v) = o.get_mut();
                            if *old_t < t { *old_t = t; *old_v = v }
                        }
                    }
                }
                trace!("map len {}", self.map.len());
                //return i == self.last_written_entry.into()
                return false
            }
        }
        return false
    }

    fn on_start(&mut self, buf: MessageBuffer) -> MessageBuffer {
        trace!("client start");
        self.after_op(buf)
    }

    fn after_op(&mut self, mut buf: MessageBuffer) -> MessageBuffer {
        trace!("after op");
        match self.next_state {
            ReadForeign => {
                trace!("Fread");
                buf.as_read((self.foreign_chain.into(), 0.into()));
                self.next_state = ReadLocal;
            }
            ReadLocal => {
                trace!("Rread");
                buf.as_read((self.local_chain.into(), 0.into()));
                self.next_state = Write;
            }
            Write => {
                trace!("Write");
                let send_time = curr_time();
                let val1: u32 = rand::thread_rng().gen();
                let val2 = rand::thread_rng().gen();
                let val3 = rand::thread_rng().gen();
                buf.as_append(self.local_chain.into(),
                    &[(send_time, val1, val1), (send_time, val2, val2), (send_time, val3, val3)],
                    &[(self.foreign_chain.into(), self.foreign_dep.into())]);
                self.next_state = match self.connectivity {
                    Connected => ReadForeign,
                    Partitioned(..) => {
                        //TODO on timeout, check if healed
                        ReadLocal
                    }
                };
                self.num_writes += 1;
            }
        }
        buf
    }
}

#[cfg(test)]
pub mod test {

    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    use clients::test::local_run;
    use clients::causal::CausalClient;
    use async_log::AsyncLog;

    #[test]
    fn causal1() {
        let _ = ::env_logger::init();
        let client = Rc::new(RefCell::new(CausalClient::new(3, 5)));
        {
            local_run(1, |port| AsyncLog::new(client.clone() /*TODO*/, port), 100);
        }
        {
            let mut client = client.borrow_mut();
            println!("o: num writes {}", client.num_writes());
            client.reset_write_counter();
            client.assume_partitioned(0)
        }
        {
            local_run(1, |port| AsyncLog::new(client.clone() /*TODO*/, port), 100);
        }
        {
            let mut client = client.borrow_mut();
            println!("p: num writes {}", client.num_writes());
            client.reset_write_counter();
            client.assume_connected()
        }
        {
            local_run(1, |port| AsyncLog::new(client.clone() /*TODO*/, port), 100);
        }
        {
            let client = client.borrow();
            println!("r: num writes {}", client.num_writes());
        }
    }

    #[test]
    fn causal2() {
        let _ = ::env_logger::init();
        let client1 = Rc::new(RefCell::new(AsyncLog::new(CausalClient::new(3, 5), 1)));
        let client2 = Rc::new(RefCell::new(AsyncLog::new(CausalClient::new(5, 3), 2)));
        let count = Rc::new(Cell::new(0));
        {
            let constructor = |_| {
                count.set(count.get() + 1);
                if count.get() & 1 != 0 { client1.clone() }
                else { client2.clone() }
            };
            local_run(2, constructor, 100);
        }
        {
            let mut client1 = client1.borrow_mut();
            let mut client2 = client2.borrow_mut();
            println!("o2: {}, {}", client1.client.num_writes(), client2.client.num_writes());
            client1.client.reset_write_counter();
            client2.client.reset_write_counter();
            client1.client.assume_partitioned(0);
            client2.client.assume_partitioned(0)
        }
        {
            let constructor = |_| {
                count.set(count.get() + 1);
                if count.get() & 1 != 0 { client1.clone() }
                else { client2.clone() }
            };
            local_run(2, constructor, 100);
        }
        {
            let mut client1 = client1.borrow_mut();
            let mut client2 = client2.borrow_mut();
            println!("p2: {}, {}", client1.client.num_writes(), client2.client.num_writes());
            client1.client.reset_write_counter();
            client2.client.reset_write_counter();
            client1.client.assume_connected();
            client2.client.assume_connected()
        }
        {
            let constructor = |_| {
                count.set(count.get() + 1);
                if count.get() & 1 != 0 { client1.clone() }
                else { client2.clone() }
            };
            local_run(2, constructor, 100);
        }
        {
            let client1 = client1.borrow();
            let client2 = client2.borrow();
            println!("r2: {}, {}", client1.client.num_writes(), client2.client.num_writes());
        }
    }

    #[test]
    fn causal3() {
        let _ = ::env_logger::init();
        let client1 = Rc::new(RefCell::new(AsyncLog::new(CausalClient::new(3, 5), 1)));
        let client2 = Rc::new(RefCell::new(AsyncLog::new(CausalClient::new(5, 3), 2)));
        let count = Rc::new(Cell::new(0));
        {
            let constructor = |_| {
                count.set(count.get() + 1);
                if count.get() & 1 != 0 { client1.clone() }
                else { client2.clone() }
            };
            local_run(2, constructor, 100);
        }
        {
            let mut client1 = client1.borrow_mut();
            let mut client2 = client2.borrow_mut();
            println!("o3: {}, {}", client1.client.num_writes() + client2.client.num_writes(),
                client1.client.num_reads() + client2.client.num_reads());
            client1.client.reset_write_counter();
            client2.client.reset_write_counter();
            client1.client.reset_read_counter();
            client2.client.reset_read_counter();
            client1.client.assume_partitioned(0);
            client2.client.assume_partitioned(0)
        }
        {
            let constructor = |_| {
                count.set(count.get() + 1);
                if count.get() & 1 != 0 { client1.clone() }
                else { client2.clone() }
            };
            local_run(2, constructor, 100);
        }
        {
            let mut client1 = client1.borrow_mut();
            let mut client2 = client2.borrow_mut();
            println!("p3: {}, {}", client1.client.num_writes() + client2.client.num_writes(),
                client1.client.num_reads() + client2.client.num_reads());
            client1.client.reset_write_counter();
            client2.client.reset_write_counter();
            client1.client.reset_read_counter();
            client2.client.reset_read_counter();
            client1.client.assume_connected();
            client2.client.assume_connected()
        }
        {
            let constructor = |_| {
                count.set(count.get() + 1);
                if count.get() & 1 != 0 { client1.clone() }
                else { client2.clone() }
            };
            local_run(2, constructor, 100);
        }
        {
            let client1 = client1.borrow();
            let client2 = client2.borrow();
            println!("r3: {}, {}", client1.client.num_writes() + client2.client.num_writes(),
                client1.client.num_reads() + client2.client.num_reads());
        }
    }

}
