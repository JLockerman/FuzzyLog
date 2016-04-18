
use std::collections::HashMap;

use fuzzy_log::prelude::*;
use types::*;
use types::ReceiveEvent::*;

use scheduler::{AsyncClient, NextActivity};
use scheduler::NextActivity::*;

use self::AsyncLogState::*;

pub trait AsyncLogClient {
    type V: ?Sized + Storeable;

    ///returns true if 'finished' for wmw
    fn on_new_entry(&mut self, &Self::V, &[OrderIndex], &[OrderIndex]) -> bool;

    fn on_start(&mut self, MessageBuffer) -> MessageBuffer;
    fn after_op(&mut self, MessageBuffer) -> MessageBuffer;
}

pub struct AsyncLog<C: AsyncLogClient> {
    local_horizon: HashMap<order, entry>,
    client: C,
    port: u16,
    last_send: MessageBuffer,
    nd_last_send: MessageBuffer,
    play_stack: Vec<(Box<C::V>, Vec<OrderIndex>, Vec<OrderIndex>)>,
    state: AsyncLogState,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
enum AsyncLogState {
    NoState,
    AfterWrite,
    AfterRead(OrderIndex, order),
    AfterWriteSearch(OrderIndex, order, Uuid),
}

impl<C> AsyncClient for AsyncLog<C>
where C: AsyncLogClient {
    fn gen_event(&mut self, buffer: MessageBuffer) -> NextActivity {
        let buffer = self.client.on_start(buffer);
        self.handle_new_buffer(buffer)
    }

    fn handle_event(&mut self, event: ReceiveEvent) -> NextActivity { //TODO handle wait
        let next_buffer = match (self.state, event) {
            (NoState, event)  => {
                println!("not expecting event");
                return self.gen_event(event.into_message_buffer())
            }

            (AfterWrite, Message(buffer)) => {
                println!("expecting write ack");
                self.try_finishing_write(buffer)
            }
            (AfterRead(oi, chain), Message(buffer)) =>  {
                println!("expecting read ack");
                self.try_finishing_read(buffer, oi, chain)
            }
            (AfterWriteSearch(loc, _, id), Message(buffer)) => {
                println!("expecting wtimeout read ack");
                self.try_finishing_write_search(buffer)
            }

            (AfterWrite, Timeout(buffer, port)) => {
                assert_eq!(self.port, port);
                println!("timeout during write");
                self.retry_write(buffer)
            }

            (AfterRead(loc, chain), Timeout(buffer, port)) | (AfterWriteSearch(loc, chain, _), Timeout(buffer, port)) => {
                println!("timeout during read");
                assert_eq!(self.port, port);
                self.retry_read(buffer)
            }
        };
        next_buffer.map_send(|buffer, _, _| self.handle_new_buffer(buffer))
    }

}

impl<C> AsyncLog<C>
where C: AsyncLogClient {

    pub fn new(client: C, port: u16) -> Self {
        AsyncLog {
            local_horizon: HashMap::new(),
            client: client,
            port: port,
            last_send: MessageBuffer { buffer: unsafe { alloc_mbuf() }},
            nd_last_send: MessageBuffer { buffer: unsafe { alloc_mbuf() }},
            play_stack: Vec::new(),
            state: NoState,
        }
    }

    fn handle_new_buffer(&mut self, mut buffer: MessageBuffer) -> NextActivity {
        self.state = match buffer.kind() {
            EntryKind::Data => AfterWrite,
            EntryKind::Multiput => AfterWrite,
            EntryKind::Read => {
                let chain = buffer.read_loc().0;
                let next_entry = self.local_horizon.get(&chain)
                    .cloned().unwrap_or(0.into()) + 1;
                buffer.set_read_entry(next_entry);
                assert_eq!((chain, next_entry), buffer.locs()[0]);
                assert!(next_entry > 0.into());
                AfterRead((chain, next_entry), chain)
            }
            _ => {
                self.state = NoState;
                //TODO differentiate between, done, put me back on the ready queue, and retry
                //unimplemented!();
                return Ready
            }
        };
        println!("now in state {:?}", self.state);
        Send(self.prepare_mbuf(buffer), self.port, 250000) //TODO RTT stuff
    }

    fn try_finishing_write(&mut self, buffer: MessageBuffer) -> NextActivity {
        if buffer.get_id() != self.last_send.get_id() {
            println!("wrong id got: {:?}, expected: {:?}", buffer.get_id(), self.last_send.get_id());
            return KeepWaiting
        }
        assert!(buffer.layout() == self.last_send.layout());
        //for &(o, e) in buffer.locs() {
            //let old_horizon = self.local_horizon.entry(o).or_insert(e);
            //if *old_horizon < e {
            //    *old_horizon = e;
            //}
        //}
        println!("finished write");
        return Send(self.client.after_op(buffer), 0, 0)
    }

    fn try_finishing_read(&mut self, mut buffer: MessageBuffer, (o, _): OrderIndex, chain: order) -> NextActivity {
        if !buffer.locs().contains(&self.last_send.locs()[0]) { //reads only contain one val
            if buffer.kind() != EntryKind::NoValue {
                println!("finished a chain!"); 
                return Ready //TODO gen-event? 
            }
            else if buffer.locs().iter().fold(true, |b, &(c, _)| b && o != c) {
                println!("invalid ack!");
                return KeepWaiting
            }
        }
        match buffer.kind() {
            EntryKind::NoValue => {
                assert!(self.play_stack.is_empty());
                println!("finished chain");
                Send(self.client.after_op(buffer), 0, 0)
            }
            EntryKind::ReadData /*| EntryKind::ReadMulti */ => {
                //TODO do we need to validate against the expected (o,i)?
                let finished_deps = {
                    let (_, _, deps) = buffer.val_locs_and_deps::<C::V>();
                    //self.local_horizon.entry(panic!());
                    self.finished_deps(deps)
                };
                //println!("finished deps? {}", finished_deps);
                if finished_deps {
                    {
                        let (val, locs, deps) = buffer.val_locs_and_deps();
                        for &(o, e) in locs {
                            let old_horizon = self.local_horizon.entry(o).or_insert(e);
                            if *old_horizon < e { *old_horizon = e; }
                        }
                        self.client.on_new_entry(val, locs, deps);
                    }
                    self.flush_finished_reads();
                    //if self.play_stack.is_empty() {
                    //    println!("finished reads");
                    //    return Some(self.client.after_op(buffer))
                    //} this only works if we have a know horizon to read to
                }
                else {
                    let (val, locs, deps) = buffer.val_locs_and_deps::<C::V>();
                    unsafe { self.play_stack.push((val.clone_box(): Box<C::V>, locs.to_vec(), deps.to_vec())) };
                }

                if !self.play_stack.is_empty() {
                    Send(self.play_next_dep(buffer, chain), 0, 0)
                }
                else {
                    let next_entry = self.last_read_in_chain(o) + 1u32.into();
                    self.state = AfterRead((chain, next_entry), chain);
                    buffer.as_read((chain, next_entry));
                    Send(buffer, 0, 0)
                }
                //TODO take snapshot of horizon first?
            }
            EntryKind::ReadMulti => panic!("unimplemendted"),
            _ => return KeepWaiting,
        }
    }

    fn play_next_dep(&mut self, mut buffer: MessageBuffer, chain: order) -> MessageBuffer {
        let next_loc = self.next_dep();
        self.state = AfterRead(next_loc, chain);
        buffer.as_read(next_loc);
        return buffer
    }

    fn try_finishing_write_search(&mut self, buffer: MessageBuffer) -> NextActivity {
        panic!("unimplemented")
    }

    fn retry_write(&mut self, buffer: MessageBuffer) -> NextActivity {
        panic!("unimplemented")
    }

    fn retry_read(&mut self, buffer: MessageBuffer) -> NextActivity {
        panic!("unimplemented")
    }

    fn flush_finished_reads(&mut self) {
        while self.play_stack.last().map_or(false, |&(_, _, ref deps)| self.finished_deps(&**deps)) { //TODO multiappends
            let (val, locs, deps) = self.play_stack.pop().unwrap();
            self.client.on_new_entry(&*val, &*locs, &*deps);
        }
    }

    fn next_dep(&self) -> OrderIndex {
        let &(_, _, ref deps) = self.play_stack.last().unwrap();
        for &(o, e) in deps {
            let last_read = self.last_read_in_chain(o);
            if last_read < e {
                return (o, last_read + 1u32.into());
            }
        }
        unreachable!()
    }

    fn finished_deps(&self, deps: &[OrderIndex]) -> bool {
        for &(o, e) in deps {
            if self.last_read_in_chain(o) < e {
                return false;
            }
        }
        true
    }

    fn prepare_mbuf(&mut self, buffer: MessageBuffer) -> MessageBuffer {
        unsafe {
            mbuf_set_src_port(buffer.buffer, self.port);
            copy_mbuf(self.last_send.buffer, buffer.buffer);
        }
        assert_eq!(buffer.get_id(), self.last_send.get_id());
        buffer //TODO RTT estimation
    }

    fn last_read_in_chain(&self, chain: order) -> entry {
        self.local_horizon.get(&chain).cloned().unwrap_or(0.into())
    }
}

///////////////////////////////////////

#[cfg(FALSE)]
impl () {
    pub fn multiappend(&mut self, columns: Vec<order>, data: &V, deps: Vec<OrderIndex>) {
        let columns: Vec<OrderIndex> = columns.into_iter().map(|i| (i, 0.into())).collect();
        self.store.multi_append(&columns[..], data, &deps[..]); //TODO error handling
        for &(column, _) in &*columns {
            let next_entry = self.horizon.get_horizon(column) + 1; //TODO
            self.horizon.update_horizon(column, next_entry); //TODO
        }
    }

    pub fn get_next_unseen(&mut self, column: order) -> Option<OrderIndex> {
        let index = self.local_horizon.get(&column).cloned().unwrap_or(0.into()) + 1;
        trace!("next unseen: {:?}", (column, index));
        let ent = self.store.get((column, index)).clone();
        let ent = match ent { Err(GetErr::NoValue) => return None, Ok(e) => e };
        self.play_deps(ent.dependencies());
        match ent.contents() {
            Multiput{data, uuid, columns, deps} => {
                //TODO
                trace!("Multiput {:?}", deps);
                self.read_multiput(column, data, uuid, columns);
            }
            Data(data, deps) => {
                trace!("Data {:?}", deps);
                self.upcalls.get(&column).map(|f| f(data.clone())); //TODO clone
            }
        }
        self.local_horizon.insert(column, index);
        Some((column, index))
    }

    fn read_multiput(&mut self, first_seen_column: order, data: &V, put_id: &Uuid,
        columns: &[OrderIndex]) {

        //XXX note multiserver validation happens at the store layer
        self.upcalls.get(&first_seen_column).map(|f| f(data)); //TODO clone

        for &(column, _) in columns { //TODO only relevent cols
            trace!("play multiput for col {:?}", column);
            self.play_until_multiput(column, put_id);
            self.upcalls.get(&column).map(|f| f(data)); //TODO clone
        }
    }

    fn play_until_multiput(&mut self, column: order, put_id: &Uuid) {
        //TODO instead, just mark all interesting columns not in the
        //     transaction as stale, and only read the interesting
        //     columns of the transaction
        let index = self.local_horizon.get(&column).cloned().unwrap_or(0.into()) + 1;
        'search: loop {
            trace!("seatching for multiput {:?}\n\tat: {:?}", put_id, (column, index));
            let ent = self.store.get((column, index)).clone();
            let ent = match ent {
                Err(GetErr::NoValue) => panic!("invalid multiput."),
                Ok(e) => e
            };
            self.play_deps(ent.dependencies());
            match ent.contents() {
                Multiput{uuid, ..} if uuid == put_id => break 'search,
                Multiput{data, uuid, columns, ..} => {
                    //TODO
                    trace!("Multiput");
                    self.read_multiput(column, data, uuid, columns);
                }
                Data(data, _) => {
                    trace!("Data");
                    self.upcalls.get(&column).map(|f| f(data)); //TODO clone
                }
            }
        }
        trace!("found multiput {:?} for {:?} at: {:?}", put_id, column, index);
        self.local_horizon.insert(column, index);
	}

    fn play_deps(&mut self, deps: &[OrderIndex]) {
        for &dep in deps {
            self.play_until(dep)
        }
    }

    pub fn play_until(&mut self, dep: OrderIndex) {
        //TODO end if run out?
        while self.local_horizon.get(&dep.0).cloned().unwrap_or(0.into()) < dep.1 {
            self.get_next_unseen(dep.0);
        }
    }

    pub fn play_foward(&mut self, column: order) -> Option<OrderIndex> {
        trace!("play_foward");
        //let index = self.horizon.get_horizon(column);
        //trace!("play until {:?}", index);
        //if index == 0.into() { return None }//TODO
        //self.play_until((column, index));
        //Some((column, index))
        let mut res = None;
        while let Some(index) = self.get_next_unseen(column) {
            res = Some(index);
        }
        res
    }

    pub fn local_horizon(&self) -> &HashMap<order, entry> {
        &self.local_horizon
    }
}

extern "C" {
    fn alloc_mbuf() -> *mut Mbuf;
    fn mbuf_set_src_port(_: *mut Mbuf, _:u16);
    fn copy_mbuf(_: *mut Mbuf, _: *const Mbuf);
    //fn rdtsc() -> ns;
}