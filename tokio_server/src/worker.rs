use std::collections::{vec_deque, VecDeque};
use std::marker::PhantomData;

use ToLog;
use ToWorker;

use buffer_stream::BufferStream;

use fuzzy_log_packets::{EntryContents, EntryFlag, EntryLayout, OrderIndex};
use fuzzy_log_packets::buffer::Buffer;
use fuzzy_log_server::{worker_thread, ChainReader, SkeensMultiStorage, Troption};

use futures::{Async, Poll};
use stream::Stream;

pub struct Batcher {
    log_batch: VecDeque<ToLog<u64>>,
    read_batch: VecDeque<Buffer>,
    reader: ChainReader<u64>,
    client: u64,
}

impl Batcher {
    pub fn new(client: u64, reader: ChainReader<u64>) -> Self {
        Batcher {
            log_batch: VecDeque::new(),
            read_batch: VecDeque::new(),
            client,
            reader,
        }
    }

    pub fn batch_size(&self) -> usize {
        self.log_batch.capacity()
    }

    pub fn add_client_msg(&mut self, mut msg: Buffer) {
        let storage = match msg.contents().layout() {
            EntryLayout::Read => {
                self.read_batch.push_back(msg);
                return;
            }
            EntryLayout::Multiput | EntryLayout::Sentinel => {
                let (size, senti_size, num_locs, has_senti, is_unlock, flag) = {
                    let e = msg.contents();
                    let locs = e.locs();
                    let num_locs = locs.len();
                    //FIXME
                    let has_senti = locs.contains(&OrderIndex(0.into(), 0.into()))
                        || !e.flag().contains(EntryFlag::TakeLock);
                    (
                        e.len(),
                        e.sentinel_entry_size(),
                        num_locs,
                        has_senti,
                        e.flag().contains(EntryFlag::Unlock),
                        e.flag().clone(),
                    )
                };
                if is_unlock {
                    Troption::None
                } else {
                    assert!(flag.contains(EntryFlag::NewMultiPut) || !flag.contains(EntryFlag::TakeLock));
                    let senti_size = if has_senti { Some(senti_size) } else { None };
                    let mut storage = SkeensMultiStorage::new(num_locs, size, senti_size);
                    if !flag.contains(EntryFlag::TakeLock) {
                        storage.fill_from(&mut msg)
                    }
                    Troption::Left(storage)
                }
                // } else {
                //     use fuzzy_log_server::shared_slice::RcSlice;
                //     let m = RcSlice::with_len(size);
                //     let s = RcSlice::with_len(senti_size);
                //     Troption::Right(Box::new((m, s)))
                // }
            }
            EntryLayout::Snapshot => {
                let (size, num_locs, is_unlock) = {
                    let e = msg.contents();
                    let locs = e.locs();
                    (e.len(), locs.len(), e.flag().contains(EntryFlag::Unlock))
                };
                if is_unlock {
                    Troption::None
                } else {
                    let mut storage = SkeensMultiStorage::new(num_locs, size, None);
                    storage.fill_from(&mut msg);
                    Troption::Left(storage)
                }
            }
            EntryLayout::Data => Troption::None,
            EntryLayout::GC => Troption::None,
            EntryLayout::Lock => unreachable!("No Locks"),
        };
        let to_send = ToLog::New(msg, storage, self.client);
        self.log_batch.push_back(to_send)
    }

    pub fn log_batch<E>(self) -> LogBatch<E> {
        LogBatch(self, PhantomData)
    }

    //FIXME make future
    //FIXME backpressure
    pub fn handle_buffered_reads<SendFn>(&mut self, mut send: SendFn)
    where SendFn: for<'a> FnMut(Result<&'a [u8], EntryContents<'a>>) {
        for buffer in self.read_batch.drain(..) {
            worker_thread::handle_read(&self.reader, &buffer, self.client as usize, &mut send);
        }
    }
}

pub struct LogBatch<E>(Batcher, PhantomData<fn() -> E>);

impl<E> LogBatch<E> {
    pub fn batcher(self) -> Batcher {
        self.0
    }
}

impl<E> Stream for LogBatch<E> {
    type Item = ToLog<u64>;
    type Error = E;

    fn poll(&mut self) -> Poll<Option<Self::Item>, E> {
        Ok(Async::Ready(self.0.log_batch.pop_front()))
    }
}

// pub struct ReadBatch<'a, E>(Batcher, PhantomData<(&'a ChainReader<u64>, fn() -> E)>);

// impl<'a, E> Stream for ReadBatch<'a, E> {
//     type Item = (&'a ChainReader<u64>, u64, Buffer);
//     type Error = E;

//     fn poll(&mut self) -> Poll<Option<Self::Item>, E> {
//         let buffer = self.0.read_batch.pop_front();
//         Ok(Async::Ready(buffer.map(|b| (&self.0.reader, self.0.client, b))))
//     }
// }

pub fn add_response(msg: ToWorker<u64>, write_buffer: &mut BufferStream, worker_num: u64)
-> Option<Buffer> {
    //TODO downstream?
    worker_thread::handle_to_worker2(msg, worker_num as usize, /*continue_replication*/ false, |to_send, _, _| {
        use worker_thread::ToSend;
        match to_send {
            ToSend::Nothing => return,
            ToSend::OldReplication(..) => unreachable!(),

            ToSend::Contents(to_send) | ToSend::OldContents(to_send, _) => write_buffer.add_contents(to_send),

            ToSend::Slice(to_send) => write_buffer.add_slice(to_send),

            ToSend::StaticSlice(to_send) | ToSend::Read(to_send) => write_buffer.add_slice(to_send),
        }
    }).0
}
