extern crate bincode;
extern crate fuzzy_log_client;
pub extern crate serde;

#[macro_use]
extern crate serde_derive;

#[cfg(test)]
extern crate env_logger;

#[cfg(test)]
extern crate fuzzy_log_server;
// #[cfg(test)]
// #[macro_use]
// extern crate matches;

use std::collections::{HashMap, VecDeque};
pub use std::io;
use std::mem::swap;
use std::marker::PhantomData;
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};

pub use fuzzy_log_client::fuzzy_log::log_handle::{entry, order, GetRes, LogHandle, OrderIndex,
                                                  ReadHandle, TryWaitRes, Uuid, WriteHandle};

use bincode::{deserialize, serialize, Infinite};

pub use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
enum Message<Red, Blue> {
    Red(Red),
    Blue(Blue),
    NewClient(u64),
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
pub enum RedBlue<Red, Blue> {
    Red(Red),
    Blue(Blue),
}

pub enum ToSync<T> {
    Observation(T),
    Mutation(Uuid),
    EndOfSnapshot,
}

pub struct RedBlueClient<RedMessage, BlueMessage, Observation> {
    handle: WriteHandle<[u8]>,
    red_chain: order,
    my_chain: order,

    //FIXME use AtomicPtr (AtomicArc?) instead of mutex
    causes: Arc<Mutex<HashMap<order, entry>>>,
    to_sync: Sender<ToSync<Observation>>,

    happens_after: Vec<OrderIndex>,
    cached_map: HashMap<order, entry>,
    _pd: PhantomData<(RedMessage, BlueMessage)>,
}

impl<RedMessage, BlueMessage, Observation> RedBlueClient<RedMessage, BlueMessage, Observation>
where
    RedMessage: Serialize + for<'a> Deserialize<'a>,
    BlueMessage: Serialize + for<'a> Deserialize<'a>,
    Observation: Send + 'static,
{
    pub fn new<I, ObservationResult, Data, Ack>(
        handle: LogHandle<[u8]>,
        my_chain: order,
        red_chain: order,
        other_chains: I,
        data: Data,
        ack: Ack,
        to_client: Sender<ObservationResult>,
    ) -> Self
    where
        I: IntoIterator<Item = order>,
        ObservationResult: Send + 'static,
        Data: RBStruct<RedMessage, BlueMessage, Observation, ObservationResult> + Send + 'static,
        Ack: MutationAck + Send + 'static,
    {
        use std::thread::{spawn, yield_now};
        let mut all_chains: Vec<_> = vec![my_chain, red_chain];
        all_chains.extend(other_chains);
        let (reader, handle) = handle.split();
        let (to_sync, from_client) = channel();
        let loopback = to_sync.clone();
        let causes = Arc::new(Mutex::new(HashMap::new()));
        let ack = Box::new(ack);
        let causes2 = causes.clone();
        spawn(move || {
            let mut m = Materializer {
                ack,
                to_client,
                my_chain,
                red_chain,
                all_chains,
                causes: causes2,
                data: data,
                early_mutations: Default::default(),
                waiting_observations: Default::default(),
                waiting_mutations: Default::default(),
                from_client,
                handle: reader,
                loopback,
                idle: false,
                mutation_index: 0,
                observation_index: 0,
                _pd: PhantomData,
            };
            loop {
                m.sync().unwrap();
                yield_now();
            }
        });
        RedBlueClient {
            handle,
            red_chain,
            my_chain,
            causes,
            to_sync,
            happens_after: vec![],
            cached_map: HashMap::new(),
            _pd: PhantomData,
        }
    }

    pub fn do_red(&mut self, msg: RedMessage) -> Result<Uuid, bincode::Error> {
        let message: Message<_, BlueMessage> = Message::Red(msg);
        let chain = self.red_chain;
        self.send(chain, &message)
    }

    pub fn do_blue(&mut self, msg: BlueMessage) -> Result<Uuid, bincode::Error> {
        let message: Message<RedMessage, _> = Message::Blue(msg);
        let chain = self.my_chain;
        self.send(chain, &message)
    }

    fn send(&mut self, chain: order, msg: &Message<RedMessage, BlueMessage>)
    -> Result<Uuid, bincode::Error> {
        self.fill_happens_after(chain);
        //TODO serialize_into
        let data = serialize(&msg, Infinite)?;
        let id = self.handle
            .async_append(chain, &data[..], &self.happens_after[..]);
        self.to_sync
            .send(ToSync::Mutation(id))
            .expect("materializer dead");
        Ok(id)
    }

    fn fill_happens_after(&mut self, chain: order) {
        self.happens_after.clear();
        {
            let mut causes = self.causes.lock().unwrap();
            swap(&mut *causes, &mut self.cached_map);
        }
        self.cached_map.remove(&chain);
        for loc in self.cached_map.drain() {
            self.happens_after.push(loc.into())
        }
    }

    pub fn do_observation(&mut self, observation: Observation) {
        self.to_sync
            .send(ToSync::Observation(observation))
            .expect("materializer dead");
    }
}

pub trait RBStruct<RedMessage, BlueMessage, Observation, ObservationResult> {
    fn update_red(&mut self, msg: RedMessage) -> Option<ObservationResult>; //TODO should be mutation result?
    fn update_blue(&mut self, msg: BlueMessage) -> Option<ObservationResult>; //TODO should be mutation result?
    fn observe(&mut self, _: Observation) -> ObservationResult;
}

pub struct Materializer<RedMessage, BlueMessage, Observation, ObservationResult, Structure>
where
    Structure: RBStruct<RedMessage, BlueMessage, Observation, ObservationResult>,
{
    data: Structure,
    from_client: Receiver<ToSync<Observation>>,
    loopback: Sender<ToSync<Observation>>,
    to_client: Sender<ObservationResult>,

    handle: ReadHandle<[u8]>,
    my_chain: order,
    red_chain: order,
    all_chains: Vec<order>,
    causes: Arc<Mutex<HashMap<order, entry>>>,
    idle: bool,

    early_mutations: VecDeque<(Uuid, Option<ObservationResult>)>,
    waiting_mutations: VecDeque<(u64, Uuid)>,
    waiting_observations: VecDeque<(u64, Observation)>,

    observation_index: u64,
    mutation_index: u64,

    ack: Box<MutationAck>,

    _pd: PhantomData<(RedMessage, BlueMessage)>,
}

impl<RedMessage, BlueMessage, Observation, ObservationResult, Structure>
    Materializer<RedMessage, BlueMessage, Observation, ObservationResult, Structure>
where
    Structure: RBStruct<RedMessage, BlueMessage, Observation, ObservationResult>,
    RedMessage: for<'a> Deserialize<'a>,
    BlueMessage: for<'a> Deserialize<'a>,
{
    pub fn sync(&mut self) -> Result<(), TryRecvError> {
        let _ = self.loopback.send(ToSync::EndOfSnapshot);
        self.snapshot();
        self.drain_pending_ops();
        self.play_log();
        self.handle_observations();
        Ok(())
    }

    fn snapshot(&mut self) {
        if self.idle {
            self.handle.snapshot_colors(&self.all_chains[..])
        } else {
            self.handle.snapshot_colors(&[self.my_chain, self.red_chain])
        };
    }

    /*
        There are 2 options which are sufficient for serving an observation
            1. a snapshot, which started after the observation, finishes
            2. a mutation, which started after the observation, finishes

        on new mutation:
            if mutation_counter <= observation_counter:
                mutation_counter <- observation_counter + 1
            mutation <- mutation_counter

        on new observation:
            if observation_counter <= mutation_counter:
                observation_counter <-  mutation_counter + 1
            observation <- observation_counter

        on read mutation(mut_count):
            while Some((count < mut_count, o)) = observations.front():
                handle_observation(o)
    */

    fn drain_pending_ops(&mut self) {
        for msg in self.from_client.try_iter() {
            match msg {
                ToSync::EndOfSnapshot => break,
                ToSync::Observation(msg) => {
                    if self.observation_index <= self.mutation_index {
                        self.observation_index = self.mutation_index + 1
                    }
                    self.waiting_observations
                        .push_back((self.observation_index, msg))
                }
                ToSync::Mutation(id) => {
                    while !self.early_mutations.is_empty() {
                        let (eid, result) = self.early_mutations.pop_front().unwrap();
                        if id == eid {
                            if let Some(result) = result {
                                self.to_client.send(result).expect("client gone");
                            }
                            self.ack.ack(id);
                            break
                        }
                    }

                    if self.mutation_index <= self.observation_index {
                        self.mutation_index = self.observation_index + 1
                    }
                    self.waiting_mutations.push_back((self.mutation_index, id));
                }
            }
        }
    }

    fn play_log(&mut self) {
        let my_color = self.my_chain;
        loop {
            match self.handle.get_next2() {
                Err(GetRes::Done) => break,
                Err(..) => panic!(""), //TODO return Err(UpdateErr::Log(e)),
                Ok((bytes, locs, &id)) => {
                    let (my_message, ready) = match self.waiting_mutations.front() {
                        Some(&(count, idm)) if idm == id => {
                            let (_, _) = self.waiting_mutations.pop_front().unwrap();
                            handle_observations_until(
                                count,
                                &mut self.data,
                                &mut self.waiting_observations,
                                &mut self.to_client,
                            );
                            (true, true)
                        }
                        //FIXME doesn't work for red
                        _ => (locs.iter().any(|l| l.0 == my_color), false),
                    };
                    let msg = deserialize(bytes).expect("bad msg");
                    let result = match msg {
                        Message::Red(msg) => {
                            // self.clear_causes();
                            clear_causes(&mut self.causes);

                            let result = self.data.update_red(msg);
                            result
                        }
                        Message::Blue(msg) => {
                            // self.update_causes(locs);
                            update_causes(my_color, &mut self.causes, locs);
                            let result = self.data.update_blue(msg);
                            result
                        }
                        Message::NewClient(..) => unimplemented!(), //TODO no ack
                    };
                    if my_message {
                        if ready {
                            if let Some(result) = result {
                                self.to_client.send(result).expect("client gone");
                            }
                            self.ack.ack(id);
                        } else {
                            self.early_mutations.push_back((id, result))
                        }
                    }
                }
            }
        }

        fn update_causes(
            _color: order,
            causes: &mut Arc<Mutex<HashMap<order, entry>>>,
            locs: &[OrderIndex],
        ) {
            let mut causes = causes.lock().unwrap();
            for &OrderIndex(o, i) in locs {
                // if o != color {
                    causes.insert(o, i);
                // }
            }
        }

        fn clear_causes(causes: &mut Arc<Mutex<HashMap<order, entry>>>) {
            let mut causes = causes.lock().unwrap();
            causes.clear();
        }

        fn handle_observations_until<R, B, O, OR, S: RBStruct<R, B, O, OR>>(
            index: u64,
            data: &mut S,
            waiting_observations: &mut VecDeque<(u64, O)>,
            to_client: &mut Sender<OR>,
        ) {
            while waiting_observations
                .front()
                .map(|&(count, _)| count < index)
                .unwrap_or(false)
            {
                let (_, observation) = waiting_observations.pop_front().unwrap();
                let result = data.observe(observation);
                let _ = to_client.send(result);
            }
        }
    }

    fn handle_observations(&mut self) {
        for (_, observation) in self.waiting_observations.drain(..) {
            let result = self.data.observe(observation);
            let _ = self.to_client.send(result);
        }
    }
}

pub trait MutationAck {
    fn ack(&mut self, id: Uuid);
}

impl MutationAck for () {
    fn ack(&mut self, _: Uuid) {}
}

impl<F> MutationAck for F
where
    F: FnMut(Uuid),
{
    fn ack(&mut self, id: Uuid) {
        self(id)
    }
}

impl MutationAck for Sender<Uuid> {
    fn ack(&mut self, id: Uuid) {
        let _ = self.send(id);
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use fuzzy_log_server::tcp::run_server;
    use std::sync::mpsc::{channel, Receiver};

    struct Bank {
        balance: u64,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Withdraw(u64);

    #[derive(Debug, Serialize, Deserialize)]
    struct Deposit(u64);

    #[derive(Debug)]
    struct Balance;

    #[derive(Debug, PartialEq, Eq)]
    enum ObservationResult {
        Empty,
        Balance(u64),
        Withdrew(bool),
    }

    impl RBStruct<Withdraw, Deposit, Balance, ObservationResult> for Bank {
        fn update_red(&mut self, Withdraw(amount): Withdraw) -> Option<ObservationResult> {
            if self.balance >= amount {
                self.balance -= amount;
                Some(ObservationResult::Withdrew(true))
            } else {
                Some(ObservationResult::Withdrew(false))
            }
        }

        fn update_blue(&mut self, Deposit(amount): Deposit) -> Option<ObservationResult> {
            self.balance += amount;
            Some(ObservationResult::Empty)
        }

        fn observe(&mut self, _: Balance) -> ObservationResult {
            ObservationResult::Balance(self.balance)
        }
    }

    struct Client(
        RedBlueClient<Withdraw, Deposit, Balance>,
        Receiver<ObservationResult>,
    );

    impl Client {
        fn new(
            handle: LogHandle<[u8]>, my_chain: order, red_chain: order, other_chains: &[order]
        ) -> Self {
            let (to_client, resp) = channel();
            let client = RedBlueClient::new(
                handle,
                my_chain,
                red_chain,
                other_chains.iter().cloned(),
                Bank { balance: 0 },
                (),
                to_client,
            );
            Client(client, resp)
        }

        fn deposit(&mut self, amount: u64) {
            self.0.do_blue(Deposit(amount)).unwrap();
            let ack = self.1.recv().unwrap();
            assert_eq!(ack, ObservationResult::Empty);
        }

        fn accrue_interest(&mut self, interest: f64) {
            self.0.do_observation(Balance);
            match self.1.recv().unwrap() {
                ObservationResult::Balance(balance) => {
                    self.deposit((balance as f64 * interest) as u64)
                }
                //self.0.do_blue(Deposit()).unwrap();
                r => unreachable!("{:?}", r),
            }
        }

        fn withdraw(&mut self, amount: u64) -> bool {
            self.0.do_red(Withdraw(amount)).unwrap();
            match self.1.recv().unwrap() {
                ObservationResult::Withdrew(b) => b,
                r => unreachable!("{:?}", r),
            }
        }

        fn balance(&mut self) -> u64 {
            self.0.do_observation(Balance);
            match self.1.recv().unwrap() {
                ObservationResult::Balance(balance) => balance,
                r => unreachable!("{:?}", r),
            }
        }
    }

    #[test]
    fn it_works() {
        use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
        static STARTED: AtomicUsize = ATOMIC_USIZE_INIT;

        let _ = env_logger::init();

        ::std::thread::spawn(|| {
            run_server(
                "0.0.0.0:14005".parse().unwrap(),
                0,
                1,
                None,
                None,
                2,
                &STARTED,
            )
        });
        while STARTED.load(Ordering::Relaxed) == 0 {
            ::std::thread::yield_now()
        }
        let mut client: Vec<_> = (1..3)
            .map(|i| {
                let handle = LogHandle::unreplicated_with_servers(&[
                    "127.0.0.1:14005".parse().unwrap(),
                ]).reads_my_writes()
                    .chains(&[1.into(), 2.into(), 3.into()])
                    .build();
                Client::new(
                    handle,
                    i.into(),
                    3.into(),
                    &if i == 1 { [2.into()] } else { [1.into()] },
                )
            })
            .collect();

        assert_eq!(client[0].balance(), 0);
        assert_eq!(client[1].balance(), 0);
        assert_eq!(client[0].withdraw(0), true);
        assert_eq!(client[0].withdraw(1), false);
        assert_eq!(client[0].withdraw(100), false);

        client[0].deposit(1_100);
        assert_eq!(client[0].balance(), 1_100);
        assert_eq!(client[1].balance(), 0);
        assert_eq!(client[0].withdraw(100), true);
        assert_eq!(client[0].balance(), 1_000);
        assert_eq!(client[1].balance(), 1_000);

        client[1].accrue_interest(0.01);

        assert_eq!(client[0].balance(), 1_000);
        assert_eq!(client[1].balance(), 1_010);
        // assert_eq!(client[0].withdraw(0), true); this is insufficient in the new impl
        assert_eq!(client[1].withdraw(0), true);
        assert_eq!(client[0].balance(), 1_010);
        assert_eq!(client[1].balance(), 1_010);
    }
}
