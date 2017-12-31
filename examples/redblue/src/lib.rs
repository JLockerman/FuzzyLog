
extern crate bincode;
extern crate fuzzy_log_client;
pub extern crate serde;

#[macro_use] extern crate serde_derive;

#[cfg(test)] extern crate fuzzy_log_server;
#[cfg(test)] #[macro_use] extern crate matches;

pub use std::io;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};

pub use fuzzy_log_client::fuzzy_log::log_handle::{
    entry,
    order,
    GetRes,
    LogHandle,
    OrderIndex,
    TryWaitRes,
    Uuid,
};

use bincode::{serialize, deserialize, Infinite};

use serde::{Serialize, Deserialize};


#[derive(Debug, Serialize, Deserialize)]
enum Message<RedMessage, BlueMessage> {
    Red(Red),
    Blue(Blue),
    NewClient(order),
}


#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
pub enum RedBlue<RedMessage, BlueMessage> {
    Red(Red),
    Blue(Blue),
}

pub enum ToSync<T> {
    Observation(T),
    Mutation(Uuid),
    EndOfSnapshot,
}

impl<RedMessage, BlueMessage> RedBlueClient<RedMessage, BlueMessage> {
    pub fn do_red(&mut self, msg: RedMessage) -> Result<(), bincode::Error> {
        let data = serialize(Message::Red(msg), Infinite)?;
        let id = self.handle.async_multiappend(&self.colors[..], &data[..], &[]);
        self.to_sync.send(ToSync::Mutation(id));
        Ok(id)
    }

    pub fn do_blue(&mut self, msg: BlueMessage) -> Result<Uuid, bincode::Error> {
        let data = serialize(Message::Blue(msg), Infinite)?;
        let id = self.data.async_append(self.my_color, &data[..], &self.happens_after[..]);
        self.to_sync.send(ToSync::Mutation(id));
        Ok(id)
    }
}

trait RBStruct<RedMessage, BlueMessage, Observation, ObservationResult> {
    fn update_red(&mut self, msg: RedMessage);
    fn update_blue(&mut self, msg: BlueMessage);
    fn observe(&mut self, _: Observation) -> ObservationResult;
}

struct Materializer<Observation, ObservationResult, Structure> {
    data: Structure,
    from_client: Receiver<ToSync<Observation>>,
    to_client: Sender<ObservationResult>,
    handle: ReadHandle<[u8]>,
    colors: Vec<order>,
    causes: HasMap<order, entry>,
}

impl<RedMessage, BlueMessage, Observation, ObservationResult, Structure>
Materializer<Observation, ObservationResult, Structure>
where Structure: RBStruct<RedMessage, BlueMessage, Observation, ObservationResult> {
    fn sync(&mut self) -> Result<(), TryRecvError> {
        self.loopback.send(ToSync::EndOfSnapshot);
        self.snapshot();
        self.drain_pending_ops();
        self.play_log();
        self.handle_observations_until(None);
    }

    fn snapshot(&mut self) {
        if idle { snapshot(&self.colors[..]) } else { snapshot(self.colors[0])};
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
        for msg in self.from_client().try_iter() {
            match msg {
                ToSync::EndOfSnapshot => break,
                ToSync::Observation(msg) => {
                    if self.observation_index <= self.mutation_index {
                        self.observation_index = self.mutation_index + 1
                    }
                    self.waiting_observations.push_back((self.observation_index, msg))
                },
                ToSync::Mutation(id) =>
                    if Some(&id) == self.early_mutations.front() {
                        self.early_mutations.pop_front();
                        ack(id);
                    } else {
                        if self.mutation_index <= self.observation_index {
                            self.mutation_index = self.observation_index + 1
                        }
                        self.waiting_mutations.push_back((self.mutation_index, msg));
                    },
            }
        }
    }

    fn play_log(&mut self) {
        loop { match self.handle.get_next2() {
            Err(GetRes::Done) => break,
            Err(e) => panic!("{}", e), //TODO return Err(UpdateErr::Log(e)),
            Ok((bytes, locs, id)) => {
                if Some(&(count, id)) == self.waiting_mutations.front() {
                    self.waiting_mutations.pop_front();
                    self.handle_observations_until(Some(count));
                    ack(id);
                } else if my_mutation {
                    self.early_mutations.push_back(id);
                }
                let msg = self.deserialize().expect("bad msg");
                match msg {
                    Message::Red(msg) => self.data.update_red(msg),
                    Message::Blue(msg) => {
                        self.update_causes(locs);
                        self.data.update_blue(msg)
                    },
                    Message::NewClient(..) => unimplemented!(),
                }
            },
        } }
    }

    fn handle_observations_until(&mut self, mutation_time: Option<u64>) {
        match mutation_time {
            Some(index) =>
                while self.waiting_observations.front()
                    .map(|&(count, _)| count < index).unwrap_or(false) {
                    let result = self.data.observe(observation);
                    let _ = self.to_client.send(result);
                },

            None =>
                for observations in self.waiting_observations.drain(..) {
                    let result = self.data.observe(observation);
                    let _ = self.to_client.send(result);
                },
        }
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
