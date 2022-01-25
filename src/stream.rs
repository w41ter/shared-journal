// Copyright 2021 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::Result;

/// An increasing number to order events.
pub type Sequence = u64;

#[derive(Debug)]
pub struct StreamReader {}

#[allow(dead_code, unused)]
impl StreamReader {
    /// Seeks to the given sequence.
    async fn seek(&mut self, sequence: Sequence) -> Result<()> {
        todo!();
    }

    /// Returns the next event.
    async fn try_next(&mut self) -> Result<Option<(Sequence, Vec<u8>)>> {
        todo!();
    }

    /// Returns the next event or waits until it is available.
    async fn wait_next(&mut self) -> Result<(Sequence, Vec<u8>)> {
        todo!();
    }
}

#[allow(unused, dead_code)]
mod writer {
    use std::collections::{HashMap, VecDeque};

    use crate::{Entry, ObserverState, Role, Sequence};

    /// Store entries for a stream.
    struct MemStorage {
        epoch: u32,
        first_index: u32,
        entries: VecDeque<Entry>,
    }

    impl MemStorage {
        fn new() -> Self {
            MemStorage {
                epoch: 0,
                first_index: 0,
                entries: VecDeque::new(),
            }
        }

        /// Returns the index of last entry.
        fn last_index(&self) -> u32 {
            todo!()
        }

        /// Save entry and assign index.
        fn append(&mut self, entry: Entry) -> Sequence {
            todo!();
        }

        /// Range values.
        fn range(&self, r: std::ops::Range<u32>) -> Option<Vec<Entry>> {
            None
        }
    }

    struct Progress {
        inflights: Vec<u32>,
    }

    impl Progress {
        /// Return which chunk needs to replicate to the target.
        fn next_chunk(&self, last_index: u32) -> (u32, u32) {
            todo!()
        }
    }

    enum Message {
        Store {
            target: String,
            seg_epoch: u32,
            epoch: u32,
            acked_seq: u64,
            first_index: u32,
            entries: Vec<Entry>,
        },
        Saved {
            target: String,
            epoch: u32,
            index: u32,
        },
    }

    enum Command {
        Tick,
        Promote {
            role: crate::Role,
            epoch: u32,
            leader: String,
        },
        Msg(Message),
        Proposal(Box<[u8]>),
    }

    enum Error {
        NotLeader(String),
    }

    enum ReplicationPolicy {}

    impl ReplicationPolicy {
        fn advance(&self, progresses: &HashMap<String, Progress>) -> Sequence {
            todo!();
        }
    }

    #[derive(Default)]
    struct Ready {
        still_active: bool,

        pending_messages: Vec<Message>,
    }

    struct StreamStateMachine {
        epoch: u32,
        role: Role,
        leader: String,
        state: ObserverState,
        mem_store: MemStorage,
        copy_set: HashMap<String, Progress>,
        replication_policy: ReplicationPolicy,

        acked_seq: Sequence,
        ready: Ready,
    }

    impl StreamStateMachine {
        fn promote(&mut self, epoch: u32, role: Role, leader: String) {
            todo!();
        }

        fn step(&mut self, msg: Message) {
            match msg {
                Message::Saved {
                    target,
                    epoch,
                    index,
                } => {
                    // todo
                }
                Message::Store {
                    target: _,
                    epoch: _,
                    seg_epoch: _,
                    first_index: _,
                    acked_seq: _,
                    entries: _,
                } => unreachable!(),
            }
        }

        fn propose(&mut self, event: Box<[u8]>) -> Result<Sequence, Error> {
            if self.role == Role::Follower {
                Err(Error::NotLeader(self.leader.clone()))
            } else {
                let entry = Entry::Event {
                    epoch: self.epoch,
                    event,
                };
                Ok(self.mem_store.append(entry))
            }
        }

        fn collect(&mut self) -> Option<Ready> {
            self.advance();
            self.broadcast();
            Some(std::mem::take(&mut self.ready))
        }

        fn advance(&mut self) {
            self.acked_seq = self.replication_policy.advance(&mut self.copy_set);
        }

        fn broadcast(&mut self) {
            let last_index = self.mem_store.last_index();
            self.copy_set.iter_mut().for_each(|(server_id, progress)| {
                let (start, end) = progress.next_chunk(last_index);
                let entries = match self.mem_store.range(start..end) {
                    Some(entries) => entries,
                    None => return,
                };
                // TODO(w41ter) need broadcast acked sequence.
                let msg = Message::Store {
                    target: server_id.clone(),
                    entries,
                    seg_epoch: self.epoch,
                    epoch: self.epoch,
                    acked_seq: 0,
                    first_index: start,
                };
                self.ready.pending_messages.push(msg);
                if end <= last_index {
                    self.ready.still_active = true;
                }
            });
        }
    }

    struct Channel {}

    impl Channel {
        fn fetch(&mut self) -> Vec<Command> {
            todo!()
        }

        fn stream_id(&self) -> u64 {
            0
        }

        fn sender(&mut self) {
            todo!()
        }
    }

    struct Selector {}

    impl Selector {
        fn select(&mut self, consumed: &[Channel]) -> Vec<Channel> {
            todo!()
        }
    }

    fn flush_messages(channel: &mut Channel, ready: &mut Ready) {
        todo!();
    }

    fn send_heartbeat(stream: &mut StreamStateMachine) {
        todo!()
    }

    struct Worker {
        selector: Selector,
        streams: HashMap<u64, StreamStateMachine>,
    }

    impl Worker {
        fn new() -> Self {
            Worker {
                selector: Selector {},
                streams: HashMap::new(),
            }
        }
    }

    async fn order_worker() {
        let mut w = Worker::new();
        let mut consumed: Vec<Channel> = Vec::new();

        loop {
            // Read entries from fired channels.
            let actives = w.selector.select(&consumed);
            for mut channel in actives {
                let commands = channel.fetch();
                let stream_id = channel.stream_id();
                let stream = w
                    .streams
                    .get_mut(&stream_id)
                    .expect("stream already exists");

                for cmd in commands {
                    match cmd {
                        Command::Msg(msg) => stream.step(msg),
                        Command::Proposal(event) => {
                            stream.propose(event);
                        }
                        Command::Tick => send_heartbeat(stream),
                        Command::Promote {
                            epoch,
                            role,
                            leader,
                        } => stream.promote(epoch, role, leader),
                    }
                }

                if let Some(mut ready) = stream.collect() {
                    flush_messages(&mut channel, &mut ready);
                    if ready.still_active {
                        // TODO(w41ter) support still active
                        continue;
                    }
                }
                consumed.push(channel);
            }
        }
    }
}

#[derive(Debug)]
pub struct StreamWriter {}

#[allow(dead_code, unused)]
impl StreamWriter {
    /// Appends an event, returns the sequence of the event just append.
    async fn append(&mut self, event: Vec<u8>) -> Result<Sequence> {
        todo!();
    }

    /// Truncates events up to a sequence (exclusive).
    async fn truncate(&mut self, sequence: Sequence) -> Result<()> {
        todo!();
    }
}
