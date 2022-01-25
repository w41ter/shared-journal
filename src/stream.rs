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

    use crate::Entry;

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

        /// Save entries and assign indexes.
        fn append(&mut self, entries: Vec<Entry>) {
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

    enum Message {}

    enum Command {
        Tick,
        Promote {
            role: crate::Role,
            epoch: u32,
            leader: String,
        },
        Msg(Message),
        Proposal(Entry),
    }

    struct Ready {
        still_active: bool,
    }

    struct StreamStateMachine {
        mem_store: MemStorage,
        copy_set: HashMap<String, Progress>,
    }

    impl StreamStateMachine {
        fn promote(&mut self, epoch: u32, role: crate::Role, leader: String) {}

        fn step(&mut self, msg: Message) {}

        fn propose(&mut self, entry: Entry) {}

        fn advance(&mut self) -> Option<Ready> {
            None
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
                        Command::Proposal(entry) => stream.propose(entry),
                        Command::Tick => send_heartbeat(stream),
                        Command::Promote {
                            epoch,
                            role,
                            leader,
                        } => stream.promote(epoch, role, leader),
                    }
                }

                if let Some(mut ready) = stream.advance() {
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
