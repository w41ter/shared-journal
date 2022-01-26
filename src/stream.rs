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

    use bitflags::bitflags;

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

    /// An abstraction for a journal server to receive and persist entries.
    ///
    /// For now, let's assume that message passing is reliable.
    struct Progress {
        matched_index: u32,
        next_index: u32,

        start: usize,
        size: usize,
        in_flights: Vec<u32>,
    }

    impl Progress {
        const WINDOW: usize = 128;

        /// Return which chunk needs to replicate to the target.
        fn next_chunk(&self, last_index: u32) -> (u32, u32) {
            if self.size == Self::WINDOW {
                (self.next_index, self.next_index)
            } else if last_index > self.next_index {
                (self.next_index, last_index)
            } else {
                (last_index, last_index)
            }
        }

        fn replicate(&mut self, next_index: u32) {
            debug_assert!(self.size < Self::WINDOW);
            debug_assert!(self.next_index < next_index);
            self.next_index = next_index;

            let off = (self.start + self.size) % Self::WINDOW;
            self.in_flights[off] = next_index;
            self.size += 1;
        }

        /// A server has stored entries.
        fn on_received(&mut self, epoch: u32, index: u32) -> bool {
            // TODO(w41ter) check epoch
            while self.size > 0 && self.in_flights[self.start] < index {
                self.start = (self.start + 1) % Self::WINDOW;
                self.size -= 1;
            }
            self.next_index = index + 1;
            self.size < Self::WINDOW
        }
    }

    /// An abstraction of data communication between `StreamStateMachine` and
    /// journal servers.
    enum Message {
        /// Store entries to.
        Store {
            target: String,
            seg_epoch: u32,
            epoch: u32,
            acked_seq: u64,
            first_index: u32,
            entries: Vec<Entry>,
        },
        /// The journal server have received store request.
        Received {
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

    bitflags! {
        struct Flags : u64 {
            const NONE = 0;
            const ACK_ADVANCED = 0x1;
        }
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

        flags: Flags,
    }

    impl StreamStateMachine {
        fn promote(&mut self, epoch: u32, role: Role, leader: String) {
            todo!();
        }

        fn step(&mut self, msg: Message) {
            match msg {
                Message::Received {
                    target,
                    epoch,
                    index,
                } => self.handle_received(target, epoch, index),
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
            self.flags = Flags::NONE;
            Some(std::mem::take(&mut self.ready))
        }

        fn advance(&mut self) {
            let acked_seq = self.replication_policy.advance(&self.copy_set);
            if self.acked_seq < acked_seq {
                self.acked_seq = acked_seq;
                self.flags |= Flags::ACK_ADVANCED;
            }
        }

        fn broadcast(&mut self) {
            self.copy_set.iter_mut().for_each(|(server_id, progress)| {
                Self::replicate(
                    &mut self.ready,
                    progress,
                    &self.mem_store,
                    self.epoch,
                    self.acked_seq,
                    server_id,
                    self.flags.contains(Flags::ACK_ADVANCED),
                );
            });
        }

        fn replicate(
            ready: &mut Ready,
            progress: &mut Progress,
            mem_store: &MemStorage,
            epoch: u32,
            acked_seq: u64,
            server_id: &str,
            bcast_acked_seq: bool,
        ) {
            let last_index = mem_store.last_index();
            let (start, end) = progress.next_chunk(last_index);
            let msg = match mem_store.range(start..end) {
                Some(entries) => {
                    /// Do not forward acked sequence to unmatched index.
                    let matched_acked_seq =
                        u64::min(acked_seq, (((epoch as u64) << 32) | (end - 1) as u64));
                    Message::Store {
                        target: server_id.to_owned(),
                        entries,
                        seg_epoch: epoch,
                        epoch,
                        acked_seq: matched_acked_seq,
                        first_index: start,
                    }
                }
                None if bcast_acked_seq => {
                    // All entries are replicated, might broadcast acked
                    // sequence.
                    Message::Store {
                        target: server_id.to_owned(),
                        entries: vec![],
                        seg_epoch: epoch,
                        epoch,
                        acked_seq,
                        first_index: start,
                    }
                }
                None => return,
            };
            ready.pending_messages.push(msg);
            if end <= last_index {
                ready.still_active = true;
            }
        }

        fn handle_received(&mut self, target: String, epoch: u32, index: u32) {
            if let Some(progress) = self.copy_set.get_mut(&target) {
                if progress.on_received(epoch, index) {
                    Self::replicate(
                        &mut self.ready,
                        progress,
                        &self.mem_store,
                        self.epoch,
                        self.acked_seq,
                        &target,
                        true,
                    );
                }
            }
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
