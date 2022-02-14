// Copyright 2022 The Engula Authors.
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

use std::collections::HashMap;

use bitflags::bitflags;
use log::{info, warn};

use super::{EpochState, MemStore, Progress, ReplicatePolicy};
use crate::{journal::master::ObserverState, Entry, Error, Result, Role, Sequence, INITIAL_EPOCH};

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub(crate) enum MsgDetail {
    /// Store entries to.
    Store {
        acked_seq: Sequence,
        first_index: u32,
        #[derivative(Debug = "ignore")]
        entries: Vec<Entry>,
    },
    /// The journal server have received store request.
    Received { index: u32 },
}

/// An abstraction of data communication between `StreamStateMachine` and
/// journal servers.
#[derive(Debug, Clone)]
pub(crate) struct Message {
    pub target: String,
    #[allow(dead_code)]
    pub seg_epoch: u32,
    pub epoch: u32,
    pub detail: MsgDetail,
}

#[derive(Default)]
pub(super) struct Ready {
    pub still_active: bool,
    pub acked_seq: Sequence,

    pub pending_epoch: Option<u32>,
    pub pending_messages: Vec<Message>,
}

bitflags! {
    struct Flags : u64 {
        const NONE = 0;
        const ACK_ADVANCED = 0x1;
    }
}

pub(super) struct StreamStateMachine {
    pub name: String,
    pub stream_id: u64,
    pub epoch: u32,
    pub role: Role,
    pub leader: String,
    pub state: ObserverState,
    pub replicate_policy: ReplicatePolicy,

    pub acked_seq: Sequence,

    mem_store: MemStore,
    copy_set: HashMap<String, Progress>,

    ready: Ready,

    flags: Flags,

    pending_epochs: Vec<u32>,
}

impl StreamStateMachine {
    pub fn new(name: String, stream_id: u64) -> Self {
        StreamStateMachine {
            name,
            stream_id,
            epoch: INITIAL_EPOCH,
            role: Role::Follower,
            leader: "".to_owned(),
            state: ObserverState::Following,
            mem_store: MemStore::new(INITIAL_EPOCH),
            copy_set: HashMap::new(),
            replicate_policy: ReplicatePolicy::Simple,
            acked_seq: Sequence::default(),
            ready: Ready::default(),
            flags: Flags::NONE,
            pending_epochs: Vec::default(),
        }
    }

    pub fn epoch_state(&self) -> EpochState {
        EpochState {
            epoch: self.epoch as u64,
            role: self.role,
            leader: if self.epoch == INITIAL_EPOCH {
                None
            } else {
                Some(self.leader.clone())
            },
        }
    }

    pub fn promote(
        &mut self,
        epoch: u32,
        role: Role,
        leader: String,
        copy_set: Vec<String>,
        pending_epochs: Vec<u32>,
    ) {
        self.epoch = epoch;
        self.state = match role {
            Role::Leader => ObserverState::Leading,
            Role::Follower => ObserverState::Following,
        };
        self.leader = leader;
        self.role = role;
        self.mem_store = MemStore::new(epoch);
        self.copy_set = copy_set
            .into_iter()
            .map(|remote| (remote, Progress::new(self.epoch)))
            .collect();
        self.pending_epochs = pending_epochs;

        // Sort in reverse to ensure that the smallest is at the end. See
        // `StreamStateMachine::handle_recovered` for details.
        self.pending_epochs.sort_by(|a, b| b.cmp(a));
        self.ready.pending_epoch = self.pending_epochs.last().cloned();
    }

    pub fn step(&mut self, msg: Message) {
        match msg.detail {
            MsgDetail::Received { index } => {
                if self.role == Role::Leader {
                    self.handle_received(msg.target, msg.epoch, index);
                } else {
                    todo!("log staled message");
                }
            }
            MsgDetail::Store {
                first_index: _,
                acked_seq: _,
                entries: _,
            } => unreachable!(),
        }
    }

    pub fn propose(&mut self, event: Box<[u8]>) -> Result<Sequence> {
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

    pub fn collect(&mut self) -> Option<Ready> {
        if self.role == Role::Leader {
            self.advance();
            self.broadcast();
            self.flags = Flags::NONE;
            self.ready.acked_seq = self.acked_seq;
            Some(std::mem::take(&mut self.ready))
        } else {
            None
        }
    }

    fn advance(&mut self) {
        debug_assert_eq!(self.role, Role::Leader);

        // Don't ack any entries if there exists a pending segment.
        if !self.pending_epochs.is_empty() {
            return;
        }

        let acked_seq = self
            .replicate_policy
            .advance_acked_sequence(self.epoch, &self.copy_set);
        if self.acked_seq < acked_seq {
            self.acked_seq = acked_seq;
            self.flags |= Flags::ACK_ADVANCED;
        }
    }

    fn broadcast(&mut self) {
        debug_assert_eq!(self.role, Role::Leader);

        // Do not replicate entries if there exists two pending segments.
        if self.pending_epochs.len() == 2 {
            return;
        }

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
        mem_store: &MemStore,
        epoch: u32,
        acked_seq: Sequence,
        server_id: &str,
        bcast_acked_seq: bool,
    ) {
        let next_index = mem_store.next_index();
        let (start, end) = progress.next_chunk(next_index);
        let detail = match mem_store.range(start..end) {
            Some(entries) => {
                // Do not forward acked sequence to unmatched index.
                let matched_acked_seq = Sequence::min(acked_seq, Sequence::new(epoch, end - 1));
                progress.replicate(end);
                MsgDetail::Store {
                    entries,
                    acked_seq: matched_acked_seq,
                    first_index: start,
                }
            }
            None if bcast_acked_seq => {
                // All entries are replicated, might broadcast acked
                // sequence.
                MsgDetail::Store {
                    entries: vec![],
                    acked_seq,
                    first_index: start,
                }
            }
            None => return,
        };

        let msg = Message {
            target: server_id.to_owned(),
            seg_epoch: epoch,
            epoch,
            detail,
        };

        ready.pending_messages.push(msg);
        if end < next_index {
            ready.still_active = true;
        }
    }

    pub fn handle_received(&mut self, target: String, epoch: u32, index: u32) {
        debug_assert_eq!(self.role, Role::Leader);
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

    pub fn handle_recovered(&mut self, seg_epoch: u32, writer_epoch: u32) {
        if writer_epoch != self.epoch {
            warn!(
                "stream {} epoch {} receive staled recovered msg, seg epoch: {}, writer epoch: {}",
                self.stream_id, self.epoch, seg_epoch, writer_epoch
            );
            return;
        }

        info!(
            "stream {} epoch {} receive recovered msg, seg epoch: {}, writer epoch: {}",
            self.stream_id, self.epoch, seg_epoch, writer_epoch
        );

        match self.pending_epochs.pop() {
            Some(first_pending_epoch) if first_pending_epoch == seg_epoch => {
                self.ready.pending_epoch = self.pending_epochs.last().cloned();
            }
            _ => panic!("should't happen"),
        }
    }
}
