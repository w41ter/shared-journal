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

use std::{collections::HashMap, fmt::Display, ops::Range};

use bitflags::bitflags;
use log::{error, info, warn};

use super::{EpochState, MemStore, Progress, ReplicatePolicy};
use crate::{journal::master::ObserverState, Entry, Error, Result, Role, Sequence, INITIAL_EPOCH};

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub(crate) struct Write {
    pub target: String,
    pub seg_epoch: u32,
    pub epoch: u32,
    pub acked_seq: Sequence,
    pub range: Range<u32>,
    pub bytes: usize,
    #[derivative(Debug = "ignore")]
    pub entries: Vec<Entry>,
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
#[allow(unused)]
pub(crate) enum MsgDetail {
    Received { index: u32 },
    Recovered,
    Rejected,
    Timeout { range: Range<u32>, bytes: usize },
}

impl Display for MsgDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let desc = match self {
            MsgDetail::Received { .. } => "RECEIVED",
            MsgDetail::Recovered => "RECOVERED",
            MsgDetail::Rejected => "REJECTED",
            MsgDetail::Timeout { .. } => "TIMEOUT",
        };
        write!(f, "{}", desc)
    }
}

/// An abstraction of data communication between `StreamStateMachine` and
/// journal servers.
#[derive(Debug, Clone)]
pub(crate) struct Message {
    pub target: String,
    pub seg_epoch: u32,
    pub epoch: u32,
    pub detail: MsgDetail,
}

pub(super) struct Replicate {
    pub epoch: u32,

    mem_store: MemStore,
    copy_set: HashMap<String, Progress>,
}

impl Replicate {
    pub fn new(epoch: u32, copy_set: Vec<String>) -> Self {
        Replicate {
            epoch,
            mem_store: MemStore::new(epoch),
            copy_set: copy_set
                .into_iter()
                .map(|c| (c, Progress::new(epoch)))
                .collect(),
        }
    }

    pub fn copy_set(&self) -> Vec<String> {
        self.copy_set.keys().cloned().collect()
    }

    #[inline(always)]
    pub fn append(&mut self, entry: Entry) {
        self.mem_store.append(entry);
    }

    pub fn broadcast(
        &mut self,
        writer_epoch: u32,
        latest_tick: usize,
        acked_seq: Sequence,
        acked_index_advanced: bool,
        pending_writes: &mut Vec<Write>,
    ) -> bool {
        let mut active = false;
        for (server_id, progress) in &mut self.copy_set {
            let next_index = self.mem_store.next_index();
            let (Range { start, mut end }, quota) = progress.next_chunk(next_index, latest_tick);
            let (acked_seq, entries, bytes) = match self.mem_store.range(start..end, quota) {
                Some((entries, bytes)) => {
                    // Do not forward acked sequence to unmatched index.
                    let matched_acked_seq =
                        Sequence::min(acked_seq, Sequence::new(self.epoch, end - 1));
                    progress.replicate(end, 0);
                    (matched_acked_seq, entries, bytes)
                }
                // TODO(w41ter) support query indexes
                None if acked_index_advanced => {
                    // All entries are replicated, might broadcast acked
                    // sequence.
                    (acked_seq, vec![], 0)
                }
                None => continue,
            };

            end = start + entries.len() as u32;
            let write = Write {
                target: server_id.to_owned(),
                seg_epoch: self.epoch,
                epoch: writer_epoch,
                range: start..end,
                bytes,
                acked_seq,
                entries,
            };
            pending_writes.push(write);
            if !active {
                active = progress.is_acked(acked_seq.index);
            }
        }
        false
    }

    pub fn handle_received(&mut self, target: String, index: u32) {
        if let Some(progress) = self.copy_set.get_mut(&target) {
            progress.on_received(index, 0);
        }
    }

    pub fn handle_timeout(&mut self, target: String, range: Range<u32>, bytes: usize) {
        if let Some(progress) = self.copy_set.get_mut(&target) {
            progress.on_timeout(range, bytes)
        }
    }
}

pub(super) enum ToBeSealed {
    Epoch(u32),
    Rep(Box<Replicate>),
}

#[derive(Default)]
pub(super) struct Ready {
    pub still_active: bool,
    pub acked_seq: Sequence,

    pub to_be_sealed: Option<ToBeSealed>,
    pub pending_writes: Vec<Write>,
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

    latest_tick: usize,

    replicate: Box<Replicate>,
    pending_epochs: Vec<u32>,

    ready: Ready,

    flags: Flags,
}

impl Display for StreamStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stream {} epoch {}", self.stream_id, self.epoch)
    }
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
            latest_tick: 0,
            replicate_policy: ReplicatePolicy::Simple,
            acked_seq: Sequence::default(),
            replicate: Box::new(Replicate::new(INITIAL_EPOCH, vec![])),
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

    pub fn tick(&mut self) {
        self.latest_tick += 1;
    }

    pub fn promote(
        &mut self,
        epoch: u32,
        role: Role,
        leader: String,
        copy_set: Vec<String>,
        pending_epochs: Vec<u32>,
    ) -> bool {
        if self.epoch >= epoch {
            warn!(
                "stream {} epoch {} reject staled promote, epoch: {}, role: {:?}, leader: {}",
                self.stream_id, self.epoch, epoch, role, leader
            );
            return false;
        }

        let prev_epoch = std::mem::replace(&mut self.epoch, epoch);
        let prev_role = self.role;
        self.leader = leader;
        self.role = role;
        self.state = match role {
            Role::Leader => ObserverState::Leading,
            Role::Follower => ObserverState::Following,
        };
        if self.role == Role::Leader {
            self.pending_epochs = pending_epochs;
            let new_replicate = Box::new(Replicate::new(self.epoch, copy_set));
            if self.replicate.epoch + 1 == self.epoch && prev_role == self.role {
                // do fast recovery
                debug_assert_eq!(self.pending_epochs.len(), 1);
                debug_assert_eq!(self.pending_epochs[0], prev_epoch);
                let prev_replicate = std::mem::replace(&mut self.replicate, new_replicate);
                self.ready.to_be_sealed = Some(ToBeSealed::Rep(prev_replicate));
                self.pending_epochs = vec![prev_epoch];
            } else {
                // Sort in reverse to ensure that the smallest is at the end. See
                // `StreamStateMachine::handle_recovered` for details.
                self.pending_epochs.sort_by(|a, b| b.cmp(a));
            }
        }

        info!(
            "stream {} promote epoch from {} to {}, new role: {:?}, leader: {}",
            self.stream_id, prev_epoch, epoch, self.role, self.leader
        );

        true
    }

    pub fn step(&mut self, msg: Message) {
        use std::cmp::Ordering;
        match msg.epoch.cmp(&self.epoch) {
            Ordering::Less => {
                warn!(
                    "{} ignore staled msg {} from {}, epoch {}",
                    self, msg.detail, msg.target, msg.epoch
                );
                return;
            }
            Ordering::Greater => {
                todo!("should promote itself epoch");
            }
            Ordering::Equal if self.role != Role::Leader => {
                error!("{} role {} receive {}", self, self.role, msg.detail);
                return;
            }
            _ => {}
        }

        match msg.detail {
            MsgDetail::Received { index } => self.handle_received(msg.target, index),
            MsgDetail::Recovered => self.handle_recovered(msg.seg_epoch),
            MsgDetail::Timeout { range, bytes } => self.handle_timeout(msg.target, range, bytes),
            MsgDetail::Rejected => {}
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
            Ok(self.replicate.mem_store.append(entry))
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
            .advance_acked_sequence(self.epoch, &self.replicate.copy_set);
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

        self.replicate.broadcast(
            self.epoch,
            self.latest_tick,
            self.acked_seq,
            self.flags.contains(Flags::ACK_ADVANCED),
            &mut self.ready.pending_writes,
        );
    }

    fn handle_received(&mut self, target: String, index: u32) {
        debug_assert_eq!(self.role, Role::Leader);
        self.replicate.handle_received(target, index);
    }

    fn handle_recovered(&mut self, seg_epoch: u32) {
        debug_assert_eq!(self.role, Role::Leader);
        info!(
            "{} receive {}, seg epoch: {}",
            self,
            MsgDetail::Recovered,
            seg_epoch
        );

        match self.pending_epochs.pop() {
            Some(pending_epoch) if pending_epoch == seg_epoch => {
                self.ready.to_be_sealed = self.pending_epochs.last().map(|e| ToBeSealed::Epoch(*e));
            }
            _ => panic!("should't happen"),
        }
    }

    fn handle_timeout(&mut self, target: String, range: Range<u32>, bytes: usize) {
        debug_assert_eq!(self.role, Role::Leader);
        self.replicate.handle_timeout(target, range, bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_leader_receives_proposal() {
        let mut sm = StreamStateMachine::new("default".into(), 1);
        let state = sm.epoch_state();
        assert_eq!(state.role, Role::Follower);

        match sm.propose(Box::new([0u8])) {
            Err(Error::NotLeader(_)) => {}
            _ => panic!("follower do not receive proposal"),
        }

        sm.promote(
            state.epoch as u32 + 1,
            Role::Leader,
            "self".into(),
            vec![],
            vec![],
        );

        let state = sm.epoch_state();
        assert_eq!(state.role, Role::Leader);

        match sm.propose(Box::new([0u8])) {
            Ok(_) => {}
            _ => panic!("leader must receive proposal"),
        }
    }

    #[test]
    fn reject_staled_promote_request() {
        let mut sm = StreamStateMachine::new("default".into(), 1);
        let state = sm.epoch_state();
        assert_eq!(state.role, Role::Follower);

        let target_epoch = state.epoch + 2;
        assert!(sm.promote(
            target_epoch as u32,
            Role::Leader,
            "self".into(),
            vec![],
            vec![],
        ));

        let state = sm.epoch_state();
        assert_eq!(state.role, Role::Leader);
        assert_eq!(state.epoch, target_epoch);

        assert!(!sm.promote(
            (target_epoch - 1) as u32,
            Role::Leader,
            "self".into(),
            vec![],
            vec![]
        ));

        let state = sm.epoch_state();
        assert_eq!(state.role, Role::Leader);
        assert_eq!(state.epoch, target_epoch);
    }
}
