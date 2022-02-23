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

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    ops::Range,
};

use bitflags::bitflags;
use log::{error, info, warn};

use super::{EpochState, MemStore, Progress, ReplicatePolicy};
use crate::{
    journal::{
        master::ObserverState,
        policy::{GroupReader, GroupState, ReaderState},
    },
    Entry, Error, Result, Role, Sequence, INITIAL_EPOCH,
};

pub(crate) struct Learn {
    pub target: String,
    pub seg_epoch: u32,
    pub writer_epoch: u32,
    pub start_index: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct Mutate {
    pub target: String,
    pub seg_epoch: u32,
    pub writer_epoch: u32,
    pub kind: MutKind,
}

#[derive(Debug, Clone)]
pub(crate) enum MutKind {
    Write(Write),
    Seal,
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub(crate) struct Write {
    pub acked_seq: Sequence,
    pub range: Range<u32>,
    pub bytes: usize,
    #[derivative(Debug = "ignore")]
    pub entries: Vec<Entry>,
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub(crate) struct Learned {
    // The end is reached if entries is empty.
    pub entries: Vec<(u32, Entry)>,
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
#[allow(unused)]
pub(crate) enum MsgDetail {
    Received { index: u32 },
    Recovered,
    Rejected,
    Timeout { range: Range<u32>, bytes: usize },
    Sealed { acked_index: u32 },
    Learned(Learned),
}

impl Display for MsgDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let desc = match self {
            MsgDetail::Received { .. } => "RECEIVED",
            MsgDetail::Recovered => "RECOVERED",
            MsgDetail::Rejected => "REJECTED",
            MsgDetail::Timeout { .. } => "TIMEOUT",
            MsgDetail::Sealed { .. } => "SEALED",
            MsgDetail::Learned(_) => "LEARNED",
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

struct LearnedProgress {
    reader_state: ReaderState,
    terminated: bool,
    entries: Vec<(u32, Entry)>,
}

impl LearnedProgress {
    pub fn new() -> LearnedProgress {
        LearnedProgress {
            reader_state: ReaderState::Polling,
            terminated: false,
            entries: Vec::new(),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.terminated || !self.entries.is_empty()
    }

    pub fn append(&mut self, mut entries: Vec<(u32, Entry)>) {
        if entries.is_empty() {
            self.terminated = true;
        } else {
            self.entries.append(&mut entries);
        }
    }
}

enum LearningState {
    None,
    Sealing {
        acked_indexes: Vec<u32>,
        /// Need send SEAL request to this target.
        pending: HashSet<String>,
    },
    Learning {
        actual_acked_index: u32,
        learned_progress: HashMap<String, LearnedProgress>,
        group_reader: GroupReader,
        /// Need send LEARN request to this target.
        pending: HashSet<String>,
    },
    Terminated,
}

/// A structure who responsible for sending received proposals to stores. There
/// will be one `Replicate` for each epoch, this `Replicate` is also responsible
/// for sealing segment and learning entries during recovery.
pub(super) struct Replicate {
    pub epoch: u32,
    writer_epoch: u32,

    policy: ReplicatePolicy,
    mem_store: MemStore,
    copy_set: HashMap<String, Progress>,

    sealed_set: HashSet<String>,
    learning_state: LearningState,
}

impl Replicate {
    pub fn new(
        epoch: u32,
        writer_epoch: u32,
        copy_set: Vec<String>,
        policy: ReplicatePolicy,
    ) -> Self {
        Replicate {
            epoch,
            writer_epoch,
            policy,
            mem_store: MemStore::new(epoch),
            copy_set: copy_set
                .into_iter()
                .map(|c| (c, Progress::new(epoch)))
                .collect(),
            sealed_set: HashSet::new(),
            learning_state: LearningState::None,
        }
    }

    pub fn recovery(
        epoch: u32,
        writer_epoch: u32,
        copy_set: Vec<String>,
        policy: ReplicatePolicy,
    ) -> Self {
        let pending = copy_set.iter().map(Clone::clone).collect();
        Replicate {
            learning_state: LearningState::Sealing {
                acked_indexes: Vec::default(),
                pending,
            },
            ..Replicate::new(epoch, writer_epoch, copy_set, policy)
        }
    }

    fn all_target_matched(&self) -> bool {
        let last_index = self.mem_store.next_index().saturating_sub(1);
        matches!(self.learning_state, LearningState::Terminated)
            && self.copy_set.iter().all(|(_, p)| p.is_matched(last_index))
    }

    pub fn broadcast(
        &mut self,
        ready: &mut Ready,
        latest_tick: usize,
        mut acked_seq: Sequence,
        acked_index_advanced: bool,
    ) -> bool {
        match &mut self.learning_state {
            LearningState::None | LearningState::Terminated => {
                let terminated = matches!(self.learning_state, LearningState::Terminated);
                if terminated {
                    acked_seq = Sequence::new(self.writer_epoch, 0);
                }
                let mut active = false;
                for (server_id, progress) in &mut self.copy_set {
                    let next_index = self.mem_store.next_index();
                    let (Range { start, mut end }, quota) =
                        progress.next_chunk(next_index, latest_tick);
                    let (acked_seq, entries, bytes) = match self.mem_store.range(start..end, quota)
                    {
                        Some((entries, bytes)) => {
                            // Do not forward acked sequence to unmatched index.
                            let matched_acked_seq =
                                Sequence::min(acked_seq, Sequence::new(self.epoch, end - 1));
                            progress.replicate(end, 0);
                            (matched_acked_seq, entries, bytes)
                        }
                        // TODO(w41ter) support query indexes
                        None if !terminated && acked_index_advanced => {
                            // All entries are replicated, might broadcast acked
                            // sequence.
                            (acked_seq, vec![], 0)
                        }
                        None => continue,
                    };

                    end = start + entries.len() as u32;
                    let write = Mutate {
                        target: server_id.to_owned(),
                        seg_epoch: self.epoch,
                        writer_epoch: self.writer_epoch,
                        kind: MutKind::Write(Write {
                            range: start..end,
                            bytes,
                            acked_seq,
                            entries,
                        }),
                    };
                    ready.pending_writes.push(write);
                    if !active {
                        active = progress.is_acked(acked_seq.index);
                    }
                }
                false
            }
            LearningState::Sealing { pending, .. } => {
                for target in std::mem::take(pending) {
                    ready.pending_writes.push(Mutate {
                        target,
                        seg_epoch: self.epoch,
                        writer_epoch: self.writer_epoch,
                        kind: MutKind::Seal,
                    });
                }

                // it should broadcast sealed request first.
                // and wait
                false
            }
            LearningState::Learning {
                pending,
                learned_progress,
                actual_acked_index,
                ..
            } => {
                for target in std::mem::take(pending) {
                    // Learn from the previous breakpoint. If no data has been read before, we need
                    // to start reading from acked_index, the reason is that we don't know whether a
                    // bridge entry has already been committed.
                    let start_index = learned_progress
                        .get(&target)
                        .and_then(|p| p.entries.last().map(|e| e.0 + 1))
                        .unwrap_or(*actual_acked_index);
                    let learn = Learn {
                        target,
                        seg_epoch: self.epoch,
                        writer_epoch: self.writer_epoch,
                        start_index,
                    };
                    ready.pending_learns.push(learn);
                }

                // TODO(w41ter) We also would replicate entries.
                false
            }
        }
    }

    /// Begin recovering from a normal replicate.
    pub fn become_recovery(&mut self, new_epoch: u32) {
        debug_assert!(matches!(self.learning_state, LearningState::None));

        self.writer_epoch = new_epoch;
        self.become_terminated(0);
    }

    pub fn handle_received(&mut self, target: &str, index: u32) {
        if let Some(progress) = self.copy_set.get_mut(target) {
            progress.on_received(index, 0);
        }
    }

    pub fn handle_timeout(&mut self, target: &str, range: Range<u32>, bytes: usize) {
        match &mut self.learning_state {
            LearningState::None | LearningState::Terminated => {
                if let Some(progress) = self.copy_set.get_mut(target) {
                    progress.on_timeout(range, bytes)
                }
            }
            LearningState::Sealing { pending, .. } => {
                // resend to target again.
                pending.insert(target.to_owned());
            }
            LearningState::Learning { pending, .. } => {
                if range.end < range.start {
                    // it means timeout by learning
                    // resend to target again.
                    pending.insert(target.to_owned());
                } else {
                    // otherwise timeout by replicating
                    if let Some(progress) = self.copy_set.get_mut(target) {
                        progress.on_timeout(range, bytes)
                    }
                }
            }
        }
    }

    pub fn handle_sealed(&mut self, target: &str, acked_index: u32) {
        if self.sealed_set.contains(target) {
            return;
        }

        self.sealed_set.insert(target.into());
        if let LearningState::Sealing { acked_indexes, .. } = &mut self.learning_state {
            acked_indexes.push(acked_index);
            if let Some(actual_acked_index) = self
                .policy
                .actual_acked_index(self.copy_set.len(), acked_indexes)
            {
                // We have received satisfied SEALED response, now changes state to learn
                // pending entries.
                self.become_learning(actual_acked_index);
            }
        }
    }

    pub fn handle_learned(&mut self, target: &str, entries: Vec<(u32, Entry)>) {
        if let LearningState::Learning {
            learned_progress,
            group_reader,
            actual_acked_index,
            ..
        } = &mut self.learning_state
        {
            if let Some(progress) = learned_progress.get_mut(target) {
                progress.append(entries);
            }

            loop {
                Self::consume_learned_entries(learned_progress, group_reader);
                let next_index = group_reader.next_index();
                if let Some(mut entry) =
                    Self::take_next_entry(self.epoch, learned_progress, group_reader)
                {
                    if !matches!(entry, Entry::Bridge { .. }) {
                        // We must read the last one of acked entry to make sure it's not a bridge.
                        if *actual_acked_index < next_index {
                            entry.set_epoch(self.writer_epoch);
                            self.mem_store.append(entry);
                        }
                        continue;
                    }

                    let actual_acked_index = *actual_acked_index;
                    self.become_terminated(actual_acked_index);
                }
                break;
            }
        }
    }

    fn become_terminated(&mut self, actual_acked_index: u32) {
        // The next index is the `actual_acked_index`, means that all entries in all
        // stores are acked and a bridge record exists.  We could skip append new bridge
        // record in that case.
        if actual_acked_index < self.mem_store.next_index() {
            self.mem_store.append(Entry::Bridge {
                epoch: self.writer_epoch,
            });
        }
        self.learning_state = LearningState::Terminated;
    }

    fn become_learning(&mut self, actual_acked_index: u32) {
        let learned_progress = self
            .copy_set
            .keys()
            .map(|k| (k.to_owned(), LearnedProgress::new()))
            .collect();
        let group_reader = self
            .policy
            .new_group_reader(actual_acked_index, self.copy_set.len());
        self.learning_state = LearningState::Learning {
            actual_acked_index,
            learned_progress,
            group_reader,
            pending: self.sealed_set.clone(),
        };

        // All progress has already received all acked entries.
        for progress in self.copy_set.values_mut() {
            progress.on_received(actual_acked_index, actual_acked_index);
        }
        // FIXME(w41ter) update entries epoch.
        self.mem_store = MemStore::recovery(self.writer_epoch, actual_acked_index + 1);
    }

    fn take_next_entry(
        epoch: u32,
        learned_progress: &mut HashMap<String, LearnedProgress>,
        group_reader: &mut GroupReader,
    ) -> Option<Entry> {
        match group_reader.state() {
            GroupState::Active => group_reader
                .next_entry(
                    learned_progress
                        .iter_mut()
                        .map(|(_, p)| &mut p.reader_state),
                )
                .or(Some(Entry::Hole)),
            GroupState::Done => Some(Entry::Bridge { epoch }),
            GroupState::Pending => None,
        }
    }

    fn consume_learned_entries(
        learned_progress: &mut HashMap<String, LearnedProgress>,
        group_reader: &mut GroupReader,
    ) {
        for progress in learned_progress.values_mut() {
            if let ReaderState::Polling = progress.reader_state {
                if !progress.is_ready() {
                    continue;
                }

                group_reader.transform(&mut progress.reader_state, progress.entries.pop());
            }
        }
    }
}

#[derive(Default)]
pub(super) struct Ready {
    pub still_active: bool,
    pub acked_seq: Sequence,

    pub pending_writes: Vec<Mutate>,
    pub pending_learns: Vec<Learn>,
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
    recovering_replicates: HashMap<u32, Box<Replicate>>,

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
            replicate: Box::new(Replicate::new(
                INITIAL_EPOCH,
                INITIAL_EPOCH,
                vec![],
                ReplicatePolicy::Simple,
            )),
            ready: Ready::default(),
            flags: Flags::NONE,
            // pending_epochs: Vec::default(),
            recovering_replicates: HashMap::new(),
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
        pending_epochs: Vec<(u32, Vec<String>)>,
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
            let new_replicate = Box::new(Replicate::new(
                self.epoch,
                self.epoch,
                copy_set,
                self.replicate_policy,
            ));
            if self.replicate.epoch + 1 == self.epoch && prev_role == self.role {
                // do fast recovery
                debug_assert_eq!(pending_epochs.len(), 1);
                debug_assert_eq!(pending_epochs[0].0, prev_epoch);

                // TODO(w41ter) when we recovery, what happen if the previous epoch is still
                // recovery?.
                debug_assert!(self.recovering_replicates.is_empty());
                let mut prev_replicate = std::mem::replace(&mut self.replicate, new_replicate);
                prev_replicate.become_recovery(self.epoch);
                self.recovering_replicates
                    .insert(prev_epoch, prev_replicate);
            } else {
                // Sort in reverse to ensure that the smallest is at the end. See
                // `StreamStateMachine::handle_recovered` for details.
                self.recovering_replicates = pending_epochs
                    .into_iter()
                    .map(|(epoch, copy_set)| {
                        (
                            epoch,
                            Box::new(Replicate::recovery(
                                epoch,
                                self.epoch,
                                copy_set,
                                self.replicate_policy,
                            )),
                        )
                    })
                    .collect();
                self.replicate = new_replicate;
            }
            debug_assert!(self.recovering_replicates.len() <= 2);
        }

        info!(
            "stream {} promote epoch from {} to {}, new role {:?}, leader {}, num copy {}",
            self.stream_id,
            prev_epoch,
            epoch,
            self.role,
            self.leader,
            self.replicate.copy_set.len(),
        );

        true
    }

    pub fn step(&mut self, msg: Message) {
        use std::cmp::Ordering;
        match msg.epoch.cmp(&self.epoch) {
            Ordering::Less => {
                // FIXME(w41ter) When fast recovery is executed, the in-flights messages's epoch
                // might be staled.
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
            MsgDetail::Received { index } => {
                self.handle_received(&msg.target, msg.seg_epoch, index)
            }
            MsgDetail::Recovered => self.handle_recovered(msg.seg_epoch),
            MsgDetail::Timeout { range, bytes } => {
                self.handle_timeout(&msg.target, msg.seg_epoch, range, bytes)
            }
            MsgDetail::Learned(learned) => {
                self.handle_learned(&msg.target, msg.seg_epoch, learned.entries)
            }
            MsgDetail::Sealed { acked_index } => {
                self.handle_sealed(&msg.target, msg.seg_epoch, acked_index)
            }
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
        if !self.recovering_replicates.is_empty() {
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

        if let Some(epoch) = self.recovering_replicates.keys().min().cloned() {
            self.recovering_replicates
                .get_mut(&epoch)
                .unwrap()
                .broadcast(&mut self.ready, self.latest_tick, self.acked_seq, false);
        }

        // Do not replicate entries if there exists two pending segments.
        if self.recovering_replicates.len() == 2 {
            return;
        }

        self.replicate.broadcast(
            &mut self.ready,
            self.latest_tick,
            self.acked_seq,
            self.flags.contains(Flags::ACK_ADVANCED),
        );
    }

    fn handle_received(&mut self, target: &str, epoch: u32, index: u32) {
        debug_assert_eq!(self.role, Role::Leader);
        if self.epoch == epoch {
            self.replicate.handle_received(target, index);
        } else if let Some(replicate) = self.recovering_replicates.get_mut(&epoch) {
            replicate.handle_received(target, index);
            if replicate.all_target_matched() {
                // FIXME(w41ter) Shall I use a special value to identify master sealing
                // operations?
                self.ready.pending_writes.push(Mutate {
                    target: "<MASTER>".into(),
                    seg_epoch: epoch,
                    writer_epoch: self.epoch,
                    kind: MutKind::Seal,
                });
            }
        } else {
            warn!(
                "{} receive staled RECEIVED from {}, epoch {}",
                self, target, epoch
            );
        }
    }

    fn handle_timeout(&mut self, target: &str, epoch: u32, range: Range<u32>, bytes: usize) {
        debug_assert_eq!(self.role, Role::Leader);
        if self.epoch == epoch {
            self.replicate.handle_timeout(target, range, bytes);
        } else if let Some(replicate) = self.recovering_replicates.get_mut(&epoch) {
            replicate.handle_timeout(target, range, bytes);
        } else {
            warn!(
                "{} receive staled TIMEOUT from {}, epoch {}",
                self, target, epoch
            );
        }
    }

    fn handle_learned(&mut self, target: &str, epoch: u32, entries: Vec<(u32, Entry)>) {
        debug_assert_eq!(self.role, Role::Leader);
        debug_assert_ne!(self.epoch, epoch, "current epoch don't need to recovery");
        if let Some(replicate) = self.recovering_replicates.get_mut(&epoch) {
            replicate.handle_learned(target, entries);
        } else {
            warn!("{} receive staled LEARNED of epoch {}", self, epoch);
        }
    }

    fn handle_sealed(&mut self, target: &str, epoch: u32, acked_index: u32) {
        debug_assert_eq!(self.role, Role::Leader);
        debug_assert_ne!(self.epoch, epoch, "current epoch don't need to recovery");
        if let Some(replicate) = self.recovering_replicates.get_mut(&epoch) {
            replicate.handle_sealed(target, acked_index);
        } else {
            warn!("{} receive staled SEALED of epoch {}", self, epoch);
        }
    }

    fn handle_recovered(&mut self, seg_epoch: u32) {
        debug_assert_eq!(self.role, Role::Leader);
        info!(
            "{} receive {}, seg epoch: {}, num pending epochs {}",
            self,
            MsgDetail::Recovered,
            seg_epoch,
            self.recovering_replicates.len()
        );

        self.recovering_replicates.remove(&seg_epoch);
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
