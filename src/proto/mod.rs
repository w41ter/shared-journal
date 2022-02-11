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

use super::{Entry, ObserverState, Role, SegmentState};

pub mod seg_store {
    tonic::include_proto!("engula.journal.v1.shared.seg_store");
}

impl From<Entry> for seg_store::Entry {
    fn from(e: Entry) -> Self {
        match e {
            Entry::Hole => seg_store::Entry {
                entry_type: seg_store::EntryType::Hole as i32,
                epoch: 0,
                event: vec![],
            },
            Entry::Event { epoch, event } => seg_store::Entry {
                entry_type: seg_store::EntryType::Event as i32,
                epoch,
                event: event.into(),
            },
            Entry::Bridge { epoch } => seg_store::Entry {
                entry_type: seg_store::EntryType::Bridge as i32,
                epoch,
                event: vec![],
            },
        }
    }
}

impl From<seg_store::Entry> for Entry {
    fn from(e: seg_store::Entry) -> Self {
        match seg_store::EntryType::from_i32(e.entry_type) {
            Some(seg_store::EntryType::Event) => Entry::Event {
                event: e.event.into(),
                epoch: e.epoch,
            },
            Some(seg_store::EntryType::Bridge) => Entry::Bridge { epoch: e.epoch },
            _ => Entry::Hole,
        }
    }
}

pub mod master {
    tonic::include_proto!("engula.journal.v1.shared.master");
}

impl From<Role> for i32 {
    fn from(role: Role) -> Self {
        match role {
            Role::Follower => master::Role::Follower as i32,
            Role::Leader => master::Role::Leader as i32,
        }
    }
}

impl From<i32> for Role {
    fn from(role: i32) -> Self {
        match master::Role::from_i32(role) {
            None | Some(master::Role::Follower) => Role::Follower,
            Some(master::Role::Leader) => Role::Leader,
        }
    }
}

impl From<ObserverState> for i32 {
    fn from(state: ObserverState) -> Self {
        match state {
            ObserverState::Following => master::ObserverState::Following as i32,
            ObserverState::Sealing => master::ObserverState::Sealing as i32,
            ObserverState::Recovering => master::ObserverState::Recovering as i32,
            ObserverState::Leading => master::ObserverState::Leading as i32,
        }
    }
}

impl From<i32> for ObserverState {
    fn from(state: i32) -> Self {
        match master::ObserverState::from_i32(state) {
            None | Some(master::ObserverState::Following) => ObserverState::Following,
            Some(master::ObserverState::Sealing) => ObserverState::Sealing,
            Some(master::ObserverState::Recovering) => ObserverState::Recovering,
            Some(master::ObserverState::Leading) => ObserverState::Leading,
        }
    }
}

impl From<i32> for SegmentState {
    fn from(state: i32) -> Self {
        match master::SegmentState::from_i32(state) {
            None | Some(master::SegmentState::Appending) => SegmentState::Appending,
            Some(master::SegmentState::Sealed) => SegmentState::Sealed,
        }
    }
}

impl From<SegmentState> for i32 {
    fn from(state: SegmentState) -> Self {
        match state {
            SegmentState::Appending => master::SegmentState::Appending as i32,
            SegmentState::Sealed => master::SegmentState::Sealed as i32,
        }
    }
}
