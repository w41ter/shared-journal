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

//! A stream storage implementations.

#![feature(result_into_ok_or_err)]

#[macro_use]
extern crate derivative;

mod error;
mod journal;
mod proto;

pub mod servers;

pub use self::{
    error::{Error, Result},
    journal::{build_journal, Journal, JournalOption, Role, StreamReader, StreamWriter},
};
use self::{
    journal::master::ObserverState,
    proto::{master as masterpb, seg_store as storepb},
};

const INITIAL_EPOCH: u32 = 0;

/// An increasing number to order events.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct Sequence {
    epoch: u32,
    index: u32,
}

impl Sequence {
    fn new(epoch: u32, index: u32) -> Self {
        Sequence { epoch, index }
    }
}

impl From<u64> for Sequence {
    fn from(v: u64) -> Self {
        Sequence {
            epoch: (v >> 32) as u32,
            index: (v as u32),
        }
    }
}

impl From<Sequence> for u64 {
    fn from(seq: Sequence) -> Self {
        (seq.epoch as u64) << 32 | (seq.index as u64)
    }
}

impl std::fmt::Display for Sequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", <Sequence as Into<u64>>::into(*self))
    }
}

/// `Entry` is the minimum unit of the journal system. A continuous entries
/// compound a stream.
#[derive(Derivative, Clone, PartialEq, Eq)]
#[derivative(Debug)]
enum Entry {
    /// A placeholder, used in recovery phase.
    Hole,
    Event {
        epoch: u32,
        #[derivative(Debug = "ignore")]
        event: Box<[u8]>,
    },
    /// A bridge record, which identify the end of a segment.
    Bridge { epoch: u32 },
}

impl Entry {
    // FIXME(w41ter) a better implementation.
    pub fn epoch(&self) -> u32 {
        match self {
            Entry::Event { epoch, event: _ } => *epoch,
            Entry::Bridge { epoch } => *epoch,
            _ => panic!("Entry::Hole no epoch field"),
        }
    }

    pub fn set_epoch(&mut self, target: u32) {
        match self {
            Entry::Event { epoch, event: _ } => *epoch = target,
            Entry::Bridge { epoch } => *epoch = target,
            _ => {}
        }
    }

    pub fn len(&self) -> usize {
        if let Entry::Event { event, .. } = self {
            core::mem::size_of::<Entry>() + event.len()
        } else {
            core::mem::size_of::<Entry>()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentState {
    /// This segment is receiving new appends.
    Appending,
    /// This segment is sealed and don't receive any appends.
    Sealed,
}

impl Default for SegmentState {
    fn default() -> Self {
        SegmentState::Appending
    }
}

/// `SegmentMeta` records the metadata for locating a segment and its data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    stream_id: u64,

    stream_name: String,

    state: SegmentState,

    /// A monotonic value in a stream. Allowing each segment's epoch value to be
    /// unique, it's easier to find a segment by its segment name and epoch.
    epoch: u32,

    /// Which journal server holds the segment's replica.
    copy_set: Vec<String>,
}

#[derive(Debug)]
#[allow(dead_code)]
enum ReplicaState {
    /// This replica isn't ready, it trying to copy entries from the other
    /// replicas.
    Placing,
    /// This replica is receiving entries.
    Receiving,
    /// This replica does not receive any entries.
    Sealed,
}

/// `ReplicaMeta` records the state of a replica and which segment it belongs
/// to.
#[derive(Debug)]
#[allow(dead_code)]
struct ReplicaMeta {
    stream_name: String,
    epoch: u32,
    state: ReplicaState,
}

#[cfg(test)]
#[ctor::ctor]
fn init() {
    pretty_env_logger::init();
}
