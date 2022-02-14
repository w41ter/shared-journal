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

pub mod remote;

use async_trait::async_trait;
use futures::Stream;

use super::StreamMeta;
use crate::{Result, Role, SegmentMeta, Sequence};

/// The state of an stream's observer. The transition of states is:
///
/// Following -> Sealing -> Recovering -> Leading
///    ^                                    |
///    +------------------------------------+
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(crate) enum ObserverState {
    /// A leader must seals the former epochs before starting to recovery a
    /// stream.
    Sealing,
    /// A leader must recovery all unfinished replications before starting to
    /// lead a stream.
    Recovering,
    /// A leader is prepared to receive incoming events.
    Leading,
    /// A follower is prepared to follow and subscribe a stream.
    Following,
}

impl ObserverState {
    fn role(&self) -> crate::Role {
        match self {
            ObserverState::Following => crate::Role::Follower,
            ObserverState::Leading | ObserverState::Recovering | ObserverState::Sealing => {
                crate::Role::Leader
            }
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct ObserverMeta {
    pub observer_id: String,

    /// Which stream is observing?
    pub stream_name: String,

    /// The value of epoch in observer's memory.
    pub epoch: u32,

    pub state: ObserverState,

    /// The acked sequence of entries it has already known. It might less than
    /// (epoch << 32).
    pub acked_seq: Sequence,
}

/// The commands of a master must be completed by a stream observer.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum Command {
    Nop,

    /// Promote the epoch and specify the new role. When receives `Promote`
    /// command, a leader must start to seal former epochs and recover the
    /// stream.
    Promote {
        role: Role,
        epoch: u32,
        leader: String,

        /// It indicates that the segments need recovery, has not been sealed
        /// yet.
        pending_epochs: Vec<u32>,
    },
}

/// An abstraction of master of shared journal.
#[async_trait]
pub(crate) trait Master: Clone {
    type MetaStream: Stream<Item = Result<SegmentMeta>>;
    type ListStream: Stream<Item = Result<StreamMeta>>;

    /// Create new stream with corresponding name.
    ///
    /// # Errors
    ///
    /// Returns `Error::AlreadyExists` if the stream is exist.
    async fn create_stream(&self, stream_name: &str) -> Result<()>;

    /// List all streams.
    async fn list_stream(&self) -> Result<Self::ListStream>;

    /// Query the meta of stream.
    async fn get_stream(&self, stream_name: &str) -> Result<Option<StreamMeta>>;

    /// Sends the state of a stream observer to master, and receives commands.
    async fn heartbeat(&self, observer_meta: ObserverMeta) -> Result<Vec<Command>>;

    /// Query the meta of segments covered by the specified sequence range.
    async fn query_segments(
        &self,
        stream_name: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Self::MetaStream>;

    /// Get segment meta of the specified epoch of a stream.
    async fn get_segment(&self, stream_name: &str, epoch: u32) -> Result<Option<SegmentMeta>>;

    /// Mark the corresponding segment as sealed.  The request is ignored if the
    /// target segment is already sealed.
    async fn seal_segment(&self, stream_id: u64) -> Result<()>;
}
