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

use futures::channel::oneshot;

use super::worker::{Channel, Command};
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

pub struct StreamWriter {
    channel: Channel,
}

impl StreamWriter {
    pub(crate) fn new(channel: Channel) -> Self {
        StreamWriter { channel }
    }
}

#[allow(dead_code, unused)]
impl StreamWriter {
    /// Appends an event, returns the sequence of the event just append.
    pub async fn append(&mut self, event: Vec<u8>) -> Result<Sequence> {
        let (sender, receiver) = oneshot::channel();
        let proposal = Command::Proposal {
            event: event.into(),
            sender,
        };
        self.channel.submit(proposal);
        receiver.await?
    }

    /// Truncates events up to a sequence (exclusive).
    async fn truncate(&mut self, sequence: Sequence) -> Result<()> {
        todo!();
    }
}
