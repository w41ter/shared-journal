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

use futures::{channel::oneshot, StreamExt};

use super::{
    segment::CompoundSegmentReader,
    worker::{Channel, Command},
};
use crate::{
    journal::{
        master::{remote::RemoteMaster, Master},
        store::segment::build_compound_segment_reader,
        ReplicatePolicy,
    },
    Entry, Error, Result, Sequence,
};

#[allow(dead_code)]
pub struct StreamReader {
    policy: ReplicatePolicy,
    stream_name: String,
    current_epoch: u32,
    start_index: Option<u32>,
    segment_reader: Option<CompoundSegmentReader>,
    master: RemoteMaster,
}

#[allow(dead_code)]
impl StreamReader {
    pub(crate) fn new(policy: ReplicatePolicy, stream_name: &str, master: RemoteMaster) -> Self {
        StreamReader {
            policy,
            stream_name: stream_name.to_string(),
            current_epoch: 0,
            start_index: None,
            segment_reader: None,
            master,
        }
    }

    async fn switch_segment(&mut self) -> Result<()> {
        let segment_meta = self
            .master
            .get_segment(&self.stream_name, self.current_epoch)
            .await?;

        let segment_meta = match segment_meta {
            Some(meta) => meta,
            None => {
                return Err(Error::NotFound("no such stream".to_string()));
            }
        };

        self.segment_reader = Some(
            build_compound_segment_reader(
                self.policy,
                segment_meta.stream_id,
                segment_meta.epoch,
                segment_meta.copy_set,
                std::mem::take(&mut self.start_index),
            )
            .await?,
        );

        Ok(())
    }
}

#[allow(dead_code, unused)]
impl StreamReader {
    /// Seeks to the given sequence.
    pub async fn seek(&mut self, sequence: Sequence) -> Result<()> {
        self.current_epoch = sequence.epoch;
        self.start_index = Some(sequence.index);
        self.switch_segment().await?;
        Ok(())
    }

    /// Returns the next event or waits until it is available.
    pub async fn wait_next(&mut self) -> Result<Option<(Sequence, Box<[u8]>)>> {
        loop {
            match &mut self.segment_reader {
                None => {
                    return Err(Error::InvalidArgument("uninitialized".to_string()));
                }
                Some(reader) => {
                    let value = match reader.next().await {
                        Some(val) => val?,
                        None => {
                            self.current_epoch += 1;
                            self.switch_segment().await?;
                            continue;
                        }
                    };
                    if let Entry::Event { event, .. } = value.1 {
                        let value = (Sequence::new(self.current_epoch, value.0), event);
                        return Ok(Some(value));
                    }
                }
            }
        }
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
