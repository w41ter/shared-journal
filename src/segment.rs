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

//! A stream of a journal is divided into multiple segments which distributed
//! in journal servers. A segment has multiple replicas which distributed in
//! journal servers.
//!
//! The sequence of entries within a segment is continuous, but it is not
//! guaranteed across segments.
//!
//! Entry sequence = (epoch << 32) | index of entry.

use futures::{StreamExt, TryStreamExt};
use tonic::Streaming;

use crate::{server::Client, serverpb, Entry, Result, SegmentMeta};

#[allow(unused)]
pub(crate) struct SegmentReader {
    client: Client,
    entries_stream: Streaming<serverpb::Entry>,
}

#[allow(dead_code)]
impl SegmentReader {
    fn new(client: Client, entries_stream: Streaming<serverpb::Entry>) -> Self {
        SegmentReader {
            client,
            entries_stream,
        }
    }
}

#[allow(unused)]
impl SegmentReader {
    /// Returns the next entry.
    pub async fn try_next(&mut self) -> Result<Option<Entry>> {
        Ok(self.entries_stream.try_next().await?.map(Into::into))
    }

    /// Returns the next entry or waits until it is available.
    /// A None means that the stream has already terminated.
    pub async fn watch_next(&mut self) -> Result<Option<Entry>> {
        match self.entries_stream.next().await {
            Some(r) => Ok(Some(r?.into())),
            None => Ok(None),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SegmentReadPolicy {
    Acked { start: u32, limit: u32 },
    Pending,
}

enum SegmentClientOpt {
    None,
    Address(String),
    Client(Client),
}

pub(crate) struct SegmentReaderBuilder {
    stream_id: u64,
    epoch: u32,

    client: SegmentClientOpt,
    read_policy: SegmentReadPolicy,
}

#[allow(dead_code)]
impl SegmentReaderBuilder {
    pub fn new(stream_id: u64, epoch: u32) -> Self {
        SegmentReaderBuilder {
            stream_id,
            epoch,
            client: SegmentClientOpt::None,
            read_policy: SegmentReadPolicy::Pending,
        }
    }

    /// Set remote address.
    pub fn bind(mut self, addr: &str) -> Self {
        self.client = SegmentClientOpt::Address(addr.to_owned());
        self
    }

    /// Set segment client address.
    pub fn set_client(mut self, client: Client) -> Self {
        self.client = SegmentClientOpt::Client(client);
        self
    }

    /// Seeks to the given index in this segment.
    pub fn read_acked_entries(mut self, start: u32, limit: u32) -> Self {
        self.read_policy = SegmentReadPolicy::Acked { start, limit };
        self
    }

    /// Seeks to the first pending entry.
    pub fn read_pending_entires(mut self) -> Self {
        self.read_policy = SegmentReadPolicy::Pending;
        self
    }

    pub async fn build(self) -> Result<SegmentReader> {
        let client = match self.client {
            SegmentClientOpt::None => panic!("Please setup the client address"),
            SegmentClientOpt::Address(addr) => Client::connect(&addr).await?,
            SegmentClientOpt::Client(client) => client,
        };

        let entries_stream = match self.read_policy {
            SegmentReadPolicy::Acked { start, limit } => {
                let req = serverpb::ReadRequest {
                    stream_id: self.stream_id,
                    seg_epoch: self.epoch,
                    start_index: start,
                    limit,
                };
                client.read(req).await?
            }
            SegmentReadPolicy::Pending => {
                unimplemented!();
            }
        };

        Ok(SegmentReader::new(client, entries_stream))
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct WriteRequest {
    /// The epoch of write request initiator, it's not always equal to segment's
    /// epoch.
    epoch: u32,
    /// The first index of entries.
    index: u32,
    /// The sequence of acked entries which:
    ///  1. the number of replicas is satisfied replication policy.
    ///  2. all previous entries are acked.
    acked: u64,
    entries: Vec<Entry>,
}

pub(crate) struct SegmentWriter {
    meta: SegmentMeta,
    client: Client,
}

#[allow(dead_code)]
impl SegmentWriter {
    pub async fn new(meta: SegmentMeta, replica: String) -> Result<Self> {
        let client = Client::connect(&replica).await?;
        Ok(SegmentWriter { meta, client })
    }
}

#[allow(dead_code, unused)]
impl SegmentWriter {
    /// Seal the `store` operations of current segment, and any write operations
    /// issued with a small epoch will be rejected.
    pub async fn seal(&self, new_epoch: u32) -> Result<()> {
        todo!()
    }

    /// Store continuously entries with assigned index.
    pub async fn store(&self, write: WriteRequest) -> Result<()> {
        let events = write
            .entries
            .into_iter()
            .filter_map(|e| match e {
                Entry::Event { event, epoch } => Some(event.into()),
                _ => None,
            })
            .collect();
        let req = serverpb::StoreRequest {
            stream_id: self.meta.stream_id,
            seg_epoch: self.meta.epoch,
            acked_seq: write.acked,
            first_index: write.index,
            epoch: write.epoch,
            events,
        };

        self.client.store(req).await?;
        Ok(())
    }
}
