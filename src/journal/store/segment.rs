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

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures::{StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use tonic::Streaming;

use super::remote::Client;
use crate::{
    journal::{segment::CompoundSegmentReader, ReplicatePolicy},
    storepb, Entry, Error, Result, Sequence,
};

pub(crate) struct SegmentReader {
    entries_stream: Streaming<storepb::ReadResponse>,
}

impl SegmentReader {
    #[allow(dead_code)]
    fn new(entries_stream: Streaming<storepb::ReadResponse>) -> Self {
        SegmentReader { entries_stream }
    }
}

#[allow(dead_code)]
impl SegmentReader {
    /// Returns the next entry.
    pub async fn try_next(&mut self) -> Result<Option<Entry>> {
        Ok(self
            .entries_stream
            .try_next()
            .await?
            .map(|resp| resp.entry.unwrap().into()))
    }

    /// Returns the next entry or waits until it is available.
    /// A None means that the stream has already terminated.
    pub async fn watch_next(&mut self) -> Result<Option<Entry>> {
        match self.entries_stream.next().await {
            Some(r) => Ok(Some(r?.entry.unwrap().into())),
            None => Ok(None),
        }
    }
}

pub(crate) struct WriteRequest {
    /// The epoch of write request initiator, it's not always equal to segment's
    /// epoch.
    pub epoch: u32,
    /// The first index of entries. It should always greater that zero, see
    /// `journal::worker::Progress` for details.
    pub index: u32,
    /// The sequence of acked entries which:
    ///  1. the number of replicas is satisfied replication policy.
    ///  2. all previous entries are acked.
    pub acked: Sequence,
    pub entries: Vec<Entry>,
}

impl std::fmt::Debug for WriteRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteRequest")
            .field("epoch", &self.epoch)
            .field("index", &self.index)
            .field("acked", &self.acked)
            .field("entries_len", &self.entries.len())
            .finish()
    }
}

lazy_static! {
    static ref CLIENTS: Arc<Mutex<HashMap<String, Client>>> = Arc::new(Mutex::new(HashMap::new()));
}

#[derive(Clone)]
pub(crate) struct SegmentWriter {
    stream_id: u64,
    epoch: u32,
    replica: String,
    client: Option<Client>,
}

impl SegmentWriter {
    pub fn new(stream_id: u64, epoch: u32, replica: String) -> Self {
        SegmentWriter {
            stream_id,
            epoch,
            replica,
            client: None,
        }
    }
}

impl SegmentWriter {
    /// Seal the `store` operations of current segment, and any write operations
    /// issued with a small epoch will be rejected.
    ///
    /// SAFETY: implementation guaranteed to no cross-await-point references.
    pub async fn seal(&mut self, new_epoch: u32) -> Result<u32> {
        let client = self.get_client().await?;
        let req = storepb::SealRequest {
            stream_id: self.stream_id,
            seg_epoch: self.epoch,
            epoch: new_epoch,
        };

        let resp = client.seal(req).await?;
        Ok(resp.acked_index)
    }

    /// Store continuously entries with assigned index.
    pub async fn write(&mut self, write: WriteRequest) -> Result<Sequence> {
        if write.index == 0 {
            return Err(Error::InvalidArgument(
                "index should always greater than zero".to_owned(),
            ));
        }

        let client = self.get_client().await?;

        let entries = write.entries.into_iter().map(Into::into).collect();
        let req = storepb::WriteRequest {
            stream_id: self.stream_id,
            seg_epoch: self.epoch,
            acked_seq: write.acked.into(),
            first_index: write.index,
            epoch: write.epoch,
            entries,
        };

        let resp = client.write(req).await?;

        Ok(resp.persisted_seq.into())
    }

    async fn get_client(&mut self) -> Result<Client> {
        if self.client.is_none() {
            // 1. query local CLIENTS
            {
                let clients = CLIENTS.lock().unwrap();
                if let Some(client) = clients.get(&self.replica) {
                    self.client = Some(client.clone());
                }
            }

            // 2. alloc new connection
            if self.client.is_none() {
                // FIXME(w41ter) too many concurrent connections.
                let client = Client::connect(&self.replica).await?;
                self.client = Some(client.clone());
                let mut clients = CLIENTS.lock().unwrap();
                clients.insert(self.replica.clone(), client);
            }
        }

        Ok(self.client.as_ref().cloned().unwrap())
    }
}

pub(crate) async fn build_compound_segment_reader(
    policy: ReplicatePolicy,
    stream_id: u64,
    epoch: u32,
    copy_set: Vec<String>,
    start: Option<u32>,
) -> Result<CompoundSegmentReader> {
    // FIXME(w41ter) more efficient implementation.
    let mut streamings = vec![];
    for addr in copy_set {
        let client = Client::connect(&addr).await?;
        let req = storepb::ReadRequest {
            stream_id,
            seg_epoch: epoch,
            start_index: start.unwrap_or(1),
            include_pending_entries: false,
            limit: 0,
        };
        streamings.push(client.read(req).await?);
    }

    Ok(CompoundSegmentReader::new(
        policy,
        epoch,
        start.unwrap_or(1),
        streamings,
    ))
}
