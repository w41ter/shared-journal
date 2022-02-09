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
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{Stream, StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use tonic::Streaming;

use super::Client;
use crate::{serverpb, Entry, Error, Result, Sequence};

#[allow(unused)]
pub(crate) struct SegmentReader {
    client: Client,
    entries_stream: Streaming<serverpb::ReadResponse>,
}

#[allow(dead_code)]
impl SegmentReader {
    fn new(client: Client, entries_stream: Streaming<serverpb::ReadResponse>) -> Self {
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

#[allow(dead_code)]
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
    pub acked: u64,
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

#[allow(dead_code)]
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

#[allow(dead_code, unused)]
impl SegmentWriter {
    /// Seal the `store` operations of current segment, and any write operations
    /// issued with a small epoch will be rejected.
    ///
    /// SAFETY: implementation guaranteed to no cross-await-point references.
    pub async fn seal(&mut self, new_epoch: u32) -> Result<u32> {
        let client = self.get_client().await?;
        let req = serverpb::SealRequest {
            stream_id: self.stream_id,
            seg_epoch: self.epoch,
            epoch: new_epoch,
        };

        let resp = client.seal(req).await?;
        Ok(resp.acked_index)
    }

    /// Store continuously entries with assigned index.
    pub async fn store(&mut self, write: WriteRequest) -> Result<Sequence> {
        if write.index == 0 {
            return Err(Error::InvalidArgument(
                "index should always greater than zero".to_owned(),
            ));
        }

        let client = self.get_client().await?;

        let entries = write.entries.into_iter().map(Into::into).collect();
        let req = serverpb::StoreRequest {
            stream_id: self.stream_id,
            seg_epoch: self.epoch,
            acked_seq: write.acked,
            first_index: write.index,
            epoch: write.epoch,
            entries,
        };

        let resp = client.store(req).await?;

        Ok(resp.persisted_seq)
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

#[derive(Debug, Clone)]
enum ReaderState {
    None,
    Polling,
    Ready { index: u32, entry: Entry },
    Done,
}

struct Reader {
    state: ReaderState,
    entries_stream: Streaming<serverpb::ReadResponse>,
}

pub(crate) struct CompoundSegmentReader {
    readers: Vec<Reader>,
}

#[allow(dead_code)]
impl CompoundSegmentReader {
    fn new(streams: Vec<Streaming<serverpb::ReadResponse>>) -> Self {
        CompoundSegmentReader {
            readers: streams
                .into_iter()
                .map(|stream| Reader {
                    state: ReaderState::None,
                    entries_stream: stream,
                })
                .collect(),
        }
    }

    fn step(&mut self, cx: &mut Context<'_>) -> Result<bool> {
        let mut active = false;
        for reader in &mut self.readers {
            if let ReaderState::None = &reader.state {
                let mut try_next = reader.entries_stream.try_next();
                match Pin::new(&mut try_next).poll(cx) {
                    Poll::Pending => {
                        reader.state = ReaderState::Polling;
                    }
                    Poll::Ready(Ok(Some(resp))) => {
                        active = true;
                        reader.state = ReaderState::Ready {
                            index: resp.index,
                            entry: resp.entry.unwrap().into(),
                        };
                    }
                    Poll::Ready(Ok(None)) => {
                        active = true;
                        reader.state = ReaderState::Done;
                    }
                    Poll::Ready(Err(err)) => {
                        return Err(err.into());
                    }
                }
            }
        }
        Ok(active)
    }
}

impl Stream for CompoundSegmentReader {
    type Item = Result<(u32, Box<[u8]>)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match this.step(cx) {
                Err(err) => return Poll::Ready(Some(Err(err))),
                Ok(false) => return Poll::Pending,
                Ok(true) => {}
            };

            // TODO(w41ter) support replication policy.
            if this
                .readers
                .iter()
                .filter_map(|reader| match &reader.state {
                    // FIXME(w41ter) now we only assume entries returned from store is continuous.
                    ReaderState::Ready { index: _, entry: _ } => Some(()),
                    _ => None,
                })
                .count()
                > 1
            {
                let mut entry_opt = None;
                let mut index = 0;
                for reader in &mut this.readers {
                    if let ReaderState::Ready { index: i, entry } = &mut reader.state {
                        entry_opt = Some(entry.clone());
                        index = *i;
                        reader.state = ReaderState::None;
                    }
                }

                if let Entry::Event { epoch: _, event } = entry_opt.unwrap() {
                    return Poll::Ready(Some(Ok((index, event))));
                }
            }
        }
    }
}

#[allow(dead_code)]
pub(crate) async fn build_compound_segment_reader(
    stream_id: u64,
    epoch: u32,
    copy_set: Vec<String>,
    start: Option<u32>,
) -> Result<CompoundSegmentReader> {
    // FIXME(w41ter) more efficient implementation.
    let mut streamings = vec![];
    for addr in copy_set {
        let client = Client::connect(&addr).await?;
        let req = serverpb::ReadRequest {
            stream_id,
            seg_epoch: epoch,
            start_index: start.unwrap_or(1),
            limit: 0,
        };
        streamings.push(client.read(req).await?);
    }

    Ok(CompoundSegmentReader::new(streamings))
}
