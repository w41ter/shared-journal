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
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, TryStreamExt};
use tonic::Streaming;

use crate::{
    journal::{
        policy::{GroupReader, GroupState, ReaderState},
        ReplicatePolicy,
    },
    storepb, Entry, Result,
};

struct Reader {
    state: ReaderState,
    entries_stream: Streaming<storepb::ReadResponse>,
}

/// Read and select pending entries, a bridge record will be appended to the
/// end of stream.
pub(crate) struct CompoundSegmentReader {
    policy: GroupReader,
    bridge_entry: Option<Entry>,
    readers: Vec<Reader>,
}

impl CompoundSegmentReader {
    pub fn new(
        policy: ReplicatePolicy,
        seg_epoch: u32,
        next_index: u32,
        streams: Vec<Streaming<storepb::ReadResponse>>,
    ) -> Self {
        let group_policy = policy.new_group_reader(next_index, streams.len());
        CompoundSegmentReader {
            bridge_entry: Some(Entry::Bridge { epoch: seg_epoch }),
            policy: group_policy,
            readers: streams
                .into_iter()
                .map(|stream| Reader {
                    state: ReaderState::Polling,
                    entries_stream: stream,
                })
                .collect(),
        }
    }

    fn advance(&mut self, cx: &mut Context<'_>) -> Result<()> {
        for reader in &mut self.readers {
            if let ReaderState::Polling = &reader.state {
                let mut try_next = reader.entries_stream.try_next();
                if let Poll::Ready(out) = Pin::new(&mut try_next).poll(cx) {
                    self.policy.transform(
                        &mut reader.state,
                        out?.map(|r| (r.index, r.entry.unwrap().into())),
                    );
                }
            }
        }
        Ok(())
    }
}

impl Stream for CompoundSegmentReader {
    type Item = Result<(u32, Entry)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // All entries is read and consumed if bridge entry has been taken.
        if this.bridge_entry.is_none() {
            return Poll::Ready(None);
        }

        loop {
            if let Err(err) = this.advance(cx) {
                return Poll::Ready(Some(Err(err)));
            }

            let next_index = this.policy.next_index();
            let next_entry = match this.policy.state() {
                GroupState::Active => {
                    let entry = this
                        .policy
                        .next_entry(this.readers.iter_mut().map(|reader| &mut reader.state));
                    match entry {
                        Some(Entry::Hole) => panic!("shouldn't receive hole from store"),
                        Some(Entry::Bridge { .. }) => {
                            std::mem::take(&mut this.bridge_entry).map(|e| Ok((next_index, e)))
                        }
                        Some(Entry::Event { epoch, event }) => {
                            Some(Ok((next_index, Entry::Event { epoch, event })))
                        }
                        None => Some(Ok((next_index, Entry::Hole))),
                    }
                }
                GroupState::Done => {
                    std::mem::take(&mut this.bridge_entry).map(|e| Ok((next_index, e)))
                }
                GroupState::Pending => {
                    continue;
                }
            };
            return Poll::Ready(next_entry);
        }
    }
}
