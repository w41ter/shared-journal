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

// Simple bypass since trait alias is not stable yet.
mod trait_alias {
    use super::*;

    type ReadResult = std::result::Result<storepb::ReadResponse, tonic::Status>;
    pub(crate) trait StreamingReader: Stream<Item = ReadResult> + Unpin {}
    impl<S> StreamingReader for S where S: Stream<Item = ReadResult> + Unpin {}
}

use self::trait_alias::StreamingReader;

struct Reader<S: StreamingReader> {
    state: ReaderState,
    entries_stream: S,
}

/// Read and select pending entries, a bridge record will be appended to the
/// end of stream.
pub(crate) struct RawCompoundSegmentReader<S: StreamingReader> {
    policy: GroupReader,
    bridge_entry: Option<Entry>,
    readers: Vec<Reader<S>>,
}

impl<S> RawCompoundSegmentReader<S>
where
    S: StreamingReader,
{
    pub fn new(policy: ReplicatePolicy, seg_epoch: u32, next_index: u32, streams: Vec<S>) -> Self {
        let group_reader = policy.new_group_reader(next_index, streams.len());
        Self::new_with_reader(group_reader, seg_epoch, streams)
    }

    fn new_with_reader(group_reader: GroupReader, seg_epoch: u32, streams: Vec<S>) -> Self {
        RawCompoundSegmentReader {
            bridge_entry: Some(Entry::Bridge { epoch: seg_epoch }),
            policy: group_reader,
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

impl<S> Stream for RawCompoundSegmentReader<S>
where
    S: StreamingReader,
{
    type Item = Result<(u32, Entry)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // All entries is read and consumed if bridge entry has been taken.
        if this.bridge_entry.is_none() {
            return Poll::Ready(None);
        }

        if let Err(err) = this.advance(cx) {
            // FIXME(w41ter) if the policy support to accept majority, how to handle errors?
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
            GroupState::Done => std::mem::take(&mut this.bridge_entry).map(|e| Ok((next_index, e))),
            GroupState::Pending => {
                return Poll::Pending;
            }
        };
        Poll::Ready(next_entry)
    }
}

type StreamingReadResponse = Streaming<storepb::ReadResponse>;
pub(crate) type CompoundSegmentReader = RawCompoundSegmentReader<StreamingReadResponse>;

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use tonic::Status;

    use super::*;
    use crate::journal::policy::GroupPolicy;

    type TonicResult<T> = std::result::Result<T, Status>;

    struct PollStream<I: Unpin> {
        items: Vec<Poll<I>>,
    }

    impl<I> PollStream<I>
    where
        I: Unpin,
    {
        fn new(items: Vec<Poll<I>>) -> Self {
            PollStream {
                items: items.into_iter().rev().collect(),
            }
        }
    }

    impl<T> Stream for PollStream<T>
    where
        T: Unpin,
    {
        type Item = T;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            match this.items.pop() {
                Some(Poll::Ready(item)) => Poll::Ready(Some(item)),
                Some(Poll::Pending) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                None => Poll::Ready(None),
            }
        }
    }

    #[allow(dead_code)]
    fn h(index: u32) -> Poll<TonicResult<storepb::ReadResponse>> {
        Poll::Ready(Ok(storepb::ReadResponse {
            index,
            entry: Some(Entry::Hole.into()),
        }))
    }

    fn e(index: u32, epoch: u32) -> Poll<TonicResult<storepb::ReadResponse>> {
        let event: Vec<u8> = index.to_le_bytes().as_slice().into();
        Poll::Ready(Ok(storepb::ReadResponse {
            index,
            entry: Some(
                Entry::Event {
                    epoch,
                    event: event.into(),
                }
                .into(),
            ),
        }))
    }

    fn b(index: u32, epoch: u32) -> Poll<TonicResult<storepb::ReadResponse>> {
        Poll::Ready(Ok(storepb::ReadResponse {
            index,
            entry: Some(Entry::Bridge { epoch }.into()),
        }))
    }

    fn eh() -> Entry {
        Entry::Hole
    }

    fn eb(epoch: u32) -> Entry {
        Entry::Bridge { epoch }
    }

    fn ee(index: u32, epoch: u32) -> Entry {
        let event: Vec<u8> = index.to_le_bytes().as_slice().into();
        Entry::Event {
            epoch,
            event: event.into(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn simple_policy_basic_test() -> Result<()> {
        struct TestCase {
            desc: &'static str,
            streams: Vec<Vec<Poll<TonicResult<storepb::ReadResponse>>>>,
            next_index: u32,
            writer_epoch: u32,
            expects: Vec<Entry>,
        }
        let cases = vec![
            TestCase {
                desc: "1. empty streams should return a bridge record.",
                streams: vec![vec![]],
                next_index: 1,
                writer_epoch: 3,
                expects: vec![eb(3)],
            },
            TestCase {
                desc: "2. only one bridge record, select largest.",
                streams: vec![vec![e(1, 1)], vec![b(1, 2)]],
                next_index: 1,
                writer_epoch: 5,
                expects: vec![eb(5)],
            },
            TestCase {
                desc: "3. select largest entry, if not found returns hole",
                streams: vec![
                    vec![e(2, 1), e(3, 2), e(4, 3), e(7, 4)],
                    vec![e(1, 1), e(3, 3), e(4, 3), e(5, 3)],
                ],
                next_index: 1,
                writer_epoch: 8,
                expects: vec![
                    ee(1, 1),
                    ee(2, 1),
                    ee(3, 3),
                    ee(4, 3),
                    ee(5, 3),
                    eh(), // six not found
                    ee(7, 4),
                    eb(8),
                ],
            },
            TestCase {
                desc: "4. hole and only one bridge",
                streams: vec![vec![b(10, 1)]],
                next_index: 5,
                writer_epoch: 10,
                expects: vec![eh(), eh(), eh(), eh(), eh(), eb(10)],
            },
        ];
        for case in cases {
            let group_reader =
                GroupReader::new(GroupPolicy::Simple, case.next_index, case.streams.len());
            let mut comp_reader = RawCompoundSegmentReader::new_with_reader(
                group_reader,
                case.writer_epoch,
                case.streams.into_iter().map(PollStream::new).collect(),
            );

            let mut entries = vec![];
            while let Some(item) = comp_reader.next().await {
                let (_idx, entry) = item?;
                entries.push(entry);
            }
            assert_eq!(entries, case.expects, "step: {}", case.desc);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn majority_policy_with_pending_state() -> Result<()> {
        struct TestCase {
            desc: &'static str,
            streams: Vec<Vec<Poll<TonicResult<storepb::ReadResponse>>>>,
            next_index: u32,
            writer_epoch: u32,
            expects: Vec<Entry>,
        }
        let cases = vec![
            TestCase {
                desc: "1. empty streams should return a bridge record.",
                streams: vec![vec![], vec![], vec![]],
                next_index: 1,
                writer_epoch: 3,
                expects: vec![eb(3)],
            },
            TestCase {
                desc: "2. only one bridge record, select largest.",
                streams: vec![vec![e(1, 1)], vec![b(1, 2)], vec![]],
                next_index: 1,
                writer_epoch: 5,
                expects: vec![eb(5)],
            },
            TestCase {
                desc: "3. select largest entry, if not found returns hole",
                streams: vec![
                    vec![e(2, 1), e(3, 2), e(4, 3), e(7, 4)],
                    vec![e(1, 1), e(3, 3), e(4, 3), e(5, 3)],
                    vec![e(3, 3), e(7, 4)],
                ],
                next_index: 1,
                writer_epoch: 8,
                expects: vec![
                    ee(1, 1),
                    ee(2, 1),
                    ee(3, 3),
                    ee(4, 3),
                    ee(5, 3),
                    eh(), // six not found
                    ee(7, 4),
                    eb(8),
                ],
            },
            TestCase {
                desc: "4. like 3, but more pending state",
                streams: vec![
                    vec![
                        Poll::Pending,
                        e(2, 1),
                        Poll::Pending,
                        Poll::Pending,
                        e(3, 2),
                        Poll::Pending,
                        e(4, 3),
                        Poll::Pending,
                        e(7, 4),
                        Poll::Pending,
                    ],
                    vec![
                        Poll::Pending,
                        e(1, 1),
                        Poll::Pending,
                        e(3, 3),
                        e(4, 3),
                        e(5, 3),
                        Poll::Pending,
                    ],
                    vec![e(3, 3), Poll::Pending, e(7, 4)],
                ],
                next_index: 1,
                writer_epoch: 8,
                expects: vec![
                    ee(1, 1),
                    ee(2, 1),
                    ee(3, 3),
                    ee(4, 3),
                    ee(5, 3),
                    eh(), // six not found
                    ee(7, 4),
                    eb(8),
                ],
            },
            TestCase {
                desc: "5. one replica done early.",
                streams: vec![
                    vec![e(2, 1), e(3, 2), e(4, 3), e(7, 4)], // Notice no any Pending item.
                    vec![e(1, 1), e(3, 3), e(4, 3), e(5, 3)],
                    vec![],
                ],
                next_index: 1,
                writer_epoch: 8,
                expects: vec![
                    ee(1, 1),
                    ee(2, 1),
                    ee(3, 3),
                    ee(4, 3),
                    ee(5, 3),
                    eh(), // six not found
                    ee(7, 4),
                    eb(8),
                ],
            },
            TestCase {
                desc: "6. one replica done early and exists pending.",
                streams: vec![
                    vec![
                        e(2, 1),
                        e(3, 2),
                        e(4, 3),
                        Poll::Pending,
                        Poll::Pending,
                        e(7, 4),
                    ],
                    vec![e(1, 1), e(3, 3), e(4, 3), e(5, 3)],
                    vec![],
                ],
                next_index: 1,
                writer_epoch: 8,
                expects: vec![ee(1, 1), ee(2, 1), ee(3, 3), ee(4, 3), ee(5, 3), eb(8)],
            },
        ];
        for case in cases {
            let group_reader =
                GroupReader::new(GroupPolicy::Majority, case.next_index, case.streams.len());
            let mut comp_reader = RawCompoundSegmentReader::new_with_reader(
                group_reader,
                case.writer_epoch,
                case.streams.into_iter().map(PollStream::new).collect(),
            );

            let mut entries = vec![];
            while let Some(item) = comp_reader.next().await {
                let (_idx, entry) = item?;
                entries.push(entry);
            }
            assert_eq!(entries, case.expects, "step: {}", case.desc);
        }
        Ok(())
    }
}
