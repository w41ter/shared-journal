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

pub(crate) mod compound;
pub(crate) mod remote;
pub(crate) mod segment;
pub(crate) mod stream;

pub(crate) use self::compound::CompoundSegmentReader;
use self::remote::Client;
pub use self::stream::{StreamReader, StreamWriter};
use super::ReplicatePolicy;
use crate::{storepb, Result};

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
            limit: u32::MAX,
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

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::{servers::store::build_seg_store, Entry, Sequence};

    fn entry(event: Vec<u8>) -> storepb::Entry {
        storepb::Entry {
            entry_type: storepb::EntryType::Event as i32,
            epoch: 1,
            event,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn compound_segment_reader() -> Result<()> {
        let mut copy_set: Vec<String> = vec![];
        for _ in 0..3 {
            let local_addr = build_seg_store().await?;
            let client = Client::connect(&local_addr.to_string()).await?;
            let write_req = storepb::WriteRequest {
                stream_id: 1,
                seg_epoch: 1,
                epoch: 1,
                acked_seq: 0,
                first_index: 1,
                entries: vec![entry(vec![1u8]), entry(vec![2u8]), entry(vec![3u8])],
            };
            client.write(write_req).await?;
            let write_req = storepb::WriteRequest {
                stream_id: 1,
                seg_epoch: 1,
                epoch: 1,
                acked_seq: Sequence::new(1, 6).into(),
                first_index: 4,
                entries: vec![entry(vec![4u8]), entry(vec![5u8]), entry(vec![6u8])],
            };
            client.write(write_req).await?;
            copy_set.push(local_addr);
        }

        struct TestCase {
            start_index: Option<u32>,
            expect: Vec<Entry>,
        }
        let cases = vec![
            TestCase {
                start_index: None,
                expect: vec![
                    entry(vec![1u8]).into(),
                    entry(vec![2u8]).into(),
                    entry(vec![3u8]).into(),
                    entry(vec![4u8]).into(),
                    entry(vec![5u8]).into(),
                    entry(vec![6u8]).into(),
                ],
            },
            TestCase {
                start_index: Some(1),
                expect: vec![
                    entry(vec![1u8]).into(),
                    entry(vec![2u8]).into(),
                    entry(vec![3u8]).into(),
                    entry(vec![4u8]).into(),
                    entry(vec![5u8]).into(),
                    entry(vec![6u8]).into(),
                ],
            },
            TestCase {
                start_index: Some(2),
                expect: vec![
                    entry(vec![2u8]).into(),
                    entry(vec![3u8]).into(),
                    entry(vec![4u8]).into(),
                    entry(vec![5u8]).into(),
                    entry(vec![6u8]).into(),
                ],
            },
        ];

        for case in cases {
            let mut reader = build_compound_segment_reader(
                ReplicatePolicy::Simple,
                1,
                1,
                copy_set.clone(),
                case.start_index,
            )
            .await?;
            for expect in case.expect {
                let (index, entry) = reader.next().await.unwrap()?;
                assert_eq!(entry, expect, "entry index {}", index);
            }
        }

        Ok(())
    }
}
