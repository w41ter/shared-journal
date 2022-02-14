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
