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

use std::{collections::HashMap, sync::Arc, time::Instant};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::{transport::Channel, Request, Response, Status};

use super::ObserverMeta;
use crate::{masterpb, Role, INITIAL_EPOCH};

#[derive(Debug)]
#[allow(unused)]
struct PolicyApplicant {
    role: Role,
    epoch: u32,
    observer_id: String,
}

pub(super) const DEFAULT_NUM_THRESHOLD: u32 = 1024;

#[derive(Debug, Clone, Copy)]
pub struct ThresholdSwitching {}

impl ThresholdSwitching {
    fn new() -> Self {
        // TODO(w41ter) support size option.
        ThresholdSwitching {}
    }

    fn apply(
        &self,
        applicant: &PolicyApplicant,
        stream_info: &mut StreamInfo,
    ) -> Option<super::Command> {
        if let Role::Leader = applicant.role {
            if let Some(segment_info) = stream_info.segments.get(&stream_info.epoch) {
                if segment_info.acked_index > DEFAULT_NUM_THRESHOLD {
                    stream_info.epoch += 1;
                    return Some(super::Command::Promote {
                        role: Role::Leader,
                        epoch: stream_info.epoch,
                        leader: applicant.observer_id.clone(),
                    });
                }
            }
        }
        None
    }
}

#[derive(Debug, Clone, Copy)]
enum SwitchPolicy {
    Threshold(ThresholdSwitching),
}

impl SwitchPolicy {
    fn apply(
        &self,
        applicant: &PolicyApplicant,
        stream_info: &mut StreamInfo,
    ) -> Option<super::Command> {
        match self {
            SwitchPolicy::Threshold(policy) => policy.apply(applicant, stream_info),
        }
    }
}

#[derive(Debug)]
#[allow(unused)]
struct ObserverInfo {
    meta: ObserverMeta,
    role: Role,
    last_heartbeat: Instant,
}

#[derive(Debug, Default)]
#[allow(unused)]
struct SegmentInfo {
    epoch: u32,
    acked_index: u32,
}

#[allow(unused)]
struct StreamInfo {
    stream_id: u64,
    stream_name: String,

    /// The latest allocated epoch of this stream.
    epoch: u32,

    switch_policy: Option<SwitchPolicy>,

    segments: HashMap<u32, SegmentInfo>,
    observers: HashMap<String, ObserverInfo>,
}

impl StreamInfo {
    fn new(stream_id: u64, stream_name: String) -> Self {
        // TODO(w41ter) support configuring switch policy.
        StreamInfo {
            stream_id,
            stream_name,
            epoch: INITIAL_EPOCH,
            switch_policy: Some(SwitchPolicy::Threshold(ThresholdSwitching::new())),
            segments: HashMap::new(),
            observers: HashMap::new(),
        }
    }

    fn observe(&mut self, observer_id: String, observer_info: ObserverInfo) {
        let acked_seq = observer_info.meta.acked_seq;
        let acked_epoch = (acked_seq >> 32) as u32;
        let acked_index = (acked_seq & u32::MAX as u64) as u32;
        println!(
            "{:?} acked epoch: {}, acked index {}",
            observer_info, acked_epoch, acked_index
        );
        self.observers.insert(observer_id, observer_info);
        let segment_info = self
            .segments
            .entry(acked_epoch)
            .or_insert_with(SegmentInfo::default);
        segment_info.epoch = acked_epoch;
        segment_info.acked_index = acked_index;
    }
}

fn apply_strategies(
    applicant: &PolicyApplicant,
    stream_info: &mut StreamInfo,
) -> Vec<super::Command> {
    if let Some(policy) = stream_info.switch_policy {
        if let Some(cmd) = policy.apply(applicant, stream_info) {
            return vec![cmd];
        }
    }
    vec![]
}

struct MasterInner {
    stream_meta: HashMap<String, u64>,
    streams: HashMap<u64, StreamInfo>,
    replicas: Vec<String>,
}

pub(crate) struct Server {
    inner: Arc<Mutex<MasterInner>>,
}

#[allow(dead_code)]
impl Server {
    pub fn new(stream_meta: HashMap<String, u64>, replicas: Vec<String>) -> Self {
        let streams = stream_meta
            .iter()
            .map(|(k, v)| {
                // Temporary implementation.
                let mut info = StreamInfo::new(*v, k.clone());
                info.epoch = 1;
                (*v, info)
            })
            .collect();
        Server {
            inner: Arc::new(Mutex::new(MasterInner {
                stream_meta,
                replicas,
                streams,
            })),
        }
    }

    #[allow(dead_code)]
    pub fn into_service(self) -> masterpb::master_server::MasterServer<Server> {
        masterpb::master_server::MasterServer::new(self)
    }
}

#[async_trait]
#[allow(unused)]
impl masterpb::master_server::Master for Server {
    async fn get_segment(
        &self,
        input: Request<masterpb::GetSegmentRequest>,
    ) -> Result<Response<masterpb::GetSegmentResponse>, Status> {
        let req = input.into_inner();
        let inner = self.inner.lock().await;
        match inner.stream_meta.get(&req.stream_name) {
            Some(s) => Ok(Response::new(masterpb::GetSegmentResponse {
                stream_id: *s,
                copy_set: inner.replicas.clone(),
            })),
            None => Err(Status::not_found("no such stream exists")),
        }
    }

    async fn heartbeat(
        &self,
        input: Request<masterpb::HeartbeatRequest>,
    ) -> Result<Response<masterpb::HeartbeatResponse>, Status> {
        let req = input.into_inner();
        let stream_name = req.stream_name.clone();
        let observer_id = req.observer_id.clone();
        let observer_info = ObserverInfo {
            meta: ObserverMeta {
                stream_name: req.stream_name,
                observer_id: req.observer_id,
                state: req.observer_state.into(),
                epoch: req.epoch,
                acked_seq: req.acked_seq,
            },
            role: req.role.into(),
            last_heartbeat: Instant::now(),
        };

        let mut inner = self.inner.lock().await;
        let stream_id = match inner.stream_meta.get(&stream_name) {
            Some(id) => *id,
            None => return Err(Status::not_found("no such stream exists")),
        };

        let stream = inner
            .streams
            .entry(stream_id)
            .or_insert_with(|| StreamInfo::new(stream_id, stream_name.clone()));

        if stream.epoch < req.epoch {
            return Err(Status::aborted("too large epoch"));
        }

        stream.observe(observer_id.clone(), observer_info);

        let applicant = PolicyApplicant {
            epoch: req.epoch,
            role: req.role.into(),
            observer_id,
        };
        let commands = apply_strategies(&applicant, stream);
        Ok(Response::new(masterpb::HeartbeatResponse {
            commands: commands.into_iter().map(Into::into).collect(),
        }))
    }
}

type MasterClient = masterpb::master_client::MasterClient<Channel>;

#[derive(Clone)]
#[allow(unused)]
pub(super) struct Client {
    client: MasterClient,
}

#[allow(dead_code)]
impl Client {
    pub async fn connect(addr: &str) -> crate::Result<Self> {
        let addr = format!("http://{}", addr);
        let client = MasterClient::connect(addr).await?;
        Ok(Client { client })
    }

    pub async fn get_segment(
        &self,
        input: masterpb::GetSegmentRequest,
    ) -> crate::Result<masterpb::GetSegmentResponse> {
        let mut client = self.client.clone();
        let resp = client.get_segment(input).await?;
        Ok(resp.into_inner())
    }

    pub async fn heartbeat(
        &self,
        input: masterpb::HeartbeatRequest,
    ) -> crate::Result<masterpb::HeartbeatResponse> {
        let mut client = self.client.clone();
        let resp = client.heartbeat(input).await?;
        Ok(resp.into_inner())
    }
}
