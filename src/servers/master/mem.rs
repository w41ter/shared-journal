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
    collections::HashMap,
    ops::DerefMut,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures::stream;
use log::info;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::{
    journal::master::{Command, ObserverMeta},
    masterpb, Role, SegmentState, INITIAL_EPOCH,
};

#[derive(Debug)]
struct PolicyApplicant {
    role: Role,
    #[allow(dead_code)]
    epoch: u32,
    observer_id: String,
}

pub(crate) const DEFAULT_NUM_THRESHOLD: u32 = 1024;

#[derive(Debug, Clone, Copy)]
pub struct ThresholdSwitching {}

impl ThresholdSwitching {
    fn new() -> Self {
        // TODO(w41ter) support size option.
        ThresholdSwitching {}
    }

    fn apply(&self, applicant: &PolicyApplicant, stream_info: &mut StreamInfo) -> Option<Command> {
        if let Role::Leader = applicant.role {
            if let Some(segment_info) = stream_info.segments.get(&stream_info.epoch) {
                if segment_info.acked_index > DEFAULT_NUM_THRESHOLD {
                    return Some(stream_info.reset_leader(applicant));
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
    fn apply(&self, applicant: &PolicyApplicant, stream_info: &mut StreamInfo) -> Option<Command> {
        match self {
            SwitchPolicy::Threshold(policy) => policy.apply(applicant, stream_info),
        }
    }
}

#[derive(Debug)]
struct ObserverInfo {
    meta: ObserverMeta,
    #[allow(dead_code)]
    role: Role,
    last_heartbeat: Instant,
}

#[derive(Debug, Default)]
struct SegmentInfo {
    epoch: u32,
    acked_index: u32,
}

struct StreamInfo {
    #[allow(dead_code)]
    stream_id: u64,
    #[allow(dead_code)]
    stream_name: String,

    /// The latest allocated epoch of this stream.
    epoch: u32,
    state: SegmentState,

    switch_policy: Option<SwitchPolicy>,

    segments: HashMap<u32, SegmentInfo>,
    leader: Option<String>,
    observers: HashMap<String, ObserverInfo>,
}

impl StreamInfo {
    fn new(stream_id: u64, stream_name: String) -> Self {
        // TODO(w41ter) support configuring switch policy.
        StreamInfo {
            stream_id,
            stream_name,
            epoch: INITIAL_EPOCH,
            state: SegmentState::Appending,
            switch_policy: Some(SwitchPolicy::Threshold(ThresholdSwitching::new())),
            segments: HashMap::new(),
            leader: None,
            observers: HashMap::new(),
        }
    }

    fn leader_desc(&self) -> &str {
        if let Some(leader) = self.leader.as_ref() {
            leader
        } else {
            ""
        }
    }

    fn observe(&mut self, observer_id: String, observer_info: ObserverInfo) {
        let acked_seq = observer_info.meta.acked_seq;
        let acked_epoch = acked_seq.epoch;
        let acked_index = acked_seq.index;
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

    fn reset_leader(&mut self, applicant: &PolicyApplicant) -> Command {
        self.epoch += 1;
        self.leader = Some(applicant.observer_id.clone());
        // TODO(w41ter) set pending epochs.
        Command::Promote {
            role: Role::Leader,
            epoch: self.epoch,
            leader: applicant.observer_id.clone(),
            recovering_segments: vec![],
        }
    }

    fn gen_promote_cmd(&self, observer_id: &str) -> Command {
        // TODO(w41ter) set pending epochs.
        if let Some(leader) = &self.leader {
            if leader == observer_id {
                return Command::Promote {
                    role: Role::Leader,
                    epoch: self.epoch,
                    leader: observer_id.to_string(),
                    recovering_segments: vec![],
                };
            }
        }
        Command::Promote {
            role: Role::Follower,
            epoch: self.epoch,
            leader: self.leader.as_ref().cloned().unwrap_or_default(),
            recovering_segments: vec![],
        }
    }
}

fn apply_strategies(
    config: &MasterConfig,
    applicant: &PolicyApplicant,
    stream_info: &mut StreamInfo,
) -> Vec<Command> {
    if let Some(policy) = stream_info.switch_policy {
        if let Some(cmd) = policy.apply(applicant, stream_info) {
            return vec![cmd];
        }
    }

    // stale request, promote it
    if applicant.epoch < stream_info.epoch {
        // The observer might lost heartbeat response, so here check and accept
        // the staled heartbeat request from current leader, and continue to promote
        // it for idempotent.
        info!("stream {} epoch {} leader {} promote observer {}, epoch: {}, by receiving staled heartbeat",
            stream_info.stream_id, stream_info.epoch, stream_info.leader_desc(),
            applicant.observer_id, applicant.epoch);
        return vec![stream_info.gen_promote_cmd(&applicant.observer_id)];
    }

    // check leader
    let now = Instant::now();
    let select_new_leader = match &stream_info.leader {
        Some(observer_id) => {
            let observer_info = stream_info
                .observers
                .get(observer_id)
                .expect("stream must exists if it is a leader");
            // Leader might lost, need select new leader
            observer_info.last_heartbeat + config.heartbeat_timeout() <= now
        }
        None => true,
    };
    if select_new_leader {
        return vec![stream_info.reset_leader(applicant)];
    }
    vec![]
}

#[derive(Debug, Clone)]
pub struct MasterConfig {
    pub heartbeat_timeout_tick: u64,
    pub heartbeat_interval_ms: u64,
}

impl MasterConfig {
    fn heartbeat_timeout(&self) -> Duration {
        Duration::from_millis(self.heartbeat_timeout_tick * self.heartbeat_interval_ms)
    }
}

struct MasterCore {
    next_stream_id: u64,
    config: MasterConfig,
    stream_meta: HashMap<String, u64>,
    streams: HashMap<u64, StreamInfo>,
    replicas: Vec<String>,
}

pub struct Server {
    core: Arc<Mutex<MasterCore>>,
}

impl Server {
    pub fn new(
        config: MasterConfig,
        stream_meta: HashMap<String, u64>,
        replicas: Vec<String>,
    ) -> Self {
        let streams = stream_meta
            .iter()
            .map(|(k, v)| (*v, StreamInfo::new(*v, k.clone())))
            .collect();
        let next_stream_id = stream_meta
            .iter()
            .map(|(_, v)| *v)
            .max()
            .unwrap_or_default()
            + 1;
        Server {
            core: Arc::new(Mutex::new(MasterCore {
                next_stream_id,
                config,
                stream_meta,
                replicas,
                streams,
            })),
        }
    }

    pub fn into_service(self) -> masterpb::master_server::MasterServer<Server> {
        masterpb::master_server::MasterServer::new(self)
    }
}

#[async_trait]
impl masterpb::master_server::Master for Server {
    type ListStreamStream =
        stream::Iter<<Vec<Result<masterpb::StreamMeta, Status>> as IntoIterator>::IntoIter>;

    async fn create_stream(
        &self,
        input: Request<masterpb::CreateStreamRequest>,
    ) -> Result<Response<masterpb::CreateStreamResponse>, Status> {
        let req = input.into_inner();
        let mut core = self.core.lock().await;
        match core.stream_meta.get(&req.stream_name) {
            Some(_) => Err(Status::already_exists("stream already exists")),
            None => {
                let stream_id = core.next_stream_id;
                core.next_stream_id += 1;
                core.stream_meta.insert(req.stream_name, stream_id);
                Ok(Response::new(masterpb::CreateStreamResponse {}))
            }
        }
    }

    async fn list_stream(
        &self,
        _input: Request<masterpb::ListStreamRequest>,
    ) -> Result<Response<Self::ListStreamStream>, Status> {
        let core = self.core.lock().await;
        Ok(Response::new(stream::iter(
            core.stream_meta
                .iter()
                .map(|(k, id)| {
                    Ok(masterpb::StreamMeta {
                        stream_name: k.clone(),
                        stream_id: *id,
                    })
                })
                .collect::<Vec<_>>(),
        )))
    }

    async fn get_stream(
        &self,
        input: Request<masterpb::GetStreamRequest>,
    ) -> Result<Response<masterpb::GetStreamResponse>, Status> {
        let req = input.into_inner();
        let core = self.core.lock().await;
        match core.stream_meta.get(&req.stream_name) {
            Some(s) => Ok(Response::new(masterpb::GetStreamResponse { stream_id: *s })),
            None => Err(Status::not_found("no such stream exists")),
        }
    }

    async fn get_segment(
        &self,
        input: Request<masterpb::GetSegmentRequest>,
    ) -> Result<Response<masterpb::GetSegmentResponse>, Status> {
        let req = input.into_inner();
        let core = self.core.lock().await;
        match core.stream_meta.get(&req.stream_name) {
            Some(s) => Ok(Response::new(masterpb::GetSegmentResponse {
                stream_id: *s,
                copy_set: core.replicas.clone(),
                state: core
                    .streams
                    .get(s)
                    .map(|si| si.state)
                    .unwrap_or_default()
                    .into(),
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
                acked_seq: req.acked_seq.into(),
            },
            role: req.role.into(),
            last_heartbeat: Instant::now(),
        };

        let mut core = self.core.lock().await;
        let core = core.deref_mut();
        let stream_id = match core.stream_meta.get(&stream_name) {
            Some(id) => *id,
            None => return Err(Status::not_found("no such stream exists")),
        };

        let stream = core
            .streams
            .entry(stream_id)
            .or_insert_with(|| StreamInfo::new(stream_id, stream_name.clone()));

        if stream.epoch < req.epoch && stream.epoch != INITIAL_EPOCH {
            return Err(Status::aborted("too large epoch"));
        }

        stream.observe(observer_id.clone(), observer_info);

        let applicant = PolicyApplicant {
            epoch: req.epoch,
            role: req.role.into(),
            observer_id,
        };
        let commands = apply_strategies(&core.config, &applicant, stream);
        Ok(Response::new(masterpb::HeartbeatResponse {
            commands: commands.into_iter().map(Into::into).collect(),
        }))
    }

    async fn seal_segment(
        &self,
        input: Request<masterpb::SealSegmentRequest>,
    ) -> Result<Response<masterpb::SealSegmentResponse>, Status> {
        let req = input.into_inner();
        let stream_id = req.stream_id;

        let mut core = self.core.lock().await;
        let stream_info = match core.streams.get_mut(&stream_id) {
            Some(si) => si,
            None => return Err(Status::not_found("no such stream exists")),
        };

        if stream_info.state != SegmentState::Sealed {
            stream_info.state = SegmentState::Sealed;
        }

        Ok(Response::new(masterpb::SealSegmentResponse {}))
    }
}
