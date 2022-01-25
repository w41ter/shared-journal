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
use crate::{masterpb, Role};

#[allow(unused)]
struct ObserverInfo {
    meta: ObserverMeta,
    role: Role,
    last_heartbeat: Instant,
}

#[allow(unused)]
struct StreamInfo {
    stream_id: u64,
    stream_name: String,
    observers: HashMap<String, ObserverInfo>,
}

impl StreamInfo {
    fn new(stream_id: u64, stream_name: String) -> Self {
        StreamInfo {
            stream_id,
            stream_name,
            observers: HashMap::new(),
        }
    }
}

struct MasterInner {
    stream_meta: HashMap<String, u64>,
    streams: HashMap<u64, StreamInfo>,
    replicas: Vec<String>,
}

pub(super) struct Server {
    inner: Arc<Mutex<MasterInner>>,
}

#[allow(dead_code)]
impl Server {
    pub fn new(stream_meta: HashMap<String, u64>, replicas: Vec<String>) -> Self {
        Server {
            inner: Arc::new(Mutex::new(MasterInner {
                stream_meta,
                replicas,
                streams: HashMap::new(),
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

        stream.observers.insert(observer_id, observer_info);

        Ok(Response::new(masterpb::HeartbeatResponse {
            commands: vec![],
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
