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

pub(crate) mod mem;

use async_trait::async_trait;
use futures::Stream;

use super::{Result, Role, SegmentMeta, Sequence};

/// The state of an stream's observer. The transition of states is:
///
/// Following -> Sealing -> Recovering -> Leading
///    ^                                    |
///    +------------------------------------+
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(super) enum ObserverState {
    /// A leader must seals the former epochs before starting to recovery a
    /// stream.
    Sealing,
    /// A leader must recovery all unfinished replications before starting to
    /// lead a stream.
    Recovering,
    /// A leader is prepared to receive incoming events.
    Leading,
    /// A follower is prepared to follow and subscribe a stream.
    Following,
}

impl ObserverState {
    fn role(&self) -> crate::Role {
        match self {
            ObserverState::Following => crate::Role::Follower,
            ObserverState::Leading | ObserverState::Recovering | ObserverState::Sealing => {
                crate::Role::Leader
            }
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(super) struct ObserverMeta {
    pub observer_id: String,

    /// Which stream is observing?
    pub stream_name: String,

    /// The value of epoch in observer's memory.
    pub epoch: u32,

    pub state: ObserverState,

    /// The acked sequence of entries it has already known. It might less than
    /// (epoch << 32).
    pub acked_seq: Sequence,
}

/// The commands of a master must be completed by a stream observer.
#[derive(Debug)]
#[allow(dead_code)]
pub enum Command {
    Nop,

    /// Promote the epoch and specify the new role. When receives `Promote`
    /// command, a leader must start to seal former epochs and recover the
    /// stream.
    Promote {
        role: Role,
        epoch: u32,
        leader: String,
    },
}

/// An abstraction of master of shared journal.
#[async_trait]
pub(super) trait Master {
    type MetaStream: Stream<Item = Result<SegmentMeta>>;

    /// Sends the state of a stream observer to master, and receives commands.
    async fn heartbeat(&self, observer_meta: ObserverMeta) -> Result<Vec<Command>>;

    /// Query the meta of segments covered by the specified sequence range.
    async fn query_segments(
        &self,
        stream_name: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Self::MetaStream>;

    /// Get segment meta of the specified epoch of a stream.
    async fn get_segment(&self, stream_name: &str, epoch: u32) -> Result<Option<SegmentMeta>>;
}

mod remote {
    use async_trait::async_trait;
    use futures::Stream;

    use super::{mem::Client, SegmentMeta};
    use crate::{masterpb, Result};

    impl From<masterpb::Command> for super::Command {
        fn from(cmd: masterpb::Command) -> Self {
            match masterpb::CommandType::from_i32(cmd.command_type) {
                None | Some(masterpb::CommandType::Nop) => super::Command::Nop,
                Some(masterpb::CommandType::Promote) => super::Command::Promote {
                    role: cmd.role.into(),
                    epoch: cmd.epoch,
                    leader: cmd.leader,
                },
            }
        }
    }

    impl From<super::Command> for masterpb::Command {
        fn from(cmd: super::Command) -> Self {
            match cmd {
                super::Command::Nop => masterpb::Command {
                    command_type: masterpb::CommandType::Nop as i32,
                    epoch: 0,
                    role: crate::Role::Follower.into(),
                    leader: "".into(),
                },
                super::Command::Promote {
                    role,
                    epoch,
                    leader,
                } => masterpb::Command {
                    command_type: masterpb::CommandType::Promote as i32,
                    epoch,
                    role: role.into(),
                    leader,
                },
            }
        }
    }

    #[allow(dead_code)]
    #[derive(Clone)]
    pub struct RemoteMaster {
        master_client: Client,
    }

    #[allow(dead_code)]
    impl RemoteMaster {
        pub async fn new(addr: &str) -> Result<Self> {
            Ok(RemoteMaster {
                master_client: Client::connect(addr).await?,
            })
        }
    }

    pub struct MetaStream {}

    impl Stream for MetaStream {
        type Item = Result<SegmentMeta>;

        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            todo!()
        }
    }

    #[async_trait]
    #[allow(dead_code, unused)]
    impl super::Master for RemoteMaster {
        type MetaStream = MetaStream;

        async fn heartbeat(
            &self,
            observer_meta: super::ObserverMeta,
        ) -> Result<Vec<super::Command>> {
            let req = masterpb::HeartbeatRequest {
                epoch: observer_meta.epoch,
                observer_id: observer_meta.observer_id,
                stream_name: observer_meta.stream_name,
                role: observer_meta.state.role().into(),
                observer_state: observer_meta.state.into(),
                acked_seq: observer_meta.acked_seq,
            };

            let resp = self.master_client.heartbeat(req).await?;
            Ok(resp.commands.into_iter().map(Into::into).collect())
        }

        async fn query_segments(
            &self,
            stream_name: &str,
            range: std::ops::Range<u64>,
        ) -> Result<Self::MetaStream> {
            todo!()
        }

        async fn get_segment(&self, stream_name: &str, epoch: u32) -> Result<Option<SegmentMeta>> {
            let req = masterpb::GetSegmentRequest {
                stream_name: stream_name.to_owned(),
                seg_epoch: epoch,
            };

            let resp = match self.master_client.get_segment(req).await {
                Err(crate::Error::NotFound(_)) => return Ok(None),
                Err(e) => return Err(e),
                Ok(resp) => resp,
            };

            let seg_meta = SegmentMeta {
                stream_id: resp.stream_id,
                stream_name: stream_name.to_owned(),
                epoch,
                copy_set: resp.copy_set,
            };

            Ok(Some(seg_meta))
        }
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use mem::Server;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn get_segment() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut segment_meta = HashMap::new();
        segment_meta.insert("default".to_owned(), 1);

        let replicas = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let replicas_clone = replicas.clone();
        let segment_meta_clone = segment_meta.clone();

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async {
            let server = Server::new(segment_meta_clone, replicas_clone);
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let master = remote::RemoteMaster::new(&local_addr.to_string()).await?;
        let resp = master.get_segment("default", 1).await?;
        assert!(
            matches!(resp, Some(segment_meta) if segment_meta == SegmentMeta {
                stream_id: 1,
                stream_name: "default".to_owned(),
                epoch: 1,
                copy_set: replicas.clone(),
            })
        );

        let resp = master.get_segment("not-exists", 1).await?;
        assert!(matches!(resp, None));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn heartbeat() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut segment_meta = HashMap::new();
        segment_meta.insert("default".to_owned(), 1);

        let replicas = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let replicas_clone = replicas.clone();
        let segment_meta_clone = segment_meta.clone();

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async {
            let server = Server::new(segment_meta_clone, replicas_clone);
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let master = remote::RemoteMaster::new(&local_addr.to_string()).await?;
        let observer_meta = ObserverMeta {
            observer_id: "1".to_owned(),
            stream_name: "default".to_owned(),
            epoch: 1,
            state: ObserverState::Leading,
            acked_seq: (1 << 32),
        };
        master.heartbeat(observer_meta).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn heartbeat_with_threshold_switching(
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut segment_meta = HashMap::new();
        segment_meta.insert("default".to_owned(), 1);

        let replicas = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let replicas_clone = replicas.clone();
        let segment_meta_clone = segment_meta.clone();

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async {
            let server = Server::new(segment_meta_clone, replicas_clone);
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let master = remote::RemoteMaster::new(&local_addr.to_string()).await?;
        let observer_meta = ObserverMeta {
            observer_id: "1".to_owned(),
            stream_name: "default".to_owned(),
            epoch: 1,
            state: ObserverState::Leading,
            acked_seq: (1 << 32) | ((super::mem::DEFAULT_NUM_THRESHOLD + 1) as u64),
        };
        let commands = master.heartbeat(observer_meta).await?;
        assert_eq!(commands.len(), 1);
        assert!(matches!(
            &commands[0],
            super::Command::Promote {
                role,
                epoch,
                leader: _
            }
            if *epoch == 2 && *role == Role::Leader
        ));
        Ok(())
    }
}

pub use self::remote::{MetaStream, RemoteMaster};
