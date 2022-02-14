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

mod mem;
pub mod segment;

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::{transport::Channel, Request, Response, Status, Streaming};

use crate::storepb;

#[derive(Debug)]
pub struct Server {
    store: Arc<Mutex<mem::Store>>,
}

impl Server {
    pub fn new() -> Self {
        Server {
            store: Arc::new(Mutex::new(mem::Store::new())),
        }
    }

    pub fn into_service(self) -> storepb::segment_store_server::SegmentStoreServer<Server> {
        storepb::segment_store_server::SegmentStoreServer::new(self)
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
#[allow(unused)]
impl storepb::segment_store_server::SegmentStore for Server {
    type ReadStream = mem::ReplicaReader;

    async fn write(
        &self,
        input: Request<storepb::WriteRequest>,
    ) -> Result<Response<storepb::WriteResponse>, Status> {
        let req = input.into_inner();
        let mut store = self.store.lock().await;
        let persisted_seq = (req.first_index as usize + req.entries.len()) as u64 - 1;
        store.write(
            req.stream_id,
            req.seg_epoch,
            req.epoch,
            req.acked_seq.into(),
            req.first_index,
            req.entries.into_iter().map(Into::into).collect(),
        )?;

        // TODO(w41ter) ensure previous sequences is acked.
        Ok(Response::new(storepb::WriteResponse { persisted_seq }))
    }

    async fn read(
        &self,
        input: Request<storepb::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let req = input.into_inner();
        let mut store = self.store.lock().await;
        let stream = store.read(
            req.stream_id,
            req.seg_epoch,
            req.start_index,
            req.limit as usize,
        )?;
        Ok(Response::new(stream))
    }

    async fn seal(
        &self,
        input: Request<storepb::SealRequest>,
    ) -> Result<Response<storepb::SealResponse>, Status> {
        let req = input.into_inner();
        let mut store = self.store.lock().await;
        let acked_index = store.seal(req.stream_id, req.seg_epoch, req.epoch)?;
        Ok(Response::new(storepb::SealResponse { acked_index }))
    }
}

type SegmentStoreClient = storepb::segment_store_client::SegmentStoreClient<Channel>;

#[derive(Clone)]
pub struct Client {
    client: SegmentStoreClient,
}

impl Client {
    pub async fn connect(addr: &str) -> crate::Result<Client> {
        let addr = format!("http://{}", addr);
        let client = SegmentStoreClient::connect(addr).await?;
        Ok(Client { client })
    }

    pub async fn write(
        &self,
        input: storepb::WriteRequest,
    ) -> crate::Result<storepb::WriteResponse> {
        let mut client = self.client.clone();
        let resp = client.write(input).await?;
        Ok(resp.into_inner())
    }

    pub async fn read(
        &self,
        input: storepb::ReadRequest,
    ) -> crate::Result<Streaming<storepb::ReadResponse>> {
        let mut client = self.client.clone();
        let resp = client.read(input).await?;
        Ok(resp.into_inner())
    }

    pub async fn seal(&self, input: storepb::SealRequest) -> crate::Result<storepb::SealResponse> {
        let mut client = self.client.clone();
        let resp = client.seal(input).await?;
        Ok(resp.into_inner())
    }
}

#[cfg(test)]
mod tests {

    use futures::StreamExt;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;
    use crate::{storepb::ReadRequest, Entry, Sequence};

    fn entry(event: Vec<u8>) -> storepb::Entry {
        storepb::Entry {
            entry_type: storepb::EntryType::Event as i32,
            epoch: 1,
            event,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn basic_store_and_read() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let writes = vec![
            storepb::WriteRequest {
                stream_id: 1,
                seg_epoch: 1,
                epoch: 1,
                acked_seq: 0,
                first_index: 0,
                entries: vec![entry(vec![0u8]), entry(vec![2u8]), entry(vec![4u8])],
            },
            storepb::WriteRequest {
                stream_id: 1,
                seg_epoch: 1,
                epoch: 1,
                acked_seq: Sequence::new(1, 2).into(),
                first_index: 3,
                entries: vec![entry(vec![6u8]), entry(vec![8u8])],
            },
            storepb::WriteRequest {
                stream_id: 1,
                seg_epoch: 1,
                epoch: 1,
                acked_seq: Sequence::new(1, 4).into(),
                first_index: 5,
                entries: vec![],
            },
        ];

        let entries = vec![
            Entry::Event {
                epoch: 1,
                event: vec![0u8].into(),
            },
            Entry::Event {
                epoch: 1,
                event: vec![2u8].into(),
            },
            Entry::Event {
                epoch: 1,
                event: vec![4u8].into(),
            },
            Entry::Event {
                epoch: 1,
                event: vec![6u8].into(),
            },
            Entry::Event {
                epoch: 1,
                event: vec![8u8].into(),
            },
        ];

        struct Test<'a> {
            from: u32,
            limit: u32,
            expect: &'a [Entry],
        }

        let tests = vec![
            Test {
                from: 0,
                limit: 1,
                expect: &entries[0..1],
            },
            Test {
                from: 3,
                limit: 2,
                expect: &entries[3..],
            },
            Test {
                from: 0,
                limit: 5,
                expect: &entries[..],
            },
        ];

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async move {
            let server = Server::new();
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let client = Client::connect(&local_addr.to_string()).await?;
        for w in writes {
            client.write(w).await?;
        }

        for test in tests {
            let req = ReadRequest {
                stream_id: 1,
                seg_epoch: 1,
                start_index: test.from,
                limit: test.limit,
                include_pending_entries: true,
            };
            let mut stream = client.read(req).await?;
            let mut got = Vec::<Entry>::new();
            while let Some(resp) = stream.next().await {
                got.push(resp?.entry.unwrap().into());
            }
            assert_eq!(got.len(), test.expect.len());
            assert!(got.iter().zip(test.expect.iter()).all(|(l, r)| l == r));
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reject_staled_sealing_request() -> crate::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async move {
            let server = Server::new();
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let client = Client::connect(&local_addr.to_string()).await?;
        client
            .seal(storepb::SealRequest {
                stream_id: 1,
                seg_epoch: 1,
                epoch: 3,
            })
            .await?;

        match client
            .seal(storepb::SealRequest {
                stream_id: 1,
                seg_epoch: 1,
                epoch: 2,
            })
            .await
        {
            Err(crate::Error::Staled(_)) => {}
            _ => {
                panic!("should reject staled sealing request");
            }
        };

        client
            .seal(storepb::SealRequest {
                stream_id: 1,
                seg_epoch: 1,
                epoch: 4,
            })
            .await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reject_staled_store_if_sealed() -> crate::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async move {
            let server = Server::new();
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let client = Client::connect(&local_addr.to_string()).await?;
        let write_req = storepb::WriteRequest {
            stream_id: 1,
            seg_epoch: 1,
            epoch: 1,
            acked_seq: 0,
            first_index: 0,
            entries: vec![entry(vec![0u8]), entry(vec![2u8]), entry(vec![4u8])],
        };
        client.write(write_req).await?;

        client
            .seal(storepb::SealRequest {
                stream_id: 1,
                seg_epoch: 1,
                epoch: 3,
            })
            .await?;

        let write_req = storepb::WriteRequest {
            stream_id: 1,
            seg_epoch: 1,
            epoch: 1,
            acked_seq: Sequence::new(1, 2).into(),
            first_index: 3,
            entries: vec![entry(vec![6u8]), entry(vec![8u8])],
        };
        match client.write(write_req).await {
            Err(crate::Error::Staled(_)) => {}
            _ => {
                panic!("should reject staled store request");
            }
        };

        Ok(())
    }
}
