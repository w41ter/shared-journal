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

pub mod stream;
mod worker;

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
    task::{Context, Poll},
    thread::{self, JoinHandle},
};

use futures::Stream;
use stream::{StreamReader, StreamWriter};

use self::worker::{Action, Channel, Selector};
use crate::{
    master::{Master, RemoteMaster},
    Error, Result, StreamMeta,
};

/// The role of a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum Role {
    /// A leader manipulate a stream.
    Leader,
    /// A follower subscribes a stream.
    Follower,
}

/// The role and leader's address of current epoch.
#[derive(Debug, Clone)]
pub struct EpochState {
    pub epoch: u64,

    /// The role of the associated stream.
    pub role: Role,

    /// The leader of the associated stream.
    pub leader: Option<String>,
}

#[derive(Debug)]
pub struct StreamLister {}

impl Stream for StreamLister {
    type Item = Result<String>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!();
    }
}

#[derive(Debug)]
pub struct EpochStateStream {}

impl Stream for EpochStateStream {
    type Item = EpochState;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!();
    }
}

/// A root structure of shared journal. This journal's streams divide time into
/// epochs, and each epoch have at most one producer.
#[allow(unused)]
pub struct Journal {
    master: RemoteMaster,
    stream_meta: HashMap<String, StreamMeta>,
    observed_streams: HashMap<u64, self::worker::Channel>,
    selector: Selector,

    worker_handle: (Arc<AtomicBool>, JoinHandle<()>),
}

#[allow(dead_code, unused)]
impl Journal {
    /// Return the current epoch state of the specified stream.
    pub fn current_state(&self, stream_name: &str) -> Result<EpochState> {
        todo!();
    }

    /// Return a endless stream which returns a new epoch state once the
    /// associated stream enters a new epoch.
    pub async fn subscribe_state(&mut self, stream_name: &str) -> Result<EpochStateStream> {
        let channel = self.open_stream_channel(stream_name).await?;

        let act = Action::Observe(channel.stream_id());
        self.selector.execute(act).await;

        // TODO fill epoch state stream.
        Ok(EpochStateStream {})
    }

    /// Lists streams.
    pub async fn list_streams(&self) -> Result<StreamLister> {
        todo!();
    }

    /// Creates a stream.
    ///
    /// # Errors
    ///
    /// Returns `Error::AlreadyExists` if the stream already exists.
    pub async fn create_stream(&self, name: &str) -> Result<()> {
        todo!();
    }

    /// Deletes a stream.
    ///
    /// Using a deleted stream is an undefined behavior.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the stream doesn't exist.
    pub async fn delete_stream(&mut self, stream_name: &str) -> Result<()> {
        let stream_meta = self.get_stream_meta(stream_name).await?;
        let act = Action::Remove(stream_meta.stream_id);
        self.selector.execute(act).await;
        Ok(())
    }

    /// Returns a stream reader.
    pub async fn new_stream_reader(&self, name: &str) -> Result<StreamReader> {
        todo!();
    }

    /// Returns a stream writer.
    pub async fn new_stream_writer(&mut self, stream_name: &str) -> Result<StreamWriter> {
        let channel = self.open_stream_channel(stream_name).await?;
        Ok(StreamWriter::new(channel))
    }
}

#[allow(dead_code)]
impl Journal {
    fn new(
        master: RemoteMaster,
        selector: Selector,
        worker_handle: (Arc<AtomicBool>, JoinHandle<()>),
    ) -> Self {
        Journal {
            master,
            stream_meta: HashMap::new(),
            observed_streams: HashMap::new(),
            selector,
            worker_handle,
        }
    }

    async fn get_stream_meta(&mut self, stream_name: &str) -> Result<StreamMeta> {
        match self.stream_meta.get(stream_name) {
            Some(meta) => Ok(meta.clone()),
            None => {
                let stream_meta = match self.master.get_stream(stream_name).await? {
                    Some(stream_meta) => stream_meta,
                    None => return Err(Error::NotFound(stream_name.to_owned())),
                };
                self.stream_meta
                    .insert(stream_name.to_owned(), stream_meta.clone());
                Ok(stream_meta)
            }
        }
    }

    async fn open_stream_channel(&mut self, stream_name: &str) -> Result<self::worker::Channel> {
        let stream_meta = self.get_stream_meta(stream_name).await?;
        let stream_id = stream_meta.stream_id;
        let channel = match self.observed_streams.get(&stream_id) {
            Some(channel) => channel.clone(),
            None => {
                let channel = Channel::new(stream_meta.stream_id);
                let act = Action::Add {
                    name: stream_meta.stream_name.clone(),
                    channel: channel.clone(),
                };
                self.observed_streams.insert(stream_id, channel.clone());
                self.selector.execute(act).await;
                channel
            }
        };

        Ok(channel)
    }
}

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct JournalOption {
    /// The **unique** ID used to identify this client.
    local_id: String,

    /// The address descriptor of the master.
    master_url: String,

    /// Heartbeat intervals in milliseconds.
    heartbeat_interval: u64,
}

/// Create and initialize a `Journal`, and start related asynchronous tasks.
#[allow(unused)]
pub async fn build_journal(opt: JournalOption) -> Result<Journal> {
    use self::worker::WorkerOption;
    use crate::master::RemoteMaster;

    let master = RemoteMaster::new(&opt.master_url).await?;
    let selector = Selector::new();
    let worker_opt = WorkerOption {
        observer_id: opt.local_id,
        master: master.clone(),
        selector: selector.clone(),
        heartbeat_interval_ms: opt.heartbeat_interval,
    };

    let exit_flag = Arc::new(AtomicBool::new(false));
    let cloned_exit_flag = exit_flag.clone();
    let join_handle = thread::spawn(|| {
        use self::worker::order_worker;

        order_worker(worker_opt, cloned_exit_flag);
    });

    Ok(Journal::new(master, selector, (exit_flag, join_handle)))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;
    use crate::master::mem::Server;

    type TResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    async fn build_master(replicas: &[&str]) -> TResult<String> {
        let mut segment_meta = HashMap::new();
        segment_meta.insert("default".to_owned(), 1);

        let replicas: Vec<String> = replicas.iter().map(ToString::to_string).collect();
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

        Ok(local_addr.to_string())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn build_journal_and_run() -> TResult<()> {
        let master_addr = build_master(&["a", "b", "c"]).await?;
        let opt = JournalOption {
            local_id: "1".to_owned(),
            master_url: master_addr.to_string(),
            heartbeat_interval: 500,
        };
        let journal = build_journal(opt).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        drop(journal);
        Ok(())
    }
}
