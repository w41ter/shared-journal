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

pub(crate) mod master;
mod policy;
pub(crate) mod store;
mod worker;

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
    task::{Context, Poll},
    thread::{self, JoinHandle},
};

use futures::{channel::oneshot, ready, Stream};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

pub(crate) use self::policy::Policy as ReplicatePolicy;
pub use self::store::{StreamReader, StreamWriter};
use self::{
    master::{remote::RemoteMaster, Master},
    worker::{Action, Channel, Command, Selector},
};
use crate::{Error, Result};

/// The meta of a stream.
#[derive(Clone, Debug)]
pub struct StreamMeta {
    /// The internal unique ID of stream.
    pub stream_id: u64,

    pub stream_name: String,
}

/// The role of a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// A leader manipulate a stream.
    Leader,
    /// A follower subscribes a stream.
    Follower,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Role::Leader => "LEADER",
                Role::Follower => "FOLLOWER",
            }
        )
    }
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

pub struct StreamLister {
    streaming: master::remote::ListStream,
}

impl Stream for StreamLister {
    type Item = Result<String>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = ready!(Pin::new(&mut self.get_mut().streaming).poll_next(cx));
        Poll::Ready(item.map(|item| item.map(|meta| meta.stream_name)))
    }
}

#[derive(Debug)]
pub struct EpochStateStream {
    receiver: UnboundedReceiver<EpochState>,
}

impl Stream for EpochStateStream {
    type Item = EpochState;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().receiver).poll_recv(cx)
    }
}

/// A root structure of shared journal. This journal's streams divide time into
/// epochs, and each epoch have at most one producer.
pub struct Journal {
    master: RemoteMaster,
    stream_meta: HashMap<String, StreamMeta>,
    observed_streams: HashMap<u64, self::worker::Channel>,
    selector: Selector,

    #[allow(dead_code)]
    worker_handle: (Arc<AtomicBool>, JoinHandle<()>),
}

impl Journal {
    /// Return the current epoch state of the specified stream.
    pub async fn current_state(&mut self, stream_name: &str) -> Result<EpochState> {
        let channel = self.open_stream_channel(stream_name).await?;

        let (sender, receiver) = oneshot::channel();
        channel.submit(Command::EpochState { sender });

        let epoch_state = receiver.await?;
        Ok(epoch_state)
    }

    /// Return a endless stream which returns a new epoch state once the
    /// associated stream enters a new epoch.
    pub async fn subscribe_state(&mut self, stream_name: &str) -> Result<EpochStateStream> {
        let channel = self.open_stream_channel(stream_name).await?;

        let (sender, receiver) = unbounded_channel();
        let act = Action::Observe {
            stream_id: channel.stream_id(),
            sender,
        };
        self.selector.execute(act);

        Ok(EpochStateStream { receiver })
    }

    /// Lists streams.
    pub async fn list_streams(&self) -> Result<StreamLister> {
        let streaming = self.master.list_stream().await?;
        Ok(StreamLister { streaming })
    }

    /// Creates a stream.
    ///
    /// # Errors
    ///
    /// Returns `Error::AlreadyExists` if the stream already exists.
    pub async fn create_stream(&self, stream_name: &str) -> Result<()> {
        self.master.create_stream(stream_name).await
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
        self.selector.execute(act);
        Ok(())
    }

    /// Returns a stream reader.
    pub async fn new_stream_reader(&self, stream_name: &str) -> Result<StreamReader> {
        // TODO(w41ter) set replicate policy
        Ok(StreamReader::new(
            ReplicatePolicy::Simple,
            stream_name,
            self.master.clone(),
        ))
    }

    /// Returns a stream writer.
    pub async fn new_stream_writer(&mut self, stream_name: &str) -> Result<StreamWriter> {
        let channel = self.open_stream_channel(stream_name).await?;
        Ok(StreamWriter::new(channel))
    }
}

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
            Some(channel) => (*channel).clone(),
            None => {
                let channel = Channel::new(stream_meta.stream_id);
                let act = Action::Add {
                    name: stream_meta.stream_name,
                    channel: channel.clone(),
                };
                self.observed_streams.insert(stream_id, channel.clone());
                self.selector.execute(act);
                channel
            }
        };

        Ok(channel)
    }
}

#[derive(Debug, Clone)]
pub struct JournalOption {
    /// The **unique** ID used to identify this client.
    pub local_id: String,

    /// The address descriptor of the master.
    pub master_url: String,

    /// Heartbeat intervals in milliseconds.
    pub heartbeat_interval: u64,
}

/// Create and initialize a `Journal`, and start related asynchronous tasks.
pub async fn build_journal(opt: JournalOption) -> Result<Journal> {
    use self::worker::WorkerOption;

    let master = RemoteMaster::new(&opt.master_url).await?;
    let selector = Selector::new();
    let worker_opt = WorkerOption {
        observer_id: opt.local_id,
        master: master.clone(),
        selector: selector.clone(),
        heartbeat_interval_ms: opt.heartbeat_interval,
        runtime_handle: tokio::runtime::Handle::current(),
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
    use std::time::Duration;

    use super::*;
    use crate::servers::{master::build_master, store::build_seg_store};

    #[tokio::test(flavor = "multi_thread")]
    async fn build_journal_and_run() -> Result<()> {
        let master_addr = build_master(&["a", "b", "c"]).await?;
        let opt = JournalOption {
            local_id: "1".to_owned(),
            master_url: master_addr.to_string(),
            heartbeat_interval: 500,
        };
        let journal = build_journal(opt).await?;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        drop(journal);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_writer_append() -> Result<()> {
        let server_addr = build_seg_store().await?;
        let master_addr = build_master(&[&server_addr]).await?;
        let opt = JournalOption {
            local_id: "1".to_owned(),
            master_url: master_addr.to_string(),
            heartbeat_interval: 10,
        };
        let mut journal = build_journal(opt).await?;
        journal.subscribe_state("default").await?;

        thread::sleep(Duration::from_millis(20));
        let mut stream_writer = journal.new_stream_writer("default").await?;
        stream_writer.append(vec![0u8; 1]).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_current_state() -> Result<()> {
        let server_addr = build_seg_store().await?;
        let master_addr = build_master(&[&server_addr]).await?;
        let opt = JournalOption {
            local_id: "1".to_owned(),
            master_url: master_addr.to_string(),
            heartbeat_interval: 10,
        };
        let mut journal = build_journal(opt).await?;
        journal.subscribe_state("default").await?;

        thread::sleep(Duration::from_millis(20));
        let epoch_state = journal.current_state("default").await?;
        assert_eq!(epoch_state.role, Role::Leader);

        Ok(())
    }
}
