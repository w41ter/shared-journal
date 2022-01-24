// Copyright 2021 The Engula Authors.
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
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;

use crate::{Result, StreamReader, StreamWriter};

/// The role of a stream.
#[derive(Debug, Clone, Copy)]
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
#[derive(Debug)]
pub struct Journal {}

#[allow(dead_code, unused)]
impl Journal {
    /// Return the current epoch state of the specified stream.
    fn current_state(&self, stream_name: &str) -> Result<EpochState> {
        todo!();
    }

    /// Return a endless stream which returns a new epoch state once the
    /// associated stream enters a new epoch.
    fn subscribe_state(&self, stream_name: &str) -> Result<EpochStateStream> {
        todo!();
    }

    /// Lists streams.
    async fn list_streams(&self) -> Result<StreamLister> {
        todo!();
    }

    /// Creates a stream.
    ///
    /// # Errors
    ///
    /// Returns `Error::AlreadyExists` if the stream already exists.
    async fn create_stream(&self, name: &str) -> Result<()> {
        todo!();
    }

    /// Deletes a stream.
    ///
    /// Using a deleted stream is an undefined behavior.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the stream doesn't exist.
    async fn delete_stream(&self, name: &str) -> Result<()> {
        todo!();
    }

    /// Returns a stream reader.
    async fn new_stream_reader(&self, name: &str) -> Result<StreamReader> {
        todo!();
    }

    /// Returns a stream writer.
    async fn new_stream_writer(&self, name: &str) -> Result<StreamWriter> {
        todo!();
    }
}
