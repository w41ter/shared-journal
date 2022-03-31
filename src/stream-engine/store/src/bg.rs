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

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::db::StreamDb;

enum Task {
    RecycleLog {
        log_number: u64,
        updated_streams: Vec<u64>,
    },
}

#[derive(Clone)]
pub struct BgTaskIssuer {
    sender: UnboundedSender<Task>,
}

impl BgTaskIssuer {
    pub fn recycle_log(&self, log_number: u64, updated_streams: Vec<u64>) {
        if self
            .sender
            .send(Task::RecycleLog {
                log_number,
                updated_streams,
            })
            .is_ok()
        {
            return;
        }

        tracing::warn!("recycling log {} task is dropped", log_number);
    }
}

pub struct BgTaskReceiver {
    receiver: UnboundedReceiver<Task>,
}

pub fn bg_channel() -> (BgTaskIssuer, BgTaskReceiver) {
    let (sender, receiver) = unbounded_channel();
    (BgTaskIssuer { sender }, BgTaskReceiver { receiver })
}

pub struct BgWorker {
    db: StreamDb,
    receiver: UnboundedReceiver<Task>,
}

impl BgWorker {
    pub fn new(db: StreamDb, receiver: BgTaskReceiver) -> Self {
        BgWorker {
            db,
            receiver: receiver.receiver,
        }
    }

    pub async fn run(&mut self) {
        while let Some(task) = self.receiver.recv().await {
            match task {
                Task::RecycleLog {
                    log_number,
                    updated_streams,
                } => self.on_recycle_log(log_number, updated_streams).await,
            }
        }
    }

    async fn on_recycle_log(&mut self, log_number: u64, updated_streams: Vec<u64>) {
        if let Err(err) = self.db.recycle_log(log_number, updated_streams).await {
            tracing::warn!("recycle log {}: {}", log_number, err);
        } else {
            tracing::info!("recycle log {} success", log_number);
        }
    }
}
