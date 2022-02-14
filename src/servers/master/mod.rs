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

pub mod mem;
pub mod orchestrator;

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::mem::{MasterConfig, Server};
    use crate::Result;

    pub async fn build_master(replicas: &[&str]) -> Result<String> {
        let master_config = MasterConfig {
            heartbeat_interval_ms: 10,
            heartbeat_timeout_tick: 3,
        };
        let mut segment_meta = HashMap::new();
        segment_meta.insert("default".to_owned(), 1);

        let replicas: Vec<String> = replicas.iter().map(ToString::to_string).collect();
        let replicas_clone = replicas.clone();
        let segment_meta_clone = segment_meta.clone();

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async {
            let server = Server::new(master_config, segment_meta_clone, replicas_clone);
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        Ok(local_addr.to_string())
    }
}

#[cfg(test)]
pub(crate) use tests::build_master;
