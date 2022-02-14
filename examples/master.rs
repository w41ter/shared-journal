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

use std::collections::HashMap;

use clap::Parser;
use engula_shared_journal::{MasterConfig, MasterServer};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value_t = String::from("0.0.0.0:8929"))]
    endpoint: String,

    #[clap(short, long, required = true)]
    stores: Vec<String>,
}

async fn bootstrap_service(endpoint: &str, replicas: &[String]) -> Result<()> {
    let cfg = MasterConfig {
        heartbeat_timeout_tick: 3,
        heartbeat_interval_ms: 500,
    };
    let master_server = MasterServer::new(cfg, HashMap::new(), replicas.into());
    let listener = TcpListener::bind(endpoint).await?;
    tonic::transport::Server::builder()
        .add_service(master_server.into_service())
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let args = Args::parse();
    bootstrap_service(&args.endpoint, &args.stores).await?;

    println!("Bye");

    Ok(())
}
