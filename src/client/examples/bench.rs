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

use std::time::Duration;

use clap::Parser;
use futures::StreamExt;
use shared_journal_client::{Engine, Error, Result, Role};
use components_metrics::run_metrics_service;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, required = true)]
    local_id: String,

    #[clap(short, long, default_value_t = String::from("0.0.0.0:21716"))]
    master: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    tokio::spawn(async {
        run_metrics_service(21722).await;
    });

    let url = format!("http://{}", args.master);
    let engine = Engine::new("1".to_owned(), url).await?;
    let tenant = match engine.create_tenant("tenant").await {
        Ok(tenant) => tenant,
        Err(Error::AlreadyExists(_)) => engine.tenant("tenant"),
        Err(err) => return Err(err),
    };
    let stream = match tenant.create_stream("stream").await {
        Ok(stream) => stream,
        Err(Error::AlreadyExists(_)) => tenant.stream("stream").await?,
        Err(err) => return Err(err),
    };

    let mut epoch_stream = stream.subscribe_state().await?;
    while let Some(epoch) = epoch_stream.next().await {
        if epoch.role == Role::Leader {
            break;
        }
    }

    for idx in 0..2 {
        let stream_cloned = stream.clone();
        tokio::spawn(async move {
            let values = vec![0u8; 1024];
            loop {
                stream_cloned.append(values.clone().into()).await.unwrap();
                println!("{} append values success", idx);
            }
        });
    }

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
