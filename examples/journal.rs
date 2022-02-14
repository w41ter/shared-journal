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

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use engula_shared_journal::{build_journal, Error as JournalError, Journal, JournalOption, Role};
use futures::StreamExt;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, required = true)]
    local_id: String,

    #[clap(short, long, default_value_t = String::from("0.0.0.0:8929"))]
    master: String,

    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Streams,
    Create {
        #[clap(long, required = true)]
        stream: String,
    },
    Delete {
        #[clap(long, required = true)]
        stream: String,
    },
    Load {
        #[clap(long, required = true)]
        stream: String,
        #[clap(long, required = true)]
        path: PathBuf,
    },
    Append {
        #[clap(long, required = true)]
        stream: String,
        #[clap(long, required = true)]
        event: Vec<u8>,
    },
    Subscribe {
        #[clap(long, required = true)]
        stream: String,
        #[clap(long)]
        start: Option<u64>,
    },
}

async fn list_streams(journal: &Journal) -> Result<()> {
    let mut streaming = journal.list_streams().await?;
    while let Some(item) = streaming.next().await {
        let item = item?;
        println!("{}", item);
    }
    Ok(())
}

async fn create_stream(journal: &Journal, stream_name: String) -> Result<()> {
    journal.create_stream(&stream_name).await?;
    Ok(())
}

async fn delete_stream(journal: &mut Journal, stream_name: String) -> Result<()> {
    journal.delete_stream(&stream_name).await?;
    Ok(())
}

async fn load_events<P: AsRef<Path>>(
    journal: &mut Journal,
    stream_name: String,
    path: P,
) -> Result<()> {
    use tokio::{
        fs::File,
        io::{AsyncBufReadExt, BufReader},
    };

    let path = path.as_ref();
    let file = File::open(path).await?;
    let buf = BufReader::new(file);
    let mut lines = buf.lines();
    let mut writer = journal.new_stream_writer(&stream_name).await?;
    let mut state_stream = journal.subscribe_state(&stream_name).await?;

    let mut line: Option<String> = None;
    'OUTER: while let Some(state) = state_stream.next().await {
        if state.role != Role::Leader {
            continue;
        }

        loop {
            if line.is_none() {
                line = lines.next_line().await?;
                if line.is_none() {
                    // All events are consumed.
                    break 'OUTER;
                }
            }
            let content = line.as_ref().unwrap();
            match writer.append(content.as_bytes().into()).await {
                Ok(seq) => {
                    println!("{}", seq);
                    line = None;
                }
                Err(JournalError::NotLeader(_)) => {
                    break;
                }
                Err(err) => Err(err)?,
            };
        }
    }

    Ok(())
}

async fn append_event(journal: &mut Journal, stream_name: String, event: Vec<u8>) -> Result<()> {
    let mut state_stream = journal.subscribe_state(&stream_name).await?;
    let mut writer = journal.new_stream_writer(&stream_name).await?;
    while let Some(state) = state_stream.next().await {
        if state.role != Role::Leader {
            continue;
        }

        match writer.append(event.clone()).await {
            Ok(seq) => {
                println!("{}", seq);
                break;
            }
            Err(JournalError::NotLeader(_)) => {}
            Err(err) => Err(err)?,
        };
    }
    Ok(())
}

async fn subscribe_events(
    journal: &Journal,
    stream_name: String,
    start: Option<u64>,
) -> Result<()> {
    let mut reader = journal.new_stream_reader(&stream_name).await?;
    reader.seek(start.unwrap_or_default().into()).await?;
    while let Some((seq, event)) = reader.wait_next().await? {
        println!("{} => {:?}", seq, event);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let args = Args::parse();
    let opt = JournalOption {
        local_id: args.local_id,
        master_url: args.master,
        heartbeat_interval: 500,
    };
    let mut journal = build_journal(opt).await?;
    match args.cmd {
        Command::Streams => list_streams(&journal).await?,
        Command::Create { stream } => create_stream(&journal, stream).await?,
        Command::Delete { stream } => delete_stream(&mut journal, stream).await?,
        Command::Load { stream, path } => load_events(&mut journal, stream, &path).await?,
        Command::Append { stream, event } => append_event(&mut journal, stream, event).await?,
        Command::Subscribe { stream, start } => subscribe_events(&journal, stream, start).await?,
    }

    println!("Bye");

    Ok(())
}
