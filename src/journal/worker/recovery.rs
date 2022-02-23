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

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};
use tokio::runtime::Handle as RuntimeHandle;

use super::{
    core::{Learn, Learned, Message, MsgDetail},
    Channel, Command,
};
use crate::{journal::store::remote::Client, storepb, Entry};

type TonicResult<T> = std::result::Result<T, tonic::Status>;

struct TryBatchNext<'a, S>
where
    S: Stream<Item = TonicResult<storepb::ReadResponse>>,
{
    stream: &'a mut S,
    terminated: Option<TonicResult<()>>,
    entries: Vec<(u32, Entry)>,
}

impl<'a, S> TryBatchNext<'a, S>
where
    S: Stream<Item = TonicResult<storepb::ReadResponse>>,
{
    fn new(stream: &'a mut S) -> Self {
        TryBatchNext {
            stream,
            terminated: None,
            entries: Vec::default(),
        }
    }
}

impl<'a, S> Stream for TryBatchNext<'a, S>
where
    S: Stream<Item = TonicResult<storepb::ReadResponse>> + Unpin,
{
    type Item = TonicResult<Vec<(u32, Entry)>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        while this.terminated.is_none() {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(resp)) => match resp {
                    Ok(resp) => this.entries.push((resp.index, resp.entry.unwrap().into())),
                    Err(status) => {
                        this.terminated = Some(Err(status));
                    }
                },
                Poll::Ready(None) => {
                    this.terminated = Some(Ok(()));
                }
                Poll::Pending => {
                    if this.entries.is_empty() {
                        return Poll::Pending;
                    } else {
                        return Poll::Ready(Some(Ok(std::mem::take(&mut this.entries))));
                    }
                }
            }
        }

        if !this.entries.is_empty() {
            return Poll::Ready(Some(Ok(std::mem::take(&mut this.entries))));
        }

        match std::mem::replace(&mut this.terminated, Some(Ok(()))) {
            Some(Ok(())) => Poll::Ready(None),
            Some(Err(status)) => Poll::Ready(Some(Err(status))),
            None => unreachable!(),
        }
    }
}

fn consume_entries(channel: &Channel, learn: &Learn, entries: Vec<(u32, Entry)>) {
    let learned = Learned { entries };
    let msg = Message {
        target: learn.target.clone(),
        seg_epoch: learn.seg_epoch,
        epoch: learn.writer_epoch,
        detail: MsgDetail::Learned(learned),
    };
    channel.submit(Command::Msg(msg));
}

#[allow(dead_code)]
pub(super) fn learn_entries(
    runtime: &RuntimeHandle,
    channel: Channel,
    learn: Learn,
) -> tokio::task::JoinHandle<()> {
    runtime.spawn(async move {
        let client = Client::connect(&learn.target).await.unwrap();
        let req = storepb::ReadRequest {
            stream_id: channel.stream_id(),
            seg_epoch: learn.seg_epoch,
            start_index: learn.start_index,
            include_pending_entries: true,
            limit: u32::MAX,
        };

        let mut streaming = client.read(req).await.unwrap();
        let mut streaming = TryBatchNext::new(&mut streaming);
        loop {
            match streaming.next().await {
                Some(Ok(entries)) => {
                    consume_entries(&channel, &learn, entries);
                }
                Some(Err(_status)) => {
                    // TODO(w41ter) handle error
                }
                None => {
                    consume_entries(&channel, &learn, vec![]);
                    break;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        journal::worker::{
            core::{MutKind, Mutate},
            Message, MsgDetail, StreamStateMachine,
        },
        Role, Sequence,
    };

    fn handle_recovered(sm: &mut StreamStateMachine, seg_epoch: u32) {
        sm.step(Message {
            target: "self".into(),
            seg_epoch,
            epoch: sm.epoch,
            detail: MsgDetail::Recovered,
        });
    }

    fn receive_writes(sm: &mut StreamStateMachine, mutates: &Vec<Mutate>) {
        for mutate in mutates {
            match &mutate.kind {
                MutKind::Write(write) if !write.entries.is_empty() => {
                    let index = write.range.start + write.entries.len() as u32 - 1;
                    sm.step(Message {
                        target: mutate.target.clone(),
                        seg_epoch: mutate.writer_epoch,
                        epoch: mutate.writer_epoch,
                        detail: MsgDetail::Received { index },
                    });
                }
                _ => {}
            }
        }
    }

    #[test]
    fn blocking_advance_until_all_previous_are_acked() {
        let mut sm = StreamStateMachine::new("default".to_string(), 1);

        let epoch = 10;
        let copy_set = vec!["a".to_string(), "b".to_string()];
        sm.promote(
            epoch,
            Role::Leader,
            "".to_string(),
            copy_set,
            vec![(9, vec![])],
        );
        sm.propose([0u8].into()).unwrap();
        sm.propose([1u8].into()).unwrap();
        sm.propose([2u8].into()).unwrap();

        let ready = sm.collect();
        assert!(ready.is_some());
        assert!(ready.as_ref().unwrap().acked_seq <= Sequence::new(10, 0));

        receive_writes(&mut sm, &ready.as_ref().unwrap().pending_writes);

        // All entries are replicated.
        let ready = sm.collect();
        assert!(ready.is_some());
        assert!(ready.as_ref().unwrap().acked_seq <= Sequence::new(10, 0));

        // All segment are recovered.
        handle_recovered(&mut sm, 9);
        let ready = sm.collect();
        assert!(ready.is_some());
        assert!(ready.as_ref().unwrap().acked_seq >= Sequence::new(10, 3));
    }

    #[test]
    fn blocking_replication_if_exists_two_pending_segments() {
        let mut sm = StreamStateMachine::new("default".to_string(), 1);

        let epoch = 10;
        let copy_set = vec!["a".to_string(), "b".to_string()];
        sm.promote(
            epoch,
            Role::Leader,
            "".to_string(),
            copy_set,
            vec![(8, vec![]), (9, vec![])],
        );
        sm.propose([0u8].into()).unwrap();
        sm.propose([1u8].into()).unwrap();
        sm.propose([2u8].into()).unwrap();

        let ready = sm.collect();
        assert!(ready.is_some());
        assert!(ready.as_ref().unwrap().acked_seq <= Sequence::new(10, 0));

        if !ready.as_ref().unwrap().pending_writes.is_empty() {
            panic!("Do not replicate entries if there exists two pending segments");
        }

        // All entries are replicated.
        let ready = sm.collect();
        assert!(ready.is_some());
        assert!(ready.as_ref().unwrap().acked_seq <= Sequence::new(10, 0));

        // A segment is recovered.
        handle_recovered(&mut sm, 8);
        let ready = sm.collect();
        assert!(ready.is_some());
        assert!(ready.as_ref().unwrap().acked_seq <= Sequence::new(10, 0));

        receive_writes(&mut sm, &ready.as_ref().unwrap().pending_writes);

        // All segment are recovered.
        handle_recovered(&mut sm, 9);
        let ready = sm.collect();
        assert!(ready.is_some());
        assert!(ready.as_ref().unwrap().acked_seq >= Sequence::new(10, 3));
    }

    fn entry(event: Vec<u8>) -> storepb::Entry {
        storepb::Entry {
            entry_type: storepb::EntryType::Event as i32,
            epoch: 1,
            event,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn learn_all_entries() -> crate::Result<()> {
        use crate::servers::store::build_seg_store;

        let store_addr = build_seg_store().await?;
        let writes = vec![
            storepb::WriteRequest {
                stream_id: 1,
                seg_epoch: 1,
                epoch: 1,
                acked_seq: 0,
                first_index: 1,
                entries: vec![entry(vec![1u8]), entry(vec![2u8]), entry(vec![3u8])],
            },
            storepb::WriteRequest {
                stream_id: 1,
                seg_epoch: 1,
                epoch: 1,
                acked_seq: Sequence::new(1, 2).into(),
                first_index: 5,
                entries: vec![entry(vec![5u8]), entry(vec![6u8])],
            },
            storepb::WriteRequest {
                stream_id: 1,
                seg_epoch: 1,
                epoch: 1,
                acked_seq: Sequence::new(1, 2).into(),
                first_index: 8,
                entries: vec![entry(vec![8u8])],
            },
        ];

        let client = Client::connect(&store_addr).await?;
        for w in writes {
            client.write(w).await?;
        }

        let handle = tokio::runtime::Handle::current();
        let learn = Learn {
            target: store_addr,
            writer_epoch: 2,
            start_index: 3,
            seg_epoch: 1,
        };
        let channel = Channel::new(1);
        learn_entries(&handle, channel.clone(), learn)
            .await
            .unwrap();

        let commands = channel.fetch();
        assert_eq!(commands.len(), 2);
        let assert_learned = |cmd: &Command, entries: Vec<(u32, Entry)>| match cmd {
            Command::Msg(msg) => match &msg.detail {
                MsgDetail::Learned(learned) => {
                    assert_eq!(entries, learned.entries);
                }
                _ => panic!("unknown msg"),
            },
            _ => panic!("unknown cmd"),
        };
        assert_learned(
            &commands[0],
            vec![
                (3, entry(vec![3u8]).into()),
                (5, entry(vec![5u8]).into()),
                (6, entry(vec![6u8]).into()),
                (8, entry(vec![8u8]).into()),
            ],
        );
        assert_learned(&commands[1], vec![]);

        Ok(())
    }
}
