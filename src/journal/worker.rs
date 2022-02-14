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

mod core;
mod mem_store;
mod progress;
mod recovery;
mod timer;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::{self, JoinHandle},
};

use futures::channel::oneshot;
use tokio::{runtime::Handle as RuntimeHandle, sync::mpsc::UnboundedSender};

pub(super) use self::progress::Progress;
use self::{
    core::{Message, MsgDetail, StreamStateMachine},
    mem_store::MemStore,
    timer::ChannelTimer,
};
use super::{EpochState, ReplicatePolicy};
use crate::{
    journal::{
        master::{remote::RemoteMaster, Command as MasterCmd, Master, ObserverMeta},
        store::segment::{SegmentWriter, WriteRequest},
    },
    Result, Sequence,
};

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) enum Command {
    Tick,
    Promote {
        role: crate::Role,
        epoch: u32,
        leader: String,
        copy_set: Vec<String>,
        pending_epochs: Vec<u32>,
    },
    Msg(Message),
    Proposal {
        #[derivative(Debug = "ignore")]
        event: Box<[u8]>,
        #[derivative(Debug = "ignore")]
        sender: oneshot::Sender<Result<Sequence>>,
    },
    EpochState {
        #[derivative(Debug = "ignore")]
        sender: oneshot::Sender<EpochState>,
    },
    Recovered {
        seg_epoch: u32,
        writer_epoch: u32,
    },
}

struct ChannelState {
    buf: VecDeque<Command>,
    launcher: Option<Launcher>,
}

#[derive(Clone)]
pub(crate) struct Channel {
    stream_id: u64,
    state: Arc<Mutex<ChannelState>>,
}

impl Channel {
    pub fn new(stream_id: u64) -> Self {
        Channel {
            stream_id,
            state: Arc::new(Mutex::new(ChannelState {
                buf: VecDeque::new(),
                launcher: None,
            })),
        }
    }

    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    fn fetch(&self) -> Vec<Command> {
        let mut state = self.state.lock().unwrap();
        std::mem::take(&mut state.buf).into_iter().collect()
    }

    fn poll(&self, launcher: Launcher) {
        let mut state = self.state.lock().unwrap();
        if state.buf.is_empty() {
            state.launcher = Some(launcher);
        } else {
            drop(state);

            launcher.fire(self.stream_id);
        }
    }

    pub fn submit(&self, val: Command) {
        if let Some(launcher) = {
            let mut state = self.state.lock().unwrap();

            state.buf.push_back(val);
            state.launcher.take()
        } {
            launcher.fire(self.stream_id);
        };
    }
}

#[derive(Clone)]
struct Launcher {
    selector: Selector,
}

impl Launcher {
    fn fire(&self, stream_id: u64) {
        let (lock, cond) = &*self.selector.state;

        let mut state = lock.lock().unwrap();
        state.actives.insert(stream_id);
        if state.wait {
            state.wait = false;
            cond.notify_one();
        }
    }
}

#[derive(Clone)]
pub(crate) enum Action {
    Add {
        name: String,
        channel: Channel,
    },
    Observe {
        stream_id: u64,
        sender: UnboundedSender<EpochState>,
    },
    Remove(u64),
}

struct SelectorState {
    wait: bool,
    actives: HashSet<u64>,
    actions: Vec<Action>,
}

#[derive(Clone)]
pub(crate) struct Selector {
    state: Arc<(Mutex<SelectorState>, Condvar)>,
}

impl Selector {
    pub fn new() -> Self {
        Selector {
            state: Arc::new((
                Mutex::new(SelectorState {
                    wait: false,
                    actives: HashSet::new(),
                    actions: Vec::new(),
                }),
                Condvar::new(),
            )),
        }
    }

    fn select(&self, consumed: &[Channel]) -> (Vec<Action>, Vec<u64>) {
        let launcher = Launcher {
            selector: self.clone(),
        };
        for channel in consumed {
            channel.poll(launcher.clone());
        }

        let (lock, cond) = &*self.state;
        let mut state = lock.lock().unwrap();
        while state.actives.is_empty() && state.actions.is_empty() {
            state.wait = true;
            state = cond.wait(state).unwrap();
        }

        (
            std::mem::take(&mut state.actions),
            std::mem::take(&mut state.actives).into_iter().collect(),
        )
    }

    pub fn execute(&self, act: Action) {
        let (lock, cond) = &*self.state;

        let mut state = lock.lock().unwrap();
        state.actions.push(act);
        if state.wait {
            state.wait = false;
            cond.notify_one();
        }
    }
}

#[derive(Clone)]
struct WriterGroup {
    /// The epoch of segment this writer group belongs to.
    epoch: u32,

    writers: HashMap<String, SegmentWriter>,
}

impl WriterGroup {
    fn new(stream_id: u64, epoch: u32, copy_set: Vec<String>) -> Self {
        WriterGroup {
            epoch,
            writers: copy_set
                .into_iter()
                .map(|replica| {
                    (
                        replica.clone(),
                        SegmentWriter::new(stream_id, epoch, replica),
                    )
                })
                .collect(),
        }
    }

    #[inline(always)]
    pub fn epoch(&self) -> u32 {
        self.epoch
    }
}

fn flush_messages(
    runtime: &RuntimeHandle,
    channel: Channel,
    writer_group: &WriterGroup,
    pending_messages: Vec<Message>,
) {
    for msg in pending_messages {
        if let MsgDetail::Store {
            acked_seq,
            first_index,
            entries,
        } = msg.detail
        {
            let mut writer = writer_group
                .writers
                .get(&msg.target)
                .expect("target not exists in copy group")
                .clone();
            let seg_epoch = writer_group.epoch();
            let target = msg.target;
            let write = WriteRequest {
                epoch: msg.epoch,
                index: first_index,
                acked: acked_seq,
                entries,
            };
            let cloned_channel = channel.clone();
            runtime.spawn(async move {
                let epoch = write.epoch;
                match writer.write(write).await {
                    Ok(persisted_seq) => {
                        // TODO(w41ter) validate persisted seq.
                        let index = persisted_seq.index;
                        let msg = Message {
                            target,
                            seg_epoch,
                            epoch,
                            detail: MsgDetail::Received { index },
                        };
                        cloned_channel.submit(Command::Msg(msg));
                    }
                    Err(err) => {
                        println!("send write request to {}: {:?}", target, err);
                    }
                }
            });
        }
    }
}

async fn execute_master_command<M>(master: &M, stream_name: &str, channel: &Channel, cmd: MasterCmd)
where
    M: Master + Send + Sync + 'static,
{
    match cmd {
        MasterCmd::Promote {
            role,
            epoch,
            leader,
            pending_epochs,
        } => match master.get_segment(stream_name, epoch).await {
            Ok(Some(segment_meta)) => {
                channel.submit(Command::Promote {
                    role,
                    epoch,
                    leader,
                    copy_set: segment_meta.copy_set,
                    pending_epochs,
                });
            }
            _ => {
                // TODO(w41ter) handle error
                //  1. send heartbeat before get_segment
                //  2. get_segment failed
                todo!("log error message");
            }
        },
        MasterCmd::Nop => {}
    }
}

fn send_heartbeat<M>(
    runtime: &RuntimeHandle,
    master: &M,
    stream: &StreamStateMachine,
    observer_id: String,
    channel: Channel,
) where
    M: Master + Clone + Send + Sync + 'static,
{
    let stream_name = stream.name.clone();
    let observer_meta = ObserverMeta {
        observer_id,
        stream_name: stream_name.clone(),
        epoch: stream.epoch,
        state: stream.state,
        acked_seq: stream.acked_seq,
    };

    let master_cloned = master.clone();
    runtime.spawn(async move {
        match master_cloned.heartbeat(observer_meta).await {
            Ok(commands) => {
                for cmd in commands {
                    execute_master_command(&master_cloned, &stream_name, &channel, cmd).await;
                }
            }
            Err(error) => {
                todo!("log error message: {}", error);
            }
        }
    });
}

struct Applier {
    proposals: VecDeque<(Sequence, oneshot::Sender<Result<Sequence>>)>,
}

impl Applier {
    fn new() -> Self {
        Applier {
            proposals: VecDeque::new(),
        }
    }

    #[inline(always)]
    fn push_proposal(&mut self, seq: Sequence, sender: oneshot::Sender<Result<Sequence>>) {
        self.proposals.push_back((seq, sender));
    }

    /// Notify acked proposals.
    fn might_advance(&mut self, acked_seq: Sequence) {
        while let Some((seq, _)) = self.proposals.front() {
            if *seq > acked_seq {
                break;
            }

            let (seq, sender) = self.proposals.pop_front().unwrap();
            // The receiving end was canceled.
            sender.send(Ok(seq)).unwrap_or(());
        }
    }
}

pub(crate) struct WorkerOption {
    pub observer_id: String,
    pub master: RemoteMaster,
    pub selector: Selector,
    pub heartbeat_interval_ms: u64,

    pub runtime_handle: RuntimeHandle,
}

struct Worker {
    observer_id: String,
    selector: Selector,
    master: RemoteMaster,
    runtime_handle: RuntimeHandle,
    state_machines: HashMap<u64, StreamStateMachine>,
    appliers: HashMap<u64, Applier>,
    state_observers: HashMap<u64, UnboundedSender<EpochState>>,

    channel_timer: (ChannelTimer, JoinHandle<()>),

    consumed: Vec<Channel>,
    streams: HashMap<u64, Channel>,
    writer_groups: HashMap<(u64, u32), WriterGroup>,
}

impl Worker {
    pub fn new(opt: WorkerOption) -> Self {
        let channel_timer = ChannelTimer::new(opt.heartbeat_interval_ms);
        let cloned_timer = channel_timer.clone();
        let join_handle = thread::spawn(|| {
            cloned_timer.run();
        });
        Worker {
            observer_id: opt.observer_id,
            master: opt.master,
            selector: opt.selector,
            runtime_handle: opt.runtime_handle,
            state_machines: HashMap::new(),
            appliers: HashMap::new(),
            state_observers: HashMap::new(),

            channel_timer: (channel_timer, join_handle),

            consumed: Vec::new(),
            streams: HashMap::new(),
            writer_groups: HashMap::new(),
        }
    }

    fn execute_actions(&mut self, actions: Vec<Action>) {
        for act in actions {
            match act {
                Action::Add { name, channel } => {
                    let stream_id = channel.stream_id();
                    let state_machine = StreamStateMachine::new(name, stream_id);
                    if self.streams.insert(stream_id, channel.clone()).is_some() {
                        panic!("add a channel multiple times");
                    }
                    self.state_machines.insert(stream_id, state_machine);
                    self.appliers.insert(stream_id, Applier::new());
                    self.consumed.push(channel);
                }
                Action::Remove(stream_id) => {
                    self.streams.remove(&stream_id);
                    self.appliers.remove(&stream_id);
                    self.state_machines.remove(&stream_id);
                    self.state_observers.remove(&stream_id);
                    self.channel_timer.0.unregister(stream_id);
                }
                Action::Observe { stream_id, sender } => {
                    if let Some(channel) = self.streams.get(&stream_id) {
                        self.channel_timer.0.register(channel.clone());

                        let state_machine = self
                            .state_machines
                            .get(&stream_id)
                            .expect("stream should exists");
                        if sender.send(state_machine.epoch_state()).is_ok() {
                            self.state_observers.insert(stream_id, sender);
                        }
                    } else {
                        panic!("no such stream exists");
                    }
                }
            }
        }
    }

    fn handle_active_channels(&mut self, actives: Vec<u64>) {
        for stream_id in actives {
            let channel = match self.streams.get_mut(&stream_id) {
                Some(channel) => channel,
                None => {
                    // This stream has been removed.
                    continue;
                }
            };
            let commands = channel.fetch();
            let state_machine = self
                .state_machines
                .get_mut(&stream_id)
                .expect("stream already exists");

            let applier = self
                .appliers
                .get_mut(&stream_id)
                .expect("stream already exists");

            for cmd in commands {
                match cmd {
                    Command::Msg(msg) => {
                        state_machine.step(msg);
                    }
                    Command::Proposal { event, sender } => match state_machine.propose(event) {
                        Ok(seq) => {
                            applier.push_proposal(seq, sender);
                        }
                        Err(err) => {
                            sender.send(Err(err)).unwrap_or_default();
                        }
                    },
                    Command::Tick => {
                        send_heartbeat(
                            &self.runtime_handle,
                            &self.master,
                            state_machine,
                            self.observer_id.clone(),
                            channel.clone(),
                        );
                    }
                    Command::Promote {
                        epoch,
                        role,
                        leader,
                        copy_set,
                        pending_epochs,
                    } => {
                        // TODO(w41ter) since the epoch has already promoted,
                        // the former one needs to be gc.
                        self.writer_groups.insert(
                            (channel.stream_id(), epoch),
                            WriterGroup::new(channel.stream_id(), epoch, copy_set.clone()),
                        );
                        state_machine.promote(epoch, role, leader, copy_set, pending_epochs);
                        let state_observer = self.state_observers.get_mut(&stream_id);
                        if let Some(sender) = state_observer {
                            sender.send(state_machine.epoch_state()).unwrap_or_default();
                        }
                    }
                    Command::EpochState { sender } => {
                        sender.send(state_machine.epoch_state()).unwrap_or_default();
                    }
                    Command::Recovered {
                        seg_epoch,
                        writer_epoch,
                    } => {
                        state_machine.handle_recovered(seg_epoch, writer_epoch);
                    }
                }
            }

            if let Some(ready) = state_machine.collect() {
                let writer_group = self
                    .writer_groups
                    .get(&(channel.stream_id(), state_machine.epoch))
                    .expect("writer group should exists");
                flush_messages(
                    &self.runtime_handle,
                    channel.clone(),
                    writer_group,
                    ready.pending_messages,
                );
                applier.might_advance(ready.acked_seq);
                if let Some(pending_epoch) = ready.pending_epoch {
                    recovery::submit(
                        state_machine.replicate_policy,
                        state_machine.name.clone(),
                        state_machine.epoch,
                        pending_epoch,
                        channel.clone(),
                        self.master.clone(),
                    );
                }
                if ready.still_active {
                    // TODO(w41ter) support still active
                    continue;
                }
            }
            self.consumed.push(channel.clone());
        }
    }

    fn run(&mut self, exit_flag: Arc<AtomicBool>) {
        while !exit_flag.load(Ordering::Acquire) {
            // Read entries and actions from fired channels.
            let (actions, actives) = self.selector.select(&self.consumed);
            self.consumed.clear();

            self.execute_actions(actions);
            self.handle_active_channels(actives);
        }
    }
}

pub(crate) fn order_worker(opt: WorkerOption, exit_flag: Arc<AtomicBool>) {
    Worker::new(opt).run(exit_flag);
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;
    use crate::{servers::master::build_master, Role};

    async fn build_observe_test_env() -> Result<Selector> {
        let master_addr = build_master(&[]).await?;
        let master = RemoteMaster::new(&master_addr).await?;
        let selector = Selector::new();
        let worker_opt = WorkerOption {
            observer_id: "".to_string(),
            master,
            selector: selector.clone(),
            heartbeat_interval_ms: 10000,
            runtime_handle: tokio::runtime::Handle::current(),
        };

        thread::spawn(|| {
            let exit_flag = Arc::new(AtomicBool::new(false));
            order_worker(worker_opt, exit_flag);
        });

        Ok(selector)
    }

    fn add_channel(selector: &mut Selector, stream_id: u64, stream_name: &str) -> Channel {
        let channel = Channel::new(stream_id);
        let act = Action::Add {
            name: stream_name.to_string(),
            channel: channel.clone(),
        };
        selector.execute(act);
        channel
    }

    // Test the state observer's behavior.
    #[tokio::test(flavor = "multi_thread")]
    async fn action_observe_will_receive_at_least_one_epoch_state() -> Result<()> {
        let mut selector = build_observe_test_env().await?;

        let stream_id = 1;
        let channel = add_channel(&mut selector, stream_id, "DEFAULT");

        let (sender, mut receiver) = unbounded_channel();
        let act = Action::Observe { stream_id, sender };
        selector.execute(act);

        // A new observe action will receive the current epoch state.
        let first_epoch_state = receiver.recv().await.unwrap();
        assert_eq!(first_epoch_state.role, Role::Follower);

        // When stream becomes a follower, notify the observer.
        let mut new_epoch = first_epoch_state.epoch as u32 + 3;
        channel.submit(Command::Promote {
            epoch: new_epoch,
            role: Role::Follower,
            leader: "leader_1".to_string(),
            copy_set: vec![],
            pending_epochs: vec![],
        });

        let second_epoch_state = receiver.recv().await.unwrap();
        assert_eq!(second_epoch_state.epoch, new_epoch as u64);
        assert_eq!(second_epoch_state.role, Role::Follower);
        assert!(matches!(second_epoch_state.leader, Some(x) if x == "leader_1"));

        // When stream becomes a leader, notify the observer.
        new_epoch += 10;
        channel.submit(Command::Promote {
            epoch: new_epoch,
            role: Role::Leader,
            leader: "leader_2".to_string(),
            copy_set: vec![],
            pending_epochs: vec![],
        });
        let third_epoch_state = receiver.recv().await.unwrap();
        assert_eq!(third_epoch_state.epoch, new_epoch as u64);
        assert_eq!(third_epoch_state.role, Role::Leader);
        assert!(matches!(third_epoch_state.leader, Some(x) if x == "leader_2"));

        Ok(())
    }
}
