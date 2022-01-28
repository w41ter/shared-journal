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
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::{self, JoinHandle},
    time::Instant,
};

use bitflags::bitflags;
use futures::channel::oneshot;
use tokio::runtime::Handle as RuntimeHandle;

use crate::{
    master::{Command as MasterCmd, Master, RemoteMaster},
    segment::{SegmentWriter, WriteRequest},
    Entry, Error, ObserverState, Role, Sequence, INITIAL_EPOCH,
};

/// Store entries for a stream.
struct MemStorage {
    epoch: u32,
    first_index: u32,
    entries: VecDeque<Entry>,
}

#[allow(unused)]
impl MemStorage {
    fn new(epoch: u32) -> Self {
        MemStorage {
            epoch,
            first_index: 0,
            entries: VecDeque::new(),
        }
    }

    /// Returns the index of next entry.
    fn next_index(&self) -> u32 {
        self.first_index + self.entries.len() as u32
    }

    /// Save entry and assign index.
    fn append(&mut self, entry: Entry) -> Sequence {
        let next_index = self.next_index();
        self.entries.push_back(entry);
        (self.epoch as u64) << 32 | (next_index as u64)
    }

    /// Range values.
    fn range(&self, r: std::ops::Range<u32>) -> Option<Vec<Entry>> {
        let next_index = self.next_index();
        if r.start < r.end && self.first_index <= r.start && r.end <= next_index {
            let start = (r.start - self.first_index) as usize;
            let end = (r.end - self.first_index) as usize;
            Some(self.entries.range(start..end).cloned().collect())
        } else {
            None
        }
    }

    /// Drain useless entries
    fn release(&mut self, until: u32) {
        let next_index = self.next_index();
        if self.first_index < until && until < next_index {
            let offset = until - self.first_index;
            self.entries.drain(..offset as usize);
        }
    }
}

/// An abstraction for a journal server to receive and persist entries.
///
/// For now, let's assume that message passing is reliable.
#[allow(unused)]
struct Progress {
    matched_index: u32,
    next_index: u32,

    start: usize,
    size: usize,
    in_flights: Vec<u32>,
}

#[allow(unused)]
impl Progress {
    const WINDOW: usize = 128;

    fn new() -> Self {
        Progress {
            matched_index: 0,
            next_index: 0,
            start: 0,
            size: 0,
            in_flights: vec![0; Self::WINDOW],
        }
    }

    /// Return which chunk needs to replicate to the target.
    fn next_chunk(&self, next_index: u32) -> (u32, u32) {
        if self.size == Self::WINDOW {
            (self.next_index, self.next_index)
        } else if next_index > self.next_index {
            (self.next_index, next_index)
        } else {
            (next_index, next_index)
        }
    }

    fn replicate(&mut self, next_index: u32) {
        debug_assert!(self.size < Self::WINDOW);
        debug_assert!(self.next_index < next_index);
        self.next_index = next_index;

        let off = (self.start + self.size) % Self::WINDOW;
        self.in_flights[off] = next_index;
        self.size += 1;
    }

    /// A server has stored entries.
    fn on_received(&mut self, epoch: u32, index: u32) -> bool {
        // TODO(w41ter) check epoch
        while self.size > 0 && self.in_flights[self.start] < index {
            self.start = (self.start + 1) % Self::WINDOW;
            self.size -= 1;
        }
        self.next_index = index + 1;
        self.size < Self::WINDOW
    }
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
#[allow(unused)]
pub(crate) enum MsgDetail {
    /// Store entries to.
    Store {
        acked_seq: u64,
        first_index: u32,
        #[derivative(Debug = "ignore")]
        entries: Vec<Entry>,
    },
    /// The journal server have received store request.
    Received { index: u32 },
}

/// An abstraction of data communication between `StreamStateMachine` and
/// journal servers.
#[derive(Debug, Clone)]
#[allow(unused)]
pub(crate) struct Message {
    target: String,
    seg_epoch: u32,
    epoch: u32,
    detail: MsgDetail,
}

#[derive(Derivative)]
#[derivative(Debug)]
#[allow(unused)]
pub(crate) enum Command {
    Tick,
    Promote {
        role: crate::Role,
        epoch: u32,
        leader: String,
        copy_set: Vec<String>,
    },
    Msg(Message),
    Proposal {
        #[derivative(Debug = "ignore")]
        event: Box<[u8]>,
        #[derivative(Debug = "ignore")]
        sender: oneshot::Sender<Result<Sequence, Error>>,
    },
}

#[allow(unused)]
enum ReplicationPolicy {
    /// A simple strategy that allows ack entries as long as one copy holds the
    /// dataset.
    ///
    /// This strategy is mainly used for testing.
    Simple,
}

#[allow(unused)]
impl ReplicationPolicy {
    fn advance(&self, epoch: u32, progresses: &HashMap<String, Progress>) -> Sequence {
        match self {
            ReplicationPolicy::Simple => progresses
                .iter()
                .map(|(_, p)| (epoch as u64) << 32 | (p.matched_index as u64))
                .max()
                .unwrap_or_default(),
        }
    }
}

#[derive(Default)]
#[allow(unused)]
struct Ready {
    still_active: bool,

    pending_messages: Vec<Message>,
}

bitflags! {
    struct Flags : u64 {
        const NONE = 0;
        const ACK_ADVANCED = 0x1;
    }
}

#[allow(unused)]
struct StreamStateMachine {
    name: String,
    epoch: u32,
    role: Role,
    leader: String,
    state: ObserverState,
    mem_store: MemStorage,
    copy_set: HashMap<String, Progress>,
    replication_policy: ReplicationPolicy,

    acked_seq: Sequence,
    ready: Ready,

    flags: Flags,
}

#[allow(unused)]
impl StreamStateMachine {
    fn new(name: String) -> Self {
        StreamStateMachine {
            name,
            epoch: INITIAL_EPOCH,
            role: Role::Follower,
            leader: "".to_owned(),
            state: ObserverState::Following,
            mem_store: MemStorage::new(INITIAL_EPOCH),
            copy_set: HashMap::new(),
            replication_policy: ReplicationPolicy::Simple,
            acked_seq: 0,
            ready: Ready::default(),
            flags: Flags::NONE,
        }
    }

    fn promote(&mut self, epoch: u32, role: Role, leader: String, copy_set: Vec<String>) {
        self.epoch = epoch;
        self.state = match role {
            Role::Leader => ObserverState::Leading,
            Role::Follower => ObserverState::Following,
        };
        self.role = role;
        self.mem_store = MemStorage::new(epoch);
        self.copy_set = copy_set
            .into_iter()
            .map(|remote| (remote, Progress::new()))
            .collect();
    }

    fn step(&mut self, msg: Message) {
        match msg.detail {
            MsgDetail::Received { index } => {
                if self.role == Role::Leader {
                    self.handle_received(msg.target, msg.epoch, index);
                } else {
                    todo!("log staled message");
                }
            }
            MsgDetail::Store {
                first_index: _,
                acked_seq: _,
                entries: _,
            } => unreachable!(),
        }
    }

    fn propose(&mut self, event: Box<[u8]>) -> Result<Sequence, Error> {
        if self.role == Role::Follower {
            Err(Error::NotLeader(self.leader.clone()))
        } else {
            let entry = Entry::Event {
                epoch: self.epoch,
                event,
            };
            Ok(self.mem_store.append(entry))
        }
    }

    fn collect(&mut self) -> Option<Ready> {
        if self.role == Role::Leader {
            self.advance();
            self.broadcast();
            self.flags = Flags::NONE;
            Some(std::mem::take(&mut self.ready))
        } else {
            None
        }
    }

    fn advance(&mut self) {
        debug_assert_eq!(self.role, Role::Leader);
        let acked_seq = self.replication_policy.advance(self.epoch, &self.copy_set);
        if self.acked_seq < acked_seq {
            self.acked_seq = acked_seq;
            self.flags |= Flags::ACK_ADVANCED;
        }
    }

    fn broadcast(&mut self) {
        debug_assert_eq!(self.role, Role::Leader);
        self.copy_set.iter_mut().for_each(|(server_id, progress)| {
            Self::replicate(
                &mut self.ready,
                progress,
                &self.mem_store,
                self.epoch,
                self.acked_seq,
                server_id,
                self.flags.contains(Flags::ACK_ADVANCED),
            );
        });
    }

    fn replicate(
        ready: &mut Ready,
        progress: &mut Progress,
        mem_store: &MemStorage,
        epoch: u32,
        acked_seq: u64,
        server_id: &str,
        bcast_acked_seq: bool,
    ) {
        let next_index = mem_store.next_index();
        let (start, end) = progress.next_chunk(next_index);
        let detail = match mem_store.range(start..end) {
            Some(entries) => {
                /// Do not forward acked sequence to unmatched index.
                let matched_acked_seq =
                    u64::min(acked_seq, (((epoch as u64) << 32) | (end - 1) as u64));
                MsgDetail::Store {
                    entries,
                    acked_seq: matched_acked_seq,
                    first_index: start,
                }
            }
            None if bcast_acked_seq => {
                // All entries are replicated, might broadcast acked
                // sequence.
                MsgDetail::Store {
                    entries: vec![],
                    acked_seq,
                    first_index: start,
                }
            }
            None => return,
        };

        let msg = Message {
            target: server_id.to_owned(),
            seg_epoch: epoch,
            epoch,
            detail,
        };

        ready.pending_messages.push(msg);
        if end < next_index {
            ready.still_active = true;
        }
    }

    fn handle_received(&mut self, target: String, epoch: u32, index: u32) {
        debug_assert_eq!(self.role, Role::Leader);
        if let Some(progress) = self.copy_set.get_mut(&target) {
            if progress.on_received(epoch, index) {
                Self::replicate(
                    &mut self.ready,
                    progress,
                    &self.mem_store,
                    self.epoch,
                    self.acked_seq,
                    &target,
                    true,
                );
            }
        }
    }
}

#[allow(unused)]
struct ChannelState {
    buf: VecDeque<Command>,
    launcher: Option<Launcher>,
}

#[derive(Clone)]
#[allow(unused)]
pub(crate) struct Channel {
    stream_id: u64,
    state: Arc<Mutex<ChannelState>>,
}

#[allow(unused)]
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
#[allow(unused)]
struct Launcher {
    selector: Selector,
}

#[allow(unused)]
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
#[allow(unused)]
pub(crate) enum Action {
    Add { name: String, channel: Channel },
    Observe(u64),
    Remove(u64),
}

#[allow(unused)]
struct SelectorState {
    wait: bool,
    actives: HashSet<u64>,
    actions: Vec<Action>,
}

#[derive(Clone)]
#[allow(unused)]
pub(crate) struct Selector {
    state: Arc<(Mutex<SelectorState>, Condvar)>,
}

#[allow(unused)]
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

    pub async fn execute(&self, act: Action) {
        let (lock, cond) = &*self.state;

        let mut state = lock.lock().unwrap();
        state.actions.push(act);
        if state.wait {
            state.wait = false;
            cond.notify_one();
        }
    }
}

#[allow(unused)]
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
}

#[allow(unused)]
fn flush_messages(
    runtime: &RuntimeHandle,
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
            let write = WriteRequest {
                epoch: msg.epoch,
                index: first_index,
                acked: acked_seq,
                entries,
            };
            runtime.spawn(async move {
                writer.store(write).await;
            });
        }
    }
}

#[allow(unused)]
async fn execute_master_command<M>(master: &M, stream_name: &str, channel: &Channel, cmd: MasterCmd)
where
    M: Master + Send + Sync + 'static,
{
    match cmd {
        MasterCmd::Promote {
            role,
            epoch,
            leader,
        } => match master.get_segment(stream_name, epoch).await {
            Ok(Some(segment_meta)) => {
                channel.submit(Command::Promote {
                    role,
                    epoch,
                    leader,
                    copy_set: segment_meta.copy_set,
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

#[allow(unused)]
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
    let observer_meta = crate::master::ObserverMeta {
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TimeEvent {
    /// The timestamp epoch since `Timer::baseline`.
    deadline_ms: u64,
    stream_id: u64,
}

#[allow(unused)]
struct TimerState {
    channels: HashMap<u64, Channel>,
    heap: BinaryHeap<Reverse<TimeEvent>>,
}

/// A monotonically increasing timer.
///
/// The smallest unit of time provided by the timer is milliseconds. The
/// object of registration timeout is channel, each channel only supports
/// registration once.
///
/// FIXME(w41ter) This implementation needs improvement, as the underlying
/// clock is not guaranteed to be completely steady.
#[derive(Clone)]
#[allow(unused)]
struct ChannelTimer {
    timeout_ms: u64,
    baseline: Instant,
    state: Arc<(Mutex<TimerState>, AtomicBool)>,
}

#[allow(unused)]
impl ChannelTimer {
    fn new(timeout_ms: u64) -> Self {
        ChannelTimer {
            timeout_ms,
            baseline: Instant::now(),
            state: Arc::new((
                Mutex::new(TimerState {
                    channels: HashMap::new(),
                    heap: BinaryHeap::new(),
                }),
                AtomicBool::new(false),
            )),
        }
    }

    /// Register channel into timer. Panic if there already exists a same
    /// channel.
    fn register(&self, channel: Channel) {
        let mut state = self.state.0.lock().unwrap();
        let stream_id = channel.stream_id();
        if state.channels.contains_key(&stream_id) {
            drop(state);
            panic!("duplicated register");
        }

        // FIXME(w41ter) shall we randomly shuffle deadlines?
        let deadline_ms = self.timestamp() + self.timeout_ms;
        state.channels.insert(stream_id, channel);
        state.heap.push(Reverse(TimeEvent {
            stream_id,
            deadline_ms,
        }));
    }

    /// Unregister channel from timer, do nothing if no such channel exists.
    fn unregister(&self, stream_id: u64) {
        let mut state = self.state.0.lock().unwrap();
        state.channels.remove(&stream_id);
    }

    /// This function allows calling thread to sleep for an interval. It not
    /// guaranteed that always returns after the specified interval has been
    /// passed, because this call might be interrupted by a single handler.
    #[cfg(target_os = "linux")]
    pub fn sleep(timeout_ms: u64) {
        use libc::{clock_nanosleep, CLOCK_MONOTONIC};

        let (sec, ms) = (timeout_ms / 1000, timeout_ms % 1000);
        let mut ts = libc::timespec {
            tv_sec: sec as i64,
            tv_nsec: (ms * 1000000) as i64,
        };
        unsafe {
            match clock_nanosleep(CLOCK_MONOTONIC, 0, &ts, std::ptr::null_mut()) {
                x if x < 0 && x != libc::EINTR => {
                    panic!("clock_nanosleep error code: {}", x);
                }
                _ => {}
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn sleep(timeout_ms: u64) {
        use std::time::Duration;
        thread::sleep(Duration::from_millis(timeout_ms));
    }

    fn run(self) {
        let mut next_timeout_ms: u64 = self.timeout_ms;
        let mut fired_channels: Vec<Channel> = Vec::new();
        while !self.state.1.load(Ordering::Acquire) {
            Self::sleep(next_timeout_ms);

            {
                let mut state = self.state.0.lock().unwrap();
                let now = self.timestamp();
                next_timeout_ms = self.timeout_ms;
                while let Some(event) = state.heap.peek() {
                    let TimeEvent {
                        mut deadline_ms,
                        stream_id,
                    } = event.0;
                    if now < deadline_ms {
                        next_timeout_ms = deadline_ms - now;
                        break;
                    }

                    if let Some(channel) = state.channels.get(&stream_id) {
                        // A channel might be fired multiple times if the calculated deadline
                        // is still expired.
                        fired_channels.push(channel.clone());
                        deadline_ms += self.timeout_ms;
                        state.heap.push(Reverse(TimeEvent {
                            stream_id,
                            deadline_ms,
                        }));
                    }

                    state.heap.pop();
                }
            }

            for channel in &fired_channels {
                channel.submit(Command::Tick);
            }
            fired_channels.clear();
        }
    }

    /// The timestamp epoch since `ChannelTimer::baseline`.
    fn timestamp(&self) -> u64 {
        std::time::Instant::now()
            .saturating_duration_since(self.baseline)
            .as_millis() as u64
    }

    fn close(&self) {
        self.state.1.store(true, Ordering::Release);
    }
}

#[allow(unused)]
pub(crate) struct WorkerOption {
    pub observer_id: String,
    pub master: RemoteMaster,
    pub selector: Selector,
    pub heartbeat_interval_ms: u64,

    pub runtime_handle: RuntimeHandle,
}

#[allow(unused)]
struct Worker {
    observer_id: String,
    selector: Selector,
    master: RemoteMaster,
    runtime_handle: RuntimeHandle,
    state_machines: HashMap<u64, StreamStateMachine>,

    channel_timer: (ChannelTimer, JoinHandle<()>),

    consumed: Vec<Channel>,
    streams: HashMap<u64, Channel>,
    writer_groups: HashMap<(u64, u32), WriterGroup>,
}

#[allow(unused)]
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
                    let state_machine = StreamStateMachine::new(name);
                    if self.streams.insert(stream_id, channel.clone()).is_some() {
                        panic!("add a channel multiple times");
                    }
                    self.state_machines.insert(stream_id, state_machine);
                    self.consumed.push(channel);
                }
                Action::Remove(stream_id) => {
                    self.streams.remove(&stream_id);
                    self.state_machines.remove(&stream_id);
                    self.channel_timer.0.unregister(stream_id);
                }
                Action::Observe(stream_id) => {
                    if let Some(channel) = self.streams.get(&stream_id) {
                        self.channel_timer.0.register(channel.clone());
                    } else {
                        panic!("no such stream exists");
                    }
                }
            }
        }
    }

    fn handle_active_channels(&mut self, actives: Vec<u64>) {
        for mut stream_id in actives {
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

            for cmd in commands {
                match cmd {
                    Command::Msg(msg) => state_machine.step(msg),
                    Command::Proposal { event, sender } => {
                        // FIXME(w41ter) event should be acked before returning.
                        sender.send(state_machine.propose(event));
                    }
                    Command::Tick => send_heartbeat(
                        &self.runtime_handle,
                        &self.master,
                        state_machine,
                        self.observer_id.clone(),
                        channel.clone(),
                    ),
                    Command::Promote {
                        epoch,
                        role,
                        leader,
                        copy_set,
                    } => {
                        /// TODO(w41ter) since the epoch has already promoted,
                        /// the former one needs to be gc.
                        self.writer_groups.insert(
                            (channel.stream_id(), epoch),
                            WriterGroup::new(channel.stream_id(), epoch, copy_set.clone()),
                        );
                        state_machine.promote(epoch, role, leader, copy_set);
                    }
                }
            }

            if let Some(mut ready) = state_machine.collect() {
                let writer_group = self
                    .writer_groups
                    .get(&(channel.stream_id(), state_machine.epoch))
                    .expect("writer group should exists");
                flush_messages(&self.runtime_handle, writer_group, ready.pending_messages);
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
    use std::time::Duration;

    use super::*;

    #[test]
    fn channel_timer_timeout() {
        let timer = ChannelTimer::new(100);
        let cloned_timer = timer.clone();
        let join_handle = std::thread::spawn(|| {
            cloned_timer.run();
        });

        let channel = Channel::new(1);
        timer.register(channel.clone());

        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(channel.fetch().len(), 0);

        std::thread::sleep(Duration::from_millis(200));
        timer.unregister(channel.stream_id());
        std::thread::sleep(Duration::from_millis(200));

        let cmds = channel.fetch();
        assert_eq!(cmds.len(), 2);
        assert!(matches!(cmds[0], Command::Tick));
        assert!(matches!(cmds[1], Command::Tick));

        timer.close();
        join_handle.join().unwrap();
    }

    #[test]
    fn mem_storage_append() {
        let mut mem_storage = MemStorage::new(0);
        for idx in 0..128 {
            let seq = mem_storage.append(Entry::Event {
                epoch: 0,
                event: Box::new([0u8]),
            });
            assert_eq!(seq, idx);
        }
    }

    #[test]
    fn mem_storage_range() {
        struct Test {
            entries: Vec<Entry>,
            range: std::ops::Range<u32>,
            expect: Option<Vec<Entry>>,
        }

        let ent = |v| Entry::Event {
            epoch: 1,
            event: vec![v].into(),
        };

        let tests = vec![
            // 1. empty request
            Test {
                entries: vec![],
                range: 0..0,
                expect: None,
            },
            // 2. empty request and out of range
            Test {
                entries: vec![],
                range: 1..1,
                expect: None,
            },
            // 3. single entry
            Test {
                entries: vec![ent(1)],
                range: 0..1,
                expect: Some(vec![ent(1)]),
            },
            // 4. out of range
            Test {
                entries: vec![ent(1)],
                range: 1..2,
                expect: None,
            },
            // 5. partially covered
            Test {
                entries: vec![ent(1), ent(2)],
                range: 1..2,
                expect: Some(vec![ent(2)]),
            },
            // 6. totally covered
            Test {
                entries: vec![ent(1), ent(2)],
                range: 0..2,
                expect: Some(vec![ent(1), ent(2)]),
            },
            // 7. out of range but partial covered
            Test {
                entries: vec![ent(1), ent(2)],
                range: 0..3,
                expect: None,
            },
            Test {
                entries: vec![ent(1), ent(2)],
                range: 1..3,
                expect: None,
            },
        ];

        for test in tests {
            let mut mem_store = MemStorage::new(1);
            for entry in test.entries {
                mem_store.append(entry);
            }
            let got = mem_store.range(test.range);
            match test.expect {
                Some(entries) => {
                    assert!(got.is_some());
                    assert!(entries
                        .into_iter()
                        .zip(got.unwrap().into_iter())
                        .all(|(l, r)| l == r));
                }
                None => {
                    assert!(got.is_none());
                }
            }
        }
    }
}