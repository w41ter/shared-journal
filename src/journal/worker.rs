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
    ops::Add,
    sync::Arc,
    time::{Duration, Instant},
};

use bitflags::bitflags;
use tokio::sync::{Mutex, Notify};

use crate::{
    master::{Command as MasterCmd, Master, RemoteMaster},
    segment::{SegmentWriter, WriteRequest},
    Entry, ObserverState, Role, Sequence,
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

    /// Returns the index of last entry.
    fn last_index(&self) -> u32 {
        self.first_index + self.entries.len() as u32
    }

    /// Save entry and assign index.
    fn append(&mut self, entry: Entry) -> Sequence {
        self.entries.push_back(entry);
        let last_index = self.last_index();
        (self.epoch as u64) << 32 | (last_index as u64)
    }

    /// Range values.
    fn range(&self, r: std::ops::Range<u32>) -> Option<Vec<Entry>> {
        let last_index = self.last_index();
        if self.first_index <= r.start && r.end <= last_index + 1 {
            let start = (r.start - self.first_index) as usize;
            let end = (r.end - self.first_index) as usize;
            Some(self.entries.range(start..end).cloned().collect())
        } else {
            None
        }
    }

    /// Drain useless entries
    fn release(&mut self, until: u32) {
        let last_index = self.last_index();
        if self.first_index < until && until <= last_index {
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
    fn next_chunk(&self, last_index: u32) -> (u32, u32) {
        if self.size == Self::WINDOW {
            (self.next_index, self.next_index)
        } else if last_index > self.next_index {
            (self.next_index, last_index)
        } else {
            (last_index, last_index)
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

#[derive(Clone)]
#[allow(unused)]
enum MsgDetail {
    /// Store entries to.
    Store {
        acked_seq: u64,
        first_index: u32,
        entries: Vec<Entry>,
    },
    /// The journal server have received store request.
    Received { index: u32 },
}

/// An abstraction of data communication between `StreamStateMachine` and
/// journal servers.
#[derive(Clone)]
#[allow(unused)]
struct Message {
    target: String,
    seg_epoch: u32,
    epoch: u32,
    detail: MsgDetail,
}

#[allow(unused)]
enum Command {
    Tick,
    Promote {
        role: crate::Role,
        epoch: u32,
        leader: String,
        copy_set: Vec<String>,
    },
    Msg(Message),
    Proposal(Box<[u8]>),
}

#[allow(unused)]
enum Error {
    NotLeader(String),
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
    fn promote(&mut self, epoch: u32, role: Role, leader: String, copy_set: Vec<String>) {
        self.epoch = epoch;
        self.state = match role {
            Role::Leader => ObserverState::Leading,
            Role::Follower => ObserverState::Following,
        };
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
        let last_index = mem_store.last_index();
        let (start, end) = progress.next_chunk(last_index);
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
        if end <= last_index {
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
struct Channel {
    stream_id: u64,
    state: Arc<Mutex<ChannelState>>,
}

#[allow(unused)]
impl Channel {
    fn new(stream_id: u64) -> Self {
        Channel {
            stream_id,
            state: Arc::new(Mutex::new(ChannelState {
                buf: VecDeque::new(),
                launcher: None,
            })),
        }
    }

    fn stream_id(&self) -> u64 {
        self.stream_id
    }

    async fn fetch(&self) -> Vec<Command> {
        let mut state = self.state.lock().await;
        std::mem::take(&mut state.buf).into_iter().collect()
    }

    async fn poll(&self, launcher: Launcher) {
        let mut state = self.state.lock().await;
        if state.buf.is_empty() {
            state.launcher = Some(launcher);
        } else {
            drop(state);

            launcher.fire(self.stream_id);
        }
    }

    async fn send(&self, val: Command) {
        if let Some(launcher) = {
            let mut state = self.state.lock().await;

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
    async fn fire(&self, stream_id: u64) {
        let (lock, notify) = &*self.selector.inner;

        let mut inner = lock.lock().await;
        inner.actives.insert(stream_id);
        if inner.wait {
            inner.wait = false;
            notify.notify_one();
        }
    }
}

#[allow(unused)]
struct SelectorState {
    wait: bool,
    actives: HashSet<u64>,
}

#[derive(Clone)]
#[allow(unused)]
pub(crate) struct Selector {
    inner: Arc<(Mutex<SelectorState>, Notify)>,
}

#[allow(unused)]
impl Selector {
    pub fn new() -> Self {
        Selector {
            inner: Arc::new((
                Mutex::new(SelectorState {
                    wait: false,
                    actives: HashSet::new(),
                }),
                Notify::new(),
            )),
        }
    }

    async fn select(&self, consumed: &[Channel]) -> Vec<u64> {
        let launcher = Launcher {
            selector: self.clone(),
        };
        for channel in consumed {
            channel.poll(launcher.clone()).await;
        }

        let (lock, notify) = &*self.inner;
        loop {
            let mut inner = lock.lock().await;
            if !inner.actives.is_empty() {
                inner.wait = true;
                drop(inner);
                notify.notified().await;
                continue;
            }
            return std::mem::take(&mut inner.actives).into_iter().collect();
        }
    }
}

#[allow(unused)]
struct WriterGroup {
    /// The epoch of segment this writer group belongs to.
    epoch: u32,

    writers: HashMap<String, SegmentWriter>,
}

#[allow(unused)]
fn flush_messages(writer_group: &WriterGroup, pending_messages: Vec<Message>) {
    for msg in pending_messages {
        if let MsgDetail::Store {
            acked_seq,
            first_index,
            entries,
        } = msg.detail
        {
            let writer = writer_group
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
            tokio::spawn(async move {
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
                channel
                    .send(Command::Promote {
                        role,
                        epoch,
                        leader,
                        copy_set: segment_meta.copy_set,
                    })
                    .await;
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
fn send_heartbeat<M>(master: &M, stream: &StreamStateMachine, observer_id: String, channel: Channel)
where
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
    tokio::spawn(async move {
        match master_cloned.heartbeat(observer_meta).await {
            Ok(commands) => {
                for cmd in commands {
                    execute_master_command(&master_cloned, &stream_name, &channel, cmd).await;
                }
            }
            Err(error) => {
                todo!("log error message");
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
    state: Arc<Mutex<TimerState>>,
}

#[allow(unused)]
impl ChannelTimer {
    fn new(timeout_ms: u64) -> Self {
        ChannelTimer {
            timeout_ms,
            baseline: Instant::now(),
            state: Arc::new(Mutex::new(TimerState {
                channels: HashMap::new(),
                heap: BinaryHeap::new(),
            })),
        }
    }

    /// Register channel into timer. Panic if there already exists a same
    /// channel.
    async fn register(&self, channel: Channel) {
        let mut state = self.state.lock().await;
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
    async fn unregister(&self, stream_id: u64) {
        let mut state = self.state.lock().await;
        state.channels.remove(&stream_id);
    }

    async fn run(&self) {
        use tokio::time::{sleep_until, Instant as TokioInstant};

        let mut next_deadline_ms: u64 = self.timeout_ms;
        let mut fired_channels: Vec<Channel> = Vec::new();
        loop {
            sleep_until(TokioInstant::from_std(
                self.baseline.add(Duration::from_millis(next_deadline_ms)),
            ))
            .await;

            {
                let mut state = self.state.lock().await;
                let now = self.timestamp();
                next_deadline_ms = now + self.timeout_ms;
                while let Some(event) = state.heap.peek() {
                    let TimeEvent {
                        mut deadline_ms,
                        stream_id,
                    } = event.0;
                    if now < deadline_ms {
                        next_deadline_ms = deadline_ms;
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
                channel.send(Command::Tick).await;
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
}

#[allow(unused)]
pub(crate) struct WorkerOption {
    pub observer_id: String,
    pub master: RemoteMaster,
    pub selector: Selector,
}

#[allow(unused)]
struct Worker {
    observer_id: String,
    selector: Selector,
    master: RemoteMaster,
    streams: HashMap<u64, StreamStateMachine>,
}

#[allow(unused)]
impl Worker {
    pub fn new(opt: WorkerOption) -> Self {
        Worker {
            observer_id: opt.observer_id,
            master: opt.master,
            selector: opt.selector,
            streams: HashMap::new(),
        }
    }
}

#[allow(unused)]
pub(crate) async fn order_worker(opt: WorkerOption) {
    let mut w = Worker::new(opt);
    let mut consumed: Vec<Channel> = Vec::new();
    let mut streams: HashMap<u64, Channel> = HashMap::new();
    let mut writer_groups: HashMap<u32, WriterGroup> = HashMap::new();

    loop {
        // Read entries from fired channels.
        let actives = w.selector.select(&consumed).await;
        consumed.clear();
        for mut stream_id in actives {
            let channel = streams.get_mut(&stream_id).expect("");
            let commands = channel.fetch().await;
            let stream = w
                .streams
                .get_mut(&stream_id)
                .expect("stream already exists");

            for cmd in commands {
                match cmd {
                    Command::Msg(msg) => stream.step(msg),
                    Command::Proposal(event) => {
                        stream.propose(event);
                    }
                    Command::Tick => {
                        send_heartbeat(&w.master, stream, w.observer_id.clone(), channel.clone())
                    }
                    Command::Promote {
                        epoch,
                        role,
                        leader,
                        copy_set,
                    } => stream.promote(epoch, role, leader, copy_set),
                }
            }

            if let Some(mut ready) = stream.collect() {
                let writer_group = writer_groups
                    .get(&stream.epoch)
                    .expect("writer group should exists");
                flush_messages(writer_group, ready.pending_messages);
                if ready.still_active {
                    // TODO(w41ter) support still active
                    continue;
                }
            }
            consumed.push(channel.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn channel_timer_timeout() {
        let timer = ChannelTimer::new(100);
        let bg_timer = timer.clone();
        tokio::spawn(async move { bg_timer.run().await });

        let channel = Channel::new(1);
        timer.register(channel.clone()).await;

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(channel.fetch().await.len(), 0);

        tokio::time::sleep(Duration::from_millis(200)).await;
        timer.unregister(channel.stream_id()).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let cmds = channel.fetch().await;
        assert_eq!(cmds.len(), 2);
        assert!(matches!(cmds[0], Command::Tick));
        assert!(matches!(cmds[1], Command::Tick));
    }
}
