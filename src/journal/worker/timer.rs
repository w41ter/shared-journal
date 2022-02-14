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
    collections::{BinaryHeap, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

use super::{Channel, Command};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TimeEvent {
    /// The timestamp epoch since `Timer::baseline`.
    deadline_ms: u64,
    stream_id: u64,
}

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
pub(super) struct ChannelTimer {
    timeout_ms: u64,
    baseline: Instant,
    state: Arc<(Mutex<TimerState>, AtomicBool)>,
}

impl ChannelTimer {
    pub fn new(timeout_ms: u64) -> Self {
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
    pub fn register(&self, channel: Channel) {
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
    pub fn unregister(&self, stream_id: u64) {
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
        let ts = libc::timespec {
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

    pub fn run(self) {
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

    #[allow(dead_code)]
    pub fn close(&self) {
        self.state.1.store(true, Ordering::Release);
    }
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
}
