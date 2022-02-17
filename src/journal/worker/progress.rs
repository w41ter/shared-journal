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

use std::{collections::VecDeque, ops::Range};

/// A mixin structure holds the fields used in congestion stage.
#[allow(dead_code)]
struct CongestMixin {
    window_size: usize,

    lost_bytes: usize,
    recoup_bytes: usize,

    latest_tick: usize,

    retransmit_ranges: VecDeque<(Range<u32>, usize)>,
}

#[allow(dead_code)]
struct SlidingWindow {
    acked_bytes: usize,
    send_bytes: usize,
    capacity: usize,

    start: usize,
    size: usize,
    window: Vec<(u32, usize)>,
}

impl SlidingWindow {
    const KB: usize = 1024;
    const MB: usize = 1024 * Self::KB;
    const MIN_WINDOW: usize = 128;

    fn new(capacity: usize) -> Self {
        let window = Self::MIN_WINDOW.max(capacity / Self::MB);
        SlidingWindow {
            acked_bytes: 0,
            send_bytes: 0,
            capacity,

            start: 0,
            size: 0,
            window: vec![(0, 0); window],
        }
    }

    /// Returns the available space for the sliding window.
    #[allow(dead_code)]
    fn available(&self) -> usize {
        if self.window.len() == self.size {
            0
        } else {
            let consumed = self.send_bytes.saturating_sub(self.acked_bytes);
            self.capacity.saturating_sub(consumed)
        }
    }

    /// Allocate space and record relevant data.
    fn replicate(&mut self, next_index: u32, bytes: usize) {
        debug_assert!(self.size < self.window.len());

        let off = (self.start + self.size) % self.window.len();
        self.window[off] = (next_index, bytes);
        self.size += 1;
        self.send_bytes += bytes;
    }

    fn release(&mut self, received_index: u32) {
        while self.size > 0 && self.window[self.start].0 <= received_index {
            self.acked_bytes += self.window[self.start].1;
            self.start = (self.start + 1) % self.window.len();
            self.size -= 1;
        }
    }
}

/// An abstraction for describing the state of a segment store, that receives
/// and persists entries.
#[allow(dead_code)]
pub(in crate::journal) struct Progress {
    epoch: u32,

    // The default value is zero, so any proposal's index should greater than zero.
    pub matched_index: u32,
    next_index: u32,

    start: usize,
    size: usize,
    in_flights: Vec<u32>,

    sliding_window: SlidingWindow,

    congest: Option<Box<CongestMixin>>,
}

impl Progress {
    const WINDOW: usize = 128;

    pub fn new(epoch: u32) -> Self {
        Progress {
            epoch,
            matched_index: 0,
            next_index: 1,
            start: 0,
            size: 0,
            in_flights: vec![0; Self::WINDOW],

            sliding_window: SlidingWindow::new(1024),
            congest: None,
        }
    }

    /// Return which chunk needs to replicate to the target.
    pub fn next_chunk(&self, next_index: u32) -> (u32, u32) {
        if self.size == Self::WINDOW {
            (self.next_index, self.next_index)
        } else if next_index > self.next_index {
            (self.next_index, next_index)
        } else {
            (next_index, next_index)
        }
    }

    pub fn replicate(&mut self, next_index: u32) {
        debug_assert!(
            self.next_index <= next_index,
            "local next_index {}, but receive {}",
            self.next_index,
            next_index
        );
        self.next_index = next_index;
        self.sliding_window.replicate(next_index, 0);
    }

    /// A server has stored entries.
    pub fn on_received(&mut self, epoch: u32, index: u32) -> bool {
        if self.epoch != epoch {
            // Staled received request.
            false
        } else {
            self.sliding_window.release(index);
            self.matched_index = index;
            self.next_index = index + 1;
            self.size < Self::WINDOW
        }
    }

    #[allow(dead_code, unused)]
    pub fn on_timeout(&mut self, range: std::ops::Range<u32>) -> bool {
        false
    }

    /// Return whether a target has been matched to the corresponding index.
    #[allow(dead_code, unused)]
    pub fn is_matched(&self, index: u32) -> bool {
        false
    }
}
