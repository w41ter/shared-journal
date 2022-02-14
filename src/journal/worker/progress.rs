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

/// An abstraction for a journal server to receive and persist entries.
///
/// For now, let's assume that message passing is reliable.
pub(in crate::journal) struct Progress {
    epoch: u32,

    // The default value is zero, so any proposal's index should greater than zero.
    pub matched_index: u32,
    next_index: u32,

    start: usize,
    size: usize,
    in_flights: Vec<u32>,
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
        debug_assert!(self.size < Self::WINDOW);
        debug_assert!(
            self.next_index <= next_index,
            "local next_index {}, but receive {}",
            self.next_index,
            next_index
        );
        self.next_index = next_index;

        let off = (self.start + self.size) % Self::WINDOW;
        self.in_flights[off] = next_index;
        self.size += 1;
    }

    /// A server has stored entries.
    pub fn on_received(&mut self, epoch: u32, index: u32) -> bool {
        if self.epoch != epoch {
            // Staled received request.
            false
        } else {
            // TODO(w41ter) check epoch
            while self.size > 0 && self.in_flights[self.start] < index {
                self.start = (self.start + 1) % Self::WINDOW;
                self.size -= 1;
            }
            self.matched_index = index;
            self.next_index = index + 1;
            self.size < Self::WINDOW
        }
    }
}
