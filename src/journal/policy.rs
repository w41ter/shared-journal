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

use super::worker::Progress;
use crate::{Entry, Sequence};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Policy {
    /// A simple strategy that allows ack entries as long as one copy holds the
    /// dataset.
    ///
    /// This strategy is mainly used for testing.
    Simple,
}

impl Policy {
    /// Calculate the `acked_sequence` based on the matched indexes.
    pub(super) fn advance_acked_sequence(
        self,
        epoch: u32,
        progresses: &HashMap<String, Progress>,
    ) -> Sequence {
        match self {
            Policy::Simple => simple::advance_acked_sequence(epoch, progresses),
        }
    }

    /// Return the actual acked index, `None` if the indexes aren't enough.
    #[inline(always)]
    pub(super) fn actual_acked_index(
        self,
        num_copies: usize,
        acked_indexes: &[u32],
    ) -> Option<u32> {
        match self {
            Policy::Simple => simple::actual_acked_index(num_copies, acked_indexes),
        }
    }

    pub(super) fn new_group_reader(self, next_index: u32, num_copies: usize) -> GroupReader {
        GroupReader::new(self, next_index, num_copies)
    }
}

#[derive(Debug, Clone)]
pub(super) enum ReaderState {
    Polling,
    Ready { index: u32, entry: Entry },
    Done,
}

pub(super) enum GroupState {
    Pending,
    Active,
    Done,
}

#[derive(Debug)]
pub(super) struct GroupReader {
    num_ready: usize,
    num_done: usize,
    num_copies: usize,
    next_index: u32,
    policy: Policy,
}

impl GroupReader {
    fn new(policy: Policy, next_index: u32, num_copies: usize) -> Self {
        GroupReader {
            num_ready: 0,
            num_done: 0,
            num_copies,
            next_index,
            policy,
        }
    }

    pub(super) fn state(&self) -> GroupState {
        match self.policy {
            Policy::Simple => {
                if self.num_ready > 1 {
                    GroupState::Active
                } else if self.num_done == self.num_copies {
                    GroupState::Done
                } else {
                    GroupState::Pending
                }
            }
        }
    }

    #[inline(always)]
    pub(super) fn next_index(&self) -> u32 {
        self.next_index
    }

    pub(super) fn transform(
        &mut self,
        reader_state: &mut ReaderState,
        input: Option<(u32, Entry)>,
    ) {
        match input {
            Some((index, entry)) => {
                self.num_ready += 1;
                *reader_state = ReaderState::Ready { index, entry };
            }
            None => {
                self.num_done += 1;
                *reader_state = ReaderState::Done;
            }
        }
    }

    /// Read next entry of group state, panic if this isn't active
    pub(super) fn next_entry<'a, I>(&mut self, i: I) -> Option<Entry>
    where
        I: IntoIterator<Item = &'a mut ReaderState>,
    {
        // 1. found matched index
        let mut fresh_entry: Option<Entry> = None;
        for state in i.into_iter() {
            if let ReaderState::Ready { index, entry } = state {
                if *index == self.next_index {
                    self.num_ready -= 1;

                    if !fresh_entry
                        .as_ref()
                        .map(|e| e.epoch() >= entry.epoch())
                        .unwrap_or_default()
                    {
                        fresh_entry = Some(std::mem::replace(entry, Entry::Hole));
                    }
                    *state = ReaderState::Polling;
                }
            }
        }

        // skip to next
        self.next_index += 1;
        fresh_entry
    }
}

mod simple {
    use std::collections::HashMap;

    use super::Progress;
    use crate::Sequence;

    #[inline(always)]
    pub(super) fn advance_acked_sequence(
        epoch: u32,
        progresses: &HashMap<String, Progress>,
    ) -> Sequence {
        progresses
            .iter()
            .filter_map(|(_, p)| {
                if p.matched_index == 0 {
                    None
                } else {
                    Some(Sequence::new(epoch, p.matched_index))
                }
            })
            .max()
            .unwrap_or_default()
    }

    #[inline(always)]
    pub(super) fn actual_acked_index(_num_copies: usize, acked_indexes: &[u32]) -> Option<u32> {
        acked_indexes.iter().max().cloned()
    }
}
