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

use std::collections::VecDeque;

use crate::{Entry, Sequence};

/// Store entries for a stream.
pub(super) struct MemStore {
    epoch: u32,

    /// It should always greater than zero, see `journal::worker::Progress` for
    /// details.
    first_index: u32,
    entries: VecDeque<Entry>,
}

impl MemStore {
    pub fn new(epoch: u32) -> Self {
        MemStore {
            epoch,
            first_index: 1,
            entries: VecDeque::new(),
        }
    }

    /// Returns the index of next entry.
    pub fn next_index(&self) -> u32 {
        self.first_index + self.entries.len() as u32
    }

    /// Save entry and assign index.
    pub fn append(&mut self, entry: Entry) -> Sequence {
        let next_index = self.next_index();
        self.entries.push_back(entry);
        Sequence::new(self.epoch, next_index)
    }

    /// Range values.
    pub fn range(&self, r: std::ops::Range<u32>) -> Option<Vec<Entry>> {
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
    #[allow(dead_code)]
    pub fn release(&mut self, until: u32) {
        let next_index = self.next_index();
        if self.first_index < until && until < next_index {
            let offset = until - self.first_index;
            self.entries.drain(..offset as usize);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mem_storage_append() {
        let mut mem_storage = MemStore::new(0);
        for idx in 0..128 {
            let seq = mem_storage.append(Entry::Event {
                epoch: 0,
                event: Box::new([0u8]),
            });
            assert_eq!(<Sequence as Into<u64>>::into(seq), idx + 1);
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
                range: 1..1,
                expect: None,
            },
            // 2. empty request and out of range
            Test {
                entries: vec![],
                range: 2..2,
                expect: None,
            },
            // 3. single entry
            Test {
                entries: vec![ent(1)],
                range: 1..2,
                expect: Some(vec![ent(1)]),
            },
            // 4. out of range
            Test {
                entries: vec![ent(1)],
                range: 2..3,
                expect: None,
            },
            // 5. partially covered
            Test {
                entries: vec![ent(1), ent(2)],
                range: 2..3,
                expect: Some(vec![ent(2)]),
            },
            // 6. totally covered
            Test {
                entries: vec![ent(1), ent(2)],
                range: 1..3,
                expect: Some(vec![ent(1), ent(2)]),
            },
            // 7. out of range but partial covered
            Test {
                entries: vec![ent(1), ent(2)],
                range: 1..4,
                expect: None,
            },
            Test {
                entries: vec![ent(1), ent(2)],
                range: 2..4,
                expect: None,
            },
        ];

        for test in tests {
            let mut mem_store = MemStore::new(1);
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
