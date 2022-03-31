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
    collections::{HashMap, HashSet, VecDeque},
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::{
    bg::BgTaskIssuer,
    fs::{layout, FileExt},
    DbOption, IoResult,
};

pub(crate) trait ReleaseReferringLogFile {
    /// All entries in the corresponding log file are acked or over written, so
    /// release the reference of the log file.
    fn release(&self, stream_id: u64, log_number: u64);
}

struct LogFileRef {
    released_streams: HashSet<u64>,
    touched_streams: HashSet<u64>,
}

#[derive(Clone)]
pub struct LogFileManager {
    opt: Arc<DbOption>,
    base_dir: PathBuf,
    issuer: BgTaskIssuer,
    inner: Arc<Mutex<LogFileManagerInner>>,
}

struct LogFileManagerInner {
    next_log_number: u64,
    recycled_log_files: VecDeque<u64>,
    log_file_refs: HashMap<u64, LogFileRef>,
}

impl LogFileManager {
    pub fn new<P: AsRef<Path>>(
        base_dir: P,
        next_log_number: u64,
        opt: Arc<DbOption>,
        issuer: BgTaskIssuer,
    ) -> Self {
        LogFileManager {
            opt,
            base_dir: base_dir.as_ref().to_path_buf(),
            issuer,
            inner: Arc::new(Mutex::new(LogFileManagerInner {
                recycled_log_files: VecDeque::new(),
                next_log_number,
                log_file_refs: HashMap::new(),
            })),
        }
    }

    pub fn recycle_all(&self, log_numbers: Vec<u64>) {
        let mut inner = self.inner.lock().unwrap();
        inner.recycled_log_files.extend(log_numbers.into_iter());
    }

    pub fn allocate_file(&self) -> IoResult<(u64, File)> {
        let (log_number, prev_log_number) = {
            let mut inner = self.inner.lock().unwrap();
            let log_number = inner.next_log_number;
            inner.next_log_number += 1;
            (log_number, inner.recycled_log_files.pop_front())
        };

        let log_file_name = layout::log(&self.base_dir, log_number);
        let prev_file_name = if let Some(prev_log_number) = prev_log_number {
            layout::log(&self.base_dir, prev_log_number)
        } else {
            let tmp = layout::temp(&self.base_dir, log_number);
            let mut file = OpenOptions::new().write(true).create(true).open(&tmp)?;
            file.preallocate(self.opt.log.log_file_size)?;
            tmp
        };
        std::fs::rename(prev_file_name, &log_file_name)?;
        let file = OpenOptions::new()
            .write(true)
            .truncate(false)
            .open(log_file_name)?;

        // See `man 2 fsync`:
        //
        // Calling fsync() does not necessarily ensure that the entry in the directory
        // containing the file has also reached disk.  For that an explicit fsync() on a
        // file descriptor for the directory is also needed.
        File::open(&self.base_dir)?.sync_all()?;

        Ok((log_number, file))
    }

    /// A log file is filled, delegate lifecycle to LogFileManager with the
    /// reference of streams.
    pub fn delegate(&self, log_number: u64, refer_streams: HashSet<u64>) {
        debug_assert!(!refer_streams.is_empty());
        let log_file_ref = LogFileRef {
            released_streams: HashSet::new(),
            touched_streams: refer_streams,
        };
        let mut inner = self.inner.lock().unwrap();
        assert!(
            inner
                .log_file_refs
                .insert(log_number, log_file_ref)
                .is_none(),
            "each file only allow to delegate once"
        );
    }

    pub fn option(&self) -> Arc<DbOption> {
        self.opt.clone()
    }
}

impl ReleaseReferringLogFile for LogFileManager {
    fn release(&self, stream_id: u64, log_number: u64) {
        let touched_streams = {
            let mut inner = self.inner.lock().unwrap();
            let file_ref = match inner.log_file_refs.get_mut(&log_number) {
                None => return,
                Some(file_ref) => file_ref,
            };
            file_ref.released_streams.insert(stream_id);
            if file_ref.released_streams.len() != file_ref.touched_streams.len() {
                return;
            }
            inner
                .log_file_refs
                .remove(&stream_id)
                .unwrap()
                .touched_streams
        };

        self.issuer
            .recycle_log(log_number, touched_streams.into_iter().collect());
    }
}
