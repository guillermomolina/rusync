use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::sync::Mutex;

use log::{debug, error};

use anyhow::Error;

use crate::entry::CopyEntry;
use crate::entry::Entry;
use crate::fsops;
use crate::fsops::SyncOutcome;
use crate::sync::SyncOptions;

pub struct CopyStatus {
    /// ID of the sync thread
    pub worker_id: u64,
    /// Name of the file being transferred
    pub current_file: String,
    /// Size of the current file (in bytes)
    pub file_size: usize,
    /// Number of bytes transfered for the current file
    pub file_transfered_size: usize,
    /// Done copying process
    pub copy_done: bool,
}

impl CopyStatus {
    pub fn new(worker_id: u64) -> CopyStatus {
        CopyStatus {
            worker_id,
            current_file: String::new(),
            file_size: 0,
            file_transfered_size: 0,
            copy_done: false,
        }
    }

    pub fn new_file(&mut self, name: &str) {
        self.current_file = name.to_string();
        self.file_size = 0;
        self.file_transfered_size = 0;
    }

    pub fn done_copying(&mut self) {
        self.new_file("");
        self.copy_done = true;
    }
}

pub struct CopyWorker {
    input: Arc<Mutex<Receiver<CopyEntry>>>,
    status: Arc<Mutex<CopyStatus>>,
}

impl CopyWorker {
    pub fn new(
        input: Arc<Mutex<Receiver<CopyEntry>>>,
        status: Arc<Mutex<CopyStatus>>,
    ) -> CopyWorker {
        CopyWorker {
            input,
            status,
        }
    }

    pub fn start(&mut self) -> () {
        while let Ok(copy_entry) = {
            let copy_entry = self.input.lock().unwrap().recv();
            copy_entry
        } {
            match self.copy(&copy_entry.src, &copy_entry.dest, &copy_entry.opts) {
                Ok(_) => {
                    if !copy_entry.src.is_chunk() || copy_entry.src.is_last_chunk() {
                        debug!("[{}] Synced: {}", self.status.lock().unwrap().worker_id, copy_entry.src.description());
                    }
                },
                Err(error) => {
                    error!("[{}] Error syncing: {} {:#}", self.status.lock().unwrap().worker_id, copy_entry.src.description(), error);
                },
            };
        }
        self.status
            .lock()
            .unwrap()
            .done_copying();
    }

    fn copy(&mut self, src: &Entry, dest: &Entry, opts: &SyncOptions) -> Result<SyncOutcome, Error> {
        self.status
            .lock()
            .unwrap()
            .new_file(src.description());
        if src.is_chunk() {
            return fsops::copy_chunk(src, dest, &opts, &self.status);
        } else {
            return fsops::copy_entry(src, dest, &opts, &self.status);
        }
        }
}
