use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::sync::Mutex;

use log::{debug, error};

use anyhow::Error;

use crate::entry::CopyEntry;
use crate::fsops;
use crate::fsops::CopyOutcome;

pub struct CopyStatus {
    /// ID of the sync thread
    pub worker_id: u64,
    /// Name of the file being transferred
    pub current_file: String,
    /// Size of the current file (in bytes)
    pub file_size: usize,
    /// Number of bytes transfered for the current file
    pub file_transfered_size: usize,
    /// Total number of bytes transfered across all files
    pub total_transfered_size: usize,
    /// Number of files transfered
    pub num_transfered_files: u64,
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
            total_transfered_size: 0,
            num_transfered_files: 0,
            copy_done: false,
        }
    }

    pub fn new_file(&mut self, name: &str, file_size: usize) {
        self.current_file = name.to_string();
        self.file_size = file_size;
        self.file_transfered_size = 0;
    }

    pub fn done_copying(&mut self) {
        self.new_file("", 0);
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
        CopyWorker { input, status }
    }

    pub fn start(&mut self) -> () {
        while let Ok(copy_entry) = {
            let copy_entry = self.input.lock().unwrap().recv();
            copy_entry
        } {
            match self.copy(&copy_entry) {
                Ok(_) => {
                    if !copy_entry.is_chunk() || copy_entry.is_last_chunk() {
                        self.status.lock().unwrap().num_transfered_files += 1;
                        debug!(
                            "[{}] Copied: {}",
                            self.status.lock().unwrap().worker_id,
                            copy_entry.src.description()
                        );
                    }
                }
                Err(error) => {
                    error!(
                        "[{}] Error copying: {} {:#}",
                        self.status.lock().unwrap().worker_id,
                        copy_entry.src.description(),
                        error
                    );
                }
            };
        }
        self.status.lock().unwrap().done_copying();
    }

    fn copy(&mut self, copy_entry: &CopyEntry) -> Result<CopyOutcome, Error> {
        self.status
            .lock()
            .unwrap()
            .new_file(copy_entry.src.description(), copy_entry.src.length().unwrap());
        let outcome = if copy_entry.is_chunk() {
            fsops::copy_chunk(copy_entry, &self.status)
        } else {
            fsops::copy_entry(copy_entry, &self.status)
        };
        #[cfg(unix)]
        if outcome.is_ok() && (!copy_entry.is_chunk() || copy_entry.is_last_chunk()) {
            let opts = &copy_entry.opts;
            if opts.preserve_permissions && !opts.perform_dry_run {
                fsops::copy_permissions(&copy_entry.src, &copy_entry.dest)?;
            }
        }
        outcome
    }
}
