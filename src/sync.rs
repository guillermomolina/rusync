use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use anyhow::Error;

use crate::entry::CopyEntry;
use crate::entry::Entry;
use crate::workers::{WalkStatus, WalkWorker, SyncStatus, SyncWorker, CopyStatus, CopyWorker, ProgressWorker};

#[derive(Copy, Clone)]
pub struct SyncOptions {
    /// Wether to preserve permissions of the source file after the destination is written.
    pub preserve_permissions: bool,
    pub perform_dry_run: bool,
    pub parallelism: usize,
    pub show_progress: bool,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            preserve_permissions: true,
            perform_dry_run: false,
            parallelism: 1,
            show_progress: false,
        }
    }
}

pub struct Sync {
    source: PathBuf,
    destination: PathBuf,
    options: SyncOptions,
}

impl Sync {
    pub fn new(source: &Path, destination: &Path, options: SyncOptions) -> Sync {
        Sync {
            source: source.to_path_buf(),
            destination: destination.to_path_buf(),
            options,
        }
    }

    pub fn sync(self) -> Result<u64, Error> {
        let (walk_output, sync_input) = channel::<Entry>();

        let walk_status = Arc::new(Mutex::new(WalkStatus::new()));
        let walk_worker = WalkWorker::new(walk_output, &self.source, Arc::clone(&walk_status));

        let (sync_output, copy_input) = channel::<CopyEntry>();
        let copy_input = Arc::new(Mutex::new(copy_input));

        let sync_status = Arc::new(Mutex::new(SyncStatus::new()));
        let mut sync_worker = SyncWorker::new(
            sync_input,
            sync_output,
            &self.source,
            &self.destination,
            Arc::clone(&sync_status),
        );

        let mut copy_workers = vec![];
        let mut copy_statuses = HashMap::new();
        for id in 0..self.options.parallelism {
            let copy_status = Arc::new(Mutex::new(CopyStatus::new(id as u64)));
            let copy_worker = CopyWorker::new(
                Arc::clone(&copy_input),
                Arc::clone(&copy_status),
            );
            copy_statuses.insert(id, copy_status);
            copy_workers.push(copy_worker);
        }

        let walk_thread = thread::spawn(move || walk_worker.start());
        let sync_thread = thread::spawn(move || sync_worker.start(&self.options));
        let mut copy_threads = vec![];
        for mut copy_worker in copy_workers {
            let copy_thread = thread::spawn(move || copy_worker.start());
            copy_threads.push(copy_thread);
        }

        let progress_thread = if self.options.show_progress {
            let progress_worker = ProgressWorker::new(walk_status, sync_status, copy_statuses);
            Some(thread::spawn(|| progress_worker.start()))
        } else {
            None
        };

        let _ = walk_thread.join();
        let _ = sync_thread.join();

        for copy_thread in copy_threads {
            let _ = copy_thread.join();
        }

        if progress_thread.is_some() {
            let _ = progress_thread.unwrap().join();
        }

        let errors = 0;
        Ok(errors)
    }
}
