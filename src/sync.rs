use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use anyhow::{anyhow, Error, Result};

use crate::entry::Entry;
use crate::fsops;
use crate::fsops::SyncOutcome::*;
use crate::workers::{ProgressWorker, SyncWorker, WalkWorker, SyncProgress, WalkProgress};

#[derive(Debug)]
pub struct Stats {
    /// Number of files in the source
    pub num_files: u64,
    /// Sum of the sizes of all the files in the source
    pub total_size: usize,
    /// Sum of the sizes of all the files that were synced
    pub total_transfered: usize,

    /// Number of files transfered (should match `num_files`
    /// if no error)
    pub num_synced: u64,
    /// Number of files for which the copy was skipped
    pub up_to_date: u64,
    /// Number of files that were copied
    pub copied: u64,
    /// Number of errors
    pub errors: u64,

    /// Number of symlink created in the destination folder
    pub symlink_created: u64,
    /// Number of symlinks updated in the destination folder
    pub symlink_updated: u64,

    /// Duration of the transfer
    pub duration: std::time::Duration,

    start: std::time::Instant,
}

impl Stats {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Stats {
        Stats {
            num_files: 0,
            total_size: 0,
            total_transfered: 0,

            num_synced: 0,
            up_to_date: 0,
            copied: 0,
            errors: 0,

            symlink_created: 0,
            symlink_updated: 0,
            start: std::time::Instant::now(),
            duration: std::time::Duration::new(0, 0),
        }
    }

    pub fn start(&mut self) {
        self.start = std::time::Instant::now();
    }

    pub fn stop(&mut self) {
        let end = std::time::Instant::now();
        self.duration = end - self.start;
    }

    pub fn duration(&self) -> std::time::Duration {
        self.duration
    }

    pub fn add_error(&mut self) {
        self.errors += 1;
    }

    #[doc(hidden)]
    pub fn add_outcome(&mut self, outcome: &fsops::SyncOutcome) {
        self.num_synced += 1;
        match outcome {
            FileCopied { size } => {
                self.copied += 1;
                self.total_transfered += size;
            }
            FileChunkCopied { offset, length } => {
                if *offset == 0 {
                    self.copied += 1;
                }
                self.total_transfered += length;
            }
            UpToDate => self.up_to_date += 1,
            SymlinkUpdated => self.symlink_updated += 1,
            SymlinkCreated => self.symlink_created += 1,
        }
    }
}

#[derive(Copy, Clone)]
pub struct SyncOptions {
    /// Wether to preserve permissions of the source file after the destination is written.
    pub preserve_permissions: bool,
    pub perform_dry_run: bool,
    pub parallelism: usize,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            preserve_permissions: true,
            perform_dry_run: false,
            parallelism: 1,
        }
    }
}

pub struct Syncer {
    source: PathBuf,
    destination: PathBuf,
    options: SyncOptions,
}

impl Syncer {
    pub fn new(
        source: &Path,
        destination: &Path,
        options: SyncOptions,
    ) -> Syncer {
        Syncer {
            source: source.to_path_buf(),
            destination: destination.to_path_buf(),
            options,
        }
    }

    pub fn sync(self) -> Result<Stats, Error> {
        let (walker_entry_output, syncer_input) = channel::<Entry>();
        let syncer_input = Arc::new(Mutex::new(syncer_input));

        let walk_progress = Arc::new(Mutex::new(WalkProgress::new()));
        let walk_worker = WalkWorker::new(&self.source, walker_entry_output, Arc::clone(&walk_progress));
        let mut sync_workers = vec![];
        let mut sync_progresses = HashMap::new();
        for id in 0..self.options.parallelism {
            let sync_progress = Arc::new(Mutex::new(SyncProgress::new(id as u64)));
            let sync_worker = SyncWorker::new(
                &self.source,
                &self.destination,
                Arc::clone(&syncer_input),
                Arc::clone(&sync_progress),
            );
            sync_progresses.insert(id, sync_progress);
            sync_workers.push(sync_worker);
        };
        let progress_worker = ProgressWorker::new(walk_progress, sync_progresses);
        let options = self.options;

        let walker_thread = thread::spawn(move || walk_worker.start());
        let mut syncer_threads = vec![];
        for mut sync_worker in sync_workers {
            let syncer_thread = thread::spawn(move || sync_worker.start(&options));
            syncer_threads.push(syncer_thread);
        }
        let progress_thread = thread::spawn(|| progress_worker.start());

        walker_thread
            .join()
            .map_err(|e| anyhow!("Could not join walker thread: {:?}", e))?;

        let mut syncer_result: Result<(), Error> = Ok(());
        for syncer_thread in syncer_threads {
            let result = syncer_thread
                .join()
                .map_err(|e| anyhow!("Could not join syncer thread: {:?}", e))?;
            if let Err(e) = result {
                syncer_result = Err(anyhow!("Syncer thread error: {:?}", e));
            }
        }

        let progress_result = progress_thread
            .join()
            .map_err(|e| anyhow!("Could not join progress thread: {:?}", e))?;

        syncer_result?;

        Ok(progress_result)
    }
}
