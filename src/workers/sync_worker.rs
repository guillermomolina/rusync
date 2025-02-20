use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::sync::Mutex;

use log::{debug, error};

use anyhow::{Context, Error};

use crate::entry::Entry;
use crate::fsops;
use crate::fsops::SyncOutcome;
use crate::progress::ProgressInfo;
use crate::sync::SyncOptions;
use crate::sync::Stats;

pub struct SyncWorker {
    id: usize,
    input: Arc<Mutex<Receiver<Entry>>>,
    source: PathBuf,
    destination: PathBuf,
    progress_info: Box<dyn ProgressInfo + Send>,
}

impl SyncWorker {
    pub fn new(
        id: usize,
        source: &Path,
        destination: &Path,
        input: Arc<Mutex<Receiver<Entry>>>,
        progress_info: Box<dyn ProgressInfo + Send>,
    ) -> SyncWorker {
        SyncWorker {
            id,
            source: source.to_path_buf(),
            destination: destination.to_path_buf(),
            input,
            progress_info,
        }
    }

    pub fn start(&mut self, opts: &SyncOptions) -> Result<Stats, Error> {
        let mut stats = Stats::new();
        stats.start();
        while let Ok(entry) = {
            let entry = self.input.lock().unwrap().recv();
            entry
        } {
            match self.sync(&entry, &opts) {
                Ok(outcome) => {
                    stats.add_outcome(&outcome);
                    debug!("[{}] Synced: {}", self.id, entry.description());
                },
                Err(error) => {
                    stats.add_error();
                    error!("[{}] Error syncing: {} {:#}", self.id, entry.description(), error);
                },
            };
        }
        stats.stop();
        Ok(stats)
    }

    fn create_missing_dest_dirs(&self, rel_path: &Path, opts: &SyncOptions) -> Result<(), Error> {
        if !opts.perform_dry_run {
            let parent_rel_path = rel_path
                .parent()
                .expect("dest directory should have a parent");
            let to_create = self.destination.join(parent_rel_path);
            fs::create_dir_all(&to_create)
                .with_context(|| format!("Could not create '{}'", to_create.display()))?;
        }
        Ok(())
    }

    fn sync(&mut self, src_entry: &Entry, opts: &SyncOptions) -> Result<SyncOutcome, Error> {
        let rel_path = fsops::get_rel_path(src_entry.path(), &self.source);
        self.create_missing_dest_dirs(&rel_path, &opts)?;
        let desc = rel_path.to_string_lossy();

        let dest_path = self.destination.join(&rel_path);
        let dest_entry = Entry::new(&desc, &dest_path);
        self.progress_info.new_file(src_entry.description());
        let outcome = fsops::sync_entries(self.id, src_entry, &dest_entry, &opts)?;
        #[cfg(unix)]
        {
            if opts.preserve_permissions && !opts.perform_dry_run {
                fsops::copy_permissions(src_entry, &dest_entry)?;
            }
        }
        Ok(outcome)
    }
}
