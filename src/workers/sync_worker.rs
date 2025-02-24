use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;

use log::{debug, error};

use anyhow::{Context, Error};

use crate::entry::{Entry, CopyEntry};
use crate::fsops;
use crate::fsops::SyncOutcome;
use crate::fsops::BUFFER_SIZE;
// use crate::fsops::SyncOutcome::*;
use crate::SyncOptions;

const CHUNK_SIZE: usize = BUFFER_SIZE * 1024;

pub struct SyncStatus {
    /// Done syncing process
    pub sync_done: bool,
    /// Number of files transfered (should match `num_files`
    /// if no error)
    pub num_synced: u64,
    /// Number of files for which the copy was skipped
    pub up_to_date: u64,
    /// Number of files that need to be copied
    pub need_copy: u64,
    /// Number of errors
    pub errors: u64,
    /// Number of symlink created in the destination folder
    pub symlink_created: u64,
    /// Number of symlinks updated in the destination folder
    pub symlink_updated: u64,
}

impl SyncStatus {
    pub fn new() -> SyncStatus {
        SyncStatus {
            sync_done: false,

            num_synced: 0,
            up_to_date: 0,
            need_copy: 0,
            errors: 0,

            symlink_created: 0,
            symlink_updated: 0,
        }
    }

    pub fn done_syncing(&mut self) {
        self.sync_done = true;
    }

    pub fn add_error(&mut self) {
        self.errors += 1;
    }
    
    pub fn add_outcome(&mut self, outcome: &fsops::SyncOutcome) {
        self.num_synced += 1;
        match outcome {
            fsops::SyncOutcome::NeedCopy => self.need_copy += 1,
            fsops::SyncOutcome::UpToDate => self.up_to_date += 1,
            fsops::SyncOutcome::SymlinkUpdated => self.symlink_updated += 1,
            fsops::SyncOutcome::SymlinkCreated => self.symlink_created += 1,
        }
    }
}

pub struct SyncWorker {
    input: Receiver<Entry>,
    output: Sender<CopyEntry>,
    source: PathBuf,
    destination: PathBuf,
    status: Arc<Mutex<SyncStatus>>,
}

impl SyncWorker {
    pub fn new(
        input: Receiver<Entry>,
        output: Sender<CopyEntry>,
        source: &Path,
        destination: &Path,
        status: Arc<Mutex<SyncStatus>>,
    ) -> SyncWorker {
        SyncWorker {
            input,
            output,
            source: source.to_path_buf(),
            destination: destination.to_path_buf(),
            status,
        }
    }

    pub fn start(&mut self, opts: &SyncOptions) -> () {
        while let Ok(entry) = {
            let entry = self.input.recv();
            entry
        } {
            match self.sync(&entry, &opts) {
                Ok(outcome) => {
                    self.status.lock().unwrap().add_outcome(&outcome);
                    debug!("Synced: {}", entry.description());
                }
                Err(error) => {
                    self.status.lock().unwrap().add_error();
                    error!("Error syncing: {} {:#}", entry.description(), error);
                }
            };
        }
        self.status.lock().unwrap().done_syncing();
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
        let outcome = self.sync_entry(src_entry, &dest_entry, &opts)?;
        Ok(outcome)
    }

    pub fn sync_entry(
        &mut self,
        src: &Entry,
        dest: &Entry,
        opts: &SyncOptions,
    ) -> Result<SyncOutcome, Error> {
        debug!("Syncing {} from {} to {}", src.description(), src.path().display(), dest.path().display());
        let is_link = src.is_link().expect("src.is_link should not be None");
        if is_link {
            return fsops::copy_link(src, dest, &opts);
        }
        let different_size = fsops::has_different_size(src, dest);
        let more_recent = fsops::is_more_recent_than(src, dest);
        // TODO: check if files really are different ?
        if src.is_file().unwrap() && (more_recent || different_size) {
            let copy_entry = CopyEntry::new(src.clone(), dest.clone(), *opts);
            let file_size = src.length().expect("file_size should not be None");
            if file_size > CHUNK_SIZE {
                let num_chunks = (file_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
                for i in 0..num_chunks {
                    let offset = i * CHUNK_SIZE;
                    let end = std::cmp::min((i + 1) * CHUNK_SIZE , file_size);
                    let len = end - offset;
            
                    let chunked_entry = copy_entry.new_chunk(offset, len);
                    self.output
                        .send(chunked_entry)
                        .with_context(|| "When syncing source dir: could not send entry to copy worker")?;
                }
            } else {
                self.output
                    .send(copy_entry)
                    .with_context(|| "When syncing source dir: could not send entry to copy worker")?;
            }
            return Ok(SyncOutcome::NeedCopy);
        }
        #[cfg(unix)]
        {
            if opts.preserve_permissions && !opts.perform_dry_run {
                fsops::copy_permissions(src, &dest)?;
            }
        }
        Ok(SyncOutcome::UpToDate)
    }
}
