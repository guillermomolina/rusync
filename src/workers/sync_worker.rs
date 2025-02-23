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
    /// Name of the file being transferred
    pub current_file: String,
    /// Size of the current file (in bytes)
    pub file_size: usize,
    /// Number of bytes transfered for the current file
    pub file_transfered_size: usize,
    /// Number of bytes transfered since the start
    pub total_transfered_size: usize,
    /// Total number of transfered files
    pub num_transfered_files: usize,
    /// Done syncing process
    pub sync_done: bool,
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
}

impl SyncStatus {
    pub fn new() -> SyncStatus {
        SyncStatus {
            current_file: String::new(),
            file_size: 0,
            file_transfered_size: 0,
            total_transfered_size: 0,
            num_transfered_files: 0,
            sync_done: false,

            num_synced: 0,
            up_to_date: 0,
            copied: 0,
            errors: 0,

            symlink_created: 0,
            symlink_updated: 0,
        }
    }

    pub fn new_file(&mut self, name: &str) {
        self.current_file = name.to_string();
        self.file_size = 0;
        self.file_transfered_size = 0;
    }

    pub fn done_syncing(&mut self) {
        self.new_file("");
        self.sync_done = true;
    }

    pub fn add_error(&mut self) {
        self.errors += 1;
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
                Ok(_) => {
                    debug!("Synced: {}", entry.description());
                }
                Err(error) => {
                    self.status.lock().unwrap().errors += 1;
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
        self.status
            .lock()
            .unwrap()
            .new_file(src_entry.description());
        let outcome = self.sync_entry(src_entry, &dest_entry, &opts)?;
        #[cfg(unix)]
        {
            if opts.preserve_permissions && !opts.perform_dry_run {
                fsops::copy_permissions(src_entry, &dest_entry)?;
            }
        }
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
            let file_size = src.length().expect("file_size should not be None");
            if file_size > CHUNK_SIZE {
                let num_chunks = (file_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
                for i in 0..num_chunks {
                    let offset = i * CHUNK_SIZE;
                    let end = std::cmp::min((i + 1) * CHUNK_SIZE , file_size);
                    let len = end - offset;
            
                    let chunked_entry = src.to_chunk(offset, len);
                    let copy_entry = CopyEntry {
                        src: chunked_entry,
                        dest: dest.clone(),
                        opts: opts.clone(),
                    };
                    self.output
                        .send(copy_entry)
                        .with_context(|| "When syncing source dir: could not send entry to copy worker")?;
                }
            } else {
                let copy_entry = CopyEntry {
                    src: src.clone(),
                    dest: dest.clone(),
                    opts: opts.clone(),
                };
                self.output
                    .send(copy_entry)
                    .with_context(|| "When syncing source dir: could not send entry to copy worker")?;
            }
        }
        Ok(SyncOutcome::UpToDate)
    }
}
