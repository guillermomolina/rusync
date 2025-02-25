use std::fs;
use std::fs::DirEntry;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::{Context, Error};

use crate::entry::Entry;
use crate::fsops;

use super::ProgressMessage;

#[derive(Clone)]
pub struct WalkStatus {
    /// Number of files discovered
    pub num_files: u64,
    /// Number of direwctories discovered
    pub num_dirs: u64,
    /// Estimated total size of the transfer (this may change during transfer)
    pub total_size: usize,
    /// Done syncing process
    pub walk_done: bool,
}

impl WalkStatus {
    pub fn new() -> WalkStatus {
        WalkStatus {
            num_files: 0,
            num_dirs: 0,
            total_size: 0,
            walk_done: false,
        }
    }
}

pub struct WalkWorker {
    source: PathBuf,
    output: Sender<Entry>,
    progress: Option<Arc<Mutex<Sender<ProgressMessage>>>>,
    status: WalkStatus,
}

impl WalkWorker {
    pub fn new(
        source: &Path,
        output: Sender<Entry>,
        progress: Option<Arc<Mutex<Sender<ProgressMessage>>>>,
    ) -> WalkWorker {
        WalkWorker {
            source: source.to_path_buf(),
            output,
            progress,
            status: WalkStatus::new(),
        }
    }

    pub fn start(&mut self) -> Result<WalkStatus, Error> {
        let mut subdirs: Vec<PathBuf> = vec![self.source.to_path_buf()];
        while let Some(subdir) = subdirs.pop() {
            // We just checked that subdirs is *not* empty, so calling pop() is safe

            let entries = fs::read_dir(&subdir).with_context(|| {
                format!(
                    "While walking source, could not read directory '{}'",
                    subdir.display()
                )
            })?;
            for entry in entries {
                let entry = entry.with_context(|| {
                    format!(
                        "While walking source dir, could not read subdir: '{}'",
                        subdir.display()
                    )
                })?;
                let path = entry.path();
                if path.is_dir() {
                    subdirs.push(path);
                    self.status.num_dirs += 1;
                } else {
                    let meta = self.process_file(&entry)?;
                    self.status.num_files += 1;
                    self.status.total_size += meta.len() as usize;
                }
                self.send_progress()
            }
        }
        self.status.walk_done = true;
        self.send_progress();
        Ok(self.status.clone())
    }

    fn process_file(&self, entry: &DirEntry) -> Result<fs::Metadata, Error> {
        let rel_path = fsops::get_rel_path(&entry.path(), &self.source);
        let desc = rel_path.to_string_lossy();
        let src_entry = Entry::new(&desc, &entry.path());
        let metadata = src_entry
            .metadata()
            .with_context(|| format!("Could not read metadata from {:?}", entry.path()))?;
        self.output
            .send(src_entry.clone())
            .with_context(|| "When walking source dir: could not send entry to sync worker")?;
        Ok(metadata.clone())
    }

    fn send_progress(&self) {
        if self.progress.is_some() {
            let _ = self.progress
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .send(ProgressMessage::WalkProgress(self.status.clone()));
        }
    }
}
