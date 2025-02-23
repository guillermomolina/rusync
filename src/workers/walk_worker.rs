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

pub struct WalkStatus {
    /// Number of files discovered
    pub num_files: usize,
    /// Estimated total size of the transfer (this may change during transfer)
    pub total_size: usize,
    /// Done syncing process
    pub walk_done: bool,
}

impl WalkStatus {
    pub fn new() -> WalkStatus {
        WalkStatus {
            num_files: 0,
            total_size: 0,
            walk_done: false,
        }
    }
}

pub struct WalkWorker {
    output: Sender<Entry>,
    source: PathBuf,
    status: Arc<Mutex<WalkStatus>>,
}

impl WalkWorker {
    pub fn new(output: Sender<Entry>, source: &Path, status: Arc<Mutex<WalkStatus>>) -> WalkWorker {
        WalkWorker {
            output,
            source: source.to_path_buf(),
            status,
        }
    }

    fn walk(&self) -> Result<(), Error> {
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
                } else {
                    let meta = self.process_file(&entry)?;
                    let mut progress = self.status.lock().unwrap();
                    progress.num_files += 1;
                    progress.total_size += meta.len() as usize;
                }
            }
        }
        self.status.lock().unwrap().walk_done = true;
        Ok(())
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
            .with_context(|| "When walking source dir: could not send entry to progress worker")?;
        Ok(metadata.clone())
    }

    pub fn start(&self) {
        let outcome = &self.walk();
        if outcome.is_err() {
            // Send err to output
        }
    }
}
