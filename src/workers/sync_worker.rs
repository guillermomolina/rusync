use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::{Context, Error};

use crate::entry::Entry;
use crate::fsops;
use crate::fsops::SyncOutcome;
use crate::progress::ProgressMessage;
use crate::sync::SyncOptions;

pub struct SyncWorker {
    input: Arc<Mutex<Receiver<Entry>>>,
    output: Sender<ProgressMessage>,
    source: PathBuf,
    destination: PathBuf,
}

impl SyncWorker {
    pub fn new(
        source: &Path,
        destination: &Path,
        input: Arc<Mutex<Receiver<Entry>>>,
        output: Sender<ProgressMessage>,
    ) -> SyncWorker {
        SyncWorker {
            source: source.to_path_buf(),
            destination: destination.to_path_buf(),
            input,
            output,
        }
    }

    pub fn start(self, opts: &SyncOptions) -> Result<(), Error> {
        while let Ok(entry) = self.input.lock().unwrap().recv() {
            let sync_outcome = self.sync(&entry, &opts);
            let progress_message = match sync_outcome {
                Ok(s) => ProgressMessage::DoneSyncing {
                    entry: entry.description().to_string(),
                    outcome: s,
                },
                Err(e) => ProgressMessage::SyncError {
                    entry: entry.description().to_string(),
                    details: format!("{:#}", e),
                },
            };
            self.output.send(progress_message)?;
        }
        Ok(())
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

    fn sync(&self, src_entry: &Entry, opts: &SyncOptions) -> Result<SyncOutcome, Error> {
        let rel_path = fsops::get_rel_path(src_entry.path(), &self.source);
        self.create_missing_dest_dirs(&rel_path, &opts)?;
        let desc = rel_path.to_string_lossy();

        let dest_path = self.destination.join(&rel_path);
        let dest_entry = Entry::new(&desc, &dest_path);
        let outcome = fsops::sync_entries(&self.output, src_entry, &dest_entry, &opts)?;
        #[cfg(unix)]
        {
            if opts.preserve_permissions && !opts.perform_dry_run {
                fsops::copy_permissions(src_entry, &dest_entry)?;
            }
        }
        Ok(outcome)
    }
}
