use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use anyhow::{anyhow, Error};
use indicatif::{ HumanBytes, HumanCount, HumanDuration };

use crate::entry::CopyEntry;
use crate::entry::Entry;
use crate::workers::{
    CopyWorker, ProgressWorker, ProgressMessage, SyncWorker, WalkWorker,
};

#[derive(Copy, Clone)]
pub struct SyncOptions {
    /// Wether to preserve permissions of the source file after the destination is written.
    pub preserve_permissions: bool,
    pub perform_dry_run: bool,
    pub parallelism: usize,
    pub show_progress: bool,
    pub show_stats: bool,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            preserve_permissions: true,
            perform_dry_run: false,
            parallelism: 1,
            show_progress: false,
            show_stats: false,
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
        let num_copy_workers = self.options.parallelism;

        let mut walk_worker = WalkWorker::new(walk_output, &self.source);

        let (sync_output, copy_input) = channel::<CopyEntry>();
        let copy_input = Arc::new(Mutex::new(copy_input));

        let mut sync_worker =
            SyncWorker::new(sync_input, sync_output, &self.source, &self.destination);

        let mut copy_workers = vec![];
        for id in 0..num_copy_workers {
            let copy_worker = CopyWorker::new(id, Arc::clone(&copy_input));
            copy_workers.push(copy_worker);
        }

        let now = Instant::now();
        let walk_thread = thread::spawn(move || walk_worker.start());
        let sync_thread = thread::spawn(move || sync_worker.start(&self.options));
        let mut copy_threads = vec![];
        for mut copy_worker in copy_workers {
            let copy_thread = thread::spawn(move || copy_worker.start());
            copy_threads.push(copy_thread);
        }

        let progress_thread = if self.options.show_progress {
            let (progress_output, progress_input) = channel::<ProgressMessage>();
            let progress_worker = ProgressWorker::new(num_copy_workers, progress_input);
            Some(thread::spawn(|| progress_worker.start()))
        } else {
            None
        };

        let walk_status = walk_thread
            .join()
            .map_err(|e| anyhow!("Could not join walker thread: {:?}", e))?
            .unwrap();

        let sync_status = sync_thread
            .join()
            .map_err(|e| anyhow!("Could not join sync thread: {:?}", e))?
            .unwrap();

        let mut copy_statuses = Vec::new();
        for copy_thread in copy_threads {
            let copy_status = copy_thread
                .join()
                .map_err(|e| anyhow!("Could not join copy thread: {:?}", e))?
                .unwrap();
            copy_statuses.push(copy_status);
        }
        let elapsed = now.elapsed();

        if progress_thread.is_some() {
            let _ = progress_thread
                .unwrap()
                .join()
                .map_err(|e| anyhow!("Could not join progress thread: {:?}", e))?;
        }

        if self.options.show_stats {
            println!("Elapsed time: {}s", HumanDuration(Duration::from_secs(elapsed.as_secs())).to_string());

            let mut copied_files = 0;
            let mut copied_size = 0;
            for copy_status in copy_statuses.iter() {
                copied_files += copy_status.num_transfered_files;
                copied_size += copy_status.total_transfered_size;
            }
            println!("Files or links discovered: {}", HumanCount(walk_status.num_files).to_string());
            println!("Directories discovered: {}", HumanCount(walk_status.num_dirs).to_string());
            println!("Symbolic link sync:");
            println!("\tTotal: {}", HumanCount(sync_status.num_symlinks()).to_string());
            println!("\tCreated: {}", HumanCount(sync_status.symlink_created).to_string());
            println!("\tUpdated: {}", HumanCount(sync_status.symlink_updated).to_string());
            println!("\tUp to date: {}", HumanCount(sync_status.symlink_up_to_date).to_string());
            println!("File sync:");
            println!("\tTotal: {}", HumanCount(sync_status.num_files()).to_string());
            println!("\tUp to date: {}", HumanCount(sync_status.num_up_to_date).to_string());
            println!("\tErrors: {}", HumanCount(sync_status.errors).to_string());
            println!("\tCopied: {}", HumanCount(copied_files).to_string());
            for id in 0..num_copy_workers as usize {
                println!("\tCopied by worker #{}: {}", id, HumanCount(copy_statuses[id].num_transfered_files).to_string());
            }
            
            println!("File data size:");
            println!("\tTotal: {}", HumanBytes(walk_status.total_size as u64).to_string());
            println!("\tUp to date: {}", HumanBytes(sync_status.up_to_date_size as u64).to_string());
            println!("\tCopied: {}", HumanBytes(copied_size as u64).to_string());
            for id in 0..num_copy_workers as usize {
                println!("\tCopied by worker #{}: {}", id, HumanBytes(copy_statuses[id].total_transfered_size as u64).to_string());
            }
            println!("Bandwidth:");
            let elapsed_precise = elapsed.as_secs_f64();
            let total_bandwith = (copied_size as f64 / elapsed_precise) as u64;
            println!("\tTotal: {}/s", HumanBytes(total_bandwith).to_string());
            for id in 0..num_copy_workers as usize {
                let copied_bandwith_worker = (copy_statuses[id].total_transfered_size as f64 / elapsed_precise) as u64;
                println!("\tCopied by worker #{}: {}/s", id, HumanBytes(copied_bandwith_worker).to_string());
            }
        }
        Ok(sync_status.errors)
    }
}
