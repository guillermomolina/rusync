use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use anyhow::{anyhow, Error};
use filetime::FileTime;
use indicatif::{HumanBytes, HumanCount, HumanDuration};
use jwalk::DirEntry;
use jwalk::Parallelism;
use jwalk::WalkDir;
use log::debug;
use log::warn;

use crate::entry::CopyEntry;
use crate::entry::Entry;
use crate::workers::CopyStatus;
use crate::workers::SyncStatus;
use crate::workers::WalkStatus;
use crate::workers::{CopyWorker, ProgressMessage, ProgressWorker, SyncWorker, WalkWorker};

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

    pub fn parallelism(&self) -> Parallelism {
        match self.options.parallelism {
            1 => Parallelism::Serial,
            n => Parallelism::RayonNewPool(n),
        }
    }

    pub fn sync(&self) -> Result<u64, Error> {
        let mut dirs = 0;
        let mut files = 0;
        let mut symlinks = 0;
        let walk_dir = WalkDir::new(self.source.clone())
            .parallelism(self.parallelism())
            .follow_links(false)
            .skip_hidden(false);
        for dir_entry in walk_dir {
            match dir_entry {
                Ok(dir_entry) => {
                    let source = dir_entry.path();
                    let destination = self.destination.join(source.strip_prefix(&self.source)?);
                    if dir_entry.file_type.is_dir() {
                        self.sync_dir(&destination)?;
                        dirs += 1;
                    } else if dir_entry.file_type.is_file() {
                        self.sync_file(&dir_entry, &destination)?;
                        files += 1;
                    } else if dir_entry.file_type.is_symlink() {
                        self.sync_symlink(&dir_entry, &destination)?;
                        symlinks += 1
                    } else {
                        warn!("Unknown file type: {:?}", dir_entry.file_type);
                    }
                    #[cfg(unix)]
                    {
                        if !self.options.perform_dry_run && self.options.preserve_permissions {
                            self.copy_permissions(&dir_entry, destination)?;
                        }
                    }
                }
                Err(error) => {
                    println!("Read dir_entry error: {}", error);
                }
            }
            // debug!("{}", entry?.path().display());
        }
        println!("Directories: {}", dirs);
        println!("Files: {}", files);
        println!("Symbolic links: {}", symlinks);
        Ok(0)
    }

    pub fn sync_dir(&self, destination: &Path) -> Result<(), Error> {
        if !destination.exists() {
            if self.options.perform_dry_run {
                debug!("Would create directory: {}", destination.display());
            } else {
                debug!("Creating directory: {}", destination.display());
                std::fs::create_dir_all(&destination)?;
            }
        }
        Ok(())
    }

    pub fn sync_symlink(
        &self,
        dir_entry: &DirEntry<((), ())>,
        destination: &PathBuf,
    ) -> Result<(), Error> {
        let source = dir_entry.path();
        let link = std::fs::read_link(&source)?;
        let destination_parent = destination.parent().unwrap();
        if !destination_parent.exists() {
            self.sync_dir(destination_parent)?;
        }
        if !destination.exists() {
            if self.options.perform_dry_run {
                debug!(
                    "Would create symlink: {} -> {}",
                    source.display(),
                    destination.display()
                );
            } else {
                debug!(
                    "Creating symlink: {} -> {}",
                    source.display(),
                    destination.display()
                );
                std::os::unix::fs::symlink(&link, &destination)?;
            }
        }
        Ok(())
    }

    pub fn sync_file(
        &self,
        dir_entry: &DirEntry<((), ())>,
        destination: &PathBuf,
    ) -> Result<(), Error> {
        let source = dir_entry.path();
        let files_differs = self.files_differs(dir_entry, destination)?;
        if !destination.exists() || files_differs {
            if self.options.perform_dry_run {
                debug!(
                    "Would copy file: {} -> {}",
                    source.display(),
                    destination.display()
                );
            } else {
                debug!(
                    "Copying file: {} -> {}",
                    source.display(),
                    destination.display()
                );
                std::fs::copy(&source, &destination)?;
            }
        }
        Ok(())
    }

    pub fn copy_permissions(
        &self,
        entry: &DirEntry<((), ())>,
        destination: PathBuf,
    ) -> Result<(), Error> {
        let metadata = entry.metadata()?;
        let permissions = metadata.permissions();
        std::fs::set_permissions(&destination, permissions)?;
        Ok(())
    }

    pub fn files_differs(&self, dir_entry: &DirEntry<((), ())>, destination: &PathBuf) -> Result<bool, Error> {
        let src_meta = dir_entry.metadata()?;
        let dest_meta = destination.metadata()?;

        let src_mtime = FileTime::from_last_modification_time(&src_meta);
        let dest_mtime = FileTime::from_last_modification_time(&dest_meta);

        let src_size = src_meta.len();
        let dest_size = dest_meta.len();

        Ok(src_mtime > dest_mtime || src_size != dest_size)
    }

    pub fn sync2(self) -> Result<u64, Error> {
        let num_copy_workers = self.options.parallelism;
        let mut walk_progress_option = None;
        let mut sync_progress_option = None;
        let mut copy_progresses_options = vec![None; num_copy_workers];
        let mut progress_thread = None;
        if self.options.show_progress {
            let (progress_output, progress_input) = channel::<ProgressMessage>();
            let progress_worker = ProgressWorker::new(num_copy_workers, progress_input);
            progress_thread = Some(thread::spawn(|| progress_worker.start()));

            walk_progress_option = Some(Arc::new(Mutex::new(progress_output.clone())));
            sync_progress_option = Some(Arc::new(Mutex::new(progress_output.clone())));

            for id in 0..num_copy_workers {
                copy_progresses_options[id] = Some(Arc::new(Mutex::new(progress_output.clone())));
            }
        }

        let (walk_output, sync_input) = channel::<Entry>();
        let mut walk_worker = WalkWorker::new(&self.source, walk_output, walk_progress_option);

        let (sync_output, copy_input) = channel::<CopyEntry>();
        let copy_input = Arc::new(Mutex::new(copy_input));

        let mut sync_worker = SyncWorker::new(
            &self.source,
            &self.destination,
            sync_input,
            sync_output,
            sync_progress_option,
        );

        let mut copy_workers = vec![];
        for id in 0..num_copy_workers {
            let copy_worker = CopyWorker::new(
                id,
                Arc::clone(&copy_input),
                copy_progresses_options[id].clone(),
            );
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

        if progress_thread.is_some() {
            let _ = progress_thread
                .unwrap()
                .join()
                .map_err(|e| anyhow!("Could not join progress thread: {:?}", e))?;
        }
        let elapsed = now.elapsed();

        if self.options.show_stats {
            self.print_statistics(
                walk_status,
                sync_status.clone(),
                copy_statuses,
                elapsed,
                num_copy_workers,
            );
        }

        Ok(sync_status.errors)
    }

    fn print_statistics(
        &self,
        walk_status: WalkStatus,
        sync_status: SyncStatus,
        copy_statuses: Vec<CopyStatus>,
        elapsed: Duration,
        num_copy_workers: usize,
    ) {
        println!(
            "Elapsed time: {}s",
            HumanDuration(Duration::from_secs(elapsed.as_secs())).to_string()
        );

        let mut copied_files = 0;
        let mut copied_size = 0;
        for copy_status in copy_statuses.iter() {
            copied_files += copy_status.num_transfered_files;
            copied_size += copy_status.total_transfered_size;
        }
        println!(
            "Files or links discovered: {}",
            HumanCount(walk_status.num_files).to_string()
        );
        println!(
            "Directories discovered: {}",
            HumanCount(walk_status.num_dirs).to_string()
        );
        println!("Symbolic link sync:");
        println!(
            "\tTotal: {}",
            HumanCount(sync_status.num_symlinks()).to_string()
        );
        println!(
            "\tCreated: {}",
            HumanCount(sync_status.symlink_created).to_string()
        );
        println!(
            "\tUpdated: {}",
            HumanCount(sync_status.symlink_updated).to_string()
        );
        println!(
            "\tUp to date: {}",
            HumanCount(sync_status.symlink_up_to_date).to_string()
        );
        println!("File sync:");
        println!(
            "\tTotal: {}",
            HumanCount(sync_status.num_files()).to_string()
        );
        println!(
            "\tUp to date: {}",
            HumanCount(sync_status.num_up_to_date).to_string()
        );
        println!("\tErrors: {}", HumanCount(sync_status.errors).to_string());
        println!("\tCopied: {}", HumanCount(copied_files).to_string());
        for id in 0..num_copy_workers as usize {
            println!(
                "\tCopied by worker #{}: {}",
                id,
                HumanCount(copy_statuses[id].num_transfered_files).to_string()
            );
        }

        println!("File data size:");
        println!(
            "\tTotal: {}",
            HumanBytes(walk_status.total_size as u64).to_string()
        );
        println!(
            "\tUp to date: {}",
            HumanBytes(sync_status.up_to_date_size as u64).to_string()
        );
        println!("\tCopied: {}", HumanBytes(copied_size as u64).to_string());
        for id in 0..num_copy_workers as usize {
            println!(
                "\tCopied by worker #{}: {}",
                id,
                HumanBytes(copy_statuses[id].total_transfered_size as u64).to_string()
            );
        }
        println!("Bandwidth:");
        let elapsed_precise = elapsed.as_secs_f64();
        let total_bandwith = (copied_size as f64 / elapsed_precise) as u64;
        println!("\tTotal: {}/s", HumanBytes(total_bandwith).to_string());
        for id in 0..num_copy_workers as usize {
            let copied_bandwith_worker =
                (copy_statuses[id].total_transfered_size as f64 / elapsed_precise) as u64;
            println!(
                "\tCopied by worker #{}: {}/s",
                id,
                HumanBytes(copied_bandwith_worker).to_string()
            );
        }
    }
}
