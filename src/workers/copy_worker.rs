use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::bail;
use anyhow::Context;
use log::{debug, error};

use anyhow::Error;

use crate::entry::CopyEntry;
use crate::fsops;
use crate::fsops::CopyOutcome;

pub const BUFFER_SIZE: usize = 32 * 1024;

#[derive(Clone)]
pub struct CopyStatus {
    /// ID of the sync thread
    pub worker_id: u64,
    /// Name of the file being transferred
    pub current_file: String,
    /// Size of the current file (in bytes)
    pub file_size: usize,
    /// Number of bytes transfered for the current file
    pub file_transfered_size: usize,
    /// Total number of bytes transfered across all files
    pub total_transfered_size: usize,
    /// Number of files transfered
    pub num_transfered_files: u64,
    /// Done copying process
    pub copy_done: bool,
}

impl CopyStatus {
    pub fn new(worker_id: u64) -> CopyStatus {
        CopyStatus {
            worker_id,
            current_file: String::new(),
            file_size: 0,
            file_transfered_size: 0,
            total_transfered_size: 0,
            num_transfered_files: 0,
            copy_done: false,
        }
    }

    pub fn new_file(&mut self, name: &str, file_size: usize) {
        self.current_file = name.to_string();
        self.file_size = file_size;
        self.file_transfered_size = 0;
    }

    pub fn done_copying(&mut self) {
        self.new_file("", 0);
        self.copy_done = true;
    }
}

pub struct CopyWorker {
    input: Arc<Mutex<Receiver<CopyEntry>>>,
    status: CopyStatus,
}

impl CopyWorker {
    pub fn new(id: u64, input: Arc<Mutex<Receiver<CopyEntry>>>) -> CopyWorker {
        CopyWorker {
            input,
            status: CopyStatus::new(id),
        }
    }

    pub fn start(&mut self) -> Result<CopyStatus, Error> {
        while let Ok(copy_entry) = {
            let copy_entry = self.input.lock().unwrap().recv();
            copy_entry
        } {
            match self.copy(&copy_entry) {
                Ok(_) => {
                    if !copy_entry.is_chunk() || copy_entry.is_last_chunk() {
                        self.status.num_transfered_files += 1;
                        debug!(
                            "[{}] Copied: {}",
                            self.status.worker_id,
                            copy_entry.src.description()
                        );
                    }
                }
                Err(error) => {
                    error!(
                        "[{}] Error copying: {} {:#}",
                        self.status.worker_id,
                        copy_entry.src.description(),
                        error
                    );
                }
            };
        }
        self.status.done_copying();
        Ok(self.status.clone())
    }

    fn copy(&mut self, copy_entry: &CopyEntry) -> Result<CopyOutcome, Error> {
        self.status.new_file(
            copy_entry.src.description(),
            copy_entry.src.length().unwrap(),
        );
        let outcome = if copy_entry.is_chunk() {
            self.copy_chunk(copy_entry)
        } else {
            self.copy_entry(copy_entry)
        };
        #[cfg(unix)]
        if outcome.is_ok() && (!copy_entry.is_chunk() || copy_entry.is_last_chunk()) {
            let opts = &copy_entry.opts;
            if opts.preserve_permissions && !opts.perform_dry_run {
                fsops::copy_permissions(&copy_entry.src, &copy_entry.dest)?;
            }
        }
        outcome
    }

    pub fn copy_entry(&mut self, copy_entry: &CopyEntry) -> Result<CopyOutcome, Error> {
        let src = &copy_entry.src;
        let dest = &copy_entry.dest;
        let opts = &copy_entry.opts;
        let src_meta = src.metadata().expect("src_meta should not be None");
        let src_size = src_meta.len() as usize;
        debug!(
            "[{}] Copying {} from {} to {} length {}",
            self.status.worker_id,
            src.description(),
            src.path().display(),
            dest.path().display(),
            src_size,
        );
        if !opts.perform_dry_run {
            let src_path = src.path();
            let dest_path = dest.path();
            let mut src_file = File::open(src_path)
                .with_context(|| format!("Could not open '{}' for reading", src.description()))?;
            let mut dest_file = File::create(dest_path)
                .with_context(|| format!("Could not open '{}' for writing", dest.description()))?;
            let bytes_copied = std::io::copy(&mut src_file, &mut dest_file).with_context(|| {
                format!(
                    "[{}] Could not copy {} from {} to {}",
                    self.status.worker_id,
                    src.description(),
                    src.path().display(),
                    dest.path().display(),
                )
            })?;
            if bytes_copied as usize != src_size {
                bail!(
                    "[{}] Could not copy {} from {} to {} length {}, only copied {}",
                    self.status.worker_id,
                    src.description(),
                    src.path().display(),
                    dest.path().display(),
                    src_size,
                    bytes_copied
                );
            }
        }
        debug!(
            "[{}] Copied {} from {} to {} length {}",
            self.status.worker_id,
            src.description(),
            src.path().display(),
            dest.path().display(),
            src_size,
        );
        self.status.file_transfered_size = src_size;
        self.status.total_transfered_size += src_size;
        Ok(CopyOutcome::FileCopied {
            size: src_size as usize,
        })
    }

    pub fn copy_chunk(&mut self, copy_entry: &CopyEntry) -> Result<CopyOutcome, Error> {
        let src = &copy_entry.src;
        let dest = &copy_entry.dest;
        let opts = &copy_entry.opts;
        let offset = copy_entry
            .chunk_offset()
            .expect("offset should not be None");
        let length = copy_entry
            .chunk_length()
            .expect("length should not be None");

        debug!(
            "[{}] Copying {} from {} to {} offset {} length {}",
            self.status.worker_id,
            src.description(),
            src.path().display(),
            dest.path().display(),
            offset,
            length,
        );

        let mut src_file = File::open(src.path())?;
        src_file.seek(SeekFrom::Start(offset as u64))?;

        let mut dest_file = if !opts.perform_dry_run {
            let mut dest_file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(dest.path())?;
            dest_file.seek(SeekFrom::Start(offset as u64))?;
            Some(dest_file)
        } else {
            None
        };

        self.status.file_transfered_size = offset;

        let mut buffer = vec![0; BUFFER_SIZE];
        let mut total_bytes_read = 0;
        while total_bytes_read < length {
            let bytes_read = src_file.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            if !opts.perform_dry_run {
                if let Some(ref mut file) = dest_file {
                    file.write(&buffer[..bytes_read])?;
                    file.flush()?;
                }
            }
            total_bytes_read += bytes_read;
            self.status.file_transfered_size += bytes_read;
            self.status.total_transfered_size += bytes_read;
        }
        if total_bytes_read != length {
            bail!(
                "[{}] Could not copy {} from {} to {} length {}, only copied {}",
                self.status.worker_id,
                src.description(),
                src.path().display(),
                dest.path().display(),
                length,
                total_bytes_read
            );
        }
        debug!(
            "[{}] Copied {} from {} to {} offset {} length {}",
            self.status.worker_id,
            src.description(),
            src.path().display(),
            dest.path().display(),
            offset,
            length,
        );
        Ok(CopyOutcome::FileChunkCopied { offset, length })
    }
}
