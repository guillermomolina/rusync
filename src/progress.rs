use log::{debug, error, info};

use crate::fsops::SyncOutcome;
use crate::console_info::{human_seconds, truncate_lossy};
use crate::sync::Stats;

#[doc(hidden)]
pub enum ProgressMessage {
    DoneSyncing {
        entry: String,
        outcome: SyncOutcome,
    },
    StartSync(String),
    Todo {
        num_files: u64,
        total_size: usize,
    },
    Syncing {
        description: String,
        size: usize,
        done: usize,
    },
    SyncError {
        entry: String,
        details: String,
    },
}

pub struct Progress {
    /// Name of the file being transferred
    pub current_file: String,
    /// Number of bytes transfered for the current file
    pub file_done: usize,
    /// Size of the current file (in bytes)
    pub file_size: usize,
    /// Number of bytes transfered since the start
    pub total_done: usize,
    /// Estimated total size of the transfer (this may change during transfer)
    pub total_size: usize,
    /// Index of the current file in the list of all files to transfer
    pub index: usize,
    /// Total number of files to transfer
    pub num_files: usize,
    /// Estimated time remaining for the transfer, in seconds
    pub eta: usize,
}

/// Trait for implementing rusync progress details
pub trait ProgressInfo {
    /// A new transfer has begun from the `source` directory to the `destination`
    /// directory
    #[allow(unused_variables)]
    fn start(&mut self, source: &str, destination: &str) {}

    /// A new file named `name` is being transfered
    #[allow(unused_variables)]
    fn new_file(&mut self, name: &str) {}

    /// The file transfer is done
    #[allow(unused_variables)]
    fn done_syncing(&mut self) {}

    /// Callback for the detailed progress
    #[allow(unused_variables)]
    fn progress(&mut self, progress: &Progress) {}

    /// The transfer between `source` and `destination` is done. Details
    /// of the transfer in the Stats struct
    #[allow(unused_variables)]
    fn end(&mut self, stats: &Stats) {}

    /// The entry could not be synced
    #[allow(unused_variables)]
    fn error(&mut self, entry: &str, details: &str) {}
}

impl Progress {
    pub fn new() -> Progress {
        Progress {
            current_file: String::from(""),
            file_done: 0,
            file_size: 0,
            total_done: 0,
            total_size: 0,
            index: 0,
            num_files: 0,
            eta: 0,
        }
    }
}


impl ProgressInfo for Progress {
    fn done_syncing(&mut self) {
        debug!("Done syncing {}", self.current_file);
        self.current_file.clear();
    }

    fn start(&mut self, source: &str, destination: &str) {
        debug!("Syncing from {} to {}", source, destination);
        self.current_file = source.to_string();
    }

    fn new_file(&mut self, name: &str) {
        debug!("New file {}", name);
    }

    fn progress(&mut self, progress: &Progress) {
        let eta_str = human_seconds(progress.eta);
        let percent_width = 3;
        let eta_width = eta_str.len();
        let index = progress.index;
        let index_width = index.to_string().len();
        let num_files = progress.num_files;
        let num_files_width = num_files.to_string().len();
        let widgets_width = percent_width + index_width + num_files_width + eta_width;
        let num_separators = 5;
        let line_width = 80;
        let file_width = line_width - widgets_width - num_separators - 1;
        let current_file = progress.current_file.clone();
        let current_file = truncate_lossy(&current_file, file_width as usize);
        let current_file = format!(
            "{filename:<pad$}",
            pad = file_width as usize,
            filename = current_file
        );
        let file_percent = (progress.file_done * 100) / progress.file_size;
        print!(
            "{:>3}% {}/{} {} {:<}\r",
            file_percent, index, num_files, current_file, eta_str
        );
    }

    fn error(&mut self, entry: &str, desc: &str) {
        error!("Errror: on file {}  {}", entry, desc);
    }

    fn end(&mut self, stats: &Stats) {
        info!(
            "Synced {} files ({} up to date)",
            stats.num_synced, stats.up_to_date
        );
        info!(
            "{} files copied, {} symlinks created, {} symlinks updated",
            stats.copied, stats.symlink_created, stats.symlink_updated
        );
        let transfered = stats.total_transfered;
        let duration = stats.duration();
        // Truncate below 1 second
        let duration = std::time::Duration::from_secs(duration.as_secs());
        let duration = humantime::format_duration(duration);
        info!("{} copied in {}", transfered, duration);
        if stats.errors != 0 {
            info!("{} errors occurred", stats.errors);
        }
    }
}

