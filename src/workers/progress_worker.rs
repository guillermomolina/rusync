use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use super::{CopyStatus, SyncStatus, WalkStatus};

pub struct ProgressWorker {
    walk_status: Arc<Mutex<WalkStatus>>,
    sync_status: Arc<Mutex<SyncStatus>>,
    copy_statusses: HashMap<usize, Arc<Mutex<CopyStatus>>>,
}

impl ProgressWorker {
    pub fn new(
        walk_status: Arc<Mutex<WalkStatus>>,
        sync_status: Arc<Mutex<SyncStatus>>,
        copy_statusses: HashMap<usize, Arc<Mutex<CopyStatus>>>,
    ) -> ProgressWorker {
        ProgressWorker {
            walk_status,
            sync_status,
            copy_statusses
        }
    }

    pub fn start(self) -> () {
        const PROGRESS_CHARS: &str = "█▉▊▋▌▍▎▏  ";
        let m = MultiProgress::new();
        let count = self.copy_statusses.len();
        let mut copy_progress_bars = vec![];

        let files_pb = m.add(ProgressBar::new(
            self.walk_status.lock().unwrap().num_files as u64,
        ));
        files_pb.set_prefix("[files]");

        let files_pb_style = ProgressStyle::with_template(
            "{prefix:.bold.dim} {bar:40.green/yellow} {pos}/{len} {elapsed_precise}, BW: <{per_sec}>, ETA: {eta_precise}",
        )
        .unwrap()
        .progress_chars(PROGRESS_CHARS);
        files_pb.set_style(files_pb_style);

        let size_pb = m.add(ProgressBar::new(
            self.walk_status.lock().unwrap().total_size as u64,
        ));
        size_pb.set_prefix("[size]");
        let size_pb_style =
            ProgressStyle::with_template("{prefix:.bold.dim} {bar:40.green/yellow} <{bytes}/{total_bytes}>, BW: <{binary_bytes_per_sec}>, ETA: {eta_precise}")
                .unwrap()        
                .progress_chars(PROGRESS_CHARS);
        size_pb.set_style(size_pb_style);

        let sync_pb_style = ProgressStyle::with_template(
            "{prefix:.bold.dim} {bar:40.green/yellow} <{bytes}/{total_bytes}> {wide_msg}",
        ).unwrap().progress_chars(PROGRESS_CHARS);
        for id in 0..count {
            let pb = m.add(ProgressBar::new(100));
            pb.set_prefix(format!("[{}/{}]", id + 1, count));
            pb.set_style(sync_pb_style.clone());
            copy_progress_bars.push(pb);
        }
        loop {
            if self
                .copy_statusses
                .values()
                .all(|s| s.lock().unwrap().copy_done)
            {
                break;
            }
            // let mut files_transfered = self.sync_status.lock().unwrap().num_synced - self.sync_status.lock().unwrap().need_copy;
            let mut files_transfered= 0;
            let mut size_transfered = 0;
            for (id, copy_progress) in &self.copy_statusses {
                files_transfered += copy_progress.lock().unwrap().num_transfered_files;
                size_transfered += copy_progress.lock().unwrap().total_transfered_size;
                let file_transfered_size = copy_progress.lock().unwrap().file_transfered_size;
                copy_progress_bars[*id].set_position(file_transfered_size as u64);
                copy_progress_bars[*id].set_length(copy_progress.lock().unwrap().file_size as u64);
                if copy_progress.lock().unwrap().copy_done {
                    copy_progress_bars[*id].finish_with_message("<done>");
                } else {
                    copy_progress_bars[*id]
                        .set_message(copy_progress.lock().unwrap().current_file.clone());
                }
            }
            files_pb.set_length(self.walk_status.lock().unwrap().num_files as u64);
            files_pb.set_position(files_transfered as u64);
            let total_size = self.walk_status.lock().unwrap().total_size as u64;
            size_pb.set_length(total_size);
            size_pb.set_position(size_transfered as u64);
            thread::sleep(Duration::from_millis(100));
        }
        // for id in 0..count {
        //     copy_progress_bars[id].finish_and_clear();
        // }
        // m.clear().unwrap();
        // m.println("done").unwrap();
    }
}
