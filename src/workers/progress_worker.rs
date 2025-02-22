use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use crate::sync::Stats;

use super::{SyncProgress, WalkProgress};

pub struct ProgressWorker {
    walk_progress: Arc<Mutex<WalkProgress>>,
    sync_progresses: HashMap<usize, Arc<Mutex<SyncProgress>>>,
}

impl ProgressWorker {
    pub fn new(
        walk_progress: Arc<Mutex<WalkProgress>>,
        sync_progresses: HashMap<usize, Arc<Mutex<SyncProgress>>>,
    ) -> ProgressWorker {
        ProgressWorker {
            walk_progress,
            sync_progresses,
        }
    }

    pub fn start(self) -> Stats {
        const PROGRESS_CHARS: &str = "█▉▊▋▌▍▎▏  ";
        let mut stats = Stats::new();
        stats.start();
        let m = MultiProgress::new();
        let count = self.sync_progresses.len();
        let mut sync_progress_bars = vec![];

        let files_pb = m.add(ProgressBar::new(
            self.walk_progress.lock().unwrap().num_files as u64,
        ));
        files_pb.set_prefix("[files]");

        let files_pb_style = ProgressStyle::with_template(
            "{prefix:.bold.dim} {bar:40.green/yellow} {pos}/{len} {elapsed_precise}, BW: <{per_sec}>, ETA: {eta_precise}",
        )
        .unwrap()
        .progress_chars(PROGRESS_CHARS);
        files_pb.set_style(files_pb_style);

        let size_pb = m.add(ProgressBar::new(
            self.walk_progress.lock().unwrap().total_size as u64,
        ));
        size_pb.set_prefix("[size]");
        let size_pb_style =
            ProgressStyle::with_template("{prefix:.bold.dim} {bar:40.green/yellow} <{bytes}/{total_bytes}>, BW: <{binary_bytes_per_sec}>, ETA: {eta_precise}")
                .unwrap()        
                .progress_chars(PROGRESS_CHARS);
        size_pb.set_style(size_pb_style);

        let sync_pb_style = ProgressStyle::with_template(
            "{prefix:.bold.dim} {bar:40.green/yellow} <{bytes}/{total_bytes}> {wide_msg}",
        )
        .unwrap().progress_chars(PROGRESS_CHARS);
        for id in 0..count {
            let pb = m.add(ProgressBar::new(100));
            pb.set_prefix(format!("[{}/{}]", id + 1, count));
            pb.set_style(sync_pb_style.clone());
            sync_progress_bars.push(pb);
        }
        loop {
            if self
                .sync_progresses
                .values()
                .all(|s| s.lock().unwrap().sync_done)
            {
                break;
            }
            let mut files_transfered = 0;
            let mut size_transfered = 0;
            for (id, sync_progress) in &self.sync_progresses {
                files_transfered += sync_progress.lock().unwrap().num_transfered_files;
                size_transfered += sync_progress.lock().unwrap().total_transfered_size;
                let file_transfered_size = sync_progress.lock().unwrap().file_transfered_size;
                sync_progress_bars[*id].set_position(file_transfered_size as u64);
                sync_progress_bars[*id].set_length(sync_progress.lock().unwrap().file_size as u64);
                if sync_progress.lock().unwrap().sync_done {
                    sync_progress_bars[*id].finish_with_message("<done>");
                } else {
                    sync_progress_bars[*id]
                        .set_message(sync_progress.lock().unwrap().current_file.clone());
                }
            }
            files_pb.set_length(self.walk_progress.lock().unwrap().num_files as u64);
            files_pb.set_position(files_transfered as u64);
            size_pb.set_length(self.walk_progress.lock().unwrap().total_size as u64);
            size_pb.set_position(size_transfered as u64);
            thread::sleep(Duration::from_millis(100));
        }
        // for id in 0..count {
        //     sync_progress_bars[id].finish_and_clear();
        // }
        // m.clear().unwrap();
        // m.println("done").unwrap();
        stats.stop();
        stats
    }
}
