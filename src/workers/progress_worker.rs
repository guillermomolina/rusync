use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use crate::sync::Stats;

use super::SyncProgress;

pub struct ProgressWorker {
    progress_info: HashMap<usize, Arc<Mutex<SyncProgress>>>,
}

impl ProgressWorker {
    pub fn new(progress_info: HashMap<usize, Arc<Mutex<SyncProgress>>>) -> ProgressWorker {
        ProgressWorker { progress_info }
    }

    pub fn start(self) -> Stats {
        let mut stats = Stats::new();
        let spinner_style = ProgressStyle::with_template("{prefix:.bold.dim} {bar:40.green/yellow} {wide_msg}")
            .unwrap();
        stats.start();
        let m = MultiProgress::new();
        let count = self.progress_info.len();
        let mut progress_bars = vec![];

        for id in 0..count {
            let pb = m.add(ProgressBar::new(100));
            pb.set_prefix(format!("[{}/{}]", id + 1, count));
            pb.set_style(spinner_style.clone());     
            progress_bars.push(pb);
        }
        loop {
            if self
                .progress_info
                .values()
                .all(|s| s.lock().unwrap().sync_done)
            {
                break;
            }
            for (id, progress) in &self.progress_info {                
                if progress.lock().unwrap().sync_done {
                    progress_bars[*id].finish_with_message("<done>");
                } else {
                    progress_bars[*id].set_message(progress.lock().unwrap().current_file.clone());
                }
            }
            thread::sleep(Duration::from_millis(10));
        }
        m.clear().unwrap();
        stats.stop();
        stats
    }
}
