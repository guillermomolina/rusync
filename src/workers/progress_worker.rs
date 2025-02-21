use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::sync::Stats;

use super::SyncProgress;

pub struct ProgressWorker {
    progress_info: HashMap<usize, Arc<Mutex<SyncProgress>>>
}

impl ProgressWorker {
    pub fn new(
        progress_info: HashMap<usize, Arc<Mutex<SyncProgress>>>
    ) -> ProgressWorker {
        ProgressWorker {
            progress_info,
        }
    }

    pub fn start(self) -> Stats {
        let mut stats = Stats::new();
        let mut index = 0;
        stats.start();
        loop {
             for (id, sync_progress) in self.progress_info.iter() {
                println!("[{}], file: {}", id, sync_progress.lock().unwrap().current_file);
            }
            index += 1;          
            if index == 100 {
                break;
            }
            thread::sleep(Duration::from_secs(1));
        }
        stats.stop();
        stats
    }
}
