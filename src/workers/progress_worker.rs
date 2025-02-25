use std::sync::mpsc::Receiver;
use std::thread;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use super::{CopyStatus, SyncStatus, WalkStatus};

#[doc(hidden)]
pub enum ProgressMessage {
    WalkProgress(WalkStatus),
    SyncProgress(SyncStatus),
    CopyProgress(usize, CopyStatus),
}

pub struct ProgressWorker {
    input: Receiver<ProgressMessage>,
    walk_status: WalkStatus,
    sync_status: SyncStatus,
    copy_statusses: Vec<CopyStatus>,
}

impl ProgressWorker {
    pub fn new(
        copy_worker_count: usize,
        input: Receiver<ProgressMessage>,
    ) -> ProgressWorker {
        ProgressWorker {
            input,
            walk_status: WalkStatus::new(),
            sync_status: SyncStatus::new(),
            copy_statusses: vec![CopyStatus::new(); copy_worker_count as usize],
         }
    }

    pub fn start(mut self) {
        let messages: Vec<_> = self.input.iter().collect();
        for progress in messages {
            match progress {
                ProgressMessage::WalkProgress(walk_status) => {
                    self.walk_status = walk_status;
                }
                ProgressMessage::SyncProgress(sync_status) => {
                    self.sync_status = sync_status;
                }
                ProgressMessage::CopyProgress(id, copy_status) => {
                    self.copy_statusses[id as usize] = copy_status;
                }
            }
            self.show_progress();
        }
    }

   pub fn show_progress(&mut self) -> () {
        const PROGRESS_CHARS: &str = "█▉▊▋▌▍▎▏  ";
        let m = MultiProgress::new();
        let count = self.copy_statusses.len();
        let mut copy_progress_bars = vec![];

        let files_pb = m.add(ProgressBar::new(
            self.walk_status.num_files as u64,
        ));
        files_pb.set_prefix("[files]");

        let files_pb_style = ProgressStyle::with_template(
            "{prefix:.bold.dim} {bar:40.green/yellow} {pos}/{len} {elapsed_precise}, BW: <{per_sec}>, ETA: {eta_precise}",
        )
        .unwrap()
        .progress_chars(PROGRESS_CHARS);
        files_pb.set_style(files_pb_style);

        let size_pb = m.add(ProgressBar::new(
            self.walk_status.total_size as u64,
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
                .iter()
                .all(|s| s.copy_done) {
                break;
            }
            // let mut files_transfered = self.sync_status.num_synced - self.sync_status.need_copy;
            let mut files_transfered= 0;
            let mut size_transfered = 0;
            for (id, copy_progress) in self.copy_statusses.iter().enumerate() {
                files_transfered += copy_progress.num_transfered_files;
                size_transfered += copy_progress.total_transfered_size;
                let file_transfered_size = copy_progress.file_transfered_size;
                copy_progress_bars[id].set_position(file_transfered_size as u64);
                copy_progress_bars[id].set_length(copy_progress.file_size as u64);
                if copy_progress.copy_done {
                    copy_progress_bars[id].finish_with_message("<done>");
                } else {
                    copy_progress_bars[id]
                        .set_message(copy_progress.current_file.clone());
                }
            }
            files_pb.set_length(self.walk_status.num_files as u64);
            files_pb.set_position(files_transfered as u64);
            let total_size = self.walk_status.total_size as u64;
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
