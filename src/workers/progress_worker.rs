use std::sync::mpsc::Receiver;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use super::{CopyStatus, SyncStatus, WalkStatus};

#[doc(hidden)]
pub enum ProgressMessage {
    WalkProgress(WalkStatus),
    SyncProgress(SyncStatus),
    CopyProgress(usize, CopyStatus),
}

pub struct ProgressWorker {
    copy_worker_count: usize,
    input: Receiver<ProgressMessage>,
    walk_status: WalkStatus,
    sync_status: SyncStatus,
    copy_statusses: Vec<CopyStatus>,
    multi_pogress: MultiProgress,
    files_pb: ProgressBar,
    size_pb: ProgressBar,
    copy_progress_bars: Vec<ProgressBar>,
}

impl ProgressWorker {
    pub fn new(
        copy_worker_count: usize,
        input: Receiver<ProgressMessage>,
    ) -> ProgressWorker {
        ProgressWorker {
            copy_worker_count,
            input,
            walk_status: WalkStatus::new(),
            sync_status: SyncStatus::new(),
            copy_statusses: vec![CopyStatus::new(); copy_worker_count],
            multi_pogress: MultiProgress::new(),
            files_pb: ProgressBar::hidden(),
            size_pb: ProgressBar::hidden(),
            copy_progress_bars: vec![],
         }
    }

    pub fn start(mut self) {
        self.initialize();
        while let Ok(progress_message) = {
            let progress_message = self.input.recv();
            progress_message
        } {
            match progress_message {
                ProgressMessage::WalkProgress(walk_status) => {
                    self.walk_status = walk_status;
                }
                ProgressMessage::SyncProgress(sync_status) => {
                    self.sync_status = sync_status;
                }
                ProgressMessage::CopyProgress(id, copy_status) => {
                    self.copy_statusses[id] = copy_status;
                }
            }
            if self.has_ended() {
                self.finish();
                break;
            }
            self.show_progress();
        }
    }

    pub fn has_ended(&self) -> bool {
        self.walk_status.walk_done
            && self.sync_status.sync_done
            && self.copy_statusses.iter().all(|s| s.copy_done)
    }

    pub fn initialize(&mut self) {
        const PROGRESS_CHARS: &str = "█▉▊▋▌▍▎▏  ";

        self.files_pb.set_prefix("[files]");
        let files_pb_style = ProgressStyle::with_template(
            "{prefix:.bold.dim} {bar:40.green/yellow} <{pos}/{len} @ {per_sec}> {elapsed_precise}, ETA: {eta_precise}",
        )
        .unwrap()
        .progress_chars(PROGRESS_CHARS);
        self.files_pb.set_style(files_pb_style);
        self.multi_pogress.add(self.files_pb.clone());

        self.size_pb.set_prefix("[size]");
        let size_pb_style =
            ProgressStyle::with_template("{prefix:.bold.dim} {bar:40.green/yellow} <{bytes}/{total_bytes}@{binary_bytes_per_sec}>, ETA: {eta_precise}")
                .unwrap()        
                .progress_chars(PROGRESS_CHARS);
        self.size_pb.set_style(size_pb_style);
        self.multi_pogress.add(self.size_pb.clone());

        let sync_pb_style = ProgressStyle::with_template(
            "{prefix:.bold.dim} {bar:40.green/yellow} <{bytes}/{total_bytes} @ {binary_bytes_per_sec}> {wide_msg}",
        ).unwrap().progress_chars(PROGRESS_CHARS);
        for id in 0..self.copy_worker_count {
            let pb = ProgressBar::hidden();
            pb.set_prefix(format!("[{}/{}]", id + 1, self.copy_worker_count));
            pb.set_style(sync_pb_style.clone());
            self.copy_progress_bars.push(self.multi_pogress.add(pb));
        }
    }

    pub fn finish(&self) {
        self.files_pb.finish();
        self.size_pb.finish();
        for pb in self.copy_progress_bars.iter() {
            pb.finish();
        }
        self.multi_pogress.clear().unwrap();
    }

    pub fn show_progress(&mut self) {
        let mut files_transfered= 0;
        let mut size_transfered = 0;
        for (id, copy_progress) in self.copy_statusses.iter().enumerate() {
            files_transfered += copy_progress.num_transfered_files;
            size_transfered += copy_progress.total_transfered_size;
            self.copy_progress_bars[id].set_position(copy_progress.entry_transfered_size as u64);
            self.copy_progress_bars[id].set_length(copy_progress.entry_size as u64);
            if copy_progress.copy_done {
                self.copy_progress_bars[id].finish_with_message("<done>");
            } else {
                let message = if copy_progress.current_chunk_id.is_some() {
                    format!(
                        "{} (#{})",
                        copy_progress.current_file,
                        copy_progress.current_chunk_id.unwrap()
                    )
                } else {
                    copy_progress.current_file.clone()
                };
                self.copy_progress_bars[id]
                    .set_message(message);
            }
        }
        self.files_pb.set_length(self.walk_status.num_files as u64);
        self.files_pb.set_position(files_transfered as u64);
        let total_size = self.walk_status.total_size as u64;
        self.size_pb.set_length(total_size);
        self.size_pb.set_position(size_transfered as u64);
    }
}
