use std::fs;
use std::option::Option;
use std::path::Path;
use std::path::PathBuf;

use crate::SyncOptions;

#[derive(Debug, Clone)]
pub struct Entry {
    description: String,
    path: PathBuf,
    metadata: Option<fs::Metadata>,
    exists: bool,
    is_link: Option<bool>,
}

impl Entry {
    pub fn new(description: &str, entry_path: &Path) -> Entry {
        let mut metadata = fs::metadata(entry_path).ok();
        let is_link;
        let symlink_metadata = fs::symlink_metadata(entry_path);
        if let Ok(data) = symlink_metadata {
            is_link = Some(data.file_type().is_symlink());
            metadata = Some(data);
        } else {
            is_link = None;
        }

        Entry {
            description: String::from(description),
            metadata,
            path: entry_path.to_path_buf(),
            exists: entry_path.exists(),
            is_link,
        }
    }

    pub fn description(&self) -> &String {
        &self.description
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
    pub fn metadata(&self) -> Option<&fs::Metadata> {
        self.metadata.as_ref()
    }
    pub fn exists(&self) -> bool {
        self.exists
    }

    pub fn is_link(&self) -> Option<bool> {
        self.is_link
    }

    pub fn is_file(&self) -> Option<bool> {
        if self.metadata.is_none() {
            return None;
        }
        Some(self.metadata().unwrap().is_file())
    }

    pub fn length(&self) -> Option<usize> {
        if self.metadata.is_none() {
            return None;
        }
        Some(self.metadata().unwrap().len() as usize)
    }
}

#[cfg(test)]
mod tests {

    use super::Entry;
    use super::Path;

    #[test]
    fn new_entry_with_non_existing_path() {
        let path = Path::new("/path/to/nosuch.txt");
        let entry = Entry::new("nosuch", path);

        assert!(!entry.exists());
        assert!(entry.metadata.is_none());
    }

    #[test]
    fn new_entry_with_existing_path() {
        let path = Path::new(file!());
        let entry = Entry::new("entry.rs", path);

        assert!(entry.exists());
        assert!(entry.metadata.is_some());
        let is_link = entry.is_link();
        assert!(is_link.is_some());
        assert!(!is_link.unwrap());
    }
}

#[derive(Clone)]
pub struct CopyEntry {
    pub src: Entry,
    pub dest: Entry,
    pub opts: SyncOptions,
    pub chunk_index: Option<u64>,
    pub chunk_offset: Option<usize>,
    pub chunk_length: Option<usize>,
}

impl CopyEntry {
    pub fn new(src: Entry, dest: Entry, opts: SyncOptions) -> CopyEntry {
        CopyEntry {
            src,
            dest,
            opts,
            chunk_index: None,
            chunk_offset: None,
            chunk_length: None,
        }
    }

    pub fn new_chunk(&self, index: u64, offset: usize, length: usize) -> CopyEntry {
        let mut entry = self.clone();
        entry.chunk_index = Some(index);
        entry.chunk_offset = Some(offset);
        entry.chunk_length = Some(length);
        entry
    }

    pub fn chunk_index(&self) -> Option<u64> {
        self.chunk_index
    }

    pub fn chunk_offset(&self) -> Option<usize> {
        self.chunk_offset
    }

    pub fn chunk_length(&self) -> Option<usize> {
        self.chunk_length
    }

    pub fn is_chunk(&self) -> bool {
        self.chunk_index.is_some() && self.chunk_offset.is_some() && self.chunk_length.is_some()
    }

    // pub fn is_first_chunk(&self) -> bool {
    //     self.offset == Some(0)
    // }

    pub fn is_last_chunk(&self) -> bool {
        self.chunk_offset == Some(self.src.length().unwrap() - self.chunk_length.unwrap())
    }
}
