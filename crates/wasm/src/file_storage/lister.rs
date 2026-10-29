use std::ops::Bound::{Excluded, Included, Unbounded};

use opendal::{Result, raw::oio};

use super::FileIndex;

pub(super) struct FileLister {
    files: FileIndex,
    prefix: String,
    cursor: Option<String>,
    finished: bool,
}

impl FileLister {
    pub(super) fn new(files: FileIndex, prefix: &str) -> Self {
        Self { files, prefix: prefix.trim_start_matches('/').to_owned(), cursor: None, finished: false }
    }
}

impl oio::List for FileLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if self.finished { return Ok(None); }
        let start = self.cursor.as_deref().map(Excluded).unwrap_or(Included(self.prefix.as_str()));
        match self.files.range::<str, _>((start, Unbounded)).next() {
            Some((path, file)) if path.starts_with(&self.prefix) => {
                self.cursor = Some(path.clone());
                Ok(Some(oio::Entry::new(path, file.metadata())))
            }
            _ => {
                self.finished = true;
                Ok(None)
            }
        }
    }
}
