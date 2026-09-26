use std::fs::{File, OpenOptions, remove_file};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{ExoError, ExoResult};

static NEXT_SPOOL_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) struct MarkSpool {
    path: PathBuf,
    file: File,
    count: u32,
}

impl MarkSpool {
    pub(crate) fn create_near(output_path: &Path, suffix: &str) -> ExoResult<Self> {
        let parent = output_path.parent().unwrap_or_else(|| Path::new("."));
        let name = output_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("exo");
        for _ in 0..16 {
            let id = NEXT_SPOOL_ID.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!(".{name}.{suffix}.{}.{id}.tmp", std::process::id()));
            match OpenOptions::new()
                .write(true)
                .read(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => {
                    return Ok(Self {
                        path,
                        file,
                        count: 0,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(ExoError::Io(error)),
            }
        }
        Err(ExoError::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not allocate a unique EXO mark spool",
        )))
    }

    pub(crate) fn append(&mut self, mark: &[u8]) -> ExoResult<()> {
        let count = self
            .count
            .checked_add(1)
            .ok_or_else(|| ExoError::CodecError("EXO granule mark count exceeds u32".into()))?;
        self.file.write_all(mark)?;
        self.count = count;
        Ok(())
    }

    pub(crate) fn copy_to<W: Write + Seek>(&mut self, output: &mut W) -> ExoResult<()> {
        self.file.flush()?;
        self.file.seek(SeekFrom::Start(0))?;
        std::io::copy(&mut self.file, output)?;
        Ok(())
    }
}

impl Drop for MarkSpool {
    fn drop(&mut self) {
        let _ = remove_file(&self.path);
    }
}
