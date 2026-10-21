//! Read-only OS mapping used to lend Parquet byte ranges to `Bytes` without a
//! range-sized user-space buffer.

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::ptr::NonNull;

pub(super) struct MappedRegion {
    pointer: NonNull<u8>,
    length: usize,
}

// The mapping is immutable and remains valid until `Drop` calls `munmap`.
unsafe impl Send for MappedRegion {}

impl MappedRegion {
    pub(super) fn open(file: &File, length: usize) -> io::Result<Self> {
        if length == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot map an empty file",
            ));
        }
        if length > isize::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mapping exceeds Rust slice addressability",
            ));
        }
        // SAFETY: The file is open for the duration of the call, the mapping is
        // read-only, and the returned owner keeps the mapping alive. Parquet
        // inputs must remain immutable while a scan is using their mapping.
        let address = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };
        if address == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let Some(pointer) = NonNull::new(address.cast()) else {
            // SAFETY: this is the successful mapping returned just above.
            unsafe {
                libc::munmap(address, length);
            }
            return Err(io::Error::other(
                "OS returned a null address for a non-empty mapping",
            ));
        };
        Ok(Self { pointer, length })
    }
}

impl AsRef<[u8]> for MappedRegion {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: `pointer` refers to `length` readable bytes in the live
        // read-only mapping owned by this value.
        unsafe { std::slice::from_raw_parts(self.pointer.as_ptr(), self.length) }
    }
}

impl Drop for MappedRegion {
    fn drop(&mut self) {
        // SAFETY: `pointer` and `length` are the exact successful mmap result.
        unsafe {
            libc::munmap(self.pointer.as_ptr().cast(), self.length);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use bytes::Bytes;

    use super::MappedRegion;

    #[test]
    fn bytes_owner_keeps_mapping_and_lends_the_original_address() {
        let mut file = tempfile::NamedTempFile::new().expect("temporary file");
        file.write_all(b"mapped-data").expect("write source bytes");
        let length = file.as_file().metadata().unwrap().len() as usize;
        let region = MappedRegion::open(file.as_file(), length).expect("map source file");
        let mapped_address = region.as_ref().as_ptr();
        let bytes = Bytes::from_owner(region);
        let range = bytes.slice(2..7);

        assert_eq!(range.as_ref(), b"pped-");
        assert_eq!(range.as_ptr(), mapped_address.wrapping_add(2));
        drop(file);
        assert_eq!(range.as_ref(), b"pped-");
    }
}
