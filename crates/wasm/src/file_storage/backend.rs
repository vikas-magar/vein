use std::sync::Arc;

use opendal::{Capability, Error, ErrorKind, MetadataBuilder, OperationContext, Result};
use opendal::raw::{oio, OpCopy, OpCreateDir, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite,
                   RpCreateDir, RpPresign, RpRename, RpStat, Service, ServiceInfo};

use super::{FileIndex, FileRangeReader, config::FileStorageConfig, lister::FileLister, reader::FileReader};

#[derive(Debug)]
pub(super) struct FileBackend<R: FileRangeReader> {
    namespace: String,
    files: FileIndex,
    reader: Arc<R>,
}

impl<R: FileRangeReader> FileBackend<R> {
    pub(super) fn new(config: FileStorageConfig<R>) -> Result<Self> {
        if config.namespace.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "registered files require a namespace"));
        }
        let reader = config.reader.ok_or_else(|| Error::new(ErrorKind::ConfigInvalid, "file range reader must be injected"))?;
        Ok(Self { namespace: config.namespace, files: config.files, reader })
    }
}

impl<R: FileRangeReader> Service for FileBackend<R> {
    type Reader = oio::PositionReader<FileReader<R>>;
    type Writer = ();
    type Lister = oio::HierarchyLister<FileLister>;
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo { ServiceInfo::new("browser-file", "/", &self.namespace) }
    fn capability(&self) -> Capability {
        Capability { read: true, stat: true, list: true, list_with_recursive: true, ..Default::default() }
    }

    async fn stat(&self, _: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        if path == "/" || path.is_empty() { return Ok(RpStat::new(MetadataBuilder::dir().build())); }
        if let Some(file) = self.files.get(path) { return Ok(RpStat::new(file.metadata())); }
        if path.ends_with('/') && self.files.range(path.to_owned()..).next().is_some_and(|(key, _)| key.starts_with(path)) {
            return Ok(RpStat::new(MetadataBuilder::dir().build()));
        }
        Err(Error::new(ErrorKind::NotFound, "registered file was not found").with_context("path", path))
    }

    fn read(&self, _: &OperationContext, path: &str, _: OpRead) -> Result<Self::Reader> {
        let file = self.files.get(path).ok_or_else(|| Error::new(ErrorKind::NotFound, "registered file was not found").with_context("path", path))?;
        Ok(oio::PositionReader::new(FileReader::new(Arc::clone(&self.reader), path, file.size)))
    }

    fn list(&self, _: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        Ok(oio::HierarchyLister::new(FileLister::new(Arc::clone(&self.files), path), path, args.recursive()))
    }

    async fn create_dir(&self, _: &OperationContext, _: &str, _: OpCreateDir) -> Result<RpCreateDir> { Err(read_only()) }
    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> { Err(read_only()) }
    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> { Err(read_only()) }
    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> { Err(read_only()) }
    async fn rename(&self, _: &OperationContext, _: &str, _: &str, _: OpRename) -> Result<RpRename> { Err(read_only()) }
    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> { Err(read_only()) }
}

fn read_only() -> Error { Error::new(ErrorKind::Unsupported, "registered browser files are read-only") }
