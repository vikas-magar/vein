use crate::daemon::pipeline::{PipelineBreaker, PipelineNode, VeinBatch};
use common::Result;
use common::transport::QuicClient;

/// A pipeline sink that partitions data and writes to a virtual file system.
pub struct VfsSink {
    /// The VFS storage interface (mocked for now)
    pub client: Option<QuicClient>,
    /// Partition buffers. We use a simple Vec of VeinBatches for now.
    pub buffers: Vec<Vec<VeinBatch>>,
    /// Threshold before pushing a buffer over QUIC (e.g., 16 * 1024 * 1024)
    pub flush_threshold_bytes: usize,
    /// Number of partitions for the sink
    pub num_partitions: usize,
}

impl VfsSink {
    /// Create a new VfsSink instance
    pub fn new(
        client: Option<QuicClient>,
        num_partitions: usize,
        flush_threshold_bytes: usize,
    ) -> Self {
        let mut buffers = Vec::with_capacity(num_partitions);
        for _ in 0..num_partitions {
            buffers.push(Vec::new());
        }
        Self {
            client,
            buffers,
            flush_threshold_bytes,
            num_partitions,
        }
    }

    /// Mock flush function. In a real system, it would serialize the Arrow batch and push via QUIC.
    pub async fn flush_partition_to_vfs(&mut self, partition_id: usize) -> Result<()> {
        // In a complete implementation, this would serialize `self.buffers[partition_id]` to Arrow IPC
        // and send a custom RPC via `self.client.connection.open_bi()`.
        // For now, we clear the buffer to represent successful flushing.
        self.buffers[partition_id].clear();
        Ok(())
    }
}

impl PipelineNode for VfsSink {
    fn push(&mut self, batch: VeinBatch) -> Result<()> {
        // 1. Mock hash routing: just push everything to partition 0 for this stub.
        if !self.buffers.is_empty() {
            self.buffers[0].push(batch);
            // 2. We mock checking the threshold.
            if self.buffers[0].len() > 100 {
                // mock threshold
                // trigger async flush (which is tricky inside a sync trait, so we just clear for the stub)
                self.buffers[0].clear();
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl PipelineBreaker for VfsSink {
    fn finalize(&mut self) -> Result<()> {
        // Flush all remaining non-empty buffers to the VFS
        for buf in &mut self.buffers {
            buf.clear();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_vfs_sink() -> VfsSink {
        VfsSink::new(None, 3, 1024)
    }

    #[test]
    fn test_vfs_sink_initialization() {
        let sink = dummy_vfs_sink();
        assert_eq!(sink.num_partitions, 3);
        assert_eq!(sink.buffers.len(), 3);
        assert_eq!(sink.flush_threshold_bytes, 1024);
    }

    #[test]
    fn test_vfs_sink_push() {
        let mut sink = dummy_vfs_sink();
        let batch =
            VeinBatch::Factorized(crate::daemon::pipeline::FactorizedChunk { blocks: vec![] });
        sink.push(batch).unwrap();
        assert_eq!(sink.buffers[0].len(), 1);
    }
}
