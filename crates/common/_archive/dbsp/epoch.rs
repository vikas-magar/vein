use std::collections::{HashMap, VecDeque};
use error::{Result, VeinError};
use crate::identifiers::{EpochId, SourceId};
use super::Chunk;

/// An epoch barrier for multi-input alignment.
///
/// Analogous to Flink checkpoint barriers / RisingWave epoch barriers.
/// Each source injects a barrier periodically. When all inputs of an operator
/// have received the same epoch's barrier, the operator runs that epoch.
#[derive(Debug, Clone)]
pub struct EpochBarrier {
    /// The epoch being aligned.
    pub epoch: EpochId,
    /// The source that emitted this barrier.
    pub source_id: SourceId,
    /// Event-time watermark in milliseconds since epoch, if available.
    pub watermark: Option<i64>,
}

/// Payload carried by a stream message.
#[derive(Debug, Clone)]
pub enum StreamPayload {
    /// Normal data — one epoch's Z-set diff.
    Data(Chunk),
    /// Epoch barrier (for multi-input synchronization).
    Barrier(EpochBarrier),
}

/// Aligns multiple input streams to the same epoch.
///
/// Buffers Data items until all sources have sent a Barrier for the same epoch,
/// then yields aligned data for all sources.
pub struct BarrierAligner {
    buffers: HashMap<SourceId, VecDeque<(u64, Chunk)>>,
    current_raw: u64,
    barriers_received: usize,
    total_sources: usize,
}

impl BarrierAligner {
    /// Create a new aligner for the given number of sources.
    pub fn new(total_sources: usize) -> Self {
        Self {
            buffers: HashMap::with_capacity(total_sources),
            current_raw: 1,
            barriers_received: 0,
            total_sources,
        }
    }

    /// Push data from a source into the aligner.
    ///
    /// If the payload is a Barrier for the current epoch, the barrier counter
    /// is incremented. If it is regular Data, it is buffered by epoch.
    pub fn push(&mut self, source_id: SourceId, payload: StreamPayload) -> Result<()> {
        match payload {
            StreamPayload::Data(chunk) => {
                self.buffers
                    .entry(source_id)
                    .or_default()
                    .push_back((self.current_raw, chunk));
                Ok(())
            }
            StreamPayload::Barrier(barrier) => {
                if barrier.epoch.into_raw() != self.current_raw {
                    return Err(VeinError::EpochMismatch {
                        source_id: source_id.into_raw(),
                        expected: self.current_raw,
                        actual: barrier.epoch.into_raw(),
                    });
                }
                self.barriers_received += 1;
                Ok(())
            }
        }
    }

    /// Check if all sources have sent barriers for the current epoch.
    ///
    /// Returns `Some(aligned_data)` if all sources are aligned,
    /// `None` if still waiting for more barriers.
    pub fn try_align(&mut self) -> Result<Option<Vec<Chunk>>> {
        if self.barriers_received < self.total_sources {
            return Ok(None);
        }

        let mut aligned = Vec::with_capacity(self.total_sources);
        for i in 0..self.total_sources as u16 {
            let source_id = SourceId::from_raw(i);
            let buf = self.buffers.entry(source_id).or_default();
            if buf.is_empty() {
                return Err(VeinError::internal(format!(
                    "source {} has no data for epoch {}",
                    source_id, self.current_raw
                )));
            }
            let (epoch, chunk) = buf.pop_front().unwrap();
            if epoch != self.current_raw {
                return Err(VeinError::EpochMismatch {
                    source_id: source_id.into_raw(),
                    expected: self.current_raw,
                    actual: epoch,
                });
            }
            aligned.push(chunk);
        }

        self.current_raw += 1;
        self.barriers_received = 0;
        Ok(Some(aligned))
    }

    /// Number of epochs pending alignment (for flow control).
    pub fn pending_epochs(&self) -> usize {
        self.buffers
            .values()
            .map(|buf| buf.len())
            .max()
            .unwrap_or(0)
    }
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;
    use crate::dbsp::Chunk;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    fn empty_chunk() -> Chunk {
        let schema = SchemaRef::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        Chunk::empty(schema)
    }

    #[test]
    fn test_barrier_align() {
        let mut aligner = BarrierAligner::new(2);
        let s0 = SourceId::from_raw(0);
        let s1 = SourceId::from_raw(1);

        aligner.push(s0, StreamPayload::Data(empty_chunk())).unwrap();
        aligner
            .push(
                s0,
                StreamPayload::Barrier(EpochBarrier {
                    epoch: EpochId::from_raw(1),
                    source_id: s0,
                    watermark: None,
                }),
            )
            .unwrap();
        assert!(aligner.try_align().unwrap().is_none());

        aligner.push(s1, StreamPayload::Data(empty_chunk())).unwrap();
        aligner
            .push(
                s1,
                StreamPayload::Barrier(EpochBarrier {
                    epoch: EpochId::from_raw(1),
                    source_id: s1,
                    watermark: None,
                }),
            )
            .unwrap();
        let result = aligner.try_align().unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_barrier_epoch_mismatch() {
        let mut aligner = BarrierAligner::new(1);
        let s0 = SourceId::from_raw(0);

        let err = aligner.push(
            s0,
            StreamPayload::Barrier(EpochBarrier {
                epoch: EpochId::from_raw(99),
                source_id: s0,
                watermark: None,
            }),
        );
        assert!(err.is_err());
    }
}
