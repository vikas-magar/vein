use error::Result;
use tokio::sync::mpsc;
use crate::identifiers::EpochId;
use super::epoch::StreamPayload;
use super::Chunk;

/// A stream of Chunk values, one per epoch.
///
/// Wraps a tokio::sync::mpsc channel. Each message is either a Chunk
/// (one epoch's data) or an EpochBarrier (for multi-input alignment).
///
/// Every operator has one output stream (producer) and N input streams (consumers).
pub struct Stream {
    _epoch: u64,
    tx: mpsc::Sender<Result<StreamPayload>>,
    rx: mpsc::Receiver<Result<StreamPayload>>,
}

impl Stream {
    /// Split into producer + consumer halves with the given buffer size.
    pub fn new(buffer: usize) -> (StreamProducer, StreamConsumer) {
        let (tx, rx) = mpsc::channel(buffer);
        (StreamProducer { raw_epoch: 1, tx }, StreamConsumer { rx })
    }

    /// Create a Stream from an existing mpsc receiver.
    #[allow(dead_code)]
    pub fn from_channel(rx: mpsc::Receiver<Result<StreamPayload>>) -> Self {
        let (tx, _) = mpsc::channel(1);
        Self { _epoch: 0, tx, rx }
    }
}

/// Producer half of a stream (used by the operator's output).
pub struct StreamProducer {
    raw_epoch: u64,
    tx: mpsc::Sender<Result<StreamPayload>>,
}

impl StreamProducer {
    /// Current epoch (incremented on each push).
    pub fn epoch(&self) -> EpochId {
        EpochId::from_raw(self.raw_epoch)
    }

    /// Push one epoch's payload (data or barrier).
    pub async fn push(&mut self, payload: StreamPayload) -> Result<()> {
        self.raw_epoch += 1;
        self.tx
            .send(Ok(payload))
            .await
            .map_err(|_| error::VeinError::internal("stream channel closed"))?;
        Ok(())
    }

    /// Send one epoch's data chunk.
    pub async fn send(&mut self, chunk: Chunk) -> Result<()> {
        self.push(StreamPayload::Data(chunk)).await
    }

    /// Signal end of stream (graceful shutdown).
    pub async fn close(self) -> Result<()> {
        drop(self.tx);
        Ok(())
    }
}

/// Consumer half of a stream (used by the downstream operator's input).
pub struct StreamConsumer {
    rx: mpsc::Receiver<Result<StreamPayload>>,
}

impl StreamConsumer {
    /// Create a consumer from a raw mpsc receiver (for testing).
    pub fn new(rx: mpsc::Receiver<Result<StreamPayload>>) -> Self {
        Self { rx }
    }

    /// Read the next epoch's payload. Returns None on graceful shutdown.
    pub async fn next(&mut self) -> Result<Option<StreamPayload>> {
        match self.rx.recv().await {
            Some(Ok(payload)) => Ok(Some(payload)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    /// Read the next epoch's data chunk, ignoring barriers.
    /// Returns None if the stream ended or a barrier was received.
    pub async fn recv_data(&mut self) -> Result<Option<Chunk>> {
        match self.next().await? {
            Some(StreamPayload::Data(chunk)) => Ok(Some(chunk)),
            Some(StreamPayload::Barrier(_)) => Ok(None),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;
    use crate::dbsp::epoch::EpochBarrier;
    use crate::identifiers::SourceId;

    #[tokio::test]
    async fn test_stream_send_recv() {
        let (mut producer, mut consumer) = Stream::new(16);

        let schema = arrow::datatypes::SchemaRef::new(arrow::datatypes::Schema::new(Vec::<arrow::datatypes::Field>::new()));
        let chunk = Chunk::empty(schema);
        producer.send(chunk).await.unwrap();

        let received = consumer.next().await.unwrap().unwrap();
        assert!(matches!(received, StreamPayload::Data(_)));
    }

    #[tokio::test]
    async fn test_stream_barrier() {
        let (mut producer, mut consumer) = Stream::new(16);

        let barrier = EpochBarrier {
            epoch: EpochId::from_raw(1),
            source_id: SourceId::from_raw(0),
            watermark: None,
        };
        producer.push(StreamPayload::Barrier(barrier)).await.unwrap();

        let received = consumer.next().await.unwrap().unwrap();
        assert!(matches!(received, StreamPayload::Barrier(_)));
    }

    #[tokio::test]
    async fn test_stream_close() {
        let (producer, mut consumer) = Stream::new(16);
        producer.close().await.unwrap();
        assert!(consumer.next().await.unwrap().is_none());
    }
}
