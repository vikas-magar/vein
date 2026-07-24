use s2n_quic::Connection;
use crate::error::{Error, ErrorKind, Result};
use std::time::Instant;
use tracing::info;

/// The strategy to use for multiplexing the data stream over QUIC.
#[derive(Debug, Clone, PartialEq)]
pub enum MultiplexStrategy {
    /// Send the entire batch over a single QUIC stream.
    Single,
    /// Partition the batch into multiple chunks and send them concurrently.
    Row,
    /// Split the batch by column and send raw buffers concurrently.
    Column,
}

use crate::traits::{DataReader, DataWriter};

/// Transmits Arrow data over a QUIC connection.
pub struct QuicTransmitter {
    /// The multiplexing strategy to use.
    pub strategy: MultiplexStrategy,
}

impl QuicTransmitter {
    /// Create a new QuicTransmitter.
    pub fn new(strategy: MultiplexStrategy) -> Self {
        Self { strategy }
    }

    /// Transmits data using the chosen multiplexing strategy.
    pub async fn transmit<R>(&self, conn: &mut Connection, reader: R) -> Result<()>
    where
        R: DataReader + Send + 'static,
    {
        match self.strategy {
            MultiplexStrategy::Single => self.transmit_single(conn, reader).await,
            MultiplexStrategy::Row => self.transmit_rows(conn, reader, 4).await, // Use 4 streams for prototype
            MultiplexStrategy::Column => self.transmit_columns(conn, reader).await,
        }
    }

    async fn transmit_rows<R>(&self, conn: &mut Connection, mut reader: R, num_streams: usize) -> Result<()> 
    where R: DataReader + Send + 'static {
        info!("Row Multiplexer: Opening {} concurrent QUIC streams.", num_streams);

        let mut streams = Vec::new();
        for _ in 0..num_streams {
            let stream = conn.open_bidirectional_stream().await
                .map_err(|e| Error::new(ErrorKind::ConnectionFailed, "Failed to open bidirectional stream").with_source(e))?;
            streams.push(stream);
        }

        let mut all_batches = Vec::new();
        while let Some(batch) = reader.read_batch().await? {
            all_batches.push(batch);
        }

        let mut tasks = tokio::task::JoinSet::new();
        let t_send = Instant::now();

        for (stream_idx, stream) in streams.into_iter().enumerate() {
            let mut partition_batches = Vec::new();
            for batch in &all_batches {
                let rows = batch.num_rows();
                let chunk_size = rows.div_ceil(num_streams);
                let offset = stream_idx * chunk_size;
                if offset < rows {
                    let length = chunk_size.min(rows - offset);
                    partition_batches.push(batch.slice(offset, length));
                }
            }
            
            tasks.spawn(async move {
                let mut writer = crate::arrow::IpcStreamEncoder::new();
                for batch in partition_batches {
                    writer.write_batch(&batch).unwrap();
                }
                let payload = writer.finish().unwrap();
                let len = payload.len();

                let (mut rx, mut tx) = stream.split();
                tx.send(bytes::Bytes::from(payload)).await.unwrap();
                tx.finish().unwrap();
                while let Ok(Some(_)) = rx.receive().await {}
                
                (stream_idx, len)
            });
        }

        let mut total_bytes = 0;
        while let Some(res) = tasks.join_next().await {
            let (stream_idx, len) = res.unwrap();
            info!("Row Stream {} finished transmitting {} bytes.", stream_idx, len);
            total_bytes += len;
        }
        
        let t_send_elapsed = t_send.elapsed();
        let throughput_mbps = (total_bytes as f64 / 1_048_576.0) / t_send_elapsed.as_secs_f64();

        info!(
            "SENDER METRICS | Strategy: Row | Streams: {} | Total Bytes: {} | Send time: {:.3} ms | Throughput: {:.4} MB/s",
            num_streams,
            total_bytes,
            t_send_elapsed.as_secs_f64() * 1000.0,
            throughput_mbps
        );

        Ok(())
    }

    async fn transmit_single<R>(&self, conn: &mut Connection, mut reader: R) -> Result<()> 
    where R: DataReader + Send + 'static {
        let t_build = Instant::now();
        let mut writer = crate::arrow::IpcStreamEncoder::new();
        while let Some(batch) = reader.read_batch().await? {
            writer.write_batch(&batch)?;
        }
        let payload = writer.finish()?;
        info!("DataFrame serialized: {} bytes in {:.3}ms", payload.len(), t_build.elapsed().as_secs_f64() * 1000.0);

        let stream = conn.open_bidirectional_stream().await
            .map_err(|e| Error::new(ErrorKind::ConnectionFailed, "Failed to open bidirectional stream").with_source(e))?;
        let (mut rx, mut tx) = stream.split();

        let total_bytes_len = payload.len();
        info!("Transmitting {} bytes via SingleStream...", total_bytes_len);
        
        let t_send = Instant::now();
        let chunk = bytes::Bytes::from(payload);
        tx.send(chunk).await.map_err(|e| Error::new(ErrorKind::ConnectionFailed, "Failed to send payload").with_source(e))?;
        tx.finish().map_err(|e| Error::new(ErrorKind::ConnectionFailed, "Failed to finish TX").with_source(e))?;

        while let Ok(Some(_)) = rx.receive().await {}
        let t_send_elapsed = t_send.elapsed();

        let throughput_mbps = (total_bytes_len as f64 / 1_048_576.0) / t_send_elapsed.as_secs_f64();

        info!(
            "SENDER METRICS | Strategy: Single | Bytes: {} | Send time: {:.3} ms | Throughput: {:.4} MB/s",
            total_bytes_len,
            t_send_elapsed.as_secs_f64() * 1000.0,
            throughput_mbps
        );

        Ok(())
    }

    async fn transmit_columns<R>(&self, conn: &mut Connection, mut reader: R) -> Result<()> 
    where R: DataReader + Send + 'static {
        // 1. Read the first batch to determine schema and column count.
        let first_batch = match reader.read_batch().await? {
            Some(b) => b,
            None => return Ok(()),
        };
        
        let num_columns = first_batch.num_columns();
        info!("Column Multiplexer: Detected {} columns. Opening {} concurrent QUIC streams.", num_columns, num_columns);

        // 2. Open N streams concurrently.
        let mut streams = Vec::new();
        for _ in 0..num_columns {
            let stream = conn.open_bidirectional_stream().await
                .map_err(|e| Error::new(ErrorKind::ConnectionFailed, "Failed to open bidirectional stream").with_source(e))?;
            streams.push(stream);
        }

        // 3. We will buffer all batches into memory for this prototype (to avoid locking reader)
        // In a production system we'd use a broadcast channel or iterators.
        let mut all_batches = vec![first_batch];
        while let Some(batch) = reader.read_batch().await? {
            all_batches.push(batch);
        }

        let mut tasks = tokio::task::JoinSet::new();
        let t_send = Instant::now();

        // 4. For each column, serialize and transmit independently.
        for (col_idx, stream) in streams.into_iter().enumerate() {
            let column_batches: Vec<_> = all_batches.iter().map(|b| b.project(&[col_idx]).unwrap()).collect();
            
            tasks.spawn(async move {
                let mut writer = crate::arrow::IpcStreamEncoder::new();
                for batch in column_batches {
                    writer.write_batch(&batch).unwrap();
                }
                let payload = writer.finish().unwrap();
                let len = payload.len();

                let (mut rx, mut tx) = stream.split();
                tx.send(bytes::Bytes::from(payload)).await.unwrap();
                tx.finish().unwrap();
                while let Ok(Some(_)) = rx.receive().await {}
                
                (col_idx, len)
            });
        }

        let mut total_bytes = 0;
        while let Some(res) = tasks.join_next().await {
            let (col_idx, len) = res.unwrap();
            info!("Stream {} finished transmitting {} bytes.", col_idx, len);
            total_bytes += len;
        }
        
        let t_send_elapsed = t_send.elapsed();
        let throughput_mbps = (total_bytes as f64 / 1_048_576.0) / t_send_elapsed.as_secs_f64();

        info!(
            "SENDER METRICS | Strategy: Column | Streams: {} | Total Bytes: {} | Send time: {:.3} ms | Throughput: {:.4} MB/s",
            num_columns,
            total_bytes,
            t_send_elapsed.as_secs_f64() * 1000.0,
            throughput_mbps
        );

        Ok(())
    }
}

use crate::traits::DynVeinWriterBuilder;

/// Receives Arrow data over a QUIC connection.
pub struct QuicReceiver {
    /// The multiplexing strategy to use.
    pub strategy: MultiplexStrategy,
}

impl QuicReceiver {
    /// Create a new QuicReceiver.
    pub fn new(strategy: MultiplexStrategy) -> Self {
        Self { strategy }
    }

    /// Handles incoming data streams for the connection based on the chosen strategy.
    pub async fn receive(&self, conn: &mut Connection, builder: DynVeinWriterBuilder) -> Result<()> {
        match self.strategy {
            MultiplexStrategy::Single | MultiplexStrategy::Column | MultiplexStrategy::Row => self.receive_concurrent(conn, builder).await,
        }
    }

    async fn receive_concurrent(&self, conn: &mut Connection, builder: DynVeinWriterBuilder) -> Result<()> {
        while let Ok(Some(mut stream)) = conn.accept_bidirectional_stream().await {
            let strategy_name = format!("{:?}", self.strategy);
            let builder = builder.clone();
            
            tokio::spawn(async move {
                let mut total_bytes_received = 0usize;
                let mut transfer_start: Option<Instant> = None;
                let mut decoder = crate::arrow::AsyncIpcDecoder::new();
                let mut total_rows = 0;
                
                // Build a new writer for this specific stream
                let mut writer = match builder.build().await {
                    Ok(w) => w,
                    Err(e) => {
                        tracing::error!("Failed to build VeinWriter: {}", e);
                        return;
                    }
                };
                
                while let Ok(Some(chunk)) = stream.receive().await {
                    let chunk_len = chunk.len();
                    if transfer_start.is_none() {
                        transfer_start = Some(Instant::now());
                    }
                    total_bytes_received += chunk_len;
                    
                    match decoder.push_chunk(chunk) {
                        Ok(batches) => {
                            for batch in batches {
                                total_rows += batch.num_rows();
                                if let Err(e) = writer.write(batch).await {
                                    tracing::error!("VeinWriter failed to process batch: {}", e);
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            info!("Error decoding stream: {}", e);
                            break;
                        }
                    }
                }
                
                if let Err(e) = writer.close().await {
                    tracing::error!("VeinWriter failed to close: {}", e);
                }

                let duration = transfer_start.unwrap_or(Instant::now()).elapsed();
                let throughput_mb = (total_bytes_received as f64 / 1_048_576.0) / duration.as_secs_f64();
                info!(
                    "RECEIVER METRICS | Strategy: {} | Bytes: {} | Rows: {} | Time: {:.3} ms | Throughput: {:.4} MB/s",
                    strategy_name,
                    total_bytes_received,
                    total_rows,
                    duration.as_secs_f64() * 1000.0,
                    throughput_mb
                );
            });
        }
        Ok(())
    }
}
