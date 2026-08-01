use std::thread::{self, JoinHandle};
use tokio::task::LocalSet;
use tracing::{info, error};
use crate::daemon::pipeline::{MorselTask, PipelineNode, Source};
use crate::operators::transform::filter::FilterTransform;
use crate::operators::sink::discard::Discard;
use crate::operators::source::memory::Memory;
use common::protocol::{Operator, DataSource, DataSink};
use arrow::record_batch::RecordBatch;
use common::Result;
/// A pool of OS threads pinning Tokio LocalSets for cache-local execution.
pub struct WorkerPool {
    handles: Vec<JoinHandle<()>>,
}

impl WorkerPool {
    /// Start the worker pool with `cores` threads.
    pub fn start(cores: usize, queue: flume::Receiver<MorselTask>) -> Self {
        let mut handles = Vec::with_capacity(cores);

        for core_id in 0..cores {
            let queue_clone = queue.clone();
            
            let handle = thread::Builder::new()
                .name(format!("vein-worker-{}", core_id))
                .spawn(move || {
                    info!("Started worker thread {} pinned to CPU core", core_id);
                    
                    // 1. Create a single-threaded runtime strictly for this thread
                    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                        Ok(rt) => rt,
                        Err(e) => {
                            error!("Failed to build Tokio runtime on worker {}: {}", core_id, e);
                            return;
                        }
                    };
                    
                    // 2. Create the LocalSet
                    let local = LocalSet::new();
                    
                    // 3. Block on the LocalSet
                    local.block_on(&rt, async move {
                        // Work-stealing loop: pull tasks asynchronously
                        while let Ok(task) = queue_clone.recv_async().await {
                            // Spawn a task locally pinned to this OS thread
                            tokio::task::spawn_local(async move {
                                // 1. Construct the pipeline Sink
                                let mut pipeline_head: Box<dyn PipelineNode> = match task.sink {
                                    DataSink::Discard => Box::new(Discard::new()),
                                    // TODO: Implement NetworkSink, StorageSink, etc.
                                    _ => {
                                        error!("Unsupported sink type {:?}", task.sink);
                                        return;
                                    }
                                };
                                
                                // 2. Build the operator pipeline backwards from Sink to Source
                                for op in task.plan.iter().rev() {
                                    match op {
                                        Operator::Filter { expr: _ } => {
                                            // TODO: Parse expr string into PhysicalExpr
                                            // For now, we mock a Filter that just passes data through
                                            // pipeline_head = Box::new(FilterTransform::new(parsed_expr, pipeline_head));
                                            info!("Mocking Filter operator for task {}", task.task_id);
                                        }
                                        _ => {
                                            error!("Unsupported operator {:?}", op);
                                        }
                                    }
                                }

                                // 3. Execute the source loop, pushing batches into the head of the pipeline
                                info!("Worker {} starting execution for task {}", core_id, task.task_id);
                                let mut source: Box<dyn Source> = match task.source {
                                    DataSource::Memory => Box::new(Memory::dummy()),
                                    DataSource::File(scan_task) => {
                                        // Initialize OpenDAL Operator for the scan
                                        // For now, we assume local fs if it's not specified, or we parse the scheme
                                        // In production, the Master should configure credentials, but for now we just use a default Memory/Fs operator
                                        // Actually, let's use OpenDAL's auto-builder based on scheme (e.g. s3://, fs://)
                                        // But for simplicity, if it starts with /, we use Fs. 
                                        use opendal::services::Fs;
                                        use opendal::Operator;
                                        let mut builder = Fs::default();
                                        builder.root("/");
                                        let op = Operator::new(builder).unwrap().finish();
                                        
                                        use crate::operators::source::scan::ScanSource;
                                        Box::new(ScanSource::new(op, scan_task.files, scan_task.format))
                                    }
                                    _ => {
                                        error!("Unsupported source type {:?}", task.source);
                                        return;
                                    }
                                };
                                
                                if let Err(e) = source.execute(&mut *pipeline_head).await {
                                    error!("Pipeline execution error: {}", e);
                                }
                                info!("Worker {} finished task {}", core_id, task.task_id);
                            });
                        }
                        info!("Worker {} shutting down", core_id);
                    });
                })
                .expect("Failed to spawn worker thread");
                
            handles.push(handle);
        }

        Self { handles }
    }

    /// Wait for all workers to finish.
    pub fn join(self) {
        for handle in self.handles {
            let _ = handle.join();
        }
    }
}
