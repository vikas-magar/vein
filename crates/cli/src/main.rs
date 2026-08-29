pub mod daemon;
pub mod engine;
use clap::{Parser, Subcommand};
use exec::ExecutorNode;
use std::sync::Arc;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[derive(Parser, Debug)]
#[command(name = "vein-server")]
#[command(about = "Vein Distributed Compute Node")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start a Master node (Query Coordinator)
    Master {
        #[arg(short, long, default_value = "0.0.0.0:8080")]
        bind: String,
        #[arg(short, long, default_value = "/Users/anvi/Next/vein/certs/server.crt")]
        cert: String,
        #[arg(short, long, default_value = "/Users/anvi/Next/vein/certs/server.key")]
        key: String,
    },
    /// Start an Executor node (Compute Daemon)
    Executor {
        #[arg(short, long, default_value = "0.0.0.0:8081")]
        bind: String,
        #[arg(short, long, default_value = "127.0.0.1:8080")]
        master: String,
        #[arg(short, long, default_value = "/Users/anvi/Next/vein/certs/server.crt")]
        cert: String,
        #[arg(short, long, default_value = "/Users/anvi/Next/vein/certs/server.key")]
        key: String,
    },
    /// Run a full local cluster (Master + Executor) in a single process
    Local,
    TestQuery {
        #[arg(short, long, default_value = "127.0.0.1:8081")]
        executor: String,
        #[arg(short, long, default_value = "/certs/server.crt")]
        cert: String,
        /// The Server Name Indication (SNI) for TLS verification
        #[arg(short, long, default_value = "localhost")]
        server_name: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    match cli.command {
        Commands::Master { bind, cert, key } => {
            tracing::info!("Starting Vein Master node on {} (TLS: {})", bind, cert);

            let registry = Arc::new(orchestrator::executor::registry::ExecutorRegistry::new());
            let dispatcher =
                Arc::new(orchestrator::scheduler::quic_dispatcher::QuicTaskDispatcher::new());
            let scheduler = Arc::new(orchestrator::scheduler::Scheduler::new(
                registry.clone(),
                dispatcher,
                3,                                  // max_retries
                std::time::Duration::from_secs(30), // timeout
            ));

            let coordinator = Arc::new(
                engine::DistributedCoordinator::builder()
                    .with_binder(Arc::new(binder::QueryBinder::new(
                        Arc::new(vcs::InMemoryCatalog::new()),
                        Arc::new(binder::testing::MockFunctionRegistry::new()),
                    )))
                    .with_optimizer(Arc::new(optimizer::OptimizerPipeline::production()))
                    .with_planner(Arc::new(physical::DagPhysicalPlanner::new()))
                    .with_scheduler(scheduler)
                    .build()
                    .unwrap(),
            );

            // 2. Start the Master Daemon
            let addr: std::net::SocketAddr = bind.parse()?;
            let daemon_server = daemon::MasterDaemon::new(coordinator);

            tokio::spawn(async move {
                if let Err(e) = daemon_server.run(addr, &cert, &key).await {
                    tracing::error!("Master Daemon crashed: {}", e);
                }
            });

            tokio::signal::ctrl_c().await?;
            tracing::info!("Shutting down Master node");
        }
        Commands::Executor {
            bind,
            master,
            cert,
            key,
        } => {
            let addr: std::net::SocketAddr = bind.parse()?;
            ExecutorNode::builder()
                .bind(addr)
                .certificates(&cert, &key)
                .workers(4)
                .master(&master)
                .build()?
                .start()
                .await?;
        }
        Commands::Local => {
            tracing::info!("Starting Vein Local Cluster (Master + Executor)");
            // TODO: Start both concurrently with an in-memory or loopback QUIC transport
            tokio::signal::ctrl_c().await?;
            tracing::info!("Shutting down Local Cluster");
        }
        Commands::TestQuery {
            executor,
            cert,
            server_name,
        } => {
            tracing::info!(
                "Sending test query to Executor at {} (SNI: {})",
                executor,
                server_name
            );
            let addr: std::net::SocketAddr = executor.parse()?;

            let client = common::transport::build_quic_client(&cert)?;
            let mut conn = client
                .connect(
                    s2n_quic::client::Connect::new(addr).with_server_name(server_name.as_str()),
                )
                .await?;
            conn.keep_alive(true)?;

            let mut stream = conn.open_bidirectional_stream().await?;

            let task = common::protocol::ExecuteTask {
                mode: common::protocol::ExecutionMode::Batch,
                task_id: common::identifiers::TaskId::new(),
                query_id: common::identifiers::QueryId::new(),
                stage_id: common::identifiers::StageId::new(),
                source: common::protocol::DataSource::Memory,
                operators: vec![common::protocol::Operator::Filter(
                    common::protocol::FilterPredicate {
                        column: 0,
                        op: common::protocol::FilterOp::Gt,
                        literal: common::protocol::FilterValue::Int(0),
                    },
                )],
                sink: common::protocol::DataSink::Discard,
            };

            let event = common::protocol::VeinControlEvent::DispatchTask(task);

            tracing::info!("Serializing and sending DispatchTask event over QUIC");
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event)
                .map_err(|e| format!("rkyv error: {}", e))?;

            stream.send(bytes::Bytes::from(bytes.into_vec())).await?;

            // Explicitly drop the stream so the QUIC FIN packet is sent instantly.
            // This prevents the Executor from waiting for more bytes!
            drop(stream);

            tracing::info!("Query sent successfully!");

            // Wait a tiny bit just to ensure the OS flushes the network buffer before exit
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    Ok(())
}
