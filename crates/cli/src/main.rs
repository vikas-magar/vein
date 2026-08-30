pub mod daemon;
pub mod engine;

use clap::{Parser, Subcommand};
use exec::ExecutorNode;
use std::sync::Arc;
use tracing::{debug, error, info};
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
        #[arg(short, long, default_value = "certs/server.crt")]
        cert: String,
        #[arg(short, long, default_value = "certs/server.key")]
        key: String,
    },
    /// Start an Executor node (Compute Daemon)
    Executor {
        #[arg(short, long, default_value = "0.0.0.0:8081")]
        bind: String,
        #[arg(short, long, default_value = "127.0.0.1:8080")]
        master: String,
        #[arg(short, long, default_value = "certs/server.crt")]
        cert: String,
        #[arg(short, long, default_value = "certs/server.key")]
        key: String,
    },
    /// Run a full local cluster (Master + Executor) in a single process
    Local,
    /// Send a test task directly to an Executor node over QUIC
    TestQuery {
        #[arg(short, long, default_value = "127.0.0.1:8081")]
        executor: String,
        #[arg(short, long, default_value = "certs/server.crt")]
        cert: String,
        /// The Server Name Indication (SNI) for TLS verification
        #[arg(short, long, default_value = "localhost")]
        server_name: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(filter)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    match cli.command {
        Commands::Master { bind, cert, key } => {
            info!(target: "vein_server", %bind, %cert, "Starting Vein Master node");

            debug!(target: "vein_server", "Initializing ExecutorRegistry and Scheduler");
            let registry = Arc::new(orchestrator::executor::registry::ExecutorRegistry::new());
            let dispatcher =
                Arc::new(orchestrator::scheduler::quic_dispatcher::QuicTaskDispatcher::new());
            let scheduler = Arc::new(orchestrator::scheduler::Scheduler::new(
                registry.clone(),
                dispatcher,
                3,                                  // max_retries
                std::time::Duration::from_secs(30), // timeout
            ));

            debug!(target: "vein_server", "Building DistributedCoordinator with default subsystems");
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
                    .map_err(|e| format!("Failed to build DistributedCoordinator: {e}"))?,
            );

            // Start the Master Daemon
            let addr: std::net::SocketAddr = bind
                .parse()
                .map_err(|e| format!("Invalid bind address '{bind}': {e}"))?;
            let daemon_server = daemon::MasterDaemon::new(coordinator);

            let cert_clone = cert.clone();
            let key_clone = key.clone();
            tokio::spawn(async move {
                if let Err(e) = daemon_server.run(addr, &cert_clone, &key_clone).await {
                    error!(target: "vein_server", error = %e, "Master Daemon crashed");
                }
            });

            tokio::signal::ctrl_c().await?;
            info!(target: "vein_server", "Received shutdown signal; shutting down Master node");
        }
        Commands::Executor {
            bind,
            master,
            cert,
            key,
        } => {
            info!(target: "vein_server", %bind, %master, %cert, "Starting Vein Executor node");
            let addr: std::net::SocketAddr = bind
                .parse()
                .map_err(|e| format!("Invalid bind address '{bind}': {e}"))?;

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
            info!(target: "vein_server", "Starting Vein Local Cluster (Master + Executor)");
            tokio::signal::ctrl_c().await?;
            info!(target: "vein_server", "Shutting down Local Cluster");
        }
        Commands::TestQuery {
            executor,
            cert,
            server_name,
        } => {
            info!(
                target: "vein_test",
                %executor,
                %server_name,
                "Dispatching test query task directly to Executor"
            );
            let addr: std::net::SocketAddr = executor
                .parse()
                .map_err(|e| format!("Invalid executor address '{executor}': {e}"))?;

            debug!(target: "vein_test", %cert, "Loading CA certificate for QUIC connection");
            let client = common::transport::build_quic_client(&cert)?;
            let mut conn = client
                .connect(
                    s2n_quic::client::Connect::new(addr).with_server_name(server_name.as_str()),
                )
                .await
                .map_err(|e| format!("Failed to connect to Executor at {addr}: {e}"))?;
            conn.keep_alive(true)
                .map_err(|e| format!("Failed to set keep_alive: {e}"))?;

            debug!(target: "vein_test", "Opening bidirectional stream to Executor");
            let mut stream = conn
                .open_bidirectional_stream()
                .await
                .map_err(|e| format!("Failed to open bidirectional stream: {e}"))?;

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

            debug!(target: "vein_test", "Serializing DispatchTask event via rkyv");
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event)
                .map_err(|e| format!("rkyv serialization error: {e}"))?;

            info!(target: "vein_test", bytes_len = bytes.len(), "Sending DispatchTask payload over QUIC");
            stream.send(bytes::Bytes::from(bytes.into_vec())).await
                .map_err(|e| format!("Failed to send task bytes: {e}"))?;

            // Explicitly drop the stream so the QUIC FIN packet is sent instantly
            drop(stream);

            info!(target: "vein_test", "Test query task sent successfully");

            // Flush network buffer
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    Ok(())
}
