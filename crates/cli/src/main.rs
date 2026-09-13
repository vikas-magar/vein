pub mod daemon;
pub mod engine;

use clap::{Parser, Subcommand};
use exec::ExecutorDaemon;
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
    Local {
        #[arg(long, default_value = "127.0.0.1:8080")]
        master_bind: String,
        #[arg(long, default_value = "127.0.0.1:8081")]
        executor_bind: String,
        #[arg(short, long, default_value = "certs/server.crt")]
        cert: String,
        #[arg(short, long, default_value = "certs/server.key")]
        key: String,
        #[arg(short, long, default_value_t = 4)]
        workers: usize,
    },
}

fn build_default_coordinator() -> Result<Arc<engine::DistributedCoordinator>, String> {
    debug!(target: "vein_server", "Initializing Master subsystems and Scheduler");
    let registry = Arc::new(orchestrator::executor::registry::ExecutorRegistry::new());
    let dispatcher =
        Arc::new(orchestrator::scheduler::quic_dispatcher::QuicTaskDispatcher::new());
    let scheduler = Arc::new(orchestrator::scheduler::Scheduler::new(
        registry,
        dispatcher,
        3,                                  // max_retries
        std::time::Duration::from_secs(30), // timeout
    ));

    debug!(target: "vein_server", "Building DistributedCoordinator with default subsystems");
    let coordinator = engine::DistributedCoordinator::builder()
        .with_binder(Arc::new(binder::QueryBinder::new(
            Arc::new(vcs::InMemoryCatalog::new()),
            Arc::new(binder::testing::MockFunctionRegistry::new()),
        )))
        .with_optimizer(Arc::new(optimizer::OptimizerPipeline::production()))
        .with_planner(Arc::new(physical::DefaultPhysicalPlanner::new()))
        .with_scheduler(scheduler)
        .build()
        .map_err(|e| format!("Failed to build DistributedCoordinator: {e}"))?;

    Ok(Arc::new(coordinator))
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

            let coordinator = build_default_coordinator()?;

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

            ExecutorDaemon::builder()
                .bind(addr)
                .certificates(&cert, &key)
                .workers(4)
                .master(&master)
                .build()?
                .start()
                .await?;
        }
        Commands::Local {
            master_bind,
            executor_bind,
            cert,
            key,
            workers,
        } => {
            info!(
                target: "vein_server",
                %master_bind,
                %executor_bind,
                "Starting Vein Local Cluster (Master + Executor)"
            );

            let m_addr: std::net::SocketAddr = master_bind
                .parse()
                .map_err(|e| format!("Invalid master bind address '{master_bind}': {e}"))?;
            let e_addr: std::net::SocketAddr = executor_bind
                .parse()
                .map_err(|e| format!("Invalid executor bind address '{executor_bind}': {e}"))?;

            let coordinator = build_default_coordinator()?;
            let master_daemon = daemon::MasterDaemon::new(coordinator);
            let cert_m = cert.clone();
            let key_m = key.clone();
            tokio::spawn(async move {
                if let Err(e) = master_daemon.run(m_addr, &cert_m, &key_m).await {
                    error!(target: "vein_server", error = %e, "Local Master Daemon crashed");
                }
            });

            // Start the Local Executor Daemon
            let cert_e = cert.clone();
            let key_e = key.clone();
            let master_target = master_bind.clone();
            tokio::spawn(async move {
                let daemon = match ExecutorDaemon::builder()
                    .bind(e_addr)
                    .certificates(&cert_e, &key_e)
                    .workers(workers)
                    .master(&master_target)
                    .build()
                {
                    Ok(d) => d,
                    Err(e) => {
                        error!(target: "vein_server", error = %e, "Failed to build Local ExecutorDaemon");
                        return;
                    }
                };

                if let Err(e) = daemon.start().await {
                    error!(target: "vein_server", error = %e, "Local Executor Daemon crashed");
                }
            });

            info!(
                target: "vein_server",
                "Vein Local Cluster running (Master: {master_bind}, Executor: {executor_bind})"
            );
            tokio::signal::ctrl_c().await?;
            info!(target: "vein_server", "Shutting down Local Cluster");
        }
    }

    Ok(())
}
