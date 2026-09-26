pub mod daemon;
pub mod engine;

use clap::{Parser, Subcommand};
use common::logging;
use distributed::{
    DurableRaftStorage, OpenRaftMetadataWriter, OpenRaftRpcHandler, QuicRaftNetworkFactory,
    start_raft_node,
};
use exec::ExecutorDaemon;
use openraft::{BasicNode, Config as RaftConfig};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::{debug, error, info};

#[derive(Parser, Debug)]
#[command(name = "vein-server")]
#[command(about = "Vein Distributed Compute Node")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the VCS metadata authority
    Vcs {
        #[arg(short, long, default_value = "0.0.0.0:9090")]
        bind: String,
        #[arg(long, default_value = "file://./vein_data/vcs-warehouse")]
        warehouse: String,
        #[arg(short, long, default_value = "certs/server.crt")]
        cert: String,
        #[arg(short, long, default_value = "certs/server.key")]
        key: String,
        /// Stable OpenRaft node identifier.
        #[arg(long, default_value_t = 1)]
        raft_id: u64,
        /// Address advertised to other VCS nodes. Defaults to the bind address.
        #[arg(long)]
        raft_advertise: Option<String>,
        /// Path for durable OpenRaft metadata and log state.
        #[arg(long, default_value = "./vein_data/vcs-raft.json")]
        raft_state: String,
        /// Bootstrap a brand-new single-node Raft group.
        #[arg(long, default_value_t = false)]
        raft_bootstrap: bool,
    },
    /// Start a Master node (Query Coordinator)
    Master {
        #[arg(short, long, default_value = "0.0.0.0:8080")]
        bind: String,
        #[arg(short, long, default_value = "certs/server.crt")]
        cert: String,
        #[arg(short, long, default_value = "certs/server.key")]
        key: String,
        /// Remote VCS catalog endpoint used by production master mode.
        #[arg(long)]
        vcs: String,
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
        /// Remote VCS metadata authority. Required for production executors.
        #[arg(long)]
        vcs: String,
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
        /// Remote Raft-backed VCS metadata authority.
        #[arg(long, default_value = "127.0.0.1:9090")]
        vcs: String,
        #[arg(short, long, default_value_t = 4)]
        workers: usize,
    },
}

async fn build_default_coordinator(
    vcs_endpoint: Option<&str>,
    ca_cert: &str,
) -> Result<Arc<engine::DistributedCoordinator>, String> {
    debug!(target: "vein_server", "Initializing Master subsystems and Scheduler");
    let registry = Arc::new(orchestrator::executor::registry::ExecutorRegistry::new());
    let dispatcher = Arc::new(orchestrator::scheduler::quic_dispatcher::QuicTaskDispatcher::new());
    let scheduler = Arc::new(orchestrator::scheduler::Scheduler::new(
        registry,
        dispatcher,
        3,
        std::time::Duration::from_secs(30),
    ));

    debug!(target: "vein_server", "Building DistributedCoordinator with catalog authority");
    let optimizer = Arc::new(optimizer::OptimizerPipeline::production());
    let planner = Arc::new(physical::DefaultPhysicalPlanner::new());

    let coordinator = if let Some(endpoint) = vcs_endpoint {
        let catalog = Arc::new(
            vcs::RemoteVcsCatalog::connect(endpoint, ca_cert)
                .map_err(|error| format!("Failed to connect to remote VCS catalog: {error}"))?,
        );
        let revision = catalog
            .health_check()
            .await
            .map_err(|error| format!("Remote VCS health check failed: {error}"))?;
        info!(target: "vein_server", vcs = %catalog.target(), catalog_revision = %revision.as_str(), "Remote VCS authority validated");
        engine::DistributedCoordinator::builder()
            .with_binder(Arc::new(binder::QueryBinder::new(
                catalog.clone(),
                Arc::new(binder::testing::MockFunctionRegistry::new()),
            )))
            .with_catalog_writer(catalog.clone())
            .with_table_write_catalog(catalog)
            .with_optimizer(optimizer)
            .with_planner(planner)
            .with_scheduler(scheduler)
            .build()
            .map_err(|e| format!("Failed to build DistributedCoordinator: {e}"))?
    } else {
        return Err(
            "A remote Raft-backed VCS endpoint is required; start the VCS service and pass --vcs"
                .to_string(),
        );
    };

    Ok(Arc::new(coordinator))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    logging::init("vein-server")?;

    let cli = Cli::parse();

    match cli.command {
        Commands::Vcs {
            bind,
            warehouse,
            cert,
            key,
            raft_id,
            raft_advertise,
            raft_state,
            raft_bootstrap,
        } => {
            info!(target: "vein_server", %bind, %warehouse, raft_id, %raft_state, raft_bootstrap, "Starting Vein VCS authority");
            let addr: std::net::SocketAddr = bind
                .parse()
                .map_err(|e| format!("Invalid VCS bind address '{bind}': {e}"))?;
            let advertise = raft_advertise.unwrap_or_else(|| bind.clone());
            let raft_config = Arc::new(
                RaftConfig {
                    cluster_name: "vein-vcs".to_string(),
                    ..Default::default()
                }
                .validate()
                .map_err(|e| format!("Invalid VCS Raft configuration: {e}"))?,
            );
            let raft_client = Arc::new(common::transport::QuicClient::new(
                common::transport::build_quic_client(&cert)?,
            ));
            let raft_network = QuicRaftNetworkFactory::new(raft_client);
            let raft_storage = DurableRaftStorage::open(&raft_state)?;
            let raft_view_storage = raft_storage.clone();
            let raft =
                Arc::new(start_raft_node(raft_id, raft_config, raft_network, raft_storage).await?);
            let raft_initialized = raft.is_initialized().await?;
            if raft_bootstrap && !raft_initialized {
                let mut members = BTreeMap::new();
                members.insert(raft_id, BasicNode::new(advertise));
                raft.initialize(members).await?;
                info!(
                    target: "vein.audit",
                    raft_id,
                    "Raft cluster bootstrapped"
                );
            } else if raft_bootstrap && raft_initialized {
                info!(
                    target: "vein.audit",
                    raft_id,
                    "Raft cluster already initialized; skipping bootstrap"
                );
            } else if !raft_initialized {
                return Err("Raft state is uninitialized; start once with --raft-bootstrap".into());
            }
            let writer = Arc::new(OpenRaftMetadataWriter::new(raft.clone(), raft_view_storage));
            let catalog = Arc::new(vcs::VcsCatalog::new(writer).await?);
            let raft_handler = Arc::new(OpenRaftRpcHandler::new(raft));
            vcs::VcsExchangeServer::new(catalog.shared_exchange_catalog())
                .with_raft_rpc_handler(raft_handler)
                .with_catalog_authority(catalog.clone(), catalog.clone(), catalog)
                .run(addr, &cert, &key)
                .await?;
        }
        Commands::Master {
            bind,
            cert,
            key,
            vcs,
        } => {
            info!(target: "vein_server", %bind, %cert, "Starting Vein Master node");

            let coordinator = build_default_coordinator(Some(&vcs), &cert).await?;

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
            vcs,
        } => {
            info!(target: "vein_server", %bind, %master, %cert, "Starting Vein Executor node");
            let addr: std::net::SocketAddr = bind
                .parse()
                .map_err(|e| format!("Invalid bind address '{bind}': {e}"))?;

            let data_root = std::env::var("VEIN_DATA_DIR")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|_| std::path::PathBuf::from("./vein_data"));
            let executor_id = bind.replace(':', "_").replace('/', "_");
            let vfs_config = vfs::VfsConfig::for_executor(&data_root, &executor_id);
            let _vfs_store = vfs::VfsTableStore::shared_for_config(&vfs_config)?;

            let mut builder = ExecutorDaemon::builder()
                .bind(addr)
                .certificates(&cert, &key)
                .workers(4)
                .master(&master);
            builder = builder.vcs_exchange(&vcs, &cert);
            builder.build()?.start().await?;
        }
        Commands::Local {
            master_bind,
            executor_bind,
            cert,
            key,
            vcs,
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

            let coordinator = build_default_coordinator(Some(&vcs), &cert).await?;
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
                    .local_development()
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
