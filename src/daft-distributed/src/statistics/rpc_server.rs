use common_error::DaftResult;
use common_metrics::RpcPayload;
use common_py_serde::bincode;
use tokio::{net::TcpListener, sync::mpsc};
use tracing::{error, info, warn};

use super::{StatisticsEvent, StatisticsManagerRef};

const RPC_SERVER_LOG_TARGET: &str = "DaftRpcServer";

/// RPC server that receives runtime metrics from worker nodes
pub struct RpcServer {
    statistics_manager: StatisticsManagerRef,
    server_handle: Option<tokio::task::JoinHandle<()>>,
    shutdown_tx: Option<mpsc::UnboundedSender<()>>,
}

impl RpcServer {
    /// Creates a new RPC server that will forward metrics to the statistics manager
    pub fn new(statistics_manager: StatisticsManagerRef) -> Self {
        Self {
            statistics_manager,
            server_handle: None,
            shutdown_tx: None,
        }
    }

    /// Starts the RPC server on the specified address
    pub async fn start(&mut self, addr: &str) -> DaftResult<()> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| common_error::DaftError::MiscTransient(Box::new(e)))?;

        info!(target: RPC_SERVER_LOG_TARGET, "RPC server starting on {}", addr);

        let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel::<()>();
        self.shutdown_tx = Some(shutdown_tx);

        let statistics_manager = self.statistics_manager.clone();

        let server_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Handle shutdown signal
                    _ = shutdown_rx.recv() => {
                        info!(target: RPC_SERVER_LOG_TARGET, "RPC server shutting down");
                        break;
                    }
                    // Handle incoming connections
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, addr)) => {
                                info!(target: RPC_SERVER_LOG_TARGET, "New connection from {}", addr);

                                let statistics_manager = statistics_manager.clone();

                                tokio::spawn(async move {
                                    if let Err(e) = Self::handle_connection(
                                        stream,
                                        addr.to_string(),
                                        statistics_manager,
                                    ).await {
                                        error!(target: RPC_SERVER_LOG_TARGET,
                                            "Error handling connection from {}: {}", addr, e);
                                    }
                                });
                            }
                            Err(e) => {
                                error!(target: RPC_SERVER_LOG_TARGET, "Failed to accept connection: {}", e);
                            }
                        }
                    }
                }
            }
        });

        self.server_handle = Some(server_handle);
        Ok(())
    }

    /// Handles an individual connection from a worker node
    async fn handle_connection(
        mut stream: tokio::net::TcpStream,
        peer_addr: String,
        statistics_manager: StatisticsManagerRef,
    ) -> DaftResult<()> {
        use tokio::io::AsyncReadExt;

        loop {
            // Read content length first (4 bytes)
            let mut len_bytes = [0u8; 4];
            match stream.read_exact(&mut len_bytes).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    info!(target: RPC_SERVER_LOG_TARGET, "Connection from {} closed", peer_addr);
                    break;
                }
                Err(e) => {
                    return Err(common_error::DaftError::MiscTransient(Box::new(e)));
                }
            }

            let content_length = u32::from_be_bytes(len_bytes) as usize;

            // Reasonable size limit to prevent memory issues
            if content_length > 10 * 1024 * 1024 {
                return Err(common_error::DaftError::MiscTransient(Box::new(
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Content length too large",
                    ),
                )));
            }

            // Read and process the payload in a scope that handles the lifetime properly
            {
                let mut buffer = vec![0u8; content_length];
                stream
                    .read_exact(&mut buffer)
                    .await
                    .map_err(|e| common_error::DaftError::MiscTransient(Box::new(e)))?;

                // Deserialize directly from owned buffer and handle immediately
                match bincode::deserialize::<RpcPayload>(&buffer) {
                    Ok(payload) => {
                        info!(target: RPC_SERVER_LOG_TARGET,
                            "Received metrics payload from {} for plan_id: {}", peer_addr, payload.0.plan_id);

                        // Convert runtime metrics to statistics events and forward them
                        if let Err(e) =
                            Self::process_metrics_payload(&statistics_manager, payload).await
                        {
                            warn!(target: RPC_SERVER_LOG_TARGET,
                                "Failed to process metrics payload: {}", e);
                        }
                    }
                    Err(e) => {
                        warn!(target: RPC_SERVER_LOG_TARGET,
                            "Failed to deserialize payload from {}: {}", peer_addr, e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Processes received metrics and forwards them to subscribers
    async fn process_metrics_payload(
        statistics_manager: &StatisticsManagerRef,
        payload: RpcPayload,
    ) -> DaftResult<()> {
        let (local_physical_node_metrics, snapshot) = payload;

        // Use the network snapshot directly with owned strings
        let event = StatisticsEvent::LocalPhysicalNodeMetrics {
            plan_id: local_physical_node_metrics.plan_id,
            stage_id: local_physical_node_metrics.stage_id,
            task_id: local_physical_node_metrics.task_id,
            logical_node_id: local_physical_node_metrics.logical_node_id,
            local_physical_node_type: local_physical_node_metrics.local_physical_node_type,
            distributed_physical_node_type: local_physical_node_metrics
                .distributed_physical_node_type,
            snapshot,
        };

        statistics_manager.handle_event(event)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn shutdown(&mut self) -> DaftResult<()> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        if let Some(server_handle) = self.server_handle.take() {
            server_handle
                .await
                .map_err(|e| common_error::DaftError::MiscTransient(Box::new(e)))?;
        }

        Ok(())
    }
}

impl Drop for RpcServer {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
    }
}
