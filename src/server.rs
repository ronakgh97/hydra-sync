use crate::protocol::{Role, perform_server_handshake, read_join_frame, read_raw_frame_into};
use crate::session::Sessions;
use crate::{BUFFER_SIZE, error, info, trace, warn};
use anyhow::Result;
use bytes::BytesMut;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::error::RecvError;

/// A light-weight multi-threaded SPMC (Single Producer Multiple Consumer) E2E relay server.
///
/// `HydraServer` implements a **minimal-copy** [tokio::broadcast](https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html) relay that:
/// - Accepts one producer and multiple consumers per concurrent sessions
/// - Routes data from producer → all connected consumers using `Arc<Bytes>`
/// - Handles slow clients smartly using [`OverflowChannelMode`](crate::channel::OverflowChannelMode)
/// - Enforces connection limits and per-payload size constraints
///
/// ```no_run
/// use hydra_sync::server::HydraServer;
///
/// #[tokio::main]
/// async fn main() {
/// // choose an OS-assigned port
///     let (server, addr) = HydraServer::bind_default().await.unwrap();
///
///     println!("Server running on: {}", addr);
///     tokio::spawn(async move{ server.run(500).await });
/// }
/// ```
/// Internals
/// - Producer: Sends encrypted frames → broadcast channel
/// - Consumers: Subscribe to broadcast, receive clones of `Arc<Bytes>` (zero-copy)
/// - Sessions: Keyed by 64-byte session_id, one producer per session allowed
/// - Errors & Logs: Error are predictable and handled gracefully by closing connections and logging without crashing the server
///
pub struct HydraServer {
    /// internal tcp listener for accepting incoming connections
    listener: TcpListener,
    /// session management for producers and consumers
    sessions: Arc<Sessions>,
    /// atomic counter to track active connections for enforcing limits
    connections: Arc<AtomicUsize>,
    /// maximum concurrent connections allowed to prevent resource exhaustion
    max_connections: usize,
    /// maximum allowed payload size for incoming frames to prevent abuse
    max_payload_length: usize,
    /// capacity of the broadcast channel for each session to handle backpressure
    channel_capacity: usize,
}

impl HydraServer {
    /// Binds the relay server with defaults
    /// - addr: OS-assigned port
    /// - max_connections: 32
    /// - max_payload_length: 64 MiB
    /// - channel_capacity: 256 messages per consumer
    pub async fn bind_default() -> Result<(Self, SocketAddr)> {
        let addr = &"127.0.0.1:0".parse::<SocketAddr>()?;
        let server = HydraServer::bind(addr, 64 * 1024 * 1024, 32, 256).await?;
        let local_addr = server.listener.local_addr()?;
        Ok((server, local_addr))
    }

    /// Binds the relay server to the specified socket address and initializes internal state
    pub async fn bind(
        addr: &SocketAddr,
        max_payload_length: usize,
        max_connections: usize,
        channel_capacity: usize,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            sessions: Arc::new(Sessions::init()),
            connections: Arc::new(AtomicUsize::new(0)),
            max_payload_length,
            max_connections,
            channel_capacity,
        })
    }

    /// Main server loop to accept incoming connections, spawn thread handlers, perform handshakes & session creation
    /// - `accept_timeout_ms` is the delay before client retries to accept new connections on server when the limit is reached
    /// - Producer errors; If read fails from client or broadcast send fails, the connection is closed and the error is logged.
    /// - Producer errors; If writing to client fails or broadcast lags or closed, the connection is closed and the error is logged.
    /// - EOF check are gracefully handled by closing the connection without logging an error.
    /// - `LOG_LEVEL` & `LOG_FILE` env vars can be set to control logging verbosity and output file (defaults to `info` level and stdout, not file).
    pub async fn run(self, accept_timeout_ms: u64) -> Result<()> {
        loop {
            if self.connections.fetch_add(1, Ordering::Relaxed) >= self.max_connections {
                self.connections.fetch_sub(1, Ordering::Relaxed);
                warn!(
                    "Max connections reached: {}, waiting {} ms before retrying",
                    self.max_connections, accept_timeout_ms
                );
                tokio::time::sleep(Duration::from_millis(accept_timeout_ms)).await;
                continue;
            }

            match self.listener.accept().await {
                Ok((stream, peer_addr)) => {
                    stream.set_nodelay(true).ok();
                    let sessions = Arc::clone(&self.sessions);
                    let connections = Arc::clone(&self.connections);
                    // spawn handler thread
                    tokio::spawn(async move {
                        trace!("Accepted connection from: {}", peer_addr);
                        if let Err(e) = Self::handle_connection(
                            stream,
                            sessions,
                            self.max_payload_length,
                            self.channel_capacity,
                        )
                        .await
                        {
                            error!("Connection handling error: {} from: {}", e, peer_addr);
                        }
                        connections.fetch_sub(1, Ordering::Release);
                    });
                }
                Err(e) => {
                    self.connections.fetch_sub(1, Ordering::Release);
                    error!("Connection accepting error: {}", e);
                }
            }
        }
    }

    /// Handles an individual client connection, performing handshake, role determination, and routing to producer/consumer handlers
    async fn handle_connection(
        mut stream: TcpStream,
        sessions: Arc<Sessions>,
        max_payload_length: usize,
        channel_capacity: usize,
    ) -> Result<()> {
        stream.set_nodelay(true)?;
        let mut mem_pool = BytesMut::with_capacity(max_payload_length + 4); // 4 bytes prefix space
        let peer_addr = stream.peer_addr()?;
        let (read_h, mut writer_raw) = stream.split();
        let mut reader = BufReader::with_capacity(BUFFER_SIZE, read_h);

        let transport_key = perform_server_handshake(&mut reader, &mut writer_raw).await?;
        let (role, session_id) =
            read_join_frame(&mut reader, &transport_key, &mut mem_pool).await?;

        match role {
            Role::Producer => {
                info!(
                    "Producer addr: {} joined session: {}",
                    peer_addr,
                    hex::encode(session_id)
                );
                Self::run_producer(
                    &mut reader,
                    sessions,
                    session_id,
                    &peer_addr,
                    &mut mem_pool,
                    max_payload_length,
                    channel_capacity,
                )
                .await
            }
            Role::Consumer => {
                info!(
                    "Consumer addr: {} joined session: {}",
                    peer_addr,
                    hex::encode(session_id)
                );
                Self::run_consumer(
                    &mut reader,
                    &mut writer_raw,
                    sessions,
                    session_id,
                    &peer_addr,
                )
                .await
            }
            Role::Admin => Ok(()), // TODO; implement this
        }
    }

    /// Handles producer clients: reads encrypted frames, decrypts, and broadcasts to consumers via the session's broadcast channel
    async fn run_producer<R: AsyncReadExt + Unpin>(
        reader: &mut R,
        sessions: Arc<Sessions>,
        session_id: [u8; 64],
        client_addr: &SocketAddr,
        mem_pool: &mut BytesMut,
        max_payload_length: usize,
        channel_capacity: usize,
    ) -> Result<()> {
        let tx = sessions.try_register_producer(session_id, channel_capacity)?;

        const MAX_WAIT_MS: u64 = 22;

        loop {
            // read from client read stream (just channel, no intervention)
            let data_len = match read_raw_frame_into(reader, mem_pool, max_payload_length).await {
                Ok(n) => n,
                Err(e) => {
                    tx.closed().await;
                    error!(
                        "Producer addr: {} session: {} read: {e}",
                        client_addr,
                        hex::encode(session_id)
                    );
                    break;
                }
            };
            let send_frame = mem_pool.split_to(data_len).freeze();

            // TODO; change this!!!
            tokio::time::sleep(Duration::from_millis(
                MAX_WAIT_MS * (send_frame.len() / channel_capacity) as u64,
            ))
            .await;

            // write to broadcast channel
            if let Err(e) = tx.send(send_frame) {
                tx.closed().await; // close channel to signal consumers
                warn!(
                    "Producer addr: {} session: {} broadcast: {e}",
                    client_addr,
                    hex::encode(session_id)
                );
                break;
            }
        }

        // clean up
        sessions.unregister_producer(session_id);
        Ok(())
    }

    /// Handles consumer clients: subscribes to the session's broadcast channel and writes received data to the client
    async fn run_consumer<R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin>(
        reader: &mut R,
        writer: &mut W,
        sessions: Arc<Sessions>,
        session_id: [u8; 64],
        client_addr: &SocketAddr,
    ) -> Result<()> {
        let tx = sessions
            .get_session(session_id)
            .ok_or_else(|| anyhow::anyhow!("Session not found"))?;

        let mut rx = tx.subscribe();

        let mut peek = [0u8; 1];
        loop {
            tokio::select! {
                // poll from channel
                result = rx.recv() => {
                    match result {
                        Ok(data) => {
                            // try writing to client read stream first or fail
                            if let Err(e) = writer.write_all(&data).await {
                                let _ = writer.shutdown().await;
                                error!("Consumer addr: {} session: {} write: {e}", client_addr, hex::encode(session_id));
                                break;
                            }
                            // let _ = writer.flush().await;
                        }
                        Err(RecvError::Lagged(n)) => {
                            let _ = writer.flush().await; // flush whatever remaining
                            let _ = writer.shutdown().await;
                            warn!("Consumer addr: {} session: {} lagged by {n} messages", client_addr, hex::encode(session_id));
                            break;
                        }
                        Err(RecvError::Closed) => {
                            let _ = writer.flush().await; // flush whatever b4 exiting
                            let _ = writer.shutdown().await;
                            info!("Producer closed session: {} consumer addr: {}", hex::encode(session_id), client_addr);
                            break;
                        },
                    }
                }
                result = reader.read(&mut peek) => {
                    match result {
                        Ok(0) => break, // eof check
                        Err(e) => {
                            error!("Consumer addr: {} session: {} read: {e}", client_addr, hex::encode(session_id));
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(())
    }
}
