use crate::BUFFER_SIZE;
use crate::protocol::{Role, perform_server_handshake, read_join_frame, read_raw_frame_into};
use crate::session::Sessions;
use anyhow::Result;
use bytes::BytesMut;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::error::RecvError;
// TODO; handles backpressure "properly", implement handler traits for invoking user defined fn for some events

/// A light-weight multi-threaded SPMC (Single Producer Multiple Consumer) E2E relay server.
///
/// `HydraServer` implements a zero-copy broadcast relay that:
/// - Accepts one producer and multiple consumers per session
/// - Routes data from producer → all connected consumers using Arc-backed `Bytes`
/// - Handles backpressure and slow consumers with broadcast channel lagging
/// - Enforces connection limits and per-payload size constraints
///
/// Internals
/// - Producer: Sends encrypted frames → broadcast channel
/// - Consumers: Subscribe to broadcast, receive clones of Arc<Bytes> (zero-copy)
/// - Sessions: Keyed by 64-byte session_id, one producer per session allowed
/// - Errors & Logs: Error are predictable and handled gracefully by closing connections and logging without crashing the server
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
    broadcast_capacity: usize,
}

impl HydraServer {
    /// Binds the relay server to the specified socket address and initializes internal state
    /// Defaults:
    /// - max_connections: 24
    /// - max_payload_length: 64 MiB
    /// - broadcast_capacity: 256 messages
    pub async fn bind(
        addr: &SocketAddr,
        max_payload_length: Option<usize>,
        max_connections: Option<usize>,
        broadcast_capacity: Option<usize>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            sessions: Arc::new(Sessions::init()),
            connections: Arc::new(AtomicUsize::new(0)),
            max_payload_length: max_payload_length.unwrap_or(64 * 1024 * 1024),
            max_connections: max_connections.unwrap_or(24),
            broadcast_capacity: broadcast_capacity.unwrap_or(256),
        })
    }

    /// Main server loop to accept incoming connections, spawn thread handlers, perform handshakes & session creation
    /// - `connections_timeout_ms` is the delay before client retries to accept new connections on server when the limit is reached
    /// - Producer errors; If read fails from client or broadcast send fails, the connection is closed and the error is logged.
    /// - Producer errors; If writing to client fails or broadcast lags or closed, the connection is closed and the error is logged.
    /// - EOF check are gracefully handled by closing the connection without logging an error.
    pub async fn run(self, connections_timeout_ms: u64) -> Result<()> {
        loop {
            if self.connections.fetch_add(1, Ordering::Acquire) >= self.max_connections {
                self.connections.fetch_sub(1, Ordering::Release);
                tokio::time::sleep(std::time::Duration::from_millis(connections_timeout_ms)).await;
                continue;
            }

            match self.listener.accept().await {
                Ok((stream, peer_addr)) => {
                    stream.set_nodelay(true).ok();
                    let sessions = Arc::clone(&self.sessions);
                    let connections = Arc::clone(&self.connections);
                    // spawn handler thread
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(
                            stream,
                            sessions,
                            self.max_payload_length,
                            self.broadcast_capacity,
                        )
                        .await
                        {
                            eprintln!("Connection handling error: {} from: {}", e, peer_addr);
                        }
                        connections.fetch_sub(1, Ordering::Release);
                    });
                }
                Err(e) => {
                    self.connections.fetch_sub(1, Ordering::Release);
                    eprintln!("Connection accepting error: {}", e);
                }
            }
        }
    }

    /// Handles an individual client connection, performing handshake, role determination, and routing to producer/consumer handlers
    async fn handle_connection(
        mut stream: TcpStream,
        sessions: Arc<Sessions>,
        max_payload_length: usize,
        broadcast_capacity: usize,
    ) -> Result<()> {
        stream.set_nodelay(true)?;
        let mut mem_pool = BytesMut::with_capacity(max_payload_length + 4); // 4 bytes prefix space 
        let (read_h, write_h) = stream.split();
        let mut writer = BufWriter::with_capacity(BUFFER_SIZE, write_h);
        let mut reader = BufReader::with_capacity(BUFFER_SIZE, read_h);

        let transport_key = perform_server_handshake(&mut reader, &mut writer).await?;
        let (role, session_id) =
            read_join_frame(&mut reader, &transport_key, &mut mem_pool).await?;

        match role {
            Role::Producer => {
                Self::run_producer(
                    &mut reader,
                    sessions,
                    session_id,
                    mem_pool,
                    max_payload_length,
                    broadcast_capacity,
                )
                .await
            }
            Role::Consumer => {
                Self::run_consumer(&mut reader, &mut writer, sessions, session_id).await
            }
        }
    }

    /// Handles producer clients: reads encrypted frames, decrypts, and broadcasts to consumers via the session's broadcast channel
    async fn run_producer<R: AsyncReadExt + Unpin>(
        reader: &mut R,
        sessions: Arc<Sessions>,
        session_id: [u8; 64],
        mut mem_pool: BytesMut,
        max_payload_length: usize,
        broadcast_capacity: usize,
    ) -> Result<()> {
        let tx = sessions.try_register_producer(session_id, broadcast_capacity)?;

        loop {
            // read from client read stream
            let n = match read_raw_frame_into(reader, &mut mem_pool, max_payload_length).await {
                Ok(n) => n,
                Err(e) => {
                    tx.closed().await;
                    eprintln!("Producer read: {e}");
                    break;
                }
            };

            // write to broadcast channel
            if let Err(e) = tx.send(mem_pool.split_to(n).freeze()) {
                tx.closed().await; // close channel to signal consumers
                eprintln!("Producer broadcast: {e}");
                break;
            }
        }

        // clean up
        sessions.unregister_producer(session_id);
        Ok(())
    }

    async fn run_consumer<R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin>(
        reader: &mut R,
        writer: &mut W,
        sessions: Arc<Sessions>,
        session_id: [u8; 64],
    ) -> Result<()> {
        let tx = sessions
            .get_for_consumer(session_id)
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
                                eprintln!("Consumer write: {e}");
                                break;
                            }
                            let _ = writer.flush().await;
                        }
                        Err(RecvError::Lagged(n)) => {
                            let _ = writer.flush().await;
                            let _ = writer.shutdown().await;
                            eprintln!("Consumer lagged behind: {n}");
                            break;
                        }
                        Err(RecvError::Closed) => {
                            let _ = writer.flush().await;
                            let _ = writer.shutdown().await;
                            eprintln!("Producer closed");
                            break;
                        },
                    }
                }
                result = reader.read(&mut peek) => {
                    match result {
                        Ok(0) => break, // eof check
                        Err(e) => {
                            eprintln!("Consumer read: {e}");
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
