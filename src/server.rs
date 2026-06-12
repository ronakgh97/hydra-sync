use crate::protocol::{self, Role, read_join_frame, perform_server_handshake};
use crate::session::Sessions;
use anyhow::Result;
use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::error::RecvError;

pub struct RelayServer {
    listener: TcpListener,
    sessions: Arc<Sessions>,
    connections: Arc<AtomicUsize>,
    max_payload_length: usize,
    max_connections: usize,
    broadcast_capacity: usize,
}

impl RelayServer {
    pub async fn bind(addr: &SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            sessions: Arc::new(Sessions::init()),
            connections: Arc::new(AtomicUsize::new(0)),
            max_payload_length: 64 * 1024 * 1024,
            max_connections: 24,
            broadcast_capacity: 256,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    pub async fn run(&self) {
        loop {
            if self.connections.fetch_add(1, Ordering::Acquire) >= self.max_connections {
                self.connections.fetch_sub(1, Ordering::Release);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }

            match self.listener.accept().await {
                Ok((stream, peer_addr)) => {
                    stream.set_nodelay(true).ok();
                    let sessions = self.sessions.clone();
                    let connections = self.connections.clone();
                    let max_payload = self.max_payload_length;
                    let capacity = self.broadcast_capacity;
                    tokio::spawn(async move {
                        if let Err(e) =
                            Self::handle_connection(stream, sessions, max_payload, capacity).await
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

    async fn handle_connection(
        mut stream: TcpStream,
        sessions: Arc<Sessions>,
        max_payload_length: usize,
        broadcast_capacity: usize,
    ) -> Result<()> {
        stream.set_nodelay(true)?;

        let mut mem_pool = Vec::with_capacity(max_payload_length + 4);
        let transport_key = perform_server_handshake(&mut stream).await?;
        let (role, session_id) =
            read_join_frame(&mut stream, &transport_key, &mut mem_pool).await?;

        match role {
            Role::Producer => {
                Self::run_producer(
                    stream,
                    sessions,
                    session_id,
                    max_payload_length,
                    broadcast_capacity,
                    mem_pool,
                )
                .await
            }
            Role::Consumer => Self::run_consumer(stream, sessions, session_id).await,
        }
    }

    async fn run_producer(
        mut producer: TcpStream,
        sessions: Arc<Sessions>,
        session_id: [u8; 64],
        max_payload_length: usize,
        capacity: usize,
        mut buf: Vec<u8>,
    ) -> Result<()> {
        let tx = sessions.get_or_create(session_id, capacity);

        loop {
            let n = match protocol::read_raw_frame_into(&mut producer, &mut buf, max_payload_length)
                .await
            {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("Producer read: {e}");
                    break;
                }
            };

            if let Err(e) = tx.send(Bytes::copy_from_slice(&buf[..n])) {
                eprintln!("Broadcast: {e}");
                break;
            }
        }

        sessions.remove(session_id);
        Ok(())
    }

    async fn run_consumer(
        stream: TcpStream,
        sessions: Arc<Sessions>,
        session_id: [u8; 64],
    ) -> Result<()> {
        let tx = sessions
            .get(session_id)
            .ok_or_else(|| anyhow::anyhow!("Session not found"))?;

        let mut rx = tx.subscribe();
        let (mut reader, mut writer) = stream.into_split();

        let mut peek = [0u8; 1];
        loop {
            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Ok(data) => {
                            if writer.write_all(&data).await.is_err() {
                                break;
                            }
                        }
                        Err(RecvError::Lagged(n)) => {
                            eprintln!("Consumer lagged: {n}");
                            break;
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
                result = reader.read(&mut peek) => {
                    match result {
                        Ok(0) => break,
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
