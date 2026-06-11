use anyhow::Result;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};

pub struct RelayServer {
    /// internal tcp stream
    listener: TcpListener,
    /// max frame length for incoming messages, exceeds this will fail the producer
    max_frame_length: usize,
    /// max concurrent sessions, exceeds this will reject new connections until some sessions are closed
    max_concurrent_sessions: usize,
    /// for in-place encryption/decryption & other ops
    /// we need a global memory pool to avoid frequent allocations
    global_memory_pool_size: usize,
}

impl RelayServer {
    pub async fn bind(addr: &SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            max_frame_length: 64 * 1024 * 1024,
            max_concurrent_sessions: 24,
            global_memory_pool_size: 1024 * 1024 * 128,
        })
    }

    pub async fn run(&mut self) {
        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream).await {
                            eprintln!("Error handling connection from {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                }
            }
        }
    }
    async fn handle_connection(mut stream: TcpStream) -> Result<()> {
        Ok(())
    }
}
