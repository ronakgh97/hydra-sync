use anyhow::Result;
use std::net::SocketAddr;
use tokio::net::TcpStream;

pub struct ProducerClient {
    /// internal tcp listener
    listener: TcpStream,
    /// memory pool for in-place encryption/decryption & other ops
    global_memory_pool_size: usize,
}

impl ProducerClient {
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            listener: stream,
            global_memory_pool_size: 1024 * 1024 * 128,
        })
    }
}
