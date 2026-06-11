use anyhow::Result;
use std::net::SocketAddr;
use tokio::net::TcpStream;
pub enum Message {
    Header,
    Payload,
}

pub struct ProducerClient {
    /// internal tcp listener
    stream: TcpStream,
    /// memory pool for in-place encryption/decryption & other ops
    global_memory_pool_size: usize,
}

impl ProducerClient {
    pub async fn connect_as_producer(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            global_memory_pool_size: 1024 * 1024 * 128,
        })
    }

    pub async fn connect_as_consumer(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            global_memory_pool_size: 1024 * 1024 * 128,
        })
    }
}
