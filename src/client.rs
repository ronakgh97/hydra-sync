use crate::protocol::{self, read_encrypted_frame, Role};
use anyhow::Result;
use std::net::SocketAddr;
use tokio::net::TcpStream;

pub struct ProducerClient {
    stream: TcpStream,
    session_key: [u8; 32],
    mem_pool: Vec<u8>,
}

impl ProducerClient {
    pub async fn connect_producer(addr: SocketAddr, session_id: &[u8; 64], session_key: [u8; 32]) -> Result<Self> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        let transport_key = protocol::perform_client_handshake(&mut stream).await?;
        let mut mem_pool = Vec::with_capacity(1024 * 1024 * 2);
        protocol::write_join_frame(&mut stream, Role::Producer, session_id, &transport_key, &mut mem_pool).await?;

        Ok(Self { stream, session_key, mem_pool })
    }

    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        protocol::write_encrypted_frame(&mut self.stream, data, &self.session_key, &mut self.mem_pool).await
    }
}

pub struct ConsumerClient {
    stream: TcpStream,
    session_key: [u8; 32],
    mem_pool: Vec<u8>,
}

impl ConsumerClient {
    pub async fn connect_consumer(addr: SocketAddr, session_id: &[u8; 64], session_key: [u8; 32]) -> Result<Self> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        let transport_key = protocol::perform_client_handshake(&mut stream).await?;
        let mut mem_pool = Vec::with_capacity(1024 * 1024 * 2);
        protocol::write_join_frame(&mut stream, Role::Consumer, session_id, &transport_key, &mut mem_pool).await?;

        Ok(Self { stream, session_key, mem_pool })
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>> {
        let decrypted = read_encrypted_frame(&mut self.stream, &self.session_key, &mut self.mem_pool).await?;
        Ok(decrypted.to_vec())
    }
}
