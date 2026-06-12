use crate::protocol::{
    Role, perform_client_handshake, read_encrypted_frame, write_encrypted_frame, write_join_frame,
};
use anyhow::Result;
use bytes::BytesMut;
use std::net::SocketAddr;
use tokio::io::{BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// ProducerClient connects to the relay server as a producer, performs handshake,
/// and can broadcast encrypted frames to all connected consumers in the same session.
pub struct ProducerClient {
    session_key: [u8; 32],
    #[allow(unused)]
    buf_reader: BufReader<OwnedReadHalf>,
    buf_writer: BufWriter<OwnedWriteHalf>,
    mem_pool: BytesMut,
}

impl ProducerClient {
    /// Connects to the relay server, performs handshake, and sends a join frame with the producer role and session_id.
    pub async fn connect(
        addr: SocketAddr,
        session_id: &[u8; 64],
        session_key: [u8; 32],
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        let (reader, writer) = stream.into_split();
        let mut writer = BufWriter::new(writer);
        let mut reader = BufReader::new(reader);
        let transport_key = perform_client_handshake(&mut reader, &mut writer).await?;
        let mut mem_pool = BytesMut::with_capacity(1024 * 1024 * 18);
        write_join_frame(
            &mut writer,
            Role::Producer,
            session_id,
            &transport_key,
            &mut mem_pool,
        )
        .await?;

        Ok(Self {
            buf_reader: reader,
            buf_writer: writer,
            session_key,
            mem_pool,
        })
    }

    /// Broadcasts the given data as an encrypted frame to all connected consumers (zero-copy) in the same session.
    pub async fn broadcast(&mut self, data: &[u8]) -> Result<()> {
        write_encrypted_frame(
            &mut self.buf_writer,
            data,
            &self.session_key,
            &mut self.mem_pool,
        )
        .await
    }
}

/// ConsumerClient connects to the relay server as a consumer, performs handshake,
/// and can receive encrypted frames broadcast by the producer in the same session.
pub struct ConsumerClient {
    session_key: [u8; 32],
    buf_reader: BufReader<OwnedReadHalf>,
    #[allow(unused)]
    buf_writer: BufWriter<OwnedWriteHalf>,
    mem_pool: BytesMut,
}

impl ConsumerClient {
    /// Connects to the relay server, performs handshake, and sends a join frame with the consumer role and session_id.
    pub async fn connect(
        addr: SocketAddr,
        session_id: &[u8; 64],
        session_key: [u8; 32],
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (reader, writer) = stream.into_split();
        let mut writer = BufWriter::new(writer);
        let mut reader = BufReader::new(reader);

        let transport_key = perform_client_handshake(&mut reader, &mut writer).await?;
        let mut mem_pool = BytesMut::with_capacity(1024 * 1024 * 18);
        write_join_frame(
            &mut writer,
            Role::Consumer,
            session_id,
            &transport_key,
            &mut mem_pool,
        )
        .await?;

        Ok(Self {
            buf_reader: reader,
            buf_writer: writer,
            session_key,
            mem_pool,
        })
    }

    /// Receives the next encrypted frame from the producer, decrypts it, and returns the plaintext data as a Vec<u8>.
    pub async fn recv(&mut self) -> Result<&[u8]> {
        let decrypted =
            read_encrypted_frame(&mut self.buf_reader, &self.session_key, &mut self.mem_pool)
                .await?;
        Ok(decrypted)
    }
}
