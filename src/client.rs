use crate::BUFFER_SIZE;
use crate::protocol::{
    Role, perform_client_handshake, read_encrypted_frame, write_encrypted_frame, write_join_frame,
};
use anyhow::Result;
use bytes::BytesMut;
use std::net::SocketAddr;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// `HydraClient` connects to the relay server as a producer or consumer, performs handshake, and sends/receives encrypted frames.
/// It maintains an internal memory pool for zero-copy encryption/decryption and buffering.
/// The `broadcast` method allows producers to send encrypted frames to all connected consumers in the same session,
/// while the `recv` method allows consumers to receive and decrypt frames from the producer.
pub struct HydraClient {
    session_key: [u8; 32],
    buf_reader: BufReader<OwnedReadHalf>,
    buf_writer: BufWriter<OwnedWriteHalf>,
    mem_pool: BytesMut,
}

impl HydraClient {
    /// Connects to the relay server, performs handshake, and sends a join frame with the producer role and session_id.
    pub async fn connect_producer(
        addr: SocketAddr,
        session_id: &[u8; 64],
        session_key: [u8; 32],
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        let (reader, writer) = stream.into_split();
        let mut writer = BufWriter::with_capacity(BUFFER_SIZE, writer);
        let mut reader = BufReader::with_capacity(BUFFER_SIZE, reader);
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

    /// Connects to the relay server, performs handshake, and sends a join frame with the consumer role and session_id.
    pub async fn connect_consumer(
        addr: SocketAddr,
        session_id: &[u8; 64],
        session_key: [u8; 32],
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (reader, writer) = stream.into_split();
        let mut writer = BufWriter::with_capacity(BUFFER_SIZE, writer);
        let mut reader = BufReader::with_capacity(BUFFER_SIZE, reader);

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

    /// Receives the next encrypted frame from the producer, decrypts it, and returns the plaintext data as a byte slice.
    /// The returned slice is valid until the next call to `recv` or `broadcast`, which may reuse the internal memory pool buffer.
    pub async fn recv(&mut self) -> Result<&[u8]> {
        let decrypted =
            read_encrypted_frame(&mut self.buf_reader, &self.session_key, &mut self.mem_pool)
                .await?;
        Ok(decrypted)
    }

    /// Closes the client connection gracefully by flushing and shutting down the writer (proper FIN).
    pub async fn close(&mut self) -> Result<()> {
        self.buf_writer.flush().await?;
        self.buf_writer.shutdown().await?;
        Ok(())
    }
}
