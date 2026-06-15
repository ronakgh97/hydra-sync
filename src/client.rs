use crate::BUFFER_SIZE;
use crate::protocol::{
    Role, perform_client_handshake, read_encrypted_frame, write_encrypted_frame, write_join_frame,
};
use anyhow::Result;
use bytes::BytesMut;
use std::marker::PhantomData;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};

/// Unit structs to represent the `Role::Producer` at the type level for better safety and clarity.
pub struct Producer;
/// Unit structs to represent the `Role::Consumer` at the type level for better safety and clarity.
pub struct Consumer;

/// `HydraClient` connects to the relay server as a producer or consumer, performs handshake, and sends/receives encrypted frames.
/// It maintains an internal memory pool (18 mb) for zero-copy crypto and buffering.
/// The `broadcast` method allows producers to send encrypted frames to all connected consumers in the same session,
/// while the `recv` method allows consumers to receive and decrypt frames from the producer.
///
/// ```no_run
/// use hydra_sync::client::{HydraClient, Producer, Consumer};
///
/// #[tokio::main]
/// async fn main() {
///     let addr = "127.0.0.1:8000";
///     let session_id = [0xFFu8; 64];
///     let session_key = [0xAAu8; 32];
///
///     let mut producer = HydraClient::<Producer>::connect(addr, &session_id, session_key).await.unwrap();
///     producer.broadcast(b"I luv you >.<").await.unwrap(); // sends to all consumer
///
///     let mut consumer = HydraClient::<Consumer>::connect(addr, &session_id, session_key).await.unwrap();
///     consumer.recv().await.unwrap(); // recv whatever next frame on ring buf
/// }
/// ```
///
pub struct HydraClient<R> {
    session_key: [u8; 32],
    buf_reader: BufReader<OwnedReadHalf>,
    buf_writer: BufWriter<OwnedWriteHalf>,
    mem_pool: BytesMut,
    _role: PhantomData<R>,
}

impl HydraClient<Producer> {
    /// Connects to the server, performs handshake, and sends a join frame with `Role::Producer` and session_id.
    pub async fn connect<A: ToSocketAddrs>(
        addr: A,
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
            _role: PhantomData,
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

impl HydraClient<Consumer> {
    /// Connects to the server, performs handshake, and sends a join frame with the `Role::Consumer` and session_id.
    pub async fn connect<A: ToSocketAddrs>(
        addr: A,
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
            _role: PhantomData,
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
}

impl<R> HydraClient<R> {
    /// Closes the client connection gracefully by flushing and shutting down the writer (proper FIN).
    pub async fn close(&mut self) -> Result<()> {
        self.buf_writer.flush().await?;
        self.buf_writer.shutdown().await?;
        Ok(())
    }
}
