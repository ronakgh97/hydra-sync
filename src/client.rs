use crate::protocol::{
    Role, perform_client_handshake, read_encrypted_frame, write_encrypted_frame, write_join_frame,
};
use anyhow::Result;
use bytes::BytesMut;
use std::net::SocketAddr;
use tokio::io::{BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

pub struct ProducerClient {
    #[allow(unused)]
    buf_reader: BufReader<OwnedReadHalf>,
    buf_writer: BufWriter<OwnedWriteHalf>,
    session_key: [u8; 32],
    mem_pool: BytesMut,
}

impl ProducerClient {
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

pub struct ConsumerClient {
    buf_reader: BufReader<OwnedReadHalf>,
    #[allow(unused)]
    buf_writer: BufWriter<OwnedWriteHalf>,
    session_key: [u8; 32],
    mem_pool: BytesMut,
}

impl ConsumerClient {
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

    pub async fn recv(&mut self) -> Result<Vec<u8>> {
        let decrypted =
            read_encrypted_frame(&mut self.buf_reader, &self.session_key, &mut self.mem_pool)
                .await?;
        Ok(decrypted.to_vec())
    }
}
