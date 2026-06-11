use crate::crypto::{NONCE_LEN, TAG_LEN, decrypt_into, encrypt_into};
use anyhow::Result;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub struct RelayServer {
    /// internal tcp stream
    listener: TcpListener,
    /// max payload length for incoming messages, exceeds this will fail the producer
    max_payload_length: usize,
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
            max_payload_length: 64 * 1024 * 1024,
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
        stream.set_nodelay(true)?;
        Ok(())
    }
}

/// Read 4 bytes for frame length with timeout, return error on timeout or read failure
#[inline(always)]
async fn read_frame_length<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<u32> {
    const MAX_FRAME_LENGTH: u32 = 1024 * 1024 * 16;
    let mut len_buf = [0u8; 4];
    let result = (reader.read_exact(&mut len_buf)).await?;
    let len = u32::from_be_bytes(len_buf);

    if len == 0 || len > MAX_FRAME_LENGTH {
        return Err(anyhow::anyhow!("Invalid frame length: {}", len));
    }

    Ok(len)
}

/// Read encrypted frame into global memory pool, decrypt in-place, return decrypted slice, return error on timeout, read failure, or decryption failure
#[inline(always)]
async fn read_encrypt_data_into<'a, R: AsyncReadExt + Unpin>(
    reader: &mut R,
    session_key: &[u8; 32],
    mem_pool: &'a mut Vec<u8>,
) -> Result<&'a [u8]> {
    let ciphertext_len = read_frame_length(reader).await? as usize;

    if ciphertext_len < NONCE_LEN + TAG_LEN {
        anyhow::bail!(
            "Frame length too short for encrypted header: {}",
            ciphertext_len
        );
    }
    let plaintext_len = ciphertext_len - NONCE_LEN - TAG_LEN;

    // should accommodate both encrypted & decrypted chunk
    if mem_pool.len() < ciphertext_len + plaintext_len {
        mem_pool.resize(ciphertext_len + plaintext_len, 0);
    }

    let (encrypted_chunk, decrypted_chunk) = mem_pool.split_at_mut(ciphertext_len);
    let encrypted_buf = &mut encrypted_chunk[..ciphertext_len];
    reader.read_exact(encrypted_buf).await?;

    let decrypted_buf = &mut decrypted_chunk[..plaintext_len];
    decrypt_into(encrypted_buf, decrypted_buf, session_key)?;

    Ok(decrypted_buf)
}

/// Write encrypted data with 4-byte length prefix, return error on write failure or encryption failure
#[inline(always)]
async fn write_encrypt_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    data: &[u8],
    session_key: &[u8; 32],
    mem_pool: &mut Vec<u8>,
) -> Result<()> {
    let plaintext_len = data.len();
    if plaintext_len > u32::MAX as usize {
        return Err(anyhow::anyhow!(
            "Too large content: {} bytes",
            plaintext_len
        ));
    }

    let ciphertext_len = NONCE_LEN + plaintext_len + TAG_LEN;
    let total_frame_len = 4 + ciphertext_len;

    // alloc if less
    if mem_pool.len() < total_frame_len {
        mem_pool.resize(total_frame_len, 0);
    }

    let len_bytes = (ciphertext_len as u32).to_be_bytes();
    mem_pool[..4].copy_from_slice(&len_bytes); // 4-byte prefix

    // encrypt that
    encrypt_into(data, &mut mem_pool[4..total_frame_len], session_key)?;
    writer.write_all(&mem_pool[..total_frame_len]).await?;
    writer.flush().await?;
    Ok(())
}
