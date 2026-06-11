use crate::crypto::{
    decrypt_into, encrypt_into, generate_x25519_keypair, NONCE_LEN, TAG_LEN,
};
use anyhow::{bail, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use x25519_dalek::PublicKey;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Producer = 0x01,
    Consumer = 0x02,
}

impl Role {
    pub fn from_u8(val: u8) -> Result<Self> {
        match val {
            0x01 => Ok(Self::Producer),
            0x02 => Ok(Self::Consumer),
            _ => bail!("Unknown role: {:#04x}", val),
        }
    }
}

pub async fn perform_client_handshake(stream: &mut TcpStream) -> Result<[u8; 32]> {
    let (secret, public) = generate_x25519_keypair()?;

    stream.write_all(public.as_bytes()).await?;

    let mut server_pub_bytes = [0u8; 32];
    stream.read_exact(&mut server_pub_bytes).await?;
    let server_pub = PublicKey::from(server_pub_bytes);

    let shared_secret = secret.diffie_hellman(&server_pub);
    Ok(*shared_secret.as_bytes())
}

pub async fn perform_server_handshake(stream: &mut TcpStream) -> Result<[u8; 32]> {
    let (secret, public) = generate_x25519_keypair()?;

    let mut client_pub_bytes = [0u8; 32];
    stream.read_exact(&mut client_pub_bytes).await?;
    let client_pub = PublicKey::from(client_pub_bytes);

    stream.write_all(public.as_bytes()).await?;

    let shared_secret = secret.diffie_hellman(&client_pub);
    Ok(*shared_secret.as_bytes())
}

pub async fn write_join_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    role: Role,
    session_id: &[u8],
    transport_key: &[u8; 32],
    mem_pool: &mut Vec<u8>,
) -> Result<()> {
    let session_id_len = session_id.len();
    if session_id_len > u16::MAX as usize {
        bail!("Session ID too long");
    }

    let payload_len = 1 + 2 + session_id_len;
    let mut payload = [0u8; 259];
    payload[0] = role as u8;
    payload[1..3].copy_from_slice(&(session_id_len as u16).to_be_bytes());
    payload[3..3 + session_id_len].copy_from_slice(session_id);

    let ciphertext_len = NONCE_LEN + payload_len + TAG_LEN;
    let total_frame_len = 4 + ciphertext_len;

    if mem_pool.len() < total_frame_len {
        mem_pool.resize(total_frame_len, 0);
    }

    mem_pool[..4].copy_from_slice(&(ciphertext_len as u32).to_be_bytes());
    encrypt_into(
        &payload[..payload_len],
        &mut mem_pool[4..total_frame_len],
        transport_key,
    )?;

    writer.write_all(&mem_pool[..total_frame_len]).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_join_frame<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    transport_key: &[u8; 32],
    mem_pool: &mut Vec<u8>,
) -> Result<(Role, Vec<u8>)> {
    let ciphertext_len = read_frame_length(reader, u32::MAX as usize).await? as usize;

    if ciphertext_len < NONCE_LEN + TAG_LEN + 3 {
        bail!("Join frame too short");
    }

    let plaintext_len = ciphertext_len - NONCE_LEN - TAG_LEN;
    let needed = ciphertext_len + plaintext_len;
    if mem_pool.len() < needed {
        mem_pool.resize(needed, 0);
    }

    let (ct, pt) = mem_pool.split_at_mut(ciphertext_len);
    reader.read_exact(&mut ct[..ciphertext_len]).await?;
    decrypt_into(&ct[..ciphertext_len], &mut pt[..plaintext_len], transport_key)?;

    let plaintext = &pt[..plaintext_len];
    let role = Role::from_u8(plaintext[0])?;
    let sid_len = u16::from_be_bytes([plaintext[1], plaintext[2]]) as usize;

    if plaintext.len() < 3 + sid_len {
        bail!("Join payload truncated");
    }

    let session_id = plaintext[3..3 + sid_len].to_vec();
    Ok((role, session_id))
}

pub async fn write_encrypted_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    data: &[u8],
    session_key: &[u8; 32],
    mem_pool: &mut Vec<u8>,
) -> Result<()> {
    let plaintext_len = data.len();
    if plaintext_len > u32::MAX as usize {
        bail!("Too large content: {} bytes", plaintext_len);
    }

    let ciphertext_len = NONCE_LEN + plaintext_len + TAG_LEN;
    let total_frame_len = 4 + ciphertext_len;

    if mem_pool.len() < total_frame_len {
        mem_pool.resize(total_frame_len, 0);
    }

    mem_pool[..4].copy_from_slice(&(ciphertext_len as u32).to_be_bytes());
    encrypt_into(data, &mut mem_pool[4..total_frame_len], session_key)?;
    writer.write_all(&mem_pool[..total_frame_len]).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_encrypted_frame<'a, R: AsyncReadExt + Unpin>(
    reader: &mut R,
    session_key: &[u8; 32],
    mem_pool: &'a mut Vec<u8>,
) -> Result<&'a [u8]> {
    let ciphertext_len = read_frame_length(reader, u32::MAX as usize).await? as usize;

    if ciphertext_len < NONCE_LEN + TAG_LEN {
        bail!("Frame too short for encrypted header: {}", ciphertext_len);
    }
    let plaintext_len = ciphertext_len - NONCE_LEN - TAG_LEN;

    if mem_pool.len() < ciphertext_len + plaintext_len {
        mem_pool.resize(ciphertext_len + plaintext_len, 0);
    }

    let (ct, pt) = mem_pool.split_at_mut(ciphertext_len);
    reader.read_exact(&mut ct[..ciphertext_len]).await?;
    decrypt_into(&ct[..ciphertext_len], &mut pt[..plaintext_len], session_key)?;

    Ok(&pt[..plaintext_len])
}

pub async fn read_raw_frame_into<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    max_frame_length: usize,
) -> Result<usize> {
    let len = read_frame_length(reader, max_frame_length).await? as usize;
    let total = 4 + len;

    if buf.len() < total {
        buf.resize(total, 0);
    }

    buf[..4].copy_from_slice(&(len as u32).to_be_bytes());
    reader.read_exact(&mut buf[4..total]).await?;
    Ok(total)
}

async fn read_frame_length<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    max_frame_length: usize,
) -> Result<u32> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);

    if len == 0 || len as usize > max_frame_length {
        bail!("Invalid frame length: {}", len);
    }

    Ok(len)
}
