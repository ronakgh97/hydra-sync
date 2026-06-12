use crate::crypto::{NONCE_LEN, TAG_LEN, decrypt_into, encrypt_into, generate_x25519_keypair};
use anyhow::{Result, bail};
use sha2::{Digest, Sha256, Sha512};
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
    Ok(Sha256::digest(shared_secret.as_bytes()).into())
}

pub async fn perform_server_handshake(stream: &mut TcpStream) -> Result<[u8; 32]> {
    let (secret, public) = generate_x25519_keypair()?;

    let mut client_pub_bytes = [0u8; 32];
    stream.read_exact(&mut client_pub_bytes).await?;
    let client_pub = PublicKey::from(client_pub_bytes);

    stream.write_all(public.as_bytes()).await?;

    let shared_secret = secret.diffie_hellman(&client_pub);
    Ok(Sha256::digest(shared_secret.as_bytes()).into())
}

pub async fn write_join_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    role: Role,
    session_id: &[u8; 64],
    transport_key: &[u8; 32],
    mem_pool: &mut Vec<u8>,
) -> Result<()> {
    let mut payload = Vec::with_capacity(1 + session_id.len());
    payload.push(role as u8);
    payload.extend_from_slice(&Sha512::digest(session_id));
    write_encrypted_frame(writer, &payload, transport_key, mem_pool).await
}

pub async fn read_join_frame<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    transport_key: &[u8; 32],
    mem_pool: &mut Vec<u8>,
) -> Result<(Role, [u8; 64])> {
    let plaintext = read_encrypted_frame(reader, transport_key, mem_pool).await?;
    if plaintext.is_empty() {
        bail!("Empty data frame");
    }
    let role = Role::from_u8(plaintext[0])?;
    let session_id = plaintext[1..].try_into()?;
    Sha512::digest(session_id);
    Ok((role, session_id))
}

pub async fn write_encrypted_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    data: &[u8],
    key: &[u8; 32],
    mem_pool: &mut Vec<u8>,
) -> Result<()> {
    let ciphertext_len = NONCE_LEN + data.len() + TAG_LEN;
    let total = 4 + ciphertext_len;

    if mem_pool.len() < total {
        mem_pool.resize(total, 0);
    }

    mem_pool[..4].copy_from_slice(&(ciphertext_len as u32).to_be_bytes());
    encrypt_into(data, &mut mem_pool[4..total], key)?;
    writer.write_all(&mem_pool[..total]).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_encrypted_frame<'a, R: AsyncReadExt + Unpin>(
    reader: &mut R,
    key: &[u8; 32],
    mem_pool: &'a mut Vec<u8>,
) -> Result<&'a [u8]> {
    let ciphertext_len = read_payload_length(reader, u32::MAX as usize).await? as usize;

    if ciphertext_len < NONCE_LEN + TAG_LEN {
        bail!("Frame too short for encrypted data: {}", ciphertext_len);
    }
    let plaintext_len = ciphertext_len - NONCE_LEN - TAG_LEN;

    if mem_pool.len() < ciphertext_len + plaintext_len {
        mem_pool.resize(ciphertext_len + plaintext_len, 0);
    }

    let (ct, pt) = mem_pool.split_at_mut(ciphertext_len);
    reader.read_exact(&mut ct[..ciphertext_len]).await?;
    decrypt_into(&ct[..ciphertext_len], &mut pt[..plaintext_len], key)?;
    Ok(&pt[..plaintext_len])
}

pub async fn read_raw_frame_into<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    max_payload_length: usize,
) -> Result<usize> {
    let len = read_payload_length(reader, max_payload_length).await? as usize;
    let total = 4 + len;

    if buf.len() < total {
        buf.resize(total, 0);
    }

    buf[..4].copy_from_slice(&(len as u32).to_be_bytes());
    reader.read_exact(&mut buf[4..total]).await?;
    Ok(total)
}

async fn read_payload_length<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    max_payload_length: usize,
) -> Result<u32> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);

    if len == 0 || len as usize > max_payload_length {
        bail!(
            "Invalid payload length: {}, must be between 1 and {}",
            len,
            max_payload_length
        );
    }

    Ok(len)
}
