use crate::crypto::{NONCE_LEN, TAG_LEN, decrypt_into, encrypt_into, generate_x25519_keypair};
use anyhow::{Result, bail};
use bytes::BytesMut;
use rand::Rng;
use sha3::{Digest, Sha3_256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use x25519_dalek::PublicKey;

const TLS_NONCE_LEN: usize = 72;
const HANDSHAKE_INFO: &[u8] =
    concat!("hydra-sync transport key/v", env!("CARGO_PKG_VERSION")).as_bytes();

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Producer = 0x01,
    Consumer = 0x02,
}

impl Role {
    #[inline]
    /// Converts an u8 to a Role, returning an error if the value is invalid
    pub fn from_u8(val: u8) -> Result<Self> {
        match val {
            0x01 => Ok(Self::Producer),
            0x02 => Ok(Self::Consumer),
            _ => bail!("Unknown role: {:#04x}", val),
        }
    }
}

/// Perform X25519 key exchange handshake on client side and return the derived shared secret
pub async fn perform_client_handshake<R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin>(
    reader: &mut R,
    writer: &mut W,
) -> Result<[u8; 32]> {
    let (secret, client_pub) = generate_x25519_keypair()?;

    let mut client_nonce = [0u8; TLS_NONCE_LEN];
    rand::rng().fill_bytes(&mut client_nonce);

    // write nonce + 32 bytes key
    writer.write_all(&client_nonce).await?;
    writer.write_all(client_pub.as_bytes()).await?;
    writer.flush().await?;

    let mut server_nonce = [0u8; TLS_NONCE_LEN];
    reader.read_exact(&mut server_nonce).await?;
    let mut server_pub_bytes = [0u8; 32];
    reader.read_exact(&mut server_pub_bytes).await?;
    let server_pub = PublicKey::from(server_pub_bytes);

    let shared_secret = secret.diffie_hellman(&server_pub);
    derive_transport_key(
        shared_secret.as_bytes(),
        client_pub.as_bytes(),
        &client_nonce,
        server_pub.as_bytes(),
        &server_nonce,
    )
}

// Perform X25519 key exchange handshake on server side and return the derived shared secret
pub async fn perform_server_handshake<R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin>(
    reader: &mut R,
    writer: &mut W,
) -> Result<[u8; 32]> {
    let (secret, server_pub) = generate_x25519_keypair()?;

    // read client nonce + 32 bytes key
    let mut client_nonce = [0u8; TLS_NONCE_LEN];
    reader.read_exact(&mut client_nonce).await?;
    let mut client_pub_bytes = [0u8; 32];
    reader.read_exact(&mut client_pub_bytes).await?;
    let client_pub = PublicKey::from(client_pub_bytes);

    let mut server_nonce = [0u8; TLS_NONCE_LEN];
    rand::rng().fill_bytes(&mut server_nonce);

    writer.write_all(&server_nonce).await?;
    writer.write_all(server_pub.as_bytes()).await?;
    writer.flush().await?;

    let shared_secret = secret.diffie_hellman(&client_pub);
    derive_transport_key(
        shared_secret.as_bytes(),
        client_pub.as_bytes(),
        &client_nonce,
        server_pub.as_bytes(),
        &server_nonce,
    )
}

#[inline(always)]
// Derives a transport key by hashing the concatenation of client/server public keys, nonce, handshake info, and shared secret using SHA3-256
fn derive_transport_key(
    shared_secret: &[u8],
    client_pub: &[u8; 32],
    client_nonce: &[u8; TLS_NONCE_LEN],
    server_pub: &[u8; 32],
    server_nonce: &[u8; TLS_NONCE_LEN],
) -> Result<[u8; 32]> {
    let mut transcript = Vec::with_capacity(32 + TLS_NONCE_LEN + 32 + TLS_NONCE_LEN + 32);
    transcript.extend_from_slice(client_pub);
    transcript.extend_from_slice(client_nonce);
    transcript.extend_from_slice(server_pub);
    transcript.extend_from_slice(server_nonce);
    transcript.extend_from_slice(shared_secret);
    transcript.extend_from_slice(HANDSHAKE_INFO);

    Ok(Sha3_256::digest(transcript).into())
}

// Send role and session_id as join request
pub async fn write_join_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    role: Role,
    session_id: &[u8; 64],
    transport_key: &[u8; 32],
    mem_pool: &mut BytesMut,
) -> Result<()> {
    let mut payload = Vec::with_capacity(1 + session_id.len());
    payload.push(role as u8);
    payload.extend_from_slice(session_id);
    write_encrypted_frame(writer, &payload, transport_key, mem_pool).await
}

// Reads the join frame from client
pub async fn read_join_frame<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    transport_key: &[u8; 32],
    mem_pool: &mut BytesMut,
) -> Result<(Role, [u8; 64])> {
    let plaintext = read_encrypted_frame(reader, transport_key, mem_pool).await?;
    if plaintext.is_empty() {
        bail!("Empty data frame");
    }
    let role = Role::from_u8(plaintext[0])?;
    let session_id = plaintext[1..].try_into()?;
    Ok((role, session_id))
}

// Writes an encrypted frame with 4-byte big-endian length prefix, nonce, ciphertext, and tag
pub async fn write_encrypted_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    data: &[u8],
    key: &[u8; 32],
    mem_pool: &mut BytesMut,
) -> Result<()> {
    let ciphertext_len = NONCE_LEN + data.len() + TAG_LEN;
    let total = 4 + ciphertext_len;

    if mem_pool.len() < total {
        mem_pool.resize(total, 0);
    }

    let (header, body) = mem_pool[..total].split_at_mut(4);
    header.copy_from_slice(&(ciphertext_len as u32).to_be_bytes());
    encrypt_into(data, body, key)?;
    writer.write_all(&mem_pool[..total]).await?;
    writer.flush().await?;
    Ok(())
}

#[inline]
/// Reads an encrypted frame with 4-byte big-endian length prefix, nonce, ciphertext, and tag,
/// decrypts it into `mem_pool`, and returns a slice to the plaintext
pub async fn read_encrypted_frame<'a, R: AsyncReadExt + Unpin>(
    reader: &mut R,
    key: &[u8; 32],
    mem_pool: &'a mut BytesMut,
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

#[inline]
/// Reads a raw frame with 4-byte big-endian length prefix and payload,
/// stores it in `mem_pool`, and returns the total frame size
pub async fn read_raw_frame_into<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    mem_pool: &mut BytesMut,
    max_payload_length: usize,
) -> Result<usize> {
    let len = read_payload_length(reader, max_payload_length).await? as usize;
    let total = 4 + len;

    if mem_pool.len() < total {
        mem_pool.resize(total, 0);
    }

    let (header, body) = mem_pool[..total].split_at_mut(4);
    header.copy_from_slice(&(len as u32).to_be_bytes());
    reader.read_exact(body).await?;
    Ok(total)
}

#[inline(always)]
/// Reads a 4-byte big-endian length prefix and validates it against `max_payload_length`
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
