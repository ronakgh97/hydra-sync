use aes_gcm::aead::Aead;
use aes_gcm::{AeadInOut, Aes256Gcm, Tag};
use aes_gcm::{KeyInit, Nonce};
use anyhow::Result;
use rand::Rng;
use x25519_dalek::{EphemeralSecret, PublicKey};

pub const NONCE_LEN: usize = 12;
pub const TAG_LEN: usize = 16;

/// Encrypt `data` with a 32-byte `key` using AES-256-GCM,
/// returns a byte vector containing the nonce, ciphertext, and tag or an error if encryption fails.
#[inline]
#[allow(unused)]
pub fn encrypt_data(data: &[u8], key: &[u8; 32]) -> Result<Vec<u8>> {
    // generate random 12-byte nonce
    let mut nonce = [0u8; NONCE_LEN];
    rand::rng().fill_bytes(&mut nonce);

    let cipher = Aes256Gcm::new(key.into());

    let ciphertext = cipher
        .encrypt(&nonce.into(), data)
        .map_err(|e| anyhow::anyhow!("Encryption failed: {}", e))?;

    // output; nonce || ciphertext || tag
    let mut out = vec![0u8; NONCE_LEN + ciphertext.len()];
    out[..NONCE_LEN].copy_from_slice(&nonce);
    out[NONCE_LEN..].copy_from_slice(&ciphertext); // aes-gcm add extra 12 bytes for tag
    Ok(out)
}

/// Decrypt `data` that was encrypted with [`encrypt_data`],
/// returns the original plaintext or an error if decryption fails.
#[inline]
#[allow(unused)]
pub fn decrypt_data(data: &[u8], key: &[u8; 32]) -> Result<Vec<u8>> {
    if data.len() < NONCE_LEN + TAG_LEN {
        anyhow::bail!("Ciphertext too short");
    }

    let (nonce, ciphertext) = data.split_at(NONCE_LEN);

    let nonce = Nonce::try_from(nonce)?;
    let cipher = Aes256Gcm::new(key.into());

    let plaintext = cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|e| anyhow::anyhow!("Decryption failed: {}", e))?;
    Ok(plaintext)
}

#[inline(always)]
/// Encrypt `input` into `output` using AES-256-GCM with the provided 32-byte `key`.
/// The `output` buffer must be at least `input.len() + NONCE_LEN + TAG_LEN` bytes long,
/// returns the total number of bytes written to `output` (nonce + ciphertext + tag) or an error if encryption fails.
pub fn encrypt_into(input: &[u8], output: &mut [u8], key: &[u8; 32]) -> Result<usize> {
    let plaintext_len = input.len();

    if output.len() < NONCE_LEN + plaintext_len + TAG_LEN {
        anyhow::bail!(
            "Output buffer too small, need at least {} bytes",
            NONCE_LEN + plaintext_len + TAG_LEN
        );
    }

    let (nonce_buf, data_tag) = output.split_at_mut(NONCE_LEN);
    let (data, tag_buf) = data_tag.split_at_mut(plaintext_len);

    // fill nonce fresh
    rand::rng().fill_bytes(nonce_buf);
    // copy plaintext
    data.copy_from_slice(input);

    let cipher = Aes256Gcm::new(key.into());
    let nonce = Nonce::try_from(&*nonce_buf)?;

    let auth_tag = cipher
        .encrypt_inout_detached(&nonce, b"", data.into())
        .map_err(|e| anyhow::anyhow!(e))?;

    tag_buf.copy_from_slice(&auth_tag);

    Ok(NONCE_LEN + plaintext_len + TAG_LEN)
}

#[inline(always)]
/// Decrypt `input` (which should be in the format produced by [`encrypt_into`]) into `output` using AES-256-GCM with the provided 32-byte `key`.
/// The `output` buffer must  be `input.len() - NONCE_LEN - TAG_LEN` bytes,
/// returns the number of bytes written to `output` (the length of the decrypted plaintext) or an error if decryption fails.
pub fn decrypt_into(input: &[u8], output: &mut [u8], key: &[u8; 32]) -> Result<usize> {
    if input.len() < NONCE_LEN + TAG_LEN {
        anyhow::bail!("Ciphertext too short");
    }
    let ciphertext_len = input.len() - NONCE_LEN - TAG_LEN;

    if output.len() < ciphertext_len {
        anyhow::bail!(
            "Output buffer too small, need at least {} bytes",
            ciphertext_len
        );
    }
    let cipher = Aes256Gcm::new(key.into());

    // extract nonce & tag
    let nonce = Nonce::try_from(&input[..NONCE_LEN])?;
    let tag = Tag::try_from(&input[NONCE_LEN + ciphertext_len..])?;
    // ciphertext region only
    let data = &mut output[..ciphertext_len];
    data.copy_from_slice(&input[NONCE_LEN..NONCE_LEN + ciphertext_len]);

    cipher
        .decrypt_inout_detached(&nonce, b"", data.into(), &tag)
        .map_err(|e| anyhow::anyhow!(e))?;

    Ok(ciphertext_len)
}

/// Generates and returns a new X25519 keypair (private_key, public_key)
#[inline(always)]
pub fn generate_x25519_keypair() -> Result<(EphemeralSecret, PublicKey)> {
    let private_key = EphemeralSecret::random_from_rng(&mut rand::rng());
    let public_key = PublicKey::from(&private_key);
    Ok((private_key, public_key))
}

#[test]
fn crypto_test() -> Result<()> {
    let mut key = [0u8; 32];
    rand::rng().fill_bytes(&mut key);

    let mut data = vec![0u8; 64 * 1024 * 1024];
    rand::rng().fill_bytes(&mut data);

    let encrypted = encrypt_data(&data, &key)?;
    assert_eq!(encrypted.len(), data.len() + NONCE_LEN + TAG_LEN);

    let decrypted = decrypt_data(&encrypted, &key)?;
    assert_eq!(decrypted, data);

    let data: &[u8] = b"";

    let encrypted = encrypt_data(data, &key)?;
    assert_eq!(encrypted.len(), NONCE_LEN + TAG_LEN);

    let decrypted = decrypt_data(&encrypted, &key)?;
    assert!(decrypted.is_empty());

    Ok(())
}

#[test]
fn crypto_in_place_test() -> Result<()> {
    let mut key = [0u8; 32];
    rand::rng().fill_bytes(&mut key);

    let mut data = vec![0u8; 64 * 1024 * 1024];
    rand::rng().fill_bytes(&mut data);

    let mut encrypted_buf = vec![0u8; NONCE_LEN + data.len() + TAG_LEN];
    encrypt_into(&data, &mut encrypted_buf, &key)?;
    assert_eq!(encrypted_buf.len(), data.len() + NONCE_LEN + TAG_LEN);

    let mut decrypted_buf = vec![0u8; data.len()];
    decrypt_into(&encrypted_buf, &mut decrypted_buf, &key)?;
    assert_eq!(decrypted_buf, data);

    let empty = Vec::new();
    let mut encrypted_buf = vec![0u8; NONCE_LEN + TAG_LEN];
    encrypt_into(&empty, &mut encrypted_buf, &key)?;
    assert_eq!(encrypted_buf.len(), NONCE_LEN + TAG_LEN);

    let mut decrypted_buf = vec![0u8; encrypted_buf.len()];
    let plaintext_len = decrypt_into(&encrypted_buf, &mut decrypted_buf, &key)?;
    assert_eq!(plaintext_len, 0);

    Ok(())
}
