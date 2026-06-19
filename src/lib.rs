//! # hydra-sync: A Lightweight Zero-Copy E2E SPMC Relay Network Library
//!
//! `hydra-sync` is a high-performance, end-to-end encrypted relay library for single producer,
//! multiple consumer (SPMC) network architectures. It provides a simple yet powerful abstraction
//! for building distributed systems where one producer broadcasts encrypted data to many consumers
//! with minimal latency, memory overhead while handling slow clients.
//!
//! ### Overview
//!
//! The library is organized around two main components:
//!
//! - **Server** ([`server`]): Manages relay state, maintains active sessions, and routes encrypted frames
//!   from producers to their respective consumers. Uses `DashMap` for thread-safe session storage.
//!
//! - **Client** ([`client`]): Connects to the relay server as either a producer or consumer. Producers
//!   broadcast encrypted frames, while consumers receive and decrypt them from the internal memory pool.
//!
//! ### Protocol
//!
//! The protocol consists of two phases:
//!
//! 1. **Handshake**: Client and server establish a transport key via X25519 Diffie-Hellman key & nonce exchange.
//!    This transport key encrypts join and control frames.
//!
//! 2. **Data Transfer**: Producers encrypt broadcast frames using AES-GCM with a session-specific key.
//!    Consumers decrypt received frames using the same session key. Each frame includes an AEAD tag
//!    for integrity verification. (NO SERVER INTERVENTION IN DATA TRANSFER, SERVER ONLY RELAYS ENCRYPTED FRAMES)
//!
//!
//! #### Quick Example
//!
//! ```no_run
//!use hydra_sync::client::{HydraClient, Producer, Consumer};
//!use hydra_sync::server::HydraServer;
//!use anyhow::Result;
//!
//!#[tokio::main]
//!async fn main() -> anyhow::Result<()> {
//!    let (server, server_addr) = HydraServer::bind_default().await?; // bind to os-assigned port
//!    let session_id = [0xFFu8; 64];
//!    let session_key = [0xAAu8; 32];
//!
//!    tokio::spawn(async move { server.run(500).await }); // run in background
//!
//!    // Producer; sends data to all consumers in the session
//!    let mut producer =
//!        HydraClient::<Producer>::connect(server_addr, &session_id, session_key).await?;
//!    producer.broadcast(b"you are an idiot").await?;
//!
//!    // Consumer; receives and decrypts frames from the producer
//!    let mut consumer =
//!        HydraClient::<Consumer>::connect(server_addr, &session_id, session_key).await?;
//!
//!    loop {
//!        let data = consumer.recv().await?;
//!        println!("received {} bytes: {:?}", data.len(), data);
//!
//!        // `data` borrows from `consumer`'s internal memory pool and is
//!        // only valid until the next `recv()` call.
//!        // Copy it out if you need to keep it longer.
//!        break;
//!     }
//!     producer.close().await?; // clean shutdown
//!
//!     Ok(())
//!}
//! ```
//!
//! ### Memory Overhead
//!
//! Each client maintains a 18 MB internal memory pool (`BytesMut`) for:
//! - Buffering encrypted frames during read/write operations
//! - In-place encryption and decryption without allocating new buffers
//! - Reusing memory across multiple `recv()`/`broadcast()` calls
//!
//! This zero-copy design minimizes garbage collection pressure and reduces latency for
//! high-throughput scenarios.
//!
pub mod channel;
pub mod client;
pub mod crypto;
pub(crate) mod log;
pub(crate) mod protocol;
pub mod server;
pub(crate) mod session;

/// Buffer size for `BufReader` and `BufWriter` in TCP operations (6 MB)
pub const BUFFER_SIZE: usize = 1024 * 1024 * 6;
