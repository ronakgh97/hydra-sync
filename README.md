**hydra-sync** is a light-weight zero-copy E2E `one-to-many` SPMC `reliable` relay network library

```rust
use hydra_sync::client::{HydraClient, Producer, Consumer};
use hydra_sync::server::HydraServer;
use std::net::SocketAddr;
use anyhow::Result;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (server, server_addr) = HydraServer::bind_default().await?; // bind to os-assigned port
    let session_id = [0xFFu8; 64];
    let session_key = [0xAAu8; 32];

    tokio::spawn(async move { server.run(500).await }); // run in background

    // Producer; sends data to all consumers in the session
    let mut producer =
        HydraClient::<Producer>::connect(server_addr, &session_id, session_key).await?;
    producer.broadcast(b"date me please girly").await?;

    // Consumer; receives and decrypts frames from the producer
    let mut consumer =
        HydraClient::<Consumer>::connect(server_addr, &session_id, session_key).await?;

    loop {
        let data = consumer.recv().await?;
        println!("received {} bytes: {:?}", data.len(), data);

        // `data` borrows from `consumer`'s internal memory pool and is
        // only valid until the next `recv()` call.
        // Copy it out (e.g. `data.to_vec()`) if you need to keep it longer.
        break;
    }

    // clean FIN shutdown
    producer.close().await?;
    consumer.close().await?;

    Ok(())
}
```