**hydra-sync** is a light-weight zero-copy E2E `one-to-many` SPMC relay network library

```rust
use hydra_sync::client::HydraClient;
use hydra_sync::server::HydraServer;
use anyhow::Result;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (server, server_addr) = HydraServer::bind_default().await?; // bind to os-assigned port
    let session_id = [0xFFu8; 64];
    let session_key = [0xAAu8; 32];

    tokio::spawn(async move { server.run(500).await }); // run in background

    // Producer; sends data to all consumers in the session
    let mut producer =
        HydraClient::connect_producer(server_addr, &session_id, session_key).await?;
    producer.broadcast(b"you are an idiot").await?;

    // Consumer; receives and decrypts frames from the producer
    let mut consumer =
        HydraClient::connect_consumer(server_addr, &session_id, session_key).await?;

    loop {
        let data = consumer.recv().await?;
        println!("received {} bytes: {:?}", data.len(), data);

        // `data` borrows from `consumer`'s internal memory pool and is
        // only valid until the next `recv()` or `broadcast()` call.
        // Copy it out (e.g. `data.to_vec()`) if you need to keep it longer.
        break;
    }

    producer.close().await?; // clean shutdown
    
    Ok(())
}
```