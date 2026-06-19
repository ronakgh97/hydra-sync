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

#### yapping about ring buffers
**some issue with using `tokio::mpsc & broadcast channel` for streaming service or general purpose relay cluster**
the naive way is use tokio::broadcast::channel(big_capacity), it stays simple, just a ring buffer
you drop client when they lagged or closed, but we want `SPEED BABY` and reliability, to achieve that we have few options 
you use `unbounded channel (time drop)` and with some few tweaks, you make it resizable `on demand`, now `"resizable"` can many factors, mainly two
because we want to stop fast producer and let slow client pace up, so we do tokio::sleep, based on some `"X"`, X can be channel size, 
some ratio or anything that is related to consumer `.recv()` (**SPOILER: IT'S HARD TO FIND THAT 'X', 
BECAUSE WE CANT JUST APPROX IT, IT MUST BE FINE-TUNED, ACCORDING TO CHANNEL RESIZE LOGIC**),
and for consumer we increase buffer size, which needs `mutex.lock`, so it became very fragile in the end,
or you never do `resize` and just throttle producer, that's works very well for most predictable cases. or you do
`disk write temporarily` which is most clean way to do it, `slowest consumer` never drops, producer can be `fast as fuck`, but you lose latency
**IN NUTSHELL, MANAGE CHANNEL SIZE AND THROTTLING CAREFULLY, OR WRITE TO DISK AND CALL IT DAY.**
but there are more walls, `ordering of packet` is not guaranteed, you would `mpsc channel` per consumer and if `try_send()` fails, you write to disk
now which one will poll consumer pull then? latest packet in channel or oldest packet in disk? how would you reorder them, or let alone keep `send` & `recv` from slow disk forever?