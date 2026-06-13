use hydra_sync::client::HydraClient;
use hydra_sync::server::HydraServer;
use rand::random;

#[tokio::test]
async fn basic_relay() {
    let (server, server_addr) = HydraServer::bind_default().await.unwrap();
    tokio::spawn(async move { server.run(500).await });

    let session_id = random();
    let session_key = random();

    let mut producer = HydraClient::connect_producer(server_addr, &session_id, session_key)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut consumer = HydraClient::connect_consumer(server_addr, &session_id, session_key)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let data = b"hello relay";
    producer.broadcast(data).await.unwrap();

    let received = tokio::time::timeout(std::time::Duration::from_secs(5), consumer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received, data);
}

#[tokio::test]
async fn concurrent_multi_consumer_relay() {
    let (server, server_addr) = HydraServer::bind_default().await.unwrap();
    tokio::spawn(async move { server.run(500).await });

    let session_id = random();
    let session_key = random();

    let mut producer = HydraClient::connect_producer(server_addr, &session_id, session_key)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let consumer_count = 16;
    let connect_handles: Vec<_> = (0..consumer_count)
        .map(|_| {
            tokio::spawn(async move {
                HydraClient::connect_consumer(server_addr, &session_id, session_key).await
            })
        })
        .collect();

    let mut consumers = Vec::with_capacity(consumer_count);
    for handle in connect_handles {
        consumers.push(handle.await.unwrap().unwrap());
    }

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let data = b"hello concurrent relay";
    producer.broadcast(data).await.unwrap();

    let recv_handles: Vec<_> = consumers
        .into_iter()
        .map(|mut consumer| {
            tokio::spawn(async move {
                tokio::time::timeout(std::time::Duration::from_secs(5), consumer.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .to_vec()
            })
        })
        .collect();

    for handle in recv_handles {
        let received = handle.await.unwrap();
        assert_eq!(received, data);
    }
}

#[tokio::test]
async fn continuous_stream_relay() {
    let (server, server_addr) = HydraServer::bind_default().await.unwrap();
    tokio::spawn(async move { server.run(500).await });

    let session_id = random();
    let session_key = random();

    let mut producer = HydraClient::connect_producer(server_addr, &session_id, session_key)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut consumer = HydraClient::connect_consumer(server_addr, &session_id, session_key)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let message_count = 128;

    let consumer_handle = tokio::spawn(async move {
        for i in 0..message_count {
            let expected_payload = format!("live_stream_packet_{}", i).into_bytes();

            let received = tokio::time::timeout(std::time::Duration::from_secs(2), consumer.recv())
                .await
                .expect("Consumer timed out waiting for a packet")
                .expect("Consumer channel closed unexpectedly");

            assert_eq!(received, expected_payload, "Packet mismatch at index {}", i);
        }
    });

    for i in 0..message_count {
        let payload = format!("live_stream_packet_{}", i).into_bytes();
        producer.broadcast(&payload).await.unwrap();

        // prevents overwhelming the local buffer instantly
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    consumer_handle.await.unwrap();
}
