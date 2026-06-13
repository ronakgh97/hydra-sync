use hydra_sync::client::HydraClient;
use hydra_sync::server::HydraServer;
use rand::random;
use std::net::SocketAddr;

#[tokio::test]
async fn basic_relay() {
    let server_addr = "127.0.0.1:6969".parse::<SocketAddr>().unwrap();
    let server = HydraServer::bind(&server_addr, None, None, None)
        .await
        .unwrap();
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
    let server_addr = "127.0.0.1:6970".parse::<SocketAddr>().unwrap();
    let server = HydraServer::bind(&server_addr, None, None, None)
        .await
        .unwrap();
    tokio::spawn(async move { server.run(500).await });

    let session_id = random();
    let session_key = random();

    let mut producer = HydraClient::connect_producer(server_addr, &session_id, session_key)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let consumer_count = 8;
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
