#[allow(unused)]
use crate::channel::{ConsumerChannel, ProducerChannel};
use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use tokio::sync::broadcast;

/// Manages active sessions, producers, and broadcast channels for consumers.
/// Each session is identified by a unique 64-byte session_id.
pub struct Sessions {
    /// inner concurrent map of session_id → broadcast sender for routing producer data to consumers
    map: DashMap<[u8; 64], broadcast::Sender<Bytes>>,
    /// tracks registered producers to enforce single producer per session constraint
    producers: DashMap<[u8; 64], ()>,
}

impl Sessions {
    /// Init Sessions struct with empty DashMaps for producers and broadcast channels.
    pub fn init() -> Self {
        Self {
            map: DashMap::with_capacity(256),
            producers: DashMap::with_capacity(256),
        }
    }

    /// Tries to register a producer for the given session_id,
    /// returns an error if a producer is already registered or returns `broadcast::Sender<Bytes>`
    pub fn try_register_producer(
        &self,
        session_id: [u8; 64],
        capacity: usize,
    ) -> Result<broadcast::Sender<Bytes>> {
        match self.producers.entry(session_id) {
            Entry::Occupied(_) => anyhow::bail!("Session already has a producer"),
            Entry::Vacant(entry) => {
                entry.insert(());
                let tx = self
                    .map
                    .entry(session_id)
                    .or_insert_with(|| broadcast::channel(capacity).0) // put Sender<()>
                    .clone();
                Ok(tx)
            }
        }
    }

    /// Removes the producer and broadcast sender for the given session_id, should be called when a producer disconnects to clean up resources.
    /// Consumers will receive a RecvError::Closed if they try to receive after this is called.
    pub fn unregister_producer(&self, session_id: [u8; 64]) {
        self.producers.remove(&session_id);
        self.map.remove(&session_id);
    }

    /// Returns a ref clone of the `broadcast::sender` for the given session_id, or None if no producer is registered
    pub fn get_session(&self, session_id: [u8; 64]) -> Option<broadcast::Sender<Bytes>> {
        self.map.get(&session_id).map(|r| r.clone()) // ref clone
    }
}
