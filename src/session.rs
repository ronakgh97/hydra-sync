use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::broadcast;

pub struct Sessions {
    map: DashMap<[u8; 64], broadcast::Sender<Bytes>>,
}

impl Sessions {
    pub fn init() -> Self {
        Self {
            map: DashMap::with_capacity(256),
        }
    }

    pub fn get_or_create(&self, session_id: [u8; 64], capacity: usize) -> broadcast::Sender<Bytes> {
        self.map
            .entry(session_id)
            .or_insert_with(|| broadcast::channel(capacity).0)
            .clone()
    }

    pub fn get(&self, session_id: [u8; 64]) -> Option<broadcast::Sender<Bytes>> {
        self.map.get(&session_id).map(|r| r.clone())
    }

    pub fn remove(&self, session_id: [u8; 64]) {
        self.map.remove(&session_id);
    }
}
