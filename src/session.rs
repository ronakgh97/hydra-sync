use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use tokio::sync::broadcast;

pub struct Sessions {
    map: DashMap<[u8; 64], broadcast::Sender<Bytes>>,
    producers: DashMap<[u8; 64], ()>,
}

impl Sessions {
    pub fn init() -> Self {
        Self {
            map: DashMap::with_capacity(256),
            producers: DashMap::with_capacity(256),
        }
    }

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
                    .or_insert_with(|| broadcast::channel(capacity).0)
                    .clone();
                Ok(tx)
            }
        }
    }

    pub fn unregister_producer(&self, session_id: [u8; 64]) {
        self.producers.remove(&session_id);
        self.map.remove(&session_id);
    }

    pub fn get_for_consumer(&self, session_id: [u8; 64]) -> Option<broadcast::Sender<Bytes>> {
        self.map.get(&session_id).map(|r| r.clone())
    }
}
