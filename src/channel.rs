use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Determines what happens when a consumer's channel is full.
/// [Issue](https://github.com/ronakgh97/hydra-sync/issues/1)
///
/// `TODO: Mode will be implement in future versions`
pub enum OverflowChannelMode {
    /// Producer blocks until channel has space.
    BackPressure,
    /// Write overflow to a temporary file on disk.
    WriteDisk,
    /// Dynamically grow/shrink the channel buffer.
    ResizableBuffer,
    /// Disconnect the slow consumers.
    DropClient,
}

pub(crate) type ProducerChannel = broadcast::Sender<Arc<Bytes>>;
pub(crate) type ConsumerChannel = broadcast::Receiver<Arc<Bytes>>;

#[allow(unused)]
/// Creates a bounded channel pair for producer-consumer communication.
pub(crate) fn channel(capacity: usize) -> (ProducerChannel, ConsumerChannel) {
    broadcast::channel(capacity)
}
