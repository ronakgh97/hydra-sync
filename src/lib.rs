pub mod client;
pub(crate) mod crypto;
pub(crate) mod protocol;
pub mod server;
pub(crate) mod session;

pub const BUFFER_SIZE: usize = 1024 * 1024 * 6;
