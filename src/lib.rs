#![deny(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unconditional_recursion
)]
//! Gimme dat hot goss

pub mod core;
pub mod error;
pub(crate) mod message_store;
pub mod state;
pub mod transport;
pub(crate) mod util;

mod proto {
    use std::net::{SocketAddr, ToSocketAddrs};
    use std::vec;

    tonic::include_proto!("hyparview");

    /// Define an extra From impl for our Peer structure for easy conversions
    impl From<std::net::SocketAddr> for Peer {
        fn from(addr: std::net::SocketAddr) -> Self {
            Peer {
                host: addr.ip().to_string(),
                port: addr.port() as u32,
            }
        }
    }

    /// Define the reciprical of `From<SocketAddr>` for peer for easier
    /// back-and-forth conversions
    impl ToSocketAddrs for Peer {
        type Iter = vec::IntoIter<SocketAddr>;

        fn to_socket_addrs(&self) -> std::io::Result<Self::Iter> {
            (self.host.clone(), self.port as u16).to_socket_addrs()
        }
    }

    /// Make `Peer` implement `Display` so we can log them more easily
    impl std::fmt::Display for Peer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Peer({}:{})", self.host, self.port)
        }
    }
}
