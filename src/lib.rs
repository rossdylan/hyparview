#![deny(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unconditional_recursion
)]
//! Gimme dat hot goss. The hyparview crate is an implementation of the
//! Hybrid Partial View gossip algorithm described in this [paper][1]. This
//! specific implementation is not intended to be generic, or one-size-fits-all.
//! It provides a concrete implementation with the only extension point being
//! how [`tonic`] (the grpc implemention) connects to peers.
//!
//! [1]: https://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf

pub mod core;
pub mod error;
pub(crate) mod failure;
pub(crate) mod message_store;
pub(crate) mod metrics;
pub mod state;
pub mod transport;
pub(crate) mod util;

pub mod prelude {
    //! Wombo Combo of important types/structs for actually using hyparview
    pub use crate::core::HyParView;
    pub use crate::error::{Error, Result};
    pub use crate::state::NetworkParameters;
    pub use crate::transport::{BootstrapSource, ConnectionManager, DefaultConnectionManager};
}

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
