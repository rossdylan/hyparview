//! The transport module defines traits and default implementations for
//! communication between hyparview instances.
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::proto::hyparview_client::HyparviewClient;
use crate::proto::Peer;

use parking_lot::Mutex;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::trace;

/// ConnectionManager is a trait to allow for different kinds of connection
/// management within the hyparview implementation. This is what will let us
/// mock out the network bits for testing
// TODO(rossdylan): We should see about making this generic across any tower
// service instead of just channel
#[async_trait::async_trait]
pub trait ConnectionManager: Send + Sync + Clone {
    /// Attempt to connect to the given peer, or return an existing connection
    async fn connect(&self, peer: &Peer) -> Result<HyparviewClient<Channel>>;
    /// Remove a given peer connection from the manager so it can be cleaned
    /// up. The actual clients are ref-counted so it may not be disconnected
    /// immediately.
    async fn disconnect(&self, peer: &Peer) -> Option<HyparviewClient<Channel>>;
}

/// The default connection manager uses tonic's standard insecure transport
/// to make connections. Internally clients are tracked using a hashmap keyed
/// by the peer struct.
#[derive(Clone, Debug)]
pub struct DefaultConnectionManager {
    connections: Arc<Mutex<HashMap<Peer, HyparviewClient<Channel>>>>,
}

impl DefaultConnectionManager {
    /// Construct an new DefaultConnectionManager
    pub fn new() -> Self {
        DefaultConnectionManager {
            connections: Default::default(),
        }
    }
}

impl Default for DefaultConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ConnectionManager for DefaultConnectionManager {
    async fn connect(&self, peer: &Peer) -> Result<HyparviewClient<Channel>> {
        if let Some(client) = (*self.connections.lock()).get(peer) {
            return Ok(client.clone());
        }
        let ep = Endpoint::from_shared(format!("http://{}:{}", peer.host, peer.port))?;
        let channel = ep.connect().await?;
        let client = HyparviewClient::new(channel);
        self.connections.lock().insert(peer.clone(), client.clone());
        Ok(client)
    }

    async fn disconnect(&self, peer: &Peer) -> Option<HyparviewClient<Channel>> {
        (*self.connections.lock()).remove(peer)
    }
}

/// A mock conneciton manager using [`tokio::io::duplex`] to build in-memory links
/// between peers. This is used in tests for mocking out the TCP based
/// connections between peers.
#[derive(Debug, Clone)]
pub struct InMemoryConnectionManager {
    connections: Arc<Mutex<HashMap<Peer, HyparviewClient<Channel>>>>,
}

impl InMemoryConnectionManager {
    /// Construct a new a [`ConnectionManager`] used for mocking peer to peer
    /// communications in tests.
    pub fn new() -> Self {
        InMemoryConnectionManager {
            connections: Default::default(),
        }
    }

    /// Given a `Peer` structure build and store the internal `tokio::io::DuplexStream`
    /// structures and return the server side. Internally everything works pretty
    /// much the same as [`DefaultConnectionManager`] since we use the duplex stream
    /// to construct a normal [`tonic::transport::Channel`]
    pub async fn register(&self, peer: &Peer) -> Result<tokio::io::DuplexStream> {
        let (client, server) = tokio::io::duplex(1024);
        let mut some_client = Some(client);
        let channel = Endpoint::try_from(format!("http://[::]:{}", peer.port))?
            .connect_with_connector(service_fn(move |_: Uri| {
                let client = some_client.take();
                async move {
                    match client {
                        Some(c) => Ok(c),
                        None => Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Client already taken",
                        )),
                    }
                }
            }))
            .await?;
        let hpv_client = HyparviewClient::new(channel);
        self.connections.lock().insert(peer.clone(), hpv_client);
        Ok(server)
    }
}

impl Default for InMemoryConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ConnectionManager for InMemoryConnectionManager {
    async fn connect(&self, peer: &Peer) -> Result<HyparviewClient<Channel>> {
        trace!("connecting to peer {}", peer);
        if let Some(client) = (*self.connections.lock()).get(peer) {
            Ok(client.clone())
        } else {
            Err(Error::MockError(format!(
                "peer {} not found in InMemoryConnectionManager",
                peer
            )))
        }
    }

    async fn disconnect(&self, peer: &Peer) -> Option<HyparviewClient<Channel>> {
        (*self.connections.lock()).remove(peer)
    }
}

/// BoostrapSource is used to generate the initial node to try and join to.
/// If we run out of bootstrap nodes an error should be returned
pub trait BootstrapSource {
    /// Retreive the next possible bootstrap node data from a BootstrapSource
    fn next_peer(&mut self) -> Result<Option<Peer>>;

    /// subset returns a set of nodes up to the given subset size. It will
    /// tolerate up to `max_failures` before returning
    /// TODO(rossdylan): This is ugly and strongly imperative, try and fix
    fn subset(&mut self, max_subset: usize, max_failures: usize) -> Result<Vec<Peer>> {
        let mut failures: usize = 0;
        let mut peers = Vec::with_capacity(max_subset);
        while peers.len() < max_subset {
            match self.next_peer() {
                Err(e) => {
                    failures += 1;
                    if failures >= max_failures && peers.is_empty() {
                        return Err(e);
                    } else {
                        return Ok(peers);
                    }
                }
                Ok(on) => {
                    if let Some(n) = on {
                        peers.push(n)
                    } else {
                        if !peers.is_empty() {
                            return Ok(peers);
                        }
                        return Err(Error::NoBootstrapAddrsFound);
                    }
                }
            }
        }
        Ok(peers)
    }
}

/// Implement `BoostrapSource` for all iterators that iterate over something that
/// can be converted into a `SocketAddr`. This lets us easily bootstrap using a
/// vector of strings.
impl<I, S> BootstrapSource for I
where
    I: Iterator<Item = S>,
    S: std::net::ToSocketAddrs,
{
    fn next_peer(&mut self) -> Result<Option<Peer>> {
        let res = match self.next() {
            None => None,
            Some(s) => {
                let mut addrs = s.to_socket_addrs()?;
                addrs.next().map(std::convert::From::from)
            }
        };
        Ok(res)
    }
}
