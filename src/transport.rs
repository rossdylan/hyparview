//! The transport module defines traits and default implementations for
//! communication between hyparview instances.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use crate::error::{Error, Result};
use crate::proto::hyparview_client::HyparviewClient;
use crate::proto::Peer;

use tokio::io::DuplexStream;
use tokio::sync::mpsc;
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
        if let Some(client) = (*self.connections.lock().unwrap()).get(peer) {
            return Ok(client.clone());
        }
        let ep = Endpoint::from_shared(format!("http://{}:{}", peer.host, peer.port))?;
        let channel = ep.connect().await?;
        let client = HyparviewClient::new(channel);
        self.connections
            .lock()
            .unwrap()
            .insert(peer.clone(), client.clone());
        Ok(client)
    }

    async fn disconnect(&self, peer: &Peer) -> Option<HyparviewClient<Channel>> {
        (*self.connections.lock().unwrap()).remove(peer)
    }
}

/// Provide a connection graph between HyParView instances using in-memory
/// channels instead of TCP
#[derive(Debug, Clone)]
pub struct InMemoryConnectionGraph {
    /// A mapping between a given peer and the [`mpsc::Sender`] we
    /// use to transmit new [`DuplexStream`] instances to a peer's tonic server
    /// This is used to replicate the full tcp connection lifecycle without
    /// actually using tcp
    incoming_channels: Arc<Mutex<HashMap<Peer, mpsc::Sender<DuplexStream>>>>,
}

impl Default for InMemoryConnectionGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryConnectionGraph {
    /// Create a new connection graph
    pub fn new() -> Self {
        InMemoryConnectionGraph {
            incoming_channels: Default::default(),
        }
    }

    /// Register a HyParView server instance with the connection graph so it
    /// can receive [`DuplexStream`] instances when other HyParView instnaces
    /// want to connect to it
    pub async fn register(&self, peer: &Peer) -> Result<mpsc::Receiver<DuplexStream>> {
        let mut channels = self.incoming_channels.lock().unwrap();
        if channels.contains_key(peer) {
            return Err(Error::MockServerAlreadyRegistered);
        }
        let (tx, rx) = mpsc::channel::<DuplexStream>(1024); // yolo
        channels.insert(peer.clone(), tx);
        Ok(rx)
    }

    /// Attempt to construct a [`DuplexStream`] to the requested peer server
    /// instance. If the server has shutdown, or hasn't been registered an error
    /// will be returned
    pub async fn connect(&self, dest: &Peer) -> Result<DuplexStream> {
        let maybe_sender = self
            .incoming_channels
            .lock()
            .unwrap()
            .get(dest)
            .map(Clone::clone);
        if let Some(sender) = maybe_sender {
            let (tx, rx) = tokio::io::duplex(1024);
            if sender.send(rx).await.is_err() {
                Err(Error::MockError(
                    "failed to send DuplexStream to server".into(),
                ))
            } else {
                Ok(tx)
            }
        } else {
            Err(Error::MockError("requested peer does not exit".into()))
        }
    }

    /// Construct a new manager for a given peer. Internally we pass a clone of
    /// ourselves to the manager so it can create new connections to peers
    pub fn new_manager(&self) -> InMemoryConnectionManager {
        InMemoryConnectionManager::new(self.clone())
    }
}
/// A mock conneciton manager using [`tokio::io::duplex`] to build in-memory links
/// between peers. This is used in tests for mocking out the TCP based
/// connections between peers.
#[derive(Debug, Clone)]
pub struct InMemoryConnectionManager {
    graph: InMemoryConnectionGraph,
    connections: Arc<Mutex<HashMap<Peer, HyparviewClient<Channel>>>>,
}

impl InMemoryConnectionManager {
    /// Construct a new a [`ConnectionManager`] used for mocking peer to peer
    /// communications in tests.
    pub fn new(graph: InMemoryConnectionGraph) -> Self {
        InMemoryConnectionManager {
            graph,
            connections: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl ConnectionManager for InMemoryConnectionManager {
    async fn connect(&self, peer: &Peer) -> Result<HyparviewClient<Channel>> {
        trace!("connecting to peer {}", peer);
        let maybe_client = (*self.connections.lock().unwrap())
            .get(peer)
            .map(Clone::clone);
        if let Some(client) = maybe_client {
            Ok(client)
        } else {
            let client = self.graph.connect(peer).await?;
            let mut some_client = Some(client);
            let channel = Endpoint::try_from(format!("http://[::]:{}", peer.port))?
                .connect_with_connector(service_fn(move |_: Uri| {
                    let client = some_client.take();
                    async move {
                        match client {
                            Some(c) => Ok(c),
                            None => Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "mock client already taken",
                            )),
                        }
                    }
                }))
                .await?;
            let hpv_client = HyparviewClient::new(channel);
            self.connections
                .lock()
                .unwrap()
                .insert(peer.clone(), hpv_client.clone());
            Ok(hpv_client)
        }
    }

    async fn disconnect(&self, peer: &Peer) -> Option<HyparviewClient<Channel>> {
        (*self.connections.lock().unwrap()).remove(peer)
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
