//! The transport module defines traits and default implementations for
//! communication between hyparview instances.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::proto::hyparview_client::HyparviewClient;
use crate::proto::Peer;

use lru::LruCache;
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
    connections: Arc<Mutex<LruCache<Peer, HyparviewClient<Channel>>>>,
}

impl DefaultConnectionManager {
    /// Construct an new DefaultConnectionManager
    pub fn new() -> Self {
        DefaultConnectionManager {
            // NOTE(rossdylan): 32 connections chosen by ~*science*~
            connections: Arc::new(Mutex::new(LruCache::new(32))),
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
        let channel = ep.timeout(Duration::from_secs(1)).connect().await?;
        let client = HyparviewClient::new(channel);
        self.connections
            .lock()
            .unwrap()
            .put(peer.clone(), client.clone());
        Ok(client)
    }

    async fn disconnect(&self, peer: &Peer) -> Option<HyparviewClient<Channel>> {
        (*self.connections.lock().unwrap()).pop(peer)
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
    connections: Arc<Mutex<LruCache<Peer, HyparviewClient<Channel>>>>,
}

impl InMemoryConnectionManager {
    /// Construct a new a [`ConnectionManager`] used for mocking peer to peer
    /// communications in tests.
    pub fn new(graph: InMemoryConnectionGraph) -> Self {
        InMemoryConnectionManager {
            graph,
            connections: Arc::new(Mutex::new(LruCache::new(32))),
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
                .timeout(Duration::from_secs(1))
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
                .put(peer.clone(), hpv_client.clone());
            Ok(hpv_client)
        }
    }

    async fn disconnect(&self, peer: &Peer) -> Option<HyparviewClient<Channel>> {
        (*self.connections.lock().unwrap()).pop(peer)
    }
}

/// BoostrapSource is used to generate the initial node to try and join to.
/// If we run out of bootstrap nodes an error should be returned
#[async_trait::async_trait]
pub trait BootstrapSource: Send + Sync {
    /// Retreive the next possible bootstrap node data from a BootstrapSource
    async fn peers(&mut self) -> Result<Vec<Peer>>;
}

#[async_trait::async_trait]
impl<S: std::net::ToSocketAddrs + Send + Sync + Clone> BootstrapSource for S {
    async fn peers(&mut self) -> Result<Vec<Peer>> {
        let addrs = self.to_socket_addrs()?;
        Ok(addrs.map(Into::into).collect())
    }
}

/// A [`BootstrapSource`] implementation that uses [`trust-dns-resolver`] to
/// find instances of hyparview via dns lookups. It is expected that users of
/// this bootstrapper know the port their systems are listening on and only
/// want to resolve addresses.
#[allow(missing_debug_implementations)]
pub struct DNSBootstrapSource {
    domain: String,
    port: u16,
    nxdomain_as_empty: bool,
}

impl DNSBootstrapSource {
    /// Create a new bootstrap source that resolves hyparview instances via
    /// DNS.
    pub fn new(domain: &str, port: u16, nxdomain_as_empty: bool) -> Self {
        Self {
            domain: domain.into(),
            port,
            nxdomain_as_empty,
        }
    }
}

#[async_trait::async_trait]
impl BootstrapSource for DNSBootstrapSource {
    async fn peers(&mut self) -> Result<Vec<Peer>> {
        let resolver = trust_dns_resolver::TokioAsyncResolver::tokio_from_system_conf()?;
        let result_res = resolver.lookup_ip(&self.domain).await;
        let records = match result_res {
            Ok(records) => records,
            Err(e) => match e.kind() {
                trust_dns_resolver::error::ResolveErrorKind::NoRecordsFound {
                    response_code,
                    ..
                } if self.nxdomain_as_empty => return Ok(vec![]),
                _ => return Err(e.into()),
            },
        };
        let peers = records
            .iter()
            .map(|r| Peer {
                host: r.to_string(),
                port: self.port as u32,
            })
            .collect();
        Ok(peers)
    }
}

#[cfg(test)]
mod tests {
    use super::BootstrapSource;
    use crate::error::Result;

    #[tokio::test]
    async fn test_dns_bootstrap_nxdomain() -> Result<()> {
        let mut bs = super::DNSBootstrapSource::new("nxdomain.internal", 8888, true);
        let peers = bs.peers().await?;
        assert!(peers.is_empty());

        let mut bs = super::DNSBootstrapSource::new("nxdomain.internal", 8888, false);
        let result = bs.peers().await;
        assert!(result.is_err());
        Ok(())
    }
}
