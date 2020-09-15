#![deny(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unconditional_recursion
)]
//! Gimme dat hot goss

use crossbeam_channel::Receiver;

/// The passive scale factor is used to scale the size of the active-view in order
/// to generate the size of the passive-view. This number is cribbed directly
/// from the HyParView paper.
const PASSIVE_SCALE_FACTOR: f64 = 6.0;

/// The active scale factor is used to scale the active-view which overwise could
/// be zero in some cases.
const ACTIVE_SCALE_FACTOR: f64 = 1.0;

const ACTIVE_RANDOM_WALK_LENGTH: u64 = 6;

const PASSIVE_RANDOM_WALK_LENGTH: u64 = 3;

mod error {
    pub type Result<T> = std::result::Result<T, Error>;

    #[derive(thiserror::Error, Debug)]
    pub enum Error {
        #[error("failed to create transport")]
        NewTransportFailed,
        #[error("unable to connect to {0:?}")]
        UnableToConnect(super::Node),
        #[error("unable to send '{0:?}' channel closed")]
        ChannelFailure(super::Message),
        #[error("unknown error in mocks")]
        Unknown,
    }
}

/// Constants used to derive active and passive view sizes
#[derive(Clone, Copy, Debug)]
pub struct NetworkParameters {
    size: f64,
    c: f64,
    k: f64,
    arwl: u64,
    prwl: u64,
}

impl NetworkParameters {
    /// Return the size of the active view given our network parameters
    pub fn active_size(&self) -> u64 {
        (self.size.log10() + self.c).ceil() as u64
    }

    /// Return the size of the passive view given our network parameters
    pub fn passive_size(&self) -> u64 {
        (self.k * (self.size.log10() + self.c)).ceil() as u64
    }

    /// The number of hops a random walk in the active-view can take
    pub fn active_rwl(&self) -> u64 {
        self.arwl
    }

    /// The number of a hops a random walk in the passive-view can take
    pub fn passive_rwl(&self) -> u64 {
        self.prwl
    }

    /// Helper function to build a NetworkParameters struct with the given size
    /// but keeping the default scaling factors.
    pub fn default_with_size(size: f64) -> Self {
        NetworkParameters {
            size: size,
            c: ACTIVE_SCALE_FACTOR,
            k: PASSIVE_SCALE_FACTOR,
            arwl: ACTIVE_RANDOM_WALK_LENGTH,
            prwl: PASSIVE_RANDOM_WALK_LENGTH,
        }
    }
}

impl Default for NetworkParameters {
    /// Default returns the default network parameters used in the HyParView
    /// paper. A network of size 10,000 with C=1, K=6
    /// TODO(rossdylan): Attempt to find actual implementations of HyParView to
    /// pull real params from
    fn default() -> Self {
        NetworkParameters {
            size: 10000.0,
            c: ACTIVE_SCALE_FACTOR,
            k: PASSIVE_SCALE_FACTOR,
            arwl: ACTIVE_RANDOM_WALK_LENGTH,
            prwl: PASSIVE_RANDOM_WALK_LENGTH,
        }
    }
}

/// Node represents a single node from the perspective of the hyparview protocol.
/// It is used as a part of the protocol to uniquely identify a HyParView instance
/// as well as an address used by the `Transport`
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Node {
    host: String,
    port: u16,
}

/// Possible messages that can be sent or received from other nodes in the
/// network.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Message {
    /// Join a node to the network
    Join {
        /// The node issuing this join command
        node: Node,
    },
    /// Forward a join message to other nodes in the network
    ForwardJoin {
        /// The node that originally issued a Join
        node: Node,

        /// The node forwarding this Join
        sender: Node,

        /// The TTL (how many hops) this join should make
        ttl: u32,
    },
    /// Trigger a clean disconnect to the given node
    Disconnect {
        /// The node that issued this Disconnect
        node: Node,
    },
    /// Data is not used for membership operations, but for sending user data
    Data(Vec<u8>),
}

/// Transport is used to describe the operations needed to drive the
/// communications the HyParView protocol uses. All serialization and transport
/// logic is abstracted away behind this trait.
trait Transport: Send + Sync {
    /// Send a Message to the provided Node
    fn send(&self, dest: &Node, msg: &Message) -> error::Result<()>;

    /// Return a clone of rx side of the channel we put incoming messages into
    fn incoming(&self) -> Receiver<Message>;
}

trait NewTransport {
    type Transport: Transport;

    fn new() -> error::Result<Self::Transport>;
}

/// MessageStore is used during normal gossip operation to keep track of the
/// data messages we've already seen. This avoids endless loops.
trait MessageStore: Send + Sync {
    fn add(&self, msg: Message);
    fn exists(&self, msg: Message) -> bool;
    fn size(&self) -> u64;
}

// BoostrapSource is used to generate the initial node to try and join to.
// If we run out of bootstrap nodes an error should be returned
trait BootstrapSource {
    /// Retreive the next possible bootstrap node data from a BootstrapSource
    fn next() -> error::Result<Option<Node>>;
}

struct HyParView<T: Transport> {
    me: Node,
    transport: T,
}

impl<T: Transport> HyParView<T> {
    fn new<NT: NewTransport<Transport = T>>(host: &str, port: u16) -> error::Result<HyParView<T>> {
        let transport = NT::new()?;
        let hpv = HyParView {
            me: Node {
                host: String::from(host),
                port: port,
            },
            transport: transport,
        };
        Ok(hpv)
    }

    fn with_transport(host: &str, port: u16, transport: T) -> HyParView<T> {
        HyParView {
            me: Node {
                host: String::from(host),
                port: port,
            },
            transport: transport,
        }
    }
}

mod mock_transport {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crossbeam_channel::{unbounded, Receiver, SendError, Sender};
    use parking_lot::Mutex;

    #[derive(Clone, Debug)]
    pub struct MockConnections {
        connections:
            Arc<Mutex<HashMap<super::Node, (Sender<super::Message>, Receiver<super::Message>)>>>,
    }

    pub struct MockTransport {
        conns: MockConnections,
        tx: Sender<super::Message>,
        rx: Receiver<super::Message>,
    }

    impl super::Transport for MockTransport {
        /// Send a Message to the provided Node
        fn send(&self, dest: &super::Node, msg: &super::Message) -> crate::error::Result<()> {
            self.conns.send_to(dest, msg)
        }

        /// Return a clone of rx side of the channel we put incoming messages into
        fn incoming(&self) -> Receiver<super::Message> {
            return self.rx.clone();
        }
    }

    impl MockConnections {
        pub fn new() -> Self {
            MockConnections {
                connections: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn send_to(&self, dest: &super::Node, msg: &super::Message) -> crate::error::Result<()> {
            let conns = self.connections.lock();
            if let Some((tx, _)) = conns.get(&dest) {
                tx.send(msg.clone())
                    .map_err(|e| crate::error::Error::ChannelFailure(e.into_inner()))
            } else {
                return Err(crate::error::Error::UnableToConnect(dest.clone()));
            }
        }

        pub fn new_transport(&self, node: &super::Node) -> MockTransport {
            let (tx, rx) = unbounded();
            let mut conns = self.connections.lock();
            conns.insert(node.clone(), (tx.clone(), rx.clone()));
            MockTransport {
                conns: self.clone(),
                tx,
                rx,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_creation() {
        let conns = mock_transport::MockConnections::new();
        let t1 = conns.new_transport(&Node {
            host: "localhost".into(),
            port: 4200,
        });
        let t2 = conns.new_transport(&Node {
            host: "localhost".into(),
            port: 6900,
        });

        let h1 = HyParView::with_transport("localhost", 4200, t1);
        let h2 = HyParView::with_transport("localhost", 6900, t2);
    }

    #[test]
    fn test_network_params() {
        let params = NetworkParameters::default();
        assert_eq!(params.active_size(), 5);
        assert_eq!(params.passive_size(), 30);
    }
}
