//! In memory implementations of the various hyparview components

use std::collections::HashMap;
use std::sync::Arc;

use crossbeam_channel::{unbounded, Receiver, Sender};
use parking_lot::Mutex;

/// MockConnections is a helper structure that tracks all the nodes in a simulated
/// gossip network. Internally we map between a [Node] and tuple of
/// ([Sender]<[Message]>, [Receiver]<[Message]>). This allows us to create a
/// [MockTransport] that can route to any node in the network.
///
/// [Sender]: crossbeam_channel::Sender
/// [Receiver]: crossbeam_channel::Receiver
/// [Message]: super::Message
/// [Node]: super::Node
#[derive(Clone, Debug)]
pub struct MockConnections {
    connections:
        Arc<Mutex<HashMap<super::Node, (Sender<super::Message>, Receiver<super::Message>)>>>,
}

/// The MockTransport is constructed via [MockConnections::new_transport] and
/// implements [Transport]. Internally it uses crossbeam channels to route
/// messages.
///
/// [Transport]: super::Transport
#[derive(Debug, Clone)]
pub struct MockTransport {
    conns: MockConnections,
    rx: Receiver<super::Message>,
    ftx: Sender<super::Failure>,
    frx: Receiver<super::Failure>,
}

impl super::Transport for MockTransport {
    /// Send a Message to the provided Node
    fn send(&self, dest: &super::Node, msg: &super::Message) {
        if let Err(_) = self.conns.send_to(dest, msg) {
            let tres = self.ftx.send(super::Failure::FailedToSend(dest.clone()));
            if let Err(e) = tres {
                panic!("mock transport failed: {}", e);
            }
        }
    }

    /// Return true if our mock connections thingy has a set of channels for
    /// the node we want to connect to
    fn connect(&self, dest: &super::Node) -> crate::error::Result<()> {
        if self.conns.connections.lock().contains_key(dest) {
            Ok(())
        } else {
            Err(crate::error::Error::UnableToConnect(dest.clone()))
        }
    }

    /// Return a rx channel for communication failures
    fn failures(&self) -> Receiver<super::Failure> {
        self.frx.clone()
    }

    /// Return the rx side of the channel we put incoming messages into
    fn incoming(&self) -> Receiver<super::Message> {
        self.rx.clone()
    }
}

impl MockConnections {
    /// Create a new MockConnections instance
    pub fn new() -> Self {
        MockConnections {
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Used internally by MockTransport to route messages to the intended Node
    fn send_to(&self, dest: &super::Node, msg: &super::Message) -> crate::error::Result<()> {
        let conns = self.connections.lock();
        if let Some((tx, _)) = conns.get(&dest) {
            tx.send(msg.clone())
                .map_err(|e| crate::error::Error::ChannelFailure(e.into_inner()))
        } else {
            return Err(crate::error::Error::UnableToConnect(dest.clone()));
        }
    }

    /// Create a new node in the network and return a MockTransport for it
    pub fn new_transport(&self, node: &super::Node) -> MockTransport {
        let (tx, rx) = unbounded();
        let (ftx, frx) = unbounded();
        let mut conns = self.connections.lock();
        conns.insert(node.clone(), (tx.clone(), rx.clone()));
        MockTransport {
            conns: self.clone(),
            rx,
            frx,
            ftx,
        }
    }
}
