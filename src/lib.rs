#![deny(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unconditional_recursion
)]
//! Gimme dat hot goss

pub mod message_store;
pub mod mock;

use std::hash::Hash;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{bounded, select, Receiver, Sender};
use indexmap::IndexSet;
use log::*;
use parking_lot::Mutex;
use rand::{seq::IteratorRandom, Rng};

/// The passive scale factor is used to scale the size of the active-view in order
/// to generate the size of the passive-view. This number is cribbed directly
/// from the HyParView paper.
const PASSIVE_SCALE_FACTOR: f64 = 6.0;

/// The active scale factor is used to scale the active-view which overwise could;
/// be zero in some cases.
const ACTIVE_SCALE_FACTOR: f64 = 1.0;

const ACTIVE_RANDOM_WALK_LENGTH: u32 = 6;

const PASSIVE_RANDOM_WALK_LENGTH: u32 = 3;

const MESSAGE_HISTORY: usize = 10_000;

const SHUFFLE_ACTIVE: usize = 3;

const SHUFFLE_PASSIVE: usize = 4;

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
        #[error("unable to parse address")]
        AddrParseFailed(#[from] std::io::Error),
        #[error("message broadcast failed")]
        BroadcastFailed,
        #[error("bootstrap source found no nodes")]
        NoBootstrapAddrsFound,
        #[error("unknown error in mocks")]
        Unknown,
    }
}

/// Constants used to derive active and passive view sizes
#[derive(Clone, Copy, Debug)]
pub struct NetworkParameters {
    /// network size
    size: f64,

    /// active-view scaling factor
    c: f64,

    /// passive-view scaling factor
    k: f64,

    /// active random walk ttl
    arwl: u32,

    /// passive random walk ttl
    prwl: u32,

    /// number of messages to track to avoid routing loops
    msg_history: usize,

    /// number of nodes from the passive-view included in a shuffle
    kp: usize,

    /// number of nodes from the active-view included in a shuffle
    ka: usize,
}

impl NetworkParameters {
    /// Return the size of the active view given our network parameters
    pub fn active_size(&self) -> usize {
        (self.size.log10() + self.c).ceil() as usize
    }

    /// Return the size of the passive view given our network parameters
    pub fn passive_size(&self) -> usize {
        (self.k * (self.size.log10() + self.c)).ceil() as usize
    }

    /// The number of hops a random walk in the active-view can take
    pub fn active_rwl(&self) -> u32 {
        self.arwl
    }

    /// The number of a hops a random walk in the passive-view can take
    pub fn passive_rwl(&self) -> u32 {
        self.prwl
    }

    /// How many messages to track in our history to prevent routing loops
    pub fn message_history(&self) -> usize {
        self.msg_history
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
            msg_history: MESSAGE_HISTORY,
            kp: SHUFFLE_PASSIVE,
            ka: SHUFFLE_ACTIVE,
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
            size: 10_000.0,
            c: ACTIVE_SCALE_FACTOR,
            k: PASSIVE_SCALE_FACTOR,
            arwl: ACTIVE_RANDOM_WALK_LENGTH,
            prwl: PASSIVE_RANDOM_WALK_LENGTH,
            msg_history: MESSAGE_HISTORY,
            kp: SHUFFLE_PASSIVE,
            ka: SHUFFLE_ACTIVE,
        }
    }
}

/// Node represents a single node from the perspective of the hyparview protocol.
/// It is used as a part of the protocol to uniquely identify a HyParView instance
/// as well as an address used by the `Transport`
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Node {
    /// The host this node is running on.
    pub host: String,
    /// The port this node is running on.
    pub port: u16,
}

impl From<std::net::SocketAddr> for Node {
    fn from(addr: std::net::SocketAddr) -> Self {
        Node {
            host: addr.ip().to_string(),
            port: addr.port(),
        }
    }
}
/// Used to configure the priority of a Neighbor message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum NeighborPriority {
    /// High priority Neighbor messages come from nodes that have no members in
    /// the active view.
    High,

    /// Low priority Neighbor messages come from nodes that have members in the
    /// active view.
    Low,
}

/// Possible messages that can be sent or received from other nodes in the
/// network.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum MessageData {
    /// Join a node to the network
    Join,
    /// Forward a join message to other nodes in the network
    ForwardJoin {
        /// The node that originally issued a Join
        node: Node,

        /// The TTL (how many hops) this join should make
        ttl: u32,
    },
    /// Trigger a clean disconnect to the given node
    Disconnect,
    /// Neighbor messages are used to promote a passive-view node into the active
    /// view
    Neighbor {
        /// The priority of this request
        priority: NeighborPriority,
    },
    /// Shuffle is used to periodically shufflerefresh
    Shuffle {
        /// The TTL (how many hops) this shuffle should make
        ttl: u32,

        /// A set of nodes from the originating node's passive view
        nodes: Vec<Node>,
    },
    /// The response to a Shuffle message.
    ShuffleReply {
        /// A set of nodes from the final destination of the Shuffle message
        nodes: Vec<Node>,
    },
    /// Data is not used for membership operations, but for sending user data
    Data(Vec<u8>),
}

/// Envelope for our message. Adds a unique ID to ensure messages hash to a
/// unique value.
/// TODO(rossdylan): Think about adding concept of headers/baggage here to piggy
/// back non-protocol messages (healthchecks/etc) onto standard messages
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Message {
    id: u64,
    from: Node,
    data: MessageData,
}

impl Message {
    /// Create a new `Message` with a randomly generated ID.
    pub fn new(from: Node, data: MessageData) -> Self {
        Self::with_id(from, data, rand::thread_rng().gen())
    }

    // Create a new `Message` with the given ID
    fn with_id(from: Node, data: MessageData, id: u64) -> Self {
        Message {
            id: id,
            from: from,
            data: data,
        }
    }
}

/// The Failure enum is used to inform the HyParView protocol handles of node
/// failures from the Transport layer. The protocol is designed around async
/// messaging so we use channels to move data in and out of the transport layer
#[derive(Debug, Clone)]
pub enum Failure {
    /// The transport failed to send a message to the given node
    FailedToSend(Node),
}

/// Transport is used to describe the operations needed to drive the
/// communications the HyParView protocol uses. All serialization and transport
/// logic is abstracted away behind this trait.
pub trait Transport: Clone + Send + Sync {
    /// Used to do the initial health check of a node, and open a tcp connection
    /// to it. This is synchronous, but actually sending messages is async.
    fn connect(&self, node: &Node) -> error::Result<()>;

    /// Send a Message to the provided Node. This *must not* block
    fn send(&self, dest: &Node, msg: &Message);

    /// Return the rx side of a channel for handling messages from other nodes
    fn incoming(&self) -> Receiver<Message>;

    /// Return the rx side of a channel for handling communication failures
    fn failures(&self) -> Receiver<Failure>;
}

/// BoostrapSource is used to generate the initial node to try and join to.
/// If we run out of bootstrap nodes an error should be returned
pub trait BootstrapSource {
    /// Retreive the next possible bootstrap node data from a BootstrapSource
    fn next_node(&mut self) -> error::Result<Option<Node>>;

    /// subset returns a set of nodes up to the given subset size. It will
    /// tolerate up to `max_failures` before returning
    /// TODO(rossdylan): This is ugly and strongly imperative, try and fix
    fn subset(&mut self, max_subset: usize, max_failures: usize) -> error::Result<Vec<Node>> {
        let mut failures: usize = 0;
        let mut nodes = Vec::with_capacity(max_subset);
        while nodes.len() < max_subset {
            match self.next_node() {
                Err(e) => {
                    failures += 1;
                    if failures >= max_failures && nodes.len() == 0 {
                        return Err(e);
                    } else {
                        return Ok(nodes);
                    }
                }
                Ok(on) => {
                    if let Some(n) = on {
                        nodes.push(n)
                    } else {
                        if nodes.len() > 0 {
                            return Ok(nodes);
                        }
                        return Err(error::Error::NoBootstrapAddrsFound);
                    }
                }
            }
        }
        Ok(nodes)
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
    fn next_node(&mut self) -> error::Result<Option<Node>> {
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

#[derive(Debug, Clone)]
struct State {
    me: Node,
    params: NetworkParameters,
    active_view: IndexSet<Node>,
    passive_view: IndexSet<Node>,
    // TODO(rossdylan): We might be able to get away with something like a bloom
    // filter here to increase space efficiency.
    messages: message_store::MessageStore,
}

fn random_node_from_view(view: &IndexSet<Node>) -> Option<Node> {
    let index = rand::thread_rng().gen_range(0, view.len());
    view.get_index(index).map(Clone::clone)
}

impl State {
    /// Given a node that represents this instance, and the parameters for our
    /// network create a new state.
    pub fn new(me: Node, params: NetworkParameters) -> Self {
        let asize = params.active_size() as usize;
        let psize = params.passive_size() as usize;
        State {
            me: me,
            params: params,
            active_view: IndexSet::with_capacity(asize),
            passive_view: IndexSet::with_capacity(psize),
            messages: message_store::MessageStore::new(params.message_history()),
        }
    }

    /// Select Kp nodes from the passive-view and Ka nodes from the active-view
    /// to service a shuffle request
    pub fn select_shuffle_nodes(&self, ignore: &Node) -> Vec<Node> {
        let len = self.active_view.len();
        let mut nodes = Vec::with_capacity(self.params.ka + self.params.kp);
        nodes.extend(
            std::iter::repeat_with(|| rand::thread_rng().gen_range(0, len))
                .take(len) // Do we need this? I want to make sure we don't iter forever
                .filter_map(|i| self.active_view.get_index(i))
                .filter(|n| *n != ignore)
                .take(self.params.ka)
                .cloned(),
        );
        nodes.extend(
            std::iter::repeat_with(|| rand::thread_rng().gen_range(0, len))
                .take(len) // Do we need this? I want to make sure we don't iter forever
                .filter_map(|i| self.passive_view.get_index(i))
                .filter(|n| *n != ignore)
                .take(self.params.ka)
                .cloned(),
        );
        nodes
    }

    pub fn add_to_active_view(&mut self, node: &Node) -> Option<Node> {
        if self.active_view.contains(node) || *node == self.me {
            return None;
        }
        let dropped = if self.active_view.len() == self.params.active_size() {
            random_node_from_view(&self.active_view)
        } else {
            None
        };
        if let Some(ref dn) = dropped {
            self.active_view.remove(dn);
            self.add_to_passive_view(dn);
        }
        self.active_view.insert(node.clone());
        dropped
    }

    pub fn add_to_passive_view(&mut self, node: &Node) {
        if self.passive_view.contains(node) || self.active_view.contains(node) || self.me == *node {
            return;
        }
        if self.passive_view.len() == self.params.active_size() {
            let to_drop = random_node_from_view(&self.passive_view).unwrap();
            self.passive_view.remove(&to_drop);
        }
        self.passive_view.insert(node.clone());
    }

    pub fn add_message(&mut self, msg: &Message) {
        self.messages.insert(msg);
    }

    pub fn seen_message(&self, msg: &Message) -> bool {
        self.messages.contains(msg)
    }
}

/// Implements the HyParView protocol over a generic transport layer. Messages
/// from the transport layer are recieved over a channel. The protocol is driven
/// by 3 threads.
/// 1. Messages from other nodes are received through the transport's channel and
///    handled in a single thread.
/// 2. Reactive changes to the active-view are handled via a 2nd thread that
///    receives failure notifications over another channel.
/// 3. Periodic passive-view shuffles are executed on a timer in a 3rd thread
#[derive(Clone, Debug)]
pub struct HyParView<T: Transport + 'static> {
    me: Node,
    params: NetworkParameters,
    transport: T,
    state: Arc<Mutex<State>>,
}

impl<T: Transport> HyParView<T> {
    /// Create a new HyParView instance with the provided transport
    pub fn with_transport(
        host: &str,
        port: u16,
        params: NetworkParameters,
        transport: T,
    ) -> HyParView<T> {
        let me = Node {
            host: String::from(host),
            port: port,
        };
        HyParView {
            me: me.clone(),
            params: params.clone(),
            transport: transport,
            state: Arc::new(Mutex::new(State::new(me, params))),
        }
    }

    /// Trigger the initialization of this HyParView instance by trying to find
    /// a bootstrap node and sending it a join.
    pub fn init(&self, boots: &mut impl BootstrapSource) -> error::Result<()> {
        loop {
            if let Some(contact_node) = boots.next_node()? {
                match self.transport.connect(&contact_node) {
                    Ok(_) => {
                        self.state.lock().add_to_active_view(&contact_node);
                        self.transport.send(
                            &contact_node,
                            &Message::new(self.me.clone(), MessageData::Join),
                        );
                        return Ok(());
                    }
                    Err(e) => warn!(
                        "failed to contact bootstrap node: {:?}: {}",
                        contact_node, e
                    ),
                };
            } else {
                return Err(error::Error::NoBootstrapAddrsFound);
            }
        }
    }

    /// Send a message to all nodes in the active-view. If all nodes in the
    /// active-view have failed we return an Err to notify the caller that this
    /// message wasn't sent to anyone.
    fn broadcast(&self, ignore: &Node, msg: &Message) {
        let nodes = self.state.lock().active_view.clone();
        for node in nodes.iter() {
            if node == ignore {
                continue;
            }
            self.transport.send(node, msg);
        }
    }

    fn send_disconnect(&self, node: &Node) {
        self.transport.send(
            node,
            &Message::new(self.me.clone(), MessageData::Disconnect),
        );
    }

    fn handle_join(&self, from: &Node) {
        // Treat this as a warning in case its transient, if it isn't the
        // failure detection will clean it up properly soon
        if let Err(e) = self.transport.connect(from) {
            warn!("failed to connect to {:?} who sent us a JOIN: {}", from, e);
        }
        self.state
            .lock()
            .add_to_active_view(from)
            .map(|n| self.send_disconnect(&n));
        let fwd_message = Message::new(
            self.me.clone(),
            MessageData::ForwardJoin {
                node: from.clone(),
                ttl: self.params.active_rwl(),
            },
        );
        self.broadcast(from, &fwd_message);
    }

    fn handle_neighbor(&self, from: &Node, prio: NeighborPriority) {
        match prio {
            NeighborPriority::High => {
                self.state
                    .lock()
                    .add_to_active_view(from)
                    .map(|n| self.send_disconnect(&n));
            }
            NeighborPriority::Low => {
                let mut state = self.state.lock();
                if state.active_view.len() < self.params.active_size() {
                    // We explicitly make sure we have room so there will be no
                    // node to disconnect.
                    state.add_to_active_view(from);
                }
            }
        }
    }

    fn handle_disconnect(&self, from: &Node) {
        let mut state = self.state.lock();
        let to_drop = state.active_view.take(from);
        to_drop.map(|n| state.add_to_passive_view(&n));
    }

    fn handle_forward_join(&self, from: &Node, node: &Node, ttl: u32) {
        let mut state = self.state.lock();
        // NOTE(rossdylan): the len(active-view) clause has two different values
        // in the paper. The formalism has == 0, and the textual has == 1. The
        // correct one is == 1, since without at least one node in our
        // active-view we wouldn't get getting any messages.
        if state.active_view.len() == 1 || ttl == 0 {
            if let Ok(_) = self.transport.connect(node) {
                // NOTE(rossdylan): Another error in the paper is the lack of
                // a `Send(NEIGHBOR, n, LowPriority)` which is needed to maintain
                // the symetry of active-views. I dove into the source of partisan
                // which contains the OG implementation and confirmed that it
                // sends a NEIGHBOR request on the end of FORWARD_JOIN random walk
                self.send_neighbor(node, NeighborPriority::Low);
                state
                    .add_to_active_view(node)
                    .map(|n| self.send_disconnect(&n));
            } else {
                error!(
                    "failed to connect to {:?} during ForwardJoin, ignoring",
                    node
                )
            }
        } else {
            if ttl == self.params.passive_rwl() {
                state.add_to_passive_view(node)
            }
            let random_n = state
                .active_view
                .iter()
                .filter(|n| *n != from)
                .choose(&mut rand::thread_rng());
            if let Some(n) = random_n {
                self.transport.send(
                    n,
                    &Message::new(
                        self.me.clone(),
                        MessageData::ForwardJoin {
                            node: node.clone(),
                            ttl: ttl - 1,
                        },
                    ),
                );
            }
        }
    }

    fn send_neighbor(&self, dest: &Node, prio: NeighborPriority) {
        self.transport.send(
            dest,
            &Message::new(self.me.clone(), MessageData::Neighbor { priority: prio }),
        )
    }

    fn handle_message(&self, msg: &Message) {
        // explict scoping to force a drop of the mutex guard. The individual
        // helper functions all acquire the mutex on their own.
        {
            let mut state = self.state.lock();
            if state.seen_message(msg) {
                return;
            }
            trace!("[{:?}] received message: {:?}", &self.me, msg);
            state.add_message(msg);
        }
        match msg.data {
            MessageData::Data(ref data) => {
                info!("received user broadcast message of length {}", data.len());
            }
            MessageData::Join => self.handle_join(&msg.from),
            MessageData::Disconnect => self.handle_disconnect(&msg.from),
            MessageData::ForwardJoin { ref node, ttl } => {
                self.handle_forward_join(&msg.from, node, ttl)
            }
            MessageData::Neighbor { priority } => self.handle_neighbor(&msg.from, priority),
            _ => {}
        }
    }

    /// The message_thread processing incoming messages from other members in
    /// our network.
    fn message_thread(&self, shutdown: Receiver<()>) {
        let incoming_messages = self.transport.incoming();
        loop {
            select! {
                recv(incoming_messages) -> msg => match msg {
                    Ok(m) => self.handle_message(&m),
                    Err(e) => panic!("failed to fail wait for incoming messages {}", e),
                },
                recv(shutdown) -> _ => {
                    info!("shutting down message thread");
                    return
                },
            }
        }
    }

    fn handle_failure(&self, f: &Failure) {}

    fn failure_thread(&self, shutdown: Receiver<()>) {
        let failures = self.transport.failures();
        loop {
            select! {
                recv(failures) -> msg => match msg {
                    Ok(f) => self.handle_failure(&f),
                    Err(e) => panic!("failed to fail wait for incoming messages {}", e),
                },
                recv(shutdown) -> _ => {
                    info!("shutting down failure thread");
                    return
                },
            }
        }
    }

    /// Start the background threads that drive the HyParView protocol
    pub fn start(&self) -> (Sender<()>, Vec<JoinHandle<()>>) {
        let mut handles = Vec::with_capacity(3);
        let (tx, rx) = bounded(1);
        let self_ref = self.clone();
        let mrx = rx.clone();
        handles.push(thread::spawn(move || {
            self_ref.message_thread(mrx);
        }));
        let self_ref = self.clone();
        let frx = rx.clone();
        handles.push(thread::spawn(move || self_ref.failure_thread(frx)));
        (tx, handles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::*;
    #[test]
    fn test_creation() {
        let conns = MockConnections::new();
        let t1 = conns.new_transport(&Node {
            host: "localhost".into(),
            port: 4200,
        });
        let t2 = conns.new_transport(&Node {
            host: "localhost".into(),
            port: 6900,
        });

        let _h1 = HyParView::with_transport("localhost", 4200, Default::default(), t1);
        let _h2 = HyParView::with_transport("localhost", 6900, Default::default(), t2);
    }

    #[test]
    fn test_mock_transport() {
        let conns = MockConnections::new();
        let node1 = Node {
            host: "localhost".into(),
            port: 4200,
        };
        let node2 = Node {
            host: "localhost".into(),
            port: 6900,
        };
        let t1 = conns.new_transport(&node1);
        let t2 = conns.new_transport(&node2);

        t1.send(&node2, &Message::new(node1.clone(), MessageData::Join));
        assert!(t1.failures().is_empty());
        assert_eq!(t2.incoming().len(), 1);
        let rx_res = t2.incoming().try_recv();
        assert!(rx_res.is_ok());
        let msg = rx_res.unwrap();
        assert_eq!(msg.from, node1);
        assert_eq!(msg.data, MessageData::Join);
    }

    #[test]
    fn test_join() {
        let fres = fern::Dispatch::default()
            .level(log::LevelFilter::Trace)
            .format(|out, message, record| {
                out.finish(format_args!(
                    "{}[{}][{}] {}",
                    chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                    record.target(),
                    record.level(),
                    message
                ))
            })
            .chain(std::io::stderr())
            .apply();
        let conns = MockConnections::new();
        let node1 = Node {
            host: "localhost".into(),
            port: 4200,
        };
        let node2 = Node {
            host: "localhost".into(),
            port: 6900,
        };
        let t1 = conns.new_transport(&node1);
        let t2 = conns.new_transport(&node2);

        let h1 = HyParView::with_transport("localhost", 4200, Default::default(), t1);
        let h2 = HyParView::with_transport("localhost", 6900, Default::default(), t2);

        h1.transport
            .send(&node2, &Message::new(node1.clone(), MessageData::Join));
        assert!(h1.transport.failures().is_empty());
        assert_eq!(h2.transport.incoming().len(), 1);
        let rx_res = h2.transport.incoming().try_recv();
        assert!(rx_res.is_ok());
        let msg = rx_res.unwrap();
        h2.handle_message(&msg);

        assert!(h2.state.lock().active_view.contains(&node1))
    }

    #[test]
    fn test_forward_join() {
        let conns = MockConnections::new();
        let node1 = Node {
            host: "::1".into(),
            port: 4200,
        };
        let node2 = Node {
            host: "::1".into(),
            port: 6900,
        };
        let node3 = Node {
            host: "::1".into(),
            port: 7000,
        };
        let t1 = conns.new_transport(&node1);
        let t2 = conns.new_transport(&node2);
        let t3 = conns.new_transport(&node3);

        let h1 = HyParView::with_transport("::1", 4200, Default::default(), t1);
        let h2 = HyParView::with_transport("::1", 6900, Default::default(), t2);
        let h3 = HyParView::with_transport("::1", 7000, Default::default(), t3);

        let mut chans = Vec::new();
        let mut handles = Vec::new();
        let (tx1, mut jh1) = h1.start();
        chans.push(tx1);
        handles.append(&mut jh1);
        let (tx2, mut jh2) = h2.start();
        chans.push(tx2);
        handles.append(&mut jh2);
        let (tx3, mut jh3) = h3.start();
        chans.push(tx3);
        handles.append(&mut jh3);

        assert!(h1.init(&mut ["::1:6900"].iter()).is_ok());
        assert!(h3.init(&mut ["::1:6900"].iter()).is_ok());
        // Wait 5 seconds for network to converge
        for _ in 0..5 {
            if h1.state.lock().active_view.len() == 2
                && h2.state.lock().active_view.len() == 2
                && h3.state.lock().active_view.len() == 2
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        // trigger our background threads to exit and wait for them to so do
        for c in chans.into_iter() {
            drop(c);
        }
        for jh in handles.into_iter() {
            jh.join().unwrap();
        }
    }

    #[test]
    fn test_network_params() {
        let params = NetworkParameters::default();
        assert_eq!(params.active_size(), 5);
        assert_eq!(params.passive_size(), 30);
    }

    #[test]
    fn test_iter_bootstrap() {
        let addrs = ["localhost:4445"];
        let subset_res = addrs.iter().subset(1, 0);
        assert!(subset_res.is_ok());
        let subset = subset_res.unwrap();
        assert_eq!(subset.len(), 1);
        assert!(subset[0].host == "127.0.0.1" || subset[0].host == "::1");
        assert_eq!(subset[0].port, 4445);
    }
}
