//! The state module holds structures and helpers for managing the underlying
//! hyparview protocol state
use indexmap::IndexSet;
use rand::prelude::IteratorRandom;
use rand::{thread_rng, Rng};
use tracing::trace;

use crate::message_store::MessageStore;
use crate::proto::Peer;

/// The passive scale factor is used to scale the size of the active-view in order
/// to generate the size of the passive-view. This number is cribbed directly
/// from the HyParView paper.
const PASSIVE_SCALE_FACTOR: f64 = 6.0;

/// The active scale factor is used to scale the active-view which otherwise
/// could be zero in some cases.
const ACTIVE_SCALE_FACTOR: f64 = 1.0;

const ACTIVE_RANDOM_WALK_LENGTH: u32 = 6;

const PASSIVE_RANDOM_WALK_LENGTH: u32 = 3;

const MESSAGE_HISTORY: usize = 10_000;

const SHUFFLE_ACTIVE: usize = 3;

const SHUFFLE_PASSIVE: usize = 4;

const DEFAULT_QUEUE_SIZE: usize = 2048;

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

    /// The size of our outgoing messages queue
    outgoing_queue_size: usize,
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

    /// How big of a buffer our outgoing queue has
    pub fn queue_size(&self) -> usize {
        self.outgoing_queue_size
    }

    /// Helper function to build a NetworkParameters struct with the given size
    /// but keeping the default scaling factors.
    pub fn default_with_size(size: f64) -> Self {
        NetworkParameters {
            size,
            c: ACTIVE_SCALE_FACTOR,
            k: PASSIVE_SCALE_FACTOR,
            arwl: ACTIVE_RANDOM_WALK_LENGTH,
            prwl: PASSIVE_RANDOM_WALK_LENGTH,
            msg_history: MESSAGE_HISTORY,
            kp: SHUFFLE_PASSIVE,
            ka: SHUFFLE_ACTIVE,
            outgoing_queue_size: DEFAULT_QUEUE_SIZE,
        }
    }
}

impl Default for NetworkParameters {
    /// Default returns the default network parameters used in the HyParView
    /// paper. A network of size 10,000 with C=1, K=6
    /// TODO(rossdylan): Attempt to find actual implementations of HyParView to
    /// pull real params from
    fn default() -> Self {
        Self::default_with_size(10_000.0)
    }
}

/// The State structure is used to track internal hyparview protocol state such
/// as active and passive views, and initial network parameters.
#[derive(Debug, Clone)]
pub(crate) struct State {
    me: Peer,
    /// The initial parameters we set for executing the hyparview protocol
    pub(crate) params: NetworkParameters,
    /// The set of peers that make up the active-view
    pub(crate) active_view: IndexSet<Peer>,
    /// The set of peers that make up the passive-view
    pub(crate) passive_view: IndexSet<Peer>,
    // TODO(rossdylan): We might be able to get away with something like a bloom
    // filter here to increase space efficiency.
    messages: MessageStore,
    metrics: crate::metrics::StateMetrics,
}

/// Given an `IndexSet<Peer>` representing a hyparview view we extract a random
/// peer.
fn random_node_from_view(view: &IndexSet<Peer>) -> Option<Peer> {
    let index = thread_rng().gen_range(0..view.len());
    view.get_index(index).map(Clone::clone)
}

impl State {
    /// Given a node that represents this instance, and the parameters for our
    /// network create a new state.
    pub(crate) fn new(me: Peer, params: NetworkParameters) -> Self {
        let asize = params.active_size() as usize;
        let psize = params.passive_size() as usize;
        State {
            me,
            params,
            active_view: IndexSet::with_capacity(asize),
            passive_view: IndexSet::with_capacity(psize),
            messages: MessageStore::new(params.message_history()),
            metrics: crate::metrics::StateMetrics::new(),
        }
    }

    /// Select Kp nodes from the passive-view and Ka nodes from the active-view
    /// to service a shuffle request
    pub(crate) fn select_shuffle_peers(&self, ignore: Option<&Peer>) -> Vec<Peer> {
        let len = self.active_view.len();
        let mut nodes = Vec::with_capacity(self.params.ka + self.params.kp);
        nodes.extend(
            std::iter::repeat_with(|| rand::thread_rng().gen_range(0..len))
                .take(len) // Do we need this? I want to make sure we don't iter forever
                .filter_map(|i| self.active_view.get_index(i))
                .filter(|n| ignore.is_none() || *n != ignore.unwrap())
                .take(self.params.ka)
                .cloned(),
        );
        nodes.extend(
            std::iter::repeat_with(|| rand::thread_rng().gen_range(0..len))
                .take(len) // Do we need this? I want to make sure we don't iter forever
                .filter_map(|i| self.passive_view.get_index(i))
                .filter(|n| ignore.is_none() || *n != ignore.unwrap())
                .take(self.params.ka)
                .cloned(),
        );
        nodes
    }

    /// Select a single random peer from our active view
    pub(crate) fn random_active_peer(&self, ignore: Option<&Peer>) -> Option<Peer> {
        self.active_view
            .iter()
            .filter(|n| ignore.is_none() || *n != ignore.unwrap())
            .choose(&mut rand::thread_rng())
            .map(Clone::clone)
    }

    /// Generate a vector of randomly chosen peers from our passive view
    pub(crate) fn random_passive_peers(&self, ignore: Option<&Peer>, len: usize) -> Vec<Peer> {
        let len = if len > self.passive_view.len() {
            self.passive_view.len()
        } else {
            len
        };
        // TODO(rossdylan): should this handle duplicates?
        std::iter::repeat_with(|| rand::thread_rng().gen_range(0..len))
            .take(len) // Do we need this? I want to make sure we don't iter forever
            .filter_map(|i| self.passive_view.get_index(i))
            .filter(|n| ignore.is_none() || *n != ignore.unwrap())
            .take(self.params.ka)
            .cloned()
            .collect()
    }

    /// Select a single random peer from our passive view
    pub(crate) fn random_passive_peer(&self) -> Option<Peer> {
        self.passive_view
            .iter()
            .choose(&mut rand::thread_rng())
            .map(Clone::clone)
    }

    /// Add the given peer to the active view, and if the view is full return
    /// a peer that has been dropped to make room
    pub(crate) fn add_to_active_view(&mut self, peer: &Peer) -> Option<Peer> {
        if self.active_view.contains(peer) || *peer == self.me {
            return None;
        }
        let dropped = if self.active_view.len() >= self.params.active_size() {
            let rn = random_node_from_view(&self.active_view);
            debug_assert!(
                rn.is_some(),
                "a random node from a full active view should always be Some"
            );
            rn
        } else {
            None
        };
        if let Some(ref dn) = dropped {
            self.active_view.remove(dn);
            self.add_to_passive_view(dn);
        }
        self.active_view.insert(peer.clone());
        self.metrics
            .record_view_sizes(self.active_view.len(), self.passive_view.len());
        trace!("[{}] added {} to active view", self.me, peer);
        dropped
    }

    /// Handle a disconnect request by attempting to remove the peer from our
    /// active view and adding it to our passive view
    pub(crate) fn disconnect(&mut self, peer: &Peer) {
        let removed = self.active_view.remove(peer);
        if removed {
            trace!("[{}] removed {} from active view", self.me, peer);
            self.add_to_passive_view(peer);
        }
    }

    /// Add the given peer to the passive view.
    pub(crate) fn add_to_passive_view(&mut self, peer: &Peer) {
        if self.passive_view.contains(peer) || self.active_view.contains(peer) || self.me == *peer {
            return;
        }
        if self.passive_view.len() == self.params.active_size() {
            let to_drop = random_node_from_view(&self.passive_view).unwrap();
            self.passive_view.remove(&to_drop);
        }
        self.passive_view.insert(peer.clone());
        self.metrics
            .record_view_sizes(self.active_view.len(), self.passive_view.len());
    }

    /// Replace a failed peer with one from the passive view
    pub(crate) fn replace_peer(&mut self, failed: &Peer, replacement: &Peer) {
        self.active_view.remove(failed);
        self.passive_view.remove(replacement);
        self.active_view.insert(replacement.clone());
        self.metrics
            .record_view_sizes(self.active_view.len(), self.passive_view.len());
    }

    /// Given a message ID, record that we have seen it
    pub(crate) fn record_message(&mut self, mid: &[u8; 20]) {
        self.messages.insert(mid)
    }

    /// Check to see if we have seen the given message ID before
    pub(crate) fn message_seen(&self, mid: &[u8]) -> bool {
        self.messages.contains(mid)
    }
}
