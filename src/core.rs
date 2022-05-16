//! The core module contains the actual implementation of the hyparview protocol
use std::result::Result as StdResult;
use std::sync::atomic;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use futures::stream::FuturesOrdered;
use futures::StreamExt;
use indexmap::IndexSet;
use svix_ksuid::KsuidLike;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tonic::{Request, Response, Status};
use tracing::{debug, error, trace, warn};

use crate::error::Error;
use crate::error::Result;
use crate::message_store::gen_msg_id;
use crate::proto::hyparview_server::Hyparview;
use crate::proto::{DataRequest, Empty, NeighborPriority, Peer};
use crate::state::{NetworkParameters, State};
use crate::transport::{BootstrapSource, ConnectionManager, DefaultConnectionManager};

/// This enum contains variants for every message we can send to our peers. We
/// do this so we can easily enqueue all outgoing messages and send them async.
/// This ensures that when we receive a message we don't block on sending
/// messages to other peers
#[derive(Clone, Debug)]
enum OutgoingMessage {
    ForwardJoin {
        src: Peer,
        dest: Peer,
        ttl: u32,
    },
    Disconnect {
        dest: Peer,
    },
    Neighbor {
        dest: Peer,
        prio: NeighborPriority,
    },
    Shuffle {
        src: Peer,
        dest: Peer,
        peers: Vec<Peer>,
        ttl: u32,
    },
    ShuffleReply {
        dest: Peer,
        peers: Vec<Peer>,
    },
    Data {
        dest: Peer,
        req: DataRequest,
    },
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
pub struct HyParView<C: ConnectionManager + 'static> {
    me: Peer,
    params: NetworkParameters,
    manager: C,
    failure_tx: UnboundedSender<Peer>,
    outgoing_msgs: UnboundedSender<OutgoingMessage>,
    pending_msgs: Arc<atomic::AtomicU64>,
    state: Arc<Mutex<State>>,
    broadcast_tx: Arc<broadcast::Sender<(svix_ksuid::Ksuid, Vec<u8>)>>,
    metrics: crate::metrics::ServerMetrics,
}

impl HyParView<DefaultConnectionManager> {
    /// Create a new instance of the HyParView system using the default
    /// connection manager.
    pub fn new(
        host: &str,
        port: u16,
        params: NetworkParameters,
    ) -> HyParView<DefaultConnectionManager> {
        Self::with_transport(host, port, params, DefaultConnectionManager::new())
    }
}

impl<C: ConnectionManager> HyParView<C> {
    /// Create a new HyParView instance with the provided transport
    pub fn with_transport(
        host: &str,
        port: u16,
        params: NetworkParameters,
        manager: C,
    ) -> HyParView<C> {
        let me = Peer {
            host: String::from(host),
            port: port as u32,
        };
        let (failure_tx, failure_rx) = unbounded_channel();
        let (outgoing_tx, outgoing_rx) = unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(1024);
        let hpv = HyParView {
            me: me.clone(),
            params,
            manager,
            failure_tx,
            outgoing_msgs: outgoing_tx,
            pending_msgs: Arc::new(Default::default()),
            state: Arc::new(Mutex::new(State::new(me, params))),
            broadcast_tx: Arc::new(broadcast_tx),
            metrics: crate::metrics::ServerMetrics::new(),
        };
        // NOTE(rossdylan): I really don't like having HyParView::new(...) being
        // stateful, but its kinda the only way I can make the channels work
        // the way I want.
        tokio::spawn(hpv.clone().outgoing_task(outgoing_rx));
        tokio::spawn(hpv.clone().failure_handler(failure_rx));
        tokio::spawn(hpv.clone().shuffle_task());
        hpv
    }

    /// Return a broadcast channel that will emit the contents of all data
    /// messages that this hyparview instance receives.
    pub fn subscribe(&self) -> broadcast::Receiver<(svix_ksuid::Ksuid, Vec<u8>)> {
        self.broadcast_tx.subscribe()
    }

    /// Broadcast some data into the gossip network by sending it to all our
    /// active peers. If we fail to send data to any peers, or there are no
    /// active peers we return an error
    pub async fn broadcast(&self, data: &[u8]) -> Result<()> {
        let peers = self.active_view();
        let req = DataRequest {
            source: Some(self.me.clone()),
            id: gen_msg_id(),
            data: data.into(),
        };
        // NOTE(rossdylan): We do this synchronously instead of pushing it
        // through the outgoing_task so we can report errors more easily
        // TODO(rossdylan): Consider extending enqueue_message with an optional
        // oneshot channel to act as a callback
        let mut sent = false;
        let broadcast_start_time = std::time::Instant::now();
        {
            let mut fset = FuturesOrdered::new();
            for peer in peers.iter() {
                let req_fut = self.send_data(peer, &req);
                fset.push(req_fut);
            }
            let mut stream = fset.enumerate();
            while let Some((index, res)) = stream.next().await {
                let peer = &peers[index];
                if let Err(e) = res {
                    warn!("[{}] failed to send broadcast to {}: {}", self.me, peer, e);
                    self.report_failure(peer);
                } else {
                    sent = true;
                }
            }
        }
        self.metrics
            .report_broadcast(sent, broadcast_start_time.elapsed());
        if sent {
            Ok(())
        } else {
            Err(Error::BroadcastFailed)
        }
    }

    pub(crate) fn active_view(&self) -> IndexSet<Peer> {
        self.state.lock().unwrap().active_view.clone()
    }

    pub(crate) fn _passive_view(&self) -> IndexSet<Peer> {
        self.state.lock().unwrap().passive_view.clone()
    }

    async fn send_join(&self, peer: &Peer) -> Result<()> {
        let mut client = self.manager.connect(peer).await?;
        client
            .join(crate::proto::JoinRequest {
                source: Some(self.me.clone()),
                id: gen_msg_id(),
            })
            .await?;
        Ok(())
    }

    async fn send_neighbor(&self, peer: &Peer, prio: NeighborPriority) -> Result<bool> {
        let mut client = self.manager.connect(peer).await?;
        let resp = client
            .neighbor(crate::proto::NeighborRequest {
                source: Some(self.me.clone()),
                id: gen_msg_id(),
                priority: prio as i32,
            })
            .await?;
        Ok(resp.get_ref().accepted)
    }

    /// Disconnects are best effort and we send them to attempt to ensure
    /// that healthy nodes that we remove from our views maintain symmetry by
    /// removing us.
    async fn send_disconnect(&self, peer: &Peer) {
        debug!("[{}] attempting to disconnect from {}", self.me, peer);
        let maybe_res = self.manager.connect(peer).await;
        if let Err(e) = maybe_res {
            warn!("failed to send disconnect to {:?}: {}", peer, e);
            return;
        }
        let mut client = maybe_res.unwrap();
        let resp = client
            .disconnect(crate::proto::DisconnectRequest {
                source: Some(self.me.clone()),
                id: gen_msg_id(),
            })
            .await;
        if let Err(e) = resp {
            warn!("failed to send disconnect to {:?}: {}", peer, e);
        }
    }

    /// Forward a join message through the network.
    async fn send_forward_join(&self, source: &Peer, dest: &Peer, ttl: u32) -> Result<()> {
        let mut client = self.manager.connect(dest).await?;
        client
            .forward_join(crate::proto::ForwardJoinRequest {
                source: Some(source.clone()),
                id: gen_msg_id(),
                ttl,
            })
            .await?;
        Ok(())
    }

    async fn send_data(&self, dest: &Peer, data: &crate::proto::DataRequest) -> Result<()> {
        let mut client = self.manager.connect(dest).await?;
        // TODO(rossdylan): I hate how each time we forward our data to another
        // peer we need to clone the entire data struct
        client.data(Request::new(data.clone())).await?;
        Ok(())
    }

    /// Send a shuffle message to the given peer with the given TTL.
    /// This is used to execute a random walk using the PRWL constant as the
    /// initial TTL.
    async fn send_shuffle(
        &self,
        source: &Peer,
        peer: &Peer,
        ttl: u32,
        peers: &[Peer],
    ) -> Result<()> {
        let mut client = self.manager.connect(peer).await?;
        client
            .shuffle(crate::proto::ShuffleRequest {
                source: Some(source.clone()),
                id: gen_msg_id(),
                peers: peers.to_vec(),
                ttl,
            })
            .await?;
        Ok(())
    }

    /// Send a ShuffleReply message to the given peer. It contains a shuffled
    /// slice of our current peers. This is the reciprocal message to Shuffle
    async fn send_shuffle_reply(&self, peer: &Peer, peers: &[Peer]) -> Result<()> {
        let mut client = self.manager.connect(peer).await?;
        client
            .shuffle_reply(crate::proto::ShuffleReplyRequest {
                source: Some(self.me.clone()),
                id: gen_msg_id(),
                peers: peers.to_vec(),
            })
            .await?;
        Ok(())
    }

    /// Trigger the initialization of this HyParView instance by trying to find
    /// a bootstrap node and sending it a join.
    /// init will only fail if we can't contact any peers from provided by
    /// the `BootstrapSource`
    pub async fn init(&mut self, mut boots: impl BootstrapSource) -> Result<()> {
        while let Some(ref contact_peer) = boots.next_peer()? {
            self.state.lock().unwrap().add_to_active_view(contact_peer);
            if let Err(e) = self.send_join(contact_peer).await {
                self.state.lock().unwrap().disconnect(contact_peer);
                warn!(
                    "failed to contact bootstrap node: {:?}: {}",
                    contact_peer, e
                );
            } else {
                debug!(
                    "[{}] bootstrap successful, initial peer: {}",
                    self.me, contact_peer
                );
                break;
            }
        }
        Ok(())
    }

    /// Send the peer that had a failure over to our async failure handler
    /// task.
    fn report_failure(&self, peer: &Peer) {
        self.metrics.report_peer_failure();
        self.failure_tx.send(peer.clone()).unwrap();
    }

    /// Helper function which places an outgoing message into the outgoing
    /// queue. It also increments our pending messages counter so we have an
    /// idea of what our backlog is like.
    fn enqueue_message(&self, msg: OutgoingMessage) {
        self.metrics.incr_pending();
        self.pending_msgs.fetch_add(1, atomic::Ordering::SeqCst);
        self.outgoing_msgs.send(msg).unwrap();
    }

    /// A background task that pulls from a mpsc and sends messages to other
    /// peers. We use this to decouple sending messages from receiving them.
    /// The HyParView paper makes a lot of assumptions of a fully asynchronus
    /// messaging model.
    async fn outgoing_task(self, mut rx: UnboundedReceiver<OutgoingMessage>) {
        trace!("[{}] outgoing task spawned", self.me);
        loop {
            if let Some(ref msg) = rx.recv().await {
                self.metrics.decr_pending();
                self.pending_msgs.fetch_sub(1, atomic::Ordering::SeqCst);
                let (res, dest) = match msg {
                    OutgoingMessage::ForwardJoin { src, dest, ttl } => {
                        (self.send_forward_join(src, dest, *ttl).await, dest)
                    }
                    OutgoingMessage::Disconnect { dest } => {
                        self.send_disconnect(dest).await;
                        (Ok(()), dest)
                    }
                    OutgoingMessage::Neighbor { dest, prio } => {
                        let resp = self.send_neighbor(dest, *prio).await.map(|_| ());
                        (resp, dest)
                    }
                    OutgoingMessage::Shuffle {
                        src,
                        dest,
                        peers,
                        ttl,
                    } => (self.send_shuffle(src, dest, *ttl, peers).await, dest),
                    OutgoingMessage::ShuffleReply { dest, peers } => {
                        (self.send_shuffle_reply(dest, peers).await, dest)
                    }
                    OutgoingMessage::Data { dest, req } => (self.send_data(dest, req).await, dest),
                };
                if let Err(e) = res {
                    warn!(
                        "[{}] failed to send '{:?}' to {}: {}",
                        self.me, msg, dest, e
                    );
                    self.report_failure(dest);
                }
            } else {
                return;
            }
        }
    }

    /// Helper function to encapsulate the logic of picking a replacement peer
    /// from the passive view.
    /// 1. Attempt to take a random repalcement peer from the passive view
    /// 2. Try to connect and send a Neighbor message to that peer
    /// 3. If the active view is empty the priority of the Neighbor request will
    ///    be High
    /// 4. If the Neighbor request succeeds replace the failed peer with the new
    ///    peer in the active view
    /// 5. If the Neighbor request failed try again with a different passive
    ///    view peer.
    async fn replace_peer(&self, failed: &Peer) -> Result<()> {
        let mut attempts = 0;
        loop {
            let (maybe_peer, prio) = {
                let state = self.state.lock().unwrap();
                // ensure that if a failed peer is reported multiple times we
                // only execute replacement for it once
                if !state.active_view.contains(failed) {
                    return Ok(());
                }
                // if the failed peer is the only peer in the active view the
                // resulting replacement Neighbor request should be High
                // priority
                // NOTE(rossdylan): we have an extra condition here for when
                // all our passive peers reject the request. This is a change
                // from the paper, and the partisan implementation. Partisan
                // universally uses a High priority request, the paper uses
                // only the active view length as the condition. If we don't
                // add this new condition failure replacement can stall
                let prio = if state.active_view.len() == 1 || attempts >= state.passive_view.len() {
                    NeighborPriority::High
                } else {
                    NeighborPriority::Low
                };
                (state.random_passive_peer(), prio)
            };
            if let Some(peer) = maybe_peer {
                if let Ok(accepted) = self.send_neighbor(&peer, prio).await {
                    if accepted {
                        self.state.lock().unwrap().replace_peer(failed, &peer);
                        debug!("[{}] replacing {} with {}", self.me, failed, peer);
                        return Ok(());
                    } else {
                        debug!(
                            "[{}] failed to replace {} with {}, peer rejected neighbor request",
                            self.me, failed, peer
                        )
                    }
                } else {
                    debug!(
                        "[{}] failed to replace {} with {}, removing from passive view",
                        self.me, failed, peer
                    );
                    self.state.lock().unwrap().passive_view.remove(&peer);
                }
            } else {
                return Err(Error::PassiveViewEmpty);
            }
            attempts += 1;
        }
    }

    /// Handle failures in the background by listening on a channel and running
    /// the failure algorithm.
    async fn failure_handler(self, mut rx: UnboundedReceiver<Peer>) {
        loop {
            if let Some(failed_peer) = rx.recv().await {
                warn!(
                    "[{}] peer {} has been marked as failed",
                    self.me, failed_peer
                );
                if let Err(e) = self.replace_peer(&failed_peer).await {
                    self.metrics.report_peer_replacement(false);
                    warn!(
                        "[{}] unable to replace {}, no healthy peers in passive view: {}",
                        self.me, failed_peer, e
                    );
                } else {
                    self.metrics.report_peer_replacement(true);
                }
            } else {
                return;
            }
        }
    }

    /// The shuffle task is started up on init and periodically issues a shuffle
    /// request into the network to update our passive views.
    async fn shuffle_task(self) {
        loop {
            // TODO(rossdylan): I've randomly chosen 60s as the basis for our
            // periodic shuffle, but idk what the paper expects here. This should
            // be a configuration value in `NetworkParameters`
            tokio::time::sleep(crate::util::jitter(Duration::from_secs(60))).await;
            self.metrics.report_shuffle();
            {
                let state = self.state.lock().unwrap();
                let destination = match state.random_active_peer(None) {
                    None => {
                        debug!("[{}] shuffle aborted, no peers in active view", self.me);
                        continue;
                    }
                    Some(p) => p,
                };
                let selected_peers = state.select_shuffle_peers(None);
                if !selected_peers.is_empty() {
                    debug!(
                        "[{}] sending shuffle request to {} with peers: {:?}",
                        self.me, destination, selected_peers
                    );
                    self.enqueue_message(OutgoingMessage::Shuffle {
                        src: self.me.clone(),
                        dest: destination,
                        peers: selected_peers,
                        ttl: self.params.passive_rwl(),
                    });
                } else {
                    debug!("[{}] shuffle aborted, no peers to send", self.me)
                }
            };
        }
    }

    /// Return a tonic service wrapper that can be registered with a server
    pub fn get_service(&self) -> crate::proto::hyparview_server::HyparviewServer<Self> {
        crate::proto::hyparview_server::HyparviewServer::new(self.clone())
    }
}

#[async_trait::async_trait]
impl<C: ConnectionManager> Hyparview for HyParView<C> {
    async fn join(
        &self,
        request: Request<crate::proto::JoinRequest>,
    ) -> StdResult<Response<Empty>, Status> {
        let req_ref = request.get_ref();
        if req_ref.source.is_none() {
            return Err(Status::invalid_argument("no source peer specified"));
        }
        let source = req_ref.source.as_ref().unwrap();
        if source == &self.me {
            return Ok(Response::new(Empty {}));
        }
        // Treat this as a warning in case its transient, if it isn't the
        // failure detection will clean it up properly soon
        if let Err(e) = self.manager.connect(source).await {
            warn!(
                "failed to form reverse connection to {:?} who sent us a JOIN: {}",
                source, e
            );
        }
        let mut state = self.state.lock().unwrap();
        if let Some(dropped) = state.add_to_active_view(source) {
            self.enqueue_message(OutgoingMessage::Disconnect { dest: dropped });
        }
        let active_peers = state.active_view.clone();
        debug!(
            "[{}] received join from {}, attempting to forward to {:?}",
            self.me, source, active_peers
        );
        for peer in active_peers.into_iter().filter(|p| p != source) {
            debug_assert!(peer != self.me, "active set should never contain self");
            self.enqueue_message(OutgoingMessage::ForwardJoin {
                src: source.clone(),
                dest: peer,
                ttl: self.params.active_rwl(),
            });
        }
        Ok(Response::new(Empty {}))
    }

    async fn forward_join(
        &self,
        request: Request<crate::proto::ForwardJoinRequest>,
    ) -> StdResult<Response<Empty>, Status> {
        let req_ref = request.get_ref();
        if req_ref.source.is_none() {
            return Err(Status::invalid_argument("no source peer specified"));
        }
        let source = req_ref.source.as_ref().unwrap();
        // NOTE(rossdylan): the len(active-view) clause has two different values
        // in the paper. The formalism has == 0, and the textual has == 1. The
        // correct one is == 1, since without at least one node in our
        // active-view we wouldn't get getting any messages.
        let mut state = self.state.lock().unwrap();
        if state.active_view.len() == 1 || req_ref.ttl == 0 {
            debug!(
                "[{}] recieved terminal forward_join from {}, adding to active view",
                self.me, source
            );
            // NOTE(rossdylan): Another error in the paper is the lack of
            // a `Send(NEIGHBOR, n, HighPriority)` which is needed to maintain
            // the symmetry of active-views.
            // NOTE(rossdylan): After a reimplementation I thought about this
            // some more. The partisan source had this as a LowPriority NEIGHBOR
            // message, but if we want to ensure the symmetry invariant this
            // has to be HighPriority.
            self.enqueue_message(OutgoingMessage::Neighbor {
                dest: source.clone(),
                prio: NeighborPriority::High,
            });
            if let Some(dropped_peer) = state.add_to_active_view(source) {
                self.enqueue_message(OutgoingMessage::Disconnect { dest: dropped_peer });
            };
        } else {
            if req_ref.ttl == self.params.passive_rwl() {
                state.add_to_passive_view(source)
            }
            if let Some(n) = state.random_active_peer(Some(source)) {
                debug!(
                    "[{}] recieved forward_join from {}, forwarding to {}",
                    self.me, source, n
                );
                self.enqueue_message(OutgoingMessage::ForwardJoin {
                    src: source.clone(),
                    dest: n,
                    ttl: req_ref.ttl - 1,
                });
            }
        }
        Ok(Response::new(Empty {}))
    }

    async fn disconnect(
        &self,
        request: Request<crate::proto::DisconnectRequest>,
    ) -> StdResult<Response<Empty>, Status> {
        let req_ref = request.get_ref();
        if req_ref.source.is_none() {
            return Err(Status::invalid_argument("no source peer specified"));
        }
        let source = req_ref.source.as_ref().unwrap();
        debug!("[{}] recieved disconnect from {}", self.me, source);
        self.state.lock().unwrap().disconnect(source);
        debug_assert!(
            !self.active_view().contains(source),
            "active view contains peer after disconnect request"
        );
        Ok(Response::new(Empty {}))
    }

    async fn neighbor(
        &self,
        request: Request<crate::proto::NeighborRequest>,
    ) -> StdResult<Response<crate::proto::NeighborResponse>, Status> {
        let req_ref = request.get_ref();
        if req_ref.source.is_none() {
            return Err(Status::invalid_argument("no source peer specified"));
        }
        let source = req_ref.source.as_ref().unwrap();
        let mut state = self.state.lock().unwrap();
        match req_ref.priority() {
            NeighborPriority::Unknown => Err(Status::internal(
                "unknown NeighborPriority, message corrupted?",
            )),
            NeighborPriority::High => {
                debug!(
                    "[{}] received high priority neighbor from {}",
                    self.me, source
                );
                if let Some(dropped_peer) = state.add_to_active_view(source) {
                    self.enqueue_message(OutgoingMessage::Disconnect { dest: dropped_peer });
                }
                Ok(Response::new(crate::proto::NeighborResponse {
                    accepted: true,
                }))
            }
            NeighborPriority::Low => {
                debug!(
                    "[{}] received low priority neighbor from {}",
                    self.me, source
                );
                if state.active_view.len() < self.params.active_size() {
                    let maybe_peer = state.add_to_active_view(source);
                    // We explicitly make sure we have room so there will be no
                    // node to disconnect.
                    debug_assert!(maybe_peer.is_none());
                    Ok(Response::new(crate::proto::NeighborResponse {
                        accepted: true,
                    }))
                } else {
                    Ok(Response::new(crate::proto::NeighborResponse {
                        accepted: false,
                    }))
                }
            }
        }
    }

    async fn shuffle(
        &self,
        request: Request<crate::proto::ShuffleRequest>,
    ) -> StdResult<Response<Empty>, Status> {
        let req_ref = request.get_ref();
        if req_ref.source.is_none() {
            return Err(Status::invalid_argument("no source peer specified"));
        }
        let source = req_ref.source.as_ref().unwrap();
        let mut state = self.state.lock().unwrap();
        if req_ref.ttl == 0 {
            debug!("[{}] recieved terminal shuffle from {}", self.me, source);
            // NOTE(rossdylan): The HyParView paper isn't super specific here but
            // I assume that on Shuffle termination we want to send back peers
            // from our passive view **before** we integrate the peers we just
            // received.
            self.enqueue_message(OutgoingMessage::ShuffleReply {
                dest: source.clone(),
                peers: state.random_passive_peers(None, req_ref.peers.len()),
            });
            for peer in req_ref.peers.iter() {
                state.add_to_passive_view(peer);
            }
        } else if let Some(n) = state.random_active_peer(Some(source)) {
            debug!(
                "[{}] recieved shuffle from {} with ttl {}, forwarding to {}",
                self.me, req_ref.ttl, source, n
            );
            self.enqueue_message(OutgoingMessage::Shuffle {
                src: source.clone(),
                dest: n,
                peers: req_ref.peers.clone(),
                ttl: req_ref.ttl - 1,
            });
        }
        Ok(Response::new(Empty {}))
    }

    async fn shuffle_reply(
        &self,
        request: Request<crate::proto::ShuffleReplyRequest>,
    ) -> StdResult<Response<Empty>, Status> {
        let req_ref = request.get_ref();
        if req_ref.source.is_none() {
            return Err(Status::invalid_argument("no source peer specified"));
        }
        let source = req_ref.source.as_ref().unwrap();
        let mut state = self.state.lock().unwrap();
        debug!(
            "[{}] received shuffle-reply from {} with {} peers",
            self.me,
            source,
            req_ref.peers.len()
        );
        // TODO(rossdylan): The paper prioritises evicting peers in our passive
        // view that we've sent via shuffle. That's kinda tricky to implement in
        // our current architecture so I'm leaving it out.
        for peer in req_ref.peers.iter() {
            state.add_to_passive_view(peer)
        }
        Ok(Response::new(Empty {}))
    }

    async fn data(
        &self,
        request: Request<crate::proto::DataRequest>,
    ) -> StdResult<Response<Empty>, Status> {
        let req_ref = request.get_ref();
        if req_ref.source.is_none() {
            return Err(Status::invalid_argument("no source peer specified"));
        }
        let source = req_ref.source.as_ref().unwrap();
        let ksuid = match crate::util::ksuid_from_bytes(&req_ref.id) {
            Err(_) => return Err(Status::invalid_argument("invalid message id")),
            Ok(k) => k,
        };

        let mut state = self.state.lock().unwrap();
        // If we've seen this message before ignore it
        if state.message_seen(ksuid.bytes()) {
            self.metrics.report_data_msg(true);
            return Ok(Response::new(Empty {}));
        }
        self.metrics.report_data_msg(false);
        state.record_message(ksuid.bytes());

        // Only send this message to our local inprocess subscribers if we
        // actually have inproccess subscribers
        if self.broadcast_tx.receiver_count() > 0 {
            if let Err(e) = self.broadcast_tx.send((ksuid, req_ref.data.clone())) {
                error!(
                    "[{}] failed to send incoming data to local subscribers: {}",
                    self.me, e
                );
            }
        }
        let active_peers = state.active_view.clone();
        for peer in active_peers.into_iter() {
            let cloned_data = req_ref.clone();
            if peer == *source {
                continue;
            }
            self.enqueue_message(OutgoingMessage::Data {
                dest: peer,
                req: cloned_data,
            });
        }

        Ok(Response::new(Empty {}))
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::sync::atomic;

    use futures::stream::FuturesUnordered;
    use futures::stream::StreamExt;
    use futures::FutureExt;
    use rand::prelude::IteratorRandom;
    use tokio::task::JoinHandle;
    use tonic::transport::Server;
    use tracing::debug;

    use super::*;
    use crate::error::Result;
    use crate::state::*;
    use crate::transport::*;

    #[derive(Debug)]
    struct TestInstanceExtras {
        signal: Option<tokio::sync::oneshot::Sender<()>>,
        handle: JoinHandle<()>,
    }

    #[derive(Debug, Clone)]
    struct TestInstance {
        inner: HyParView<InMemoryConnectionManager>,
        extras: Arc<Mutex<TestInstanceExtras>>,
    }

    impl std::ops::DerefMut for TestInstance {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.inner
        }
    }

    impl std::ops::Deref for TestInstance {
        type Target = HyParView<InMemoryConnectionManager>;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl TestInstance {
        /// Generate a new `HyParView` instance hooked up to the `InMemoryConnectionManager`
        /// This is used to control the life-cycle of a hyparview instance for tests
        async fn new(peer: &Peer, cg: &InMemoryConnectionGraph) -> Result<Self> {
            let manager = cg.new_manager();
            let hpv = HyParView::with_transport(
                &peer.host,
                peer.port as u16,
                NetworkParameters::default(),
                manager,
            );
            let incoming = cg.register(peer).await?;
            let hpv_copy = hpv.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            let jh = tokio::spawn(async move {
                Server::builder()
                    .add_service(crate::proto::hyparview_server::HyparviewServer::new(
                        hpv_copy,
                    ))
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::ReceiverStream::new(incoming)
                            .map(Ok::<_, std::io::Error>),
                        rx.map(|_| debug!("graceful shutdown signal recevied, terminating")),
                    )
                    .await
                    .unwrap();
            });
            Ok(TestInstance {
                inner: hpv,
                extras: Arc::new(Mutex::new(TestInstanceExtras {
                    signal: Some(tx),
                    handle: jh,
                })),
            })
        }

        // Trigger a clean shutdown of the underlying tonic server
        async fn stop(&mut self) {
            let signal = self.extras.lock().unwrap().signal.take().unwrap();
            if signal.send(()).is_err() {
                error!("failed to send shutdown signal to tonic");
                return;
            }
            if let Err(e) = (&mut self.extras.lock().unwrap().handle).await {
                error!("error while shutting down tonic: {}", e);
            }
        }
    }

    /// Given a map of [`TestInstance`] structs poll their pending message
    /// counts to wait for all instances to stop processing message backlogs.
    async fn wait_for_convergence(insts: &HashMap<Peer, TestInstance>) {
        // Periodically check pending messages to wait for protocol convergence
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let remaining: u64 = insts
                .values()
                .map(|i| i.pending_msgs.load(atomic::Ordering::SeqCst))
                .sum();
            debug!("{} messages remaining in queues", remaining);
            if remaining == 0 {
                break;
            }
        }
    }

    /// Test the background failure handling system by creating a 50 peer
    /// network and then removing a single node to ensure that other nodes have
    /// removed it from their views
    #[tokio::test]
    async fn test_failure_handling() -> Result<()> {
        tracing_subscriber::fmt::try_init();
        let cg = InMemoryConnectionGraph::new();
        let mut peers = Vec::new();
        for index in 0..50 {
            peers.push(crate::proto::Peer {
                host: "127.0.0.1".into(),
                port: index,
            })
        }
        let mut peer_to_inst = HashMap::new();
        // TODO(rossdylan): Throw this into a `FuturesOrdered`
        for peer in peers.iter() {
            let instance = TestInstance::new(peer, &cg).await?;
            peer_to_inst.insert(peer.clone(), instance);
        }

        // NOTE(rossdylan): In a block to make lifetimes happy with the
        // FuturesUnordered set
        {
            let mut fset = FuturesUnordered::new();
            for (_, instance) in peer_to_inst.iter_mut() {
                let bs_peer = peers[0].clone();
                fset.push(instance.init(std::iter::once(bs_peer)));
            }
            while let Some(res) = fset.next().await {
                res?;
            }
        }

        // Periodically check pending messages to wait for protocol convergence
        wait_for_convergence(&peer_to_inst).await;

        // shutdown a random instance
        let rpeer: Peer = peers
            .iter()
            .choose(&mut rand::thread_rng())
            .map(Clone::clone)
            .unwrap();

        // Figure out what peers are connected to our selected peer
        let connected_to_rpeer: Vec<Peer> = peer_to_inst
            .iter()
            .filter_map(|(p, i)| {
                if i.active_view().contains(&rpeer) {
                    Some(p.clone())
                } else {
                    None
                }
            })
            .collect();

        peer_to_inst.get_mut(&rpeer).unwrap().stop().await;
        peer_to_inst.remove(&rpeer);

        // Attempt a broadcast into the network
        let bpeer: Peer = peers
            .iter()
            .filter(|p| **p != rpeer)
            .choose(&mut rand::thread_rng())
            .map(Clone::clone)
            .unwrap();
        let msg = "foobar".as_bytes();
        peer_to_inst.get(&bpeer).unwrap().broadcast(msg).await?;

        // Periodically check pending messages to wait for protocol convergence
        wait_for_convergence(&peer_to_inst).await;

        // Verify failed node has been removed from the active views it was in
        // previously.
        for peer in connected_to_rpeer.iter() {
            let active_view = peer_to_inst[peer].active_view();

            // Assert that the failed peer is not in any active views
            assert!(
                !active_view.contains(&rpeer),
                "found failed peer in active view"
            );
        }

        Ok(())
    }

    /// Test the network bootstrap process by spawning 100 `HyParView` instances
    /// and connecting them all together using the first instance as the
    /// bootstrap. This acts a bit like an initial stress-test to ensure on
    /// something like a mass reboot the network recovers quickly
    #[tokio::test]
    async fn test_init() -> Result<()> {
        tracing_subscriber::fmt::try_init();
        let cg = InMemoryConnectionGraph::new();
        let mut peers = Vec::new();
        for index in 0..100 {
            peers.push(crate::proto::Peer {
                host: "127.0.0.1".into(),
                port: index,
            })
        }
        let mut peer_to_inst = HashMap::new();
        // TODO(rossdylan): Throw this into a `FuturesOrdered`
        for peer in peers.iter() {
            let instance = TestInstance::new(peer, &cg).await?;
            peer_to_inst.insert(peer.clone(), instance);
        }

        // NOTE(rossdylan): In a block to make lifetimes happy with the
        // FuturesUnordered set
        {
            let mut fset = FuturesUnordered::new();
            for (_, instance) in peer_to_inst.iter_mut() {
                let bs_peer = peers[0].clone();
                fset.push(instance.init(std::iter::once(bs_peer)));
            }
            while let Some(res) = fset.next().await {
                res?;
            }
        }

        // Periodically check pending messages to wait for protocol convergence
        wait_for_convergence(&peer_to_inst).await;

        // Now we attempt to verify network invariants
        for (current_peer, instance) in peer_to_inst.iter() {
            let active_view = instance.active_view();

            assert!(current_peer == &instance.me);
            // Assert that the hpv instance doesn't contain itself
            assert!(
                active_view.get(current_peer).is_none(),
                "instance {} contains itself in the active view",
                current_peer
            );

            // Verify active view symmetry
            for peer in active_view.iter() {
                let peer_instance = &peer_to_inst[peer];
                assert!(peer == &peer_instance.me);
                assert!(
                    peer_instance.active_view().contains(current_peer),
                    "active view symmetry between {} and {} not preserved: {}:{:?} -- {}:{:?}",
                    current_peer,
                    peer,
                    current_peer,
                    active_view,
                    peer,
                    peer_instance.active_view(),
                );
            }
        }
        Ok(())
    }
}
