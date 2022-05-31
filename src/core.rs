//! The core module contains the actual implementation of the hyparview protocol
use std::result::Result as StdResult;
use std::sync::atomic;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use futures::stream::FuturesOrdered;
use futures::StreamExt;
use indexmap::IndexSet;
use leaky_bucket::RateLimiter;
use rand::prelude::IteratorRandom;
use rand::prelude::SliceRandom;
use svix_ksuid::KsuidLike;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
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

impl OutgoingMessage {
    const FJ_NAME: &'static str = "ForwardJoin";
    const DI_NAME: &'static str = "Disconnect";
    const N_NAME: &'static str = "Neighbor";
    const S_NAME: &'static str = "Shuffle";
    const SR_NAME: &'static str = "ShuffleReply";
    const DA_NAME: &'static str = "Data";

    pub fn name(&self) -> &'static str {
        match self {
            Self::ForwardJoin { .. } => Self::FJ_NAME,
            Self::Disconnect { .. } => Self::DI_NAME,
            Self::Neighbor { .. } => Self::N_NAME,
            Self::Shuffle { .. } => Self::S_NAME,
            Self::ShuffleReply { .. } => Self::SR_NAME,
            Self::Data { .. } => Self::DA_NAME,
        }
    }

    pub fn dest(&self) -> &Peer {
        match self {
            Self::ForwardJoin { dest, .. } => dest,
            Self::Disconnect { dest, .. } => dest,
            Self::Neighbor { dest, .. } => dest,
            Self::Shuffle { dest, .. } => dest,
            Self::ShuffleReply { dest, .. } => dest,
            Self::Data { dest, .. } => dest,
        }
    }
}

/// Implements the HyParView protocol over tonic based gRPC transport. Transport
/// must be done via tonic/gRPC however the underlying tonic transport is generic
/// This structure encapsulates the underlying protocol implementation and runs
/// several background tasks to handle periodic shuffles, outgoing messages, and
/// failure handling.
#[allow(missing_debug_implementations)]
#[derive(Clone)]
pub struct HyParView<C: ConnectionManager + 'static> {
    me: Peer,
    params: NetworkParameters,
    manager: C,
    ftracker: crate::failure::Tracker,
    outgoing_msgs: mpsc::Sender<OutgoingMessage>,
    pending_msgs: Arc<atomic::AtomicU64>,
    state: Arc<Mutex<State>>,
    broadcast_tx: Arc<broadcast::Sender<(svix_ksuid::Ksuid, Vec<u8>)>>,
    join_rl: Arc<RateLimiter>,
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
        let (outgoing_tx, outgoing_rx) = mpsc::channel(params.queue_size());
        let (broadcast_tx, _) = broadcast::channel(1024);
        let hpv = HyParView {
            me: me.clone(),
            params,
            manager,
            // ftracker period chosen via ~*science*~
            // TODO(rossdylan): This value should be a [`NetworkParameters`]
            // protocol value
            ftracker: crate::failure::Tracker::new(Duration::from_secs(1)),
            outgoing_msgs: outgoing_tx,
            pending_msgs: Arc::new(Default::default()),
            state: Arc::new(Mutex::new(State::new(me, params))),
            broadcast_tx: Arc::new(broadcast_tx),
            join_rl: Arc::new(
                RateLimiter::builder()
                    .max(params.join_rate())
                    .refill(params.join_rate())
                    .initial(25)
                    .interval(Duration::from_secs(1))
                    .build(),
            ),
            metrics: crate::metrics::ServerMetrics::new(),
        };
        // NOTE(rossdylan): I really don't like having HyParView::new(...) being
        // stateful, but its kinda the only way I can make the channels work
        // the way I want.
        tokio::spawn(hpv.clone().outgoing_task(outgoing_rx));
        tokio::spawn(hpv.clone().failure_handler());
        tokio::spawn(hpv.clone().shuffle_task());
        hpv
    }

    /// Return a broadcast channel that will emit the contents of all data
    /// messages that this hyparview instance receives.
    pub fn subscribe(&self) -> broadcast::Receiver<(svix_ksuid::Ksuid, Vec<u8>)> {
        self.broadcast_tx.subscribe()
    }

    /// Hint to the failure tracker that a peer has failed. Useful for higher
    /// level abstractions which may have their own concept of failure.
    pub fn hint_failure(&self, peer: &Peer) {
        if self.state.lock().unwrap().active_contains(peer) {
            self.ftracker.fail(peer);
        }
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
                    if self.ftracker.is_failed(peer) {
                        trace!(
                            "[{}] broadcast to failed peer {} succeeded, marking up",
                            self.me,
                            peer
                        );
                        self.ftracker.remove(peer)
                    }
                    sent = true;
                }
            }
        }
        self.metrics
            .report_broadcast(sent, broadcast_start_time.elapsed());
        if sent {
            Ok(())
        } else if self.passive_view().is_empty() {
            // In the event that all active-view peers have failed and we have
            // no passive view peers to replace them with inform our caller that
            // we've reached a fatal netsplit. This is a bit of a cop-out since
            // I'd prefer if we could attempt recovery on our own, but this will
            // allow our users to handle netsplits in whatever way makes the most
            // sense for their usecase
            Err(Error::FatalNetsplit)
        } else {
            Err(Error::BroadcastFailed)
        }
    }

    /// Return the current set of active peers
    pub fn active_view(&self) -> IndexSet<Peer> {
        self.state.lock().unwrap().active_view.clone()
    }

    pub(crate) fn passive_view(&self) -> IndexSet<Peer> {
        self.state.lock().unwrap().passive_view.clone()
    }

    async fn send_join(&self, peer: &Peer) -> Result<crate::proto::JoinResponse> {
        let mut client = self.manager.connect(peer).await?;
        client
            .join(crate::proto::JoinRequest {
                source: Some(self.me.clone()),
                id: gen_msg_id(),
            })
            .await
            .map(|r| r.into_inner())
            .map_err(Into::into)
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
        // Grab our initial peers from the bootstrap source and shuffle them to
        // ensure we don't hammer the first peer in the list. Additionally
        // truncate it to the size of our active view to avoid cases where we
        // attempt to contact a huge number of peers
        let mut peers: Vec<Peer> = boots
            .peers()
            .await?
            .into_iter()
            .filter(|p| *p != self.me)
            .collect();

        // This is an optimization for cold-start's of a hyparview network. If
        // there are no known nodes to bootstrap from the client application can
        // be informed and take a different path
        if peers.is_empty() {
            return Err(Error::NoBootstrapAddrsFound);
        }
        trace!("[{}] bootstrap peer list: {:?}", self.me, peers);
        peers.shuffle(&mut rand::thread_rng());
        trace!("[{}] shuffled bootstrap peer list: {:?}", self.me, peers);
        peers.truncate(
            peers
                .len()
                .min(self.params.active_size() + self.params.passive_size()),
        );
        trace!(
            "[{}] shuffled truncated bootstrap peer list: {:?}",
            self.me,
            peers
        );

        for (index, peer) in peers.iter().enumerate() {
            trace!("[{}] attempting to Join to {}", self.me, peer);
            self.state.lock().unwrap().add_to_active_view(peer);
            let join_res = self.send_join(peer).await;
            trace!("[{}] got join response: {:?}", self.me, join_res);
            match join_res {
                Ok(resp) => {
                    trace!("[{}] successfully joined to {}", self.me, peer);
                    // Combine the extra bootstrap peers, and the passive view from
                    // our bootstrap peer and use it to randomly fill our passive
                    // view
                    let new_passive: Vec<Peer> = peers[index + 1..]
                        .iter()
                        .chain(resp.passive_peers.iter())
                        .choose_multiple(&mut rand::thread_rng(), self.params.passive_size())
                        .into_iter()
                        .cloned()
                        .collect();

                    if !new_passive.is_empty() {
                        trace!(
                            "[{}] extra bootstrap passive peers: {:?}",
                            self.me,
                            new_passive
                        );
                        self.state
                            .lock()
                            .unwrap()
                            .add_peers_to_passive(&new_passive);
                    }

                    debug!(
                        "[{}] bootstrap successful, initial peer: {}, active: {:?}, passive: {:?}",
                        self.me,
                        peer,
                        self.active_view(),
                        self.passive_view()
                    );

                    return Ok(());
                }
                Err(e) => {
                    self.state.lock().unwrap().disconnect(peer);
                    warn!("failed to join bootstrap peer: {:?}: {}", peer, e);
                }
            }
        }
        Err(Error::BootstrapFailed)
    }

    /// Send the peer that had a failure over to our async failure handler
    /// task.
    fn report_failure(&self, peer: &Peer) {
        self.metrics.report_peer_failure();
        self.ftracker.fail(peer);
    }

    /// Helper function which places an outgoing message into the outgoing
    /// queue. It also increments our pending messages counter so we have an
    /// idea of what our backlog is like.
    fn enqueue_message(&self, msg: OutgoingMessage) {
        let name = msg.name();
        if let Err(e) = self.outgoing_msgs.try_send(msg) {
            trace!(
                "[{}] dropping message {}, queue error: {}",
                self.me,
                name,
                e
            );
            self.metrics.report_msg_drop(name)
        } else {
            self.metrics.incr_pending(name);
            self.pending_msgs.fetch_add(1, atomic::Ordering::SeqCst);
        }
    }

    /// A background task that pulls from a mpsc and sends messages to other
    /// peers. We use this to decouple sending messages from receiving them.
    /// The HyParView paper makes a lot of assumptions of a fully asynchronus
    /// messaging model.
    async fn outgoing_task(self, mut rx: mpsc::Receiver<OutgoingMessage>) {
        trace!("[{}] outgoing task spawned", self.me);
        loop {
            if let Some(ref msg) = rx.recv().await {
                let start_time = Instant::now();
                self.metrics.decr_pending(msg.name());
                self.pending_msgs.fetch_sub(1, atomic::Ordering::SeqCst);

                // Extract destination and check to make sure we are actually
                // connected to it (unless it is a transient connection like ShuffleReply or Disconnect)
                // We do this to ensure that during mass failures or nother
                // network churn events we efficiently discard messages that we
                // can no longer deliver
                let dest = msg.dest();
                let is_transient = matches!(
                    msg,
                    OutgoingMessage::Disconnect { .. } | OutgoingMessage::ShuffleReply { .. }
                );
                if !self.state.lock().unwrap().active_contains(dest) && !is_transient {
                    trace!(
                        "[{}] skipping outgoing {} msg to {}, not connected anymore",
                        self.me,
                        msg.name(),
                        dest
                    );
                    continue;
                }

                let res = match msg {
                    OutgoingMessage::ForwardJoin { src, dest, ttl } => {
                        self.send_forward_join(src, dest, *ttl).await
                    }
                    OutgoingMessage::Disconnect { dest } => {
                        self.send_disconnect(dest).await;
                        let _ = self.manager.disconnect(dest).await;
                        Ok(())
                    }
                    OutgoingMessage::Neighbor { dest, prio } => {
                        let resp = self.send_neighbor(dest, *prio).await.map(|_| ());
                        resp
                    }
                    OutgoingMessage::Shuffle {
                        src,
                        dest,
                        peers,
                        ttl,
                    } => self.send_shuffle(src, dest, *ttl, peers).await,
                    OutgoingMessage::ShuffleReply { dest, peers } => {
                        self.send_shuffle_reply(dest, peers).await
                    }
                    OutgoingMessage::Data { dest, req } => self.send_data(dest, req).await,
                };
                if let Err(e) = res {
                    warn!(
                        "[{}] failed to send '{}' to {}: {}",
                        self.me,
                        msg.name(),
                        dest,
                        e,
                    );
                    self.report_failure(dest);
                } else {
                    // if we succeeded mark the peer as back up
                    self.ftracker.remove(dest);
                }
                self.metrics.report_outgoing_loop_time(start_time.elapsed());
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
    async fn replace_peer(&self, failed: &Peer) -> Result<bool> {
        let mut attempts = 0;
        loop {
            let (maybe_peer, prio) = {
                let state = self.state.lock().unwrap();
                // ensure that if a failed peer is reported multiple times we
                // only execute replacement for it once
                if !state.active_contains(failed) {
                    debug!("[{}] failed peer {} was already removed", self.me, failed);
                    return Ok(false);
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
                        warn!("[{}] replacing {} with {}", self.me, failed, peer);
                        return Ok(true);
                    } else {
                        debug!(
                            "[{}] failed to replace {} with {}, peer rejected neighbor request",
                            self.me, failed, peer
                        )
                    }
                } else {
                    warn!(
                        "[{}] failed to replace {} with {}, removing from passive view",
                        self.me, failed, peer
                    );
                    self.state.lock().unwrap().passive_view.remove(&peer);
                }
            } else {
                let mut state = self.state.lock().unwrap();
                // In the case of failures where we still have peers in our
                // active view, but no passive peers to replace them with we
                // can remove the failed peer to ensure nothing tries to talk to
                // it
                if state.active_view.len() > 1 {
                    debug!(
                        "[{}] removing failed peer {} without replacement",
                        self.me, failed,
                    );
                    state.active_view.remove(failed);
                    return Ok(false);
                }
                return Err(Error::PassiveViewEmpty);
            }
            attempts += 1;
        }
    }

    /// Handle failures in the background by listening on a channel and running
    /// the failure algorithm.
    async fn failure_handler(self) {
        loop {
            let failed = self.ftracker.wait().await;
            debug!(
                "[{}] started failure handling iteration: {:?}",
                self.me, failed,
            );
            for failed_peer in failed {
                let start_time = Instant::now();
                match self.replace_peer(&failed_peer).await {
                    Ok(replaced) => {
                        self.ftracker.remove(&failed_peer);
                        let _ = self.manager.disconnect(&failed_peer).await;
                        if replaced {
                            self.metrics.report_peer_replacement(true);
                        }
                    }
                    Err(e) => {
                        self.metrics.report_peer_replacement(false);
                        warn!("[{}] unable to replace {}: {}", self.me, failed_peer, e);
                    }
                }
                self.metrics.report_failure_loop_time(start_time.elapsed());
            }
        }
    }

    /// The shuffle task is started up on init and periodically issues a shuffle
    /// request into the network to update our passive views.
    async fn shuffle_task(self) {
        loop {
            // TODO(rossdylan): I've randomly chosen 120s as the basis for our
            // periodic shuffle, but idk what the paper expects here. This should
            // be a configuration value in `NetworkParameters`
            // ---
            // NOTE(rossdylan): For mass failures early in a networks life the
            // shuffle process seems key to ensuring some kind of network recovery
            // Experimentally, in a network that has lost half its nodes some
            // instances will be stuck with an empty passive view until a shuffle
            // kicks off and fills it up again
            tokio::time::sleep(crate::util::jitter(Duration::from_secs(120))).await;
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
                let selected_peers = state.select_shuffle_peers(Some(&destination));
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
    ) -> StdResult<Response<crate::proto::JoinResponse>, Status> {
        let req_ref = request.get_ref();
        if req_ref.source.is_none() {
            return Err(Status::invalid_argument("no source peer specified"));
        }
        let source = req_ref.source.as_ref().unwrap();
        if source == &self.me {
            return Ok(Response::new(crate::proto::JoinResponse {
                passive_peers: vec![],
            }));
        }
        debug!(
            "[{}] join from {}, before rate-limit: {}",
            self.me,
            source,
            self.join_rl.balance()
        );
        // To avoid flooding the network with ForwardJoin requests we limit
        // incoming joins. This also has a nice side effect of spreading join
        // load out across other bootstrap nodes
        self.join_rl.acquire_one().await;

        debug!(
            "[{}] join from {} after rate-limit, before connect",
            self.me, source
        );
        // Treat this as a warning in case its transient, if it isn't the
        // failure detection will clean it up properly soon
        if let Err(e) = self.manager.connect(source).await {
            warn!(
                "failed to form reverse connection to {:?} who sent us a JOIN: {}",
                source, e
            );
        }
        debug!(
            "[{}] join from {} after connect before state mod",
            self.me, source
        );
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
        // NOTE(rossdylan): To improve network resilience in high churn envs
        // (like kubernetes) we cheat a little and help fill out a new nodes
        // passive view by sending them a shuffle inline with the join response
        let passive_peers = state.select_shuffle_peers(Some(source));
        Ok(Response::new(crate::proto::JoinResponse { passive_peers }))
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
        {
            let mut state = self.state.lock().unwrap();
            state.disconnect(source);
            if state.active_view.is_empty() {
                if let Some(replacement) = state.random_passive_peer() {
                    debug!(
                        "[{}] disconnect from {} would empty active-view, replacing with {}",
                        self.me, source, replacement
                    );
                    state.active_view.insert(replacement.clone());
                    state.passive_view.remove(&replacement);
                    self.enqueue_message(OutgoingMessage::Neighbor {
                        dest: replacement,
                        prio: NeighborPriority::High,
                    })
                }
            }
        }
        let _ = self.manager.disconnect(source).await;
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
                peers: state.random_passive_peers(Some(source), req_ref.peers.len()),
            });
            state.add_peers_to_passive(&req_ref.peers);
        } else if let Some(n) = state.random_active_peer(Some(source)) {
            debug!(
                "[{}] recieved shuffle from {} with ttl {}, forwarding to {}",
                self.me, source, req_ref.ttl, n
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
        debug!(
            "[{}] received shuffle-reply from {} with {} peers",
            self.me,
            source,
            req_ref.peers.len()
        );
        // TODO(rossdylan): The paper prioritises evicting peers in our passive
        // view that we've sent via shuffle. That's kinda tricky to implement in
        // our current architecture so I'm leaving it out.
        self.state
            .lock()
            .unwrap()
            .add_peers_to_passive(&req_ref.peers);
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

        let active_peers = {
            let mut state = self.state.lock().unwrap();
            // If we've seen this message before ignore it
            if state.message_seen(ksuid.bytes()) {
                self.metrics.report_data_msg(true);
                return Ok(Response::new(Empty {}));
            }
            self.metrics.report_data_msg(false);
            state.record_message(ksuid.bytes());
            state.active_view.clone()
        };

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
        for peer in active_peers.into_iter() {
            if peer == *source {
                continue;
            }
            let cloned_data = req_ref.clone();
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
        handle: Option<JoinHandle<()>>,
    }

    #[derive(Clone)]
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
                NetworkParameters::default_for_test(),
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
                debug!("tonic shutdown");
            });
            Ok(TestInstance {
                inner: hpv,
                extras: Arc::new(Mutex::new(TestInstanceExtras {
                    signal: Some(tx),
                    handle: Some(jh),
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
            let jh = self.extras.lock().unwrap().handle.take().unwrap();
            if let Err(e) = jh.await {
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
        let _ = tracing_subscriber::fmt::try_init();
        let cg = InMemoryConnectionGraph::new();
        let mut peers = Vec::new();
        for index in 0..60 {
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
            for (index, peer) in peers.iter().enumerate() {
                if index == 0 {
                    continue;
                }
                let bs_peer = peers[index - 1].clone();
                let mut instance = peer_to_inst.get_mut(peer).unwrap().clone();
                fset.push(async move { instance.init(bs_peer).await });
            }
            while let Some(res) = fset.next().await {
                res?;
            }
        }
        // Periodically check pending messages to wait for protocol convergence
        wait_for_convergence(&peer_to_inst).await;

        for (peer, inst) in peer_to_inst.iter() {
            assert!(
                !inst.active_view().is_empty(),
                "peer {} had empty active-view after init",
                peer
            )
        }

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

        debug!(
            "will fail '{}' with active view: {:?}",
            rpeer,
            peer_to_inst.get_mut(&rpeer).unwrap().active_view()
        );
        peer_to_inst.get_mut(&rpeer).unwrap().stop().await;
        peer_to_inst.remove(&rpeer);
        wait_for_convergence(&peer_to_inst).await;
        debug!("shut down {} for test", rpeer);
        // Attempt a broadcast into the network
        let bpeer: Peer = peers
            .iter()
            .filter(|p| **p != rpeer)
            .choose(&mut rand::thread_rng())
            .map(Clone::clone)
            .unwrap();
        let msg = "foobar".as_bytes();
        for peer in peers {
            if peer == rpeer {
                continue;
            }
            assert!(!peer_to_inst.get(&bpeer).unwrap().active_view().is_empty());
            peer_to_inst.get(&peer).unwrap().broadcast(msg).await?;
        }
        debug!("completed broadcast");
        // wait for the next failure tick to occur
        tokio::time::sleep(Duration::from_secs(1)).await;
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

    /// Simple test to ensure that the init future is Send. Previously there was
    /// an issue with using thread_rng() in a local variable within the init
    /// future. The Rng produced by thread_rng() is not Send, which means we
    /// can not use it in a variable within init() and then spawn init
    #[tokio::test]
    async fn ensure_init_is_send() -> Result<()> {
        let cg = InMemoryConnectionGraph::new();
        let p = crate::proto::Peer {
            host: "127.0.0.1".into(),
            port: 0,
        };
        let mut inst = TestInstance::new(&p, &cg).await?;
        let jh = tokio::spawn(async move { (*inst).init(p).await });
        let _res = jh.await;
        Ok(())
    }

    /// Test the network bootstrap process by spawning 100 `HyParView` instances
    /// and connecting them all together using the first instance as the
    /// bootstrap. This acts a bit like an initial stress-test to ensure on
    /// something like a mass reboot the network recovers quickly
    #[tokio::test]
    async fn test_init() -> Result<()> {
        let _ = tracing_subscriber::fmt::try_init();
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
            for (index, peer) in peers.iter().enumerate() {
                if index == 0 {
                    continue;
                }
                let bs_peer = peers[index - 1].clone();
                let mut instance = peer_to_inst.get_mut(peer).unwrap().clone();
                fset.push(async move { instance.init(bs_peer).await });
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
            let passive_view = instance.passive_view();

            // There should not be any overlap between active and passive views
            assert!(passive_view.is_disjoint(&active_view));

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

    /// Test to ensure that a broadcast into a healthy network reaches all peers
    #[tokio::test]
    async fn test_broadcast() -> Result<()> {
        let _ = tracing_subscriber::fmt::try_init();
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
            for (index, peer) in peers.iter().enumerate() {
                if index == 0 {
                    continue;
                }
                let bs_peer = peers[index - 1].clone();
                let mut instance = peer_to_inst.get_mut(peer).unwrap().clone();
                fset.push(async move { instance.init(bs_peer).await });
            }
            while let Some(res) = fset.next().await {
                res?;
            }
        }

        // Periodically check pending messages to wait for protocol convergence
        wait_for_convergence(&peer_to_inst).await;

        {
            let mut fset = FuturesUnordered::new();
            for (p, instance) in peer_to_inst.iter() {
                if p == &peers[0] {
                    continue;
                }
                let mut sub = instance.subscribe();
                fset.push(async move { sub.recv().await });
            }
            peer_to_inst[&peers[0]]
                .broadcast("foobar".as_bytes())
                .await?;
            let mut count = 0;
            while let Some(Ok(_)) = fset.next().await {
                count += 1;
                debug!("count={}, expected={}", count, peer_to_inst.len() - 1);
            }
            assert_eq!(
                count,
                peer_to_inst.len() - 1,
                "broadcast did not reach all peers"
            )
        }
        Ok(())
    }
}
