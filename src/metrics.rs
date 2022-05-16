//! Internal metrics definitions for hyparview
use std::time::Duration;

use metrics::{
    describe_counter, describe_gauge, describe_histogram, register_counter, register_gauge,
    register_histogram, Counter, Gauge, Histogram,
};

#[derive(Clone)]
pub struct StateMetrics {
    passive_size: Gauge,
    active_size: Gauge,
}

impl StateMetrics {
    pub fn new() -> Self {
        describe_gauge!(
            "hyparview_active_size",
            "the size of this hyparview instance's active view"
        );
        describe_gauge!(
            "hyparview_passive_size",
            "the size of this hyparview instance's passive view"
        );
        Self {
            passive_size: register_gauge!("hyparview_active_size"),
            active_size: register_gauge!("hyparview_passive_size"),
        }
    }
    pub fn record_view_sizes(&self, active: usize, passive: usize) {
        self.active_size.set(active as f64);
        self.passive_size.set(passive as f64);
    }
}

impl std::fmt::Debug for StateMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateMetrics").finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct ServerMetrics {
    /// How many outgoing messages are in our queue
    pending_messages: Gauge,
    /// Counter for data messages we've forwarded to other peers
    forwarded_data_messages: Counter,
    /// Counter for data messages we've ignored because we've already seen them
    ignored_data_messages: Counter,
    /// Counter tracking peer failures
    peer_failures: Counter,
    peer_replacement_success: Counter,
    peer_replacement_failure: Counter,
    broadcast_failure: Counter,
    broadcast_success: Counter,
    broadcast_latency_ms: Histogram,
    shuffle_triggered: Counter,
}

impl ServerMetrics {
    pub fn new() -> Self {
        describe_gauge!(
            "hyparview_pending_messages",
            "how many messages are in our outgoing messages queue"
        );
        describe_counter!(
            "hyparview_data_messages",
            "count of data messages that have been forwarded"
        );
        describe_counter!("hyparview_peer_failures", "count of peer failures");
        describe_counter!(
            "hyparview_peer_replacements",
            "status of peer replacement operations"
        );
        describe_counter!(
            "hyparview_broadcast_status",
            "status of local broadcast requests"
        );
        describe_histogram!(
            "hyparview_broadcast_latency_ms",
            metrics::Unit::Milliseconds,
            "latency in milliseconds of local broadcast operations"
        );
        describe_counter!(
            "hyparview_shuffle_triggered",
            "how often shuffles are triggered"
        );
        Self {
            pending_messages: register_gauge!("hyparview_pending_messages"),
            forwarded_data_messages: register_counter!("hyparview_data_messages", "status" => "forwarded"),
            ignored_data_messages: register_counter!("hyparview_data_messages", "status" => "ignored"),
            peer_failures: register_counter!("hyparview_peer_failures"),
            peer_replacement_success: register_counter!("hyparview_peer_replacements", "status" => "success"),
            peer_replacement_failure: register_counter!("hyparview_peer_replacements", "status" => "failure"),
            broadcast_failure: register_counter!("hyparview_broadcast_status", "status" => "failure"),
            broadcast_success: register_counter!("hyparview_broadcast_status", "status" => "success"),
            broadcast_latency_ms: register_histogram!("hyparview_broadcast_latency_ms"),
            // TODO(rossdylan): Add status and histograms for shuffles
            shuffle_triggered: register_counter!("hyparview_shuffle_triggered"),
        }
    }

    pub fn report_shuffle(&self) {
        self.shuffle_triggered.increment(1)
    }

    pub fn report_broadcast(&self, success: bool, latency: Duration) {
        self.broadcast_latency_ms.record(latency.as_millis() as f64);
        if success {
            self.broadcast_success.increment(1)
        } else {
            self.broadcast_failure.increment(1)
        }
    }

    pub fn report_peer_replacement(&self, success: bool) {
        if success {
            self.peer_replacement_success.increment(1)
        } else {
            self.peer_replacement_failure.increment(1)
        }
    }

    pub fn report_peer_failure(&self) {
        self.peer_failures.increment(1)
    }

    pub fn report_data_msg(&self, ignored: bool) {
        if ignored {
            self.ignored_data_messages.increment(1)
        } else {
            self.forwarded_data_messages.increment(1)
        }
    }

    pub fn incr_pending(&self) {
        self.pending_messages.increment(1.0);
    }
    pub fn decr_pending(&self) {
        self.pending_messages.decrement(1.0)
    }
}

impl std::fmt::Debug for ServerMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerMetrics").finish_non_exhaustive()
    }
}
