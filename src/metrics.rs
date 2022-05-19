//! Internal metrics definitions for hyparview
use std::time::Duration;

use metrics::{
    decrement_gauge, describe_counter, describe_gauge, describe_histogram, increment_counter,
    increment_gauge, register_counter, register_gauge, register_histogram, Counter, Gauge,
    Histogram,
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
    failure_process_time: Histogram,
    outgoing_process_time: Histogram,
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
        describe_histogram!(
            "hyparview_failure_process_time_ms",
            metrics::Unit::Milliseconds,
            "latency for a single iteration of the failure handler loop"
        );
        describe_histogram!(
            "hyparview_outgoing_process_time_ms",
            metrics::Unit::Milliseconds,
            "latency for a single iteration of the outgoing message loop"
        );
        describe_counter!(
            "hyparview_dropped_messages",
            "rate of dropped messages due to overflow of the outgoing queue"
        );
        register_counter!("hyparview_dropped_messages");
        Self {
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
            failure_process_time: register_histogram!("hyparview_failure_process_time_ms"),
            outgoing_process_time: register_histogram!("hyparview_outgoing_process_time_ms"),
        }
    }

    pub fn report_failure_loop_time(&self, latency: Duration) {
        self.failure_process_time.record(latency.as_millis() as f64);
    }

    pub fn report_outgoing_loop_time(&self, latency: Duration) {
        self.outgoing_process_time
            .record(latency.as_millis() as f64);
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

    pub fn report_msg_drop(&self, msg: &'static str) {
        increment_counter!("hyparview_dropped_messages", "type" => msg);
    }

    pub fn incr_pending(&self, msg: &'static str) {
        increment_gauge!("hyparview_pending_messages", 1.0, "type" => msg);
    }
    pub fn decr_pending(&self, msg: &'static str) {
        decrement_gauge!("hyparview_pending_messages", 1.0, "type" => msg);
    }
}

impl std::fmt::Debug for ServerMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerMetrics").finish_non_exhaustive()
    }
}
