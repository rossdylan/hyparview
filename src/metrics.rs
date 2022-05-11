//! Internal metrics definitions for hyparview
use metrics::{describe_counter, describe_gauge, register_counter, register_gauge, Counter, Gauge};

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
    pending_messages: Gauge,
}

impl ServerMetrics {
    pub fn new() -> Self {
        describe_gauge!(
            "hyparview_pending_messages",
            "how many messages are in our outgoing messages queue"
        );
        Self {
            pending_messages: register_gauge!("hyparview_pending_messages"),
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
