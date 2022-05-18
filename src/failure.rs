//! Structure used to efficiently track and notify the hyparview protocol of
//! failures.
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tracing::debug;

use crate::proto::Peer;

#[derive(Debug, Clone)]
struct TrackerState {
    failed: HashSet<Peer>,
    last_triggered: Option<Instant>,
}

impl TrackerState {
    pub fn new() -> Self {
        Self {
            failed: HashSet::new(),
            last_triggered: None,
        }
    }

    pub fn since_trigger(&self) -> Duration {
        self.last_triggered
            .as_ref()
            .map(Instant::elapsed)
            .unwrap_or(Duration::ZERO)
    }

    pub fn trigger(&mut self) {
        self.last_triggered.replace(Instant::now());
    }
}

impl Default for TrackerState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct Tracker {
    period: Duration,
    state: Arc<Mutex<TrackerState>>,
    notifier: Arc<Notify>,
}

impl Tracker {
    pub fn new(period: Duration) -> Self {
        let t = Self {
            period,
            state: Default::default(),
            notifier: Default::default(),
        };

        let bg_t = t.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(bg_t.period);
            loop {
                ticker.tick().await;
                let mut state = bg_t.state.lock().unwrap();
                if !state.failed.is_empty() {
                    state.trigger();
                    debug!("ticker notifying failure handler");
                    bg_t.notifier.notify_waiters();
                }
            }
        });
        t
    }

    pub fn fail(&self, p: &Peer) {
        let mut state = self.state.lock().unwrap();
        state.failed.insert(p.clone());
        if state.since_trigger() >= self.period {
            state.trigger();
            self.notifier.notify_waiters();
        }
    }

    pub async fn wait(&self) -> Vec<Peer> {
        let notifier = self.notifier.clone();
        loop {
            notifier.notified().await;
            let state = self.state.lock().unwrap();
            if !state.failed.is_empty() {
                return state.failed.iter().cloned().collect();
            }
        }
    }

    pub fn remove(&self, p: &Peer) {
        let mut state = self.state.lock().unwrap();
        state.failed.remove(p);
    }
}
