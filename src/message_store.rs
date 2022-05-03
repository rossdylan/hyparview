//! Helpers used to track messages already seen by the HyParView protocol

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

use indexmap::IndexSet;
use svix_ksuid::{Ksuid, KsuidLike};

/// Generate a new ksuid for a message id.
pub fn gen_msg_id() -> Vec<u8> {
    let bytes_ref = *Ksuid::new(None, None).bytes();
    bytes_ref.into()
}
/// The MessageStore is used to track what messages have been seen my this
/// instance of HyParView. Using an IndexSet tracks the order in which messages
/// are seen implicitly, so we can just pop the oldest when we need to evict
#[derive(Clone, Debug)]
pub struct MessageStore<S = RandomState> {
    max: usize,
    map: IndexSet<[u8; 20], S>,
}

impl MessageStore {
    /// Create a new instance of the message store with the provided max size.
    pub fn new(max: usize) -> Self {
        MessageStore {
            max,
            map: IndexSet::with_capacity(max),
        }
    }
}

impl<S: BuildHasher> MessageStore<S> {
    /// Add a message to the message store. If we have reached the limit pop
    /// the oldest entry first before adding the new one.
    pub fn insert(&mut self, mid: &[u8; 20]) {
        if self.map.len() >= self.max {
            self.map.pop();
        }
        self.map.insert(*mid);
    }

    /// Check if our message store contains the given message.
    pub fn contains(&self, mid: &[u8]) -> bool {
        self.map.contains(mid)
    }
}
