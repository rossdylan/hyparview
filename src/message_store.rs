//! Helpers used to track messages already seen by the HyParView protocol

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

use indexmap::IndexSet;

/// The MessageStore is used to track what messages have been seen my this
/// instance of HyParView. Using an IndexSet tracks the order in which messages
/// are seen implicitly, so we can just pop the oldest when we need to evict
#[derive(Clone, Debug)]
pub struct MessageStore<S = RandomState> {
    max: usize,
    map: IndexSet<u64, S>,
}

impl MessageStore {
    /// Create a new instance of the message store with the provided max size.
    pub fn new(max: usize) -> Self {
        return MessageStore {
            max: max,
            map: IndexSet::with_capacity(max),
        };
    }
}

impl<S: BuildHasher> MessageStore<S> {
    /// Create a new instance of the message store with the provided max size
    /// and the given custom hasher.
    pub fn with_hasher(max: usize, hash_builder: S) -> Self {
        return MessageStore {
            max: max,
            map: IndexSet::with_capacity_and_hasher(max, hash_builder),
        };
    }

    /// Add a message to the message store. If we have reached the limit pop
    /// the oldest entry first before adding the new one.
    pub fn insert(&mut self, m: &super::Message) {
        if self.map.len() >= self.max {
            self.map.pop();
        }
        self.map.insert(m.id);
    }

    /// Check if our message store contains the given message.
    pub fn contains(&self, m: &super::Message) -> bool {
        self.map.contains(&m.id)
    }
}
