//! Helpers used to track messages already seen by the HyParView protocol

use std::cmp::Ordering;
use std::collections::hash_map::RandomState;
use std::collections::{BinaryHeap, HashSet};
use std::hash::{BuildHasher, Hash, Hasher};
use std::time::SystemTime;

#[derive(Debug, Clone)]
struct Entry(u64, SystemTime);

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.1.eq(&other.1)
    }
}

impl Eq for Entry {}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        // NOTE(rossdylan): invert the comparison so we can use our max-heap as
        // a min-heap
        other.1.cmp(&self.1)
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The MessageStore is used to track what messages have been seen my this
/// instance of HyParView. I wanted lookups to be fast, and to evict the oldest
/// messages first. To do so I sacrifice some space and store hashes in a set
/// and timestamps in a heap. This makes our time complexities:
/// * insert:   O(1)~
/// * evict:    O(log(n))
/// * contains: O(1)
/// and our space complexity 2n. More concretely /// we are storing 3 64bit
/// integers for every message which leads us to 24 bytes per message.
/// There is probably a better/more efficient way to do this, but at least we
/// aren't storing the full messages anymore
#[derive(Clone, Debug)]
pub struct MessageStore<S = RandomState> {
    max: usize,
    set: HashSet<u64, S>,
    heap: BinaryHeap<Entry>,
}

impl MessageStore {
    /// Create a new instance of the message store with the provided max size.
    pub fn new(max: usize) -> Self {
        return MessageStore {
            max: max,
            set: HashSet::with_capacity(max),
            heap: BinaryHeap::with_capacity(max),
        };
    }
}

impl<S: BuildHasher> MessageStore<S> {
    /// Create a new instance of the message store with the provided max size
    /// and the given custom hasher.
    pub fn with_hasher(max: usize, hash_builder: S) -> Self {
        return MessageStore {
            max: max,
            set: HashSet::with_capacity_and_hasher(max, hash_builder),
            heap: BinaryHeap::with_capacity(max),
        };
    }

    /// Add a message to the message store. If we have reached the limit pop
    /// the oldest entry first before adding the new one.
    pub fn insert(&mut self, m: &super::Message) {
        if self.set.len() >= self.max {
            if let Some(e) = self.heap.pop() {
                self.set.remove(&e.0);
            } else {
                panic!(
                    "len(MessageStore.map) = {}, len(MessageStore.heap) = {} NOT EQUAL",
                    self.set.len(),
                    self.heap.len()
                );
            }
        }
        let mut hasher = self.set.hasher().build_hasher();
        m.hash(&mut hasher);
        let mid = hasher.finish();
        self.heap.push(Entry(mid, SystemTime::now()));
        self.set.insert(mid);
    }

    /// Check if our message store contains the given message.
    pub fn contains(&self, m: &super::Message) -> bool {
        let mut hasher = self.set.hasher().build_hasher();
        m.hash(&mut hasher);
        let mid = hasher.finish();
        return self.set.contains(&mid);
    }
}
