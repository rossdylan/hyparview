//! Helpers used to track messages already seen by the HyParView protocol

use lru::LruCache;
use svix_ksuid::{Ksuid, KsuidLike};

/// Generate a new ksuid for a message id.
pub fn gen_msg_id() -> Vec<u8> {
    let bytes_ref = *Ksuid::new(None, None).bytes();
    bytes_ref.into()
}
/// The MessageStore is used to track what messages have been seen my this
/// instance of HyParView. Using an IndexSet tracks the order in which messages
/// are seen implicitly, so we can just pop the oldest when we need to evict
#[derive(Debug)]
pub struct MessageStore {
    messages: LruCache<[u8; 20], ()>,
}

impl MessageStore {
    /// Create a new instance of the message store with the provided max size.
    pub fn new(max: usize) -> Self {
        MessageStore {
            messages: LruCache::new(std::num::NonZeroUsize::new(max).unwrap()),
        }
    }
}

impl MessageStore {
    /// Add a message to the message store. If we have reached the limit pop
    /// the oldest entry first before adding the new one.
    pub fn insert(&mut self, mid: &[u8; 20]) {
        self.messages.push(*mid, ());
    }

    /// Check if our message store contains the given message.
    pub fn contains(&self, mid: &[u8; 20]) -> bool {
        self.messages.contains(mid)
    }
}

#[cfg(test)]
mod test {
    use crate::error::Result;
    #[test]
    /// Ensure that our message store evicts the oldest message first when we
    /// reach the size limit
    fn test_const_size() -> Result<()> {
        let mut ms = super::MessageStore::new(2);
        let m1: [u8; 20] = [0; 20];
        let m2: [u8; 20] = [1; 20];
        let m3: [u8; 20] = [2; 20];
        ms.insert(&m1);
        assert!(ms.contains(&m1));
        ms.insert(&m2);
        assert!(ms.contains(&m1));
        assert!(ms.contains(&m2));
        ms.insert(&m3);
        assert!(ms.contains(&m2));
        assert!(ms.contains(&m3));
        Ok(())
    }
}
