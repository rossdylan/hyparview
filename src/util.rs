//! Util is the junk-drawer of the project
use rand::{thread_rng, Rng};
use std::ops::{Add, Sub};
use std::time::Duration;

use svix_ksuid::{Ksuid, KsuidLike};

use crate::error::{Error, Result};

/// Generate a duration that has been jittered +/- 25%
pub(crate) fn jitter(dur: Duration) -> Duration {
    let mut rng = thread_rng();
    let high = dur.add(dur.mul_f64(0.25));
    let low = dur.sub(dur.mul_f64(0.25));
    rng.gen_range(low..high)
}

/// Turn a byte slice back into a `Ksuid` instance
pub(crate) fn ksuid_from_bytes(b: &[u8]) -> Result<svix_ksuid::Ksuid> {
    if b.len() != 20 {
        return Err(Error::InvalidMessageID);
    }
    let mut array_mid: [u8; 20] = [0; 20];
    array_mid.copy_from_slice(&b[0..20]);
    let ksuid = Ksuid::from_bytes(array_mid);
    Ok(ksuid)
}
