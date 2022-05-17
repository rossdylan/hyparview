//! Module for the set of errors the hyparview lib uses

/// A customized Result type for use with [`thiserror`]
pub type Result<T> = std::result::Result<T, Error>;

/// A custom Error type defined using [`thiserror`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// We have already initialized the hyparview instance. Calling the init
    /// function multiple times is an error
    #[error("hyparview already initialized")]
    AlreadyInitialized,
    /// Tonic has failed to execute an RPC and returned a status error
    #[error("rpc failure")]
    RPCFailure(#[from] tonic::Status),
    /// Tonic's transport layer has failed
    #[error("tonic failure")]
    TonicError(#[from] tonic::transport::Error),
    /// A mock error returned by parts of our test harness
    #[error("mock error: {0}")]
    MockError(String),
    /// We have failed to parse a socket address
    #[error("unable to parse address")]
    AddrParseFailed(#[from] std::io::Error),
    /// An error that acts as a sentinal when we have run out of bootstrap
    /// peers for first-contact
    #[error("bootstrap source found no nodes")]
    NoBootstrapAddrsFound,
    /// Passive View is empty so we can not replace a failed active peer
    #[error("unable to replace failed peer, passive view empty")]
    PassiveViewEmpty,
    /// The message we received had an invalid ID
    #[error("message identifier invalid")]
    InvalidMessageID,
    /// We failed to send a data message broadcast to any peers
    #[error("failed to send broadcast to any peer")]
    BroadcastFailed,
    /// We attempted to double register a peer's server instance
    #[error("a mock server has already been registered for this peer")]
    MockServerAlreadyRegistered,
    /// An Error from the trust-dns-resolver system
    #[error("dns error")]
    DNSError(#[from] trust_dns_resolver::error::ResolveError),
    /// A fatal netsplit occurred and a reinitialization is required
    #[error("fatal netsplit occured")]
    FatalNetsplit,
    /// An unknown error, used as a catch-all
    #[error("unknown error in mocks")]
    Unknown,
}
