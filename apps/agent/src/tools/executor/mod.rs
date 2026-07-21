//! Generation-fenced JSON Lines RPC contracts for executor processes.

#![cfg(target_os = "linux")]

mod artifact_broker;
mod client;
mod protocol;
mod service;

// The service and broker slices consume these stable contracts later in T13.
#[allow(unused_imports)]
pub use artifact_broker::{ArtifactBroker, ArtifactGrepMatch, ArtifactResponse};
pub use client::ArtifactBrokerClient;
#[allow(unused_imports)]
pub use protocol::{
    ArtifactOperation, ExecutorOperation, InputRoute, MAX_RPC_LINE_BYTES, MAX_RPC_READ_BYTES,
    RpcError, RpcFrame, RpcIdentity, RpcLifecycleTracker, RpcOperationValidation, RpcRequest,
    decode_rpc_frame, decode_rpc_line, encode_rpc_frame, resolve_input,
};
pub use service::{run_artifact_broker_mode, run_tool_executor_mode};
