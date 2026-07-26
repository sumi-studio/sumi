//! Generation-fenced JSON Lines RPC contracts for executor processes.

#![cfg(target_os = "linux")]

mod artifact_broker;
mod client;
mod executor_client;
mod protocol;
mod remote;
mod service;

// The service and broker slices consume these stable contracts later in T13.
#[allow(unused_imports)]
pub use artifact_broker::{ArtifactBroker, ArtifactGrepMatch, ArtifactResponse};
pub use client::ArtifactBrokerClient;
#[allow(unused_imports)]
pub use executor_client::ExecutorClient;
#[allow(unused_imports)]
pub use protocol::{
    ArtifactOperation, ExecutorOperation, ExecutorResponse, InputRoute, MAX_RPC_LINE_BYTES,
    MAX_RPC_READ_BYTES, RpcError, RpcFrame, RpcLifecycleTracker, RpcOperationValidation,
    RpcRequest, decode_rpc_frame, decode_rpc_line, encode_rpc_frame, resolve_input,
};
#[allow(unused_imports)]
pub use remote::remote_executor_registry;
pub use service::{
    run_artifact_broker_mode, run_tool_executor_mode, run_tool_executor_socket_mode,
};
pub(crate) use service::{set_dumpable, wait_for_unix_socket};
