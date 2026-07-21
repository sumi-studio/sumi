//! Generation-fenced JSON Lines RPC contracts for executor processes.

#![cfg(target_os = "linux")]

mod protocol;

// The service and broker slices consume these stable contracts later in T13.
#[allow(unused_imports)]
pub use protocol::{
    ArtifactOperation, ExecutorOperation, InputRoute, MAX_RPC_LINE_BYTES, MAX_RPC_READ_BYTES,
    RpcError, RpcFrame, RpcIdentity, RpcLifecycleTracker, RpcOperationValidation, RpcRequest,
    decode_rpc_frame, decode_rpc_line, encode_rpc_frame, resolve_input,
};
