//! Agent session orchestration and turn lifecycle.

mod events;
mod provider_projection;

pub(crate) use events::{
    AgentEvent, ApprovalRequest, ApprovalResolution, PublicStreamEvent, SteerMode,
};
#[allow(unused_imports, reason = "consumed by the later T15 Session run loop")]
pub(crate) use provider_projection::{
    ProjectedProviderEvent, ProviderEventProjector, ProviderTerminal, ProviderTerminalKind,
};
