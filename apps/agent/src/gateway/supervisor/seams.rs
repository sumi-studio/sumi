//! Compile-safe adapter seams for T17 and T26 integrations.
//!
//! These types compile against `ConnectionSupervisor` but return descriptive
//! errors until the concrete T17/T26 boundary is wired. The error messages
//! document the exact integration contract.

use std::fmt;
use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;

use super::{
    CommandCursors, CredentialProvider, DurableSource, EventCursors, GatewayCredential,
    HydrationLatch, HydrationReady, OutboundFrame,
};
use crate::runtime::contracts::ProcessGeneration;

/// Compile-safe T17 hydration seam. T17 will replace this with a
/// `WatchHydrationLatch` driven by the production hydration receipt.
#[derive(Clone, Debug)]
pub struct T17HydrationLatch;

#[async_trait]
impl HydrationLatch for T17HydrationLatch {
    async fn wait_for(&self, generation: ProcessGeneration) -> Result<HydrationReady> {
        let _ = generation;
        bail!(
            "T17 integration seam: HydrationLatch::wait_for({generation}) is not wired. \
             T17 must emit HydrationReady{{generation, receipt_identity}} through a watch channel."
        )
    }
}

/// Compile-safe T17 durable source seam. T17 will replace this with a
/// store-backed adapter. The exact integration contract is documented in
/// the error messages so the T17 boundary can be filled without ambiguity.
#[derive(Clone)]
pub struct T17StoreAdapter {
    #[allow(dead_code)]
    store: Arc<crate::store::Store>,
}

impl fmt::Debug for T17StoreAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("T17StoreAdapter").finish_non_exhaustive()
    }
}

impl T17StoreAdapter {
    #[allow(dead_code)]
    pub(crate) fn new(store: Arc<crate::store::Store>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl DurableSource for T17StoreAdapter {
    async fn event_cursor(&self) -> Result<EventCursors> {
        bail!(
            "T17 integration seam: Store::event_cursor() is not wired. \
             Contract: SELECT MAX(seq) FROM agent_events WHERE conversation_id = <scope.conversation_id>; \
             return EventCursors{{ last_sent: <max or 0> }}."
        )
    }

    async fn events_after(&self, after_seq: u64, limit: usize) -> Result<Vec<OutboundFrame>> {
        let _ = (after_seq, limit);
        bail!(
            "T17 integration seam: Store::events_after(after_seq, limit) is not wired. \
             Contract: SELECT seq, raw_ciphertext, raw_key_ref FROM agent_events \
             WHERE conversation_id = <scope.conversation_id> AND seq > ? ORDER BY seq LIMIT ?; \
             decrypt raw_ciphertext with the conversation event data key, deserialize to OutboundFrame."
        )
    }

    async fn command_cursors(&self) -> Result<CommandCursors> {
        bail!(
            "T17 integration seam: Store::command_cursors() is not wired. \
             Contract: SELECT MAX(seq) FROM inbound_commands WHERE status IN ('received','applying') AS received; \
             SELECT MAX(seq) FROM inbound_commands WHERE status IN ('applied','superseded','rejected') AS terminal; \
             return CommandCursors{{ received, terminal }}."
        )
    }
}

/// Placeholder credential provider for the compile-safe seam. T26 will
/// replace this with a workload-identity / rotating-file implementation.
#[derive(Clone, Debug)]
pub struct T26CredentialProvider;

#[async_trait]
impl CredentialProvider for T26CredentialProvider {
    async fn fresh_credential(&mut self) -> Result<GatewayCredential> {
        bail!(
            "T26 integration seam: CredentialProvider is not wired. \
             Contract: read the current short-lived agent token from the control-plane source."
        )
    }
}
