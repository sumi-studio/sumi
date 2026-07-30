//! Durable post-commit publication shared by every `EventWriter` handle.
//!
//! `agent_events` is the canonical FIFO.  This feed deliberately keeps only a
//! monotonic high-water mark and a wake hint: a slow or stopped live dispatcher
//! cannot accumulate an unbounded in-memory copy of durable events, and a lost
//! wakeup is recovered by scanning the authenticated durable prefix.

use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow, bail};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard};
use tokio_util::sync::CancellationToken;

use crate::runtime::{
    authority::RuntimeEpochAuthority,
    contracts::{GenerationRecoveryFence, ProcessGenerationLease},
};

#[derive(Clone, Debug)]
struct HydratedEpoch {
    lease: ProcessGenerationLease,
    fence: GenerationRecoveryFence,
    lifecycle: Arc<EpochLifecycle>,
    #[allow(
        dead_code,
        reason = "used once T26 production bootstrap issues its epoch"
    )]
    issued: Option<PostCommitEpochCapability>,
}

#[derive(Debug)]
struct EpochLifecycle {
    invalidated: CancellationToken,
    admission_gate: Arc<AsyncMutex<()>>,
}

impl EpochLifecycle {
    fn new() -> Self {
        Self {
            invalidated: CancellationToken::new(),
            admission_gate: Arc::new(AsyncMutex::new(())),
        }
    }

    async fn invalidate_and_wait(&self) {
        self.invalidated.cancel();
        let _drained = self.admission_gate.clone().lock_owned().await;
    }
}

#[derive(Debug)]
struct EpochCapabilityInner {
    authority: Option<RuntimeEpochAuthority>,
    hydration_lifecycle: Arc<EpochLifecycle>,
    runtime_invalidated: CancellationToken,
    owner_invalidated: CancellationToken,
}

/// Exact, shared runtime-epoch capability for post-COMMIT dispatch.
///
/// Clones retain pointer identity and the same three lifecycle invalidations:
/// Store hydration rollover, the owning runtime cancellation, and dispatcher
/// teardown. It is therefore not a copyable PAID/generation value witness.
///
/// This capability is process-local. Cross-process exclusion for two `Store`
/// instances opened on the same SQLite file remains a supervisor/bootstrap
/// obligation: the old process must stop and join before the new runtime is
/// made Ready.
#[derive(Clone, Debug)]
pub(crate) struct PostCommitEpochCapability {
    inner: Arc<EpochCapabilityInner>,
}

impl PostCommitEpochCapability {
    #[allow(
        dead_code,
        reason = "used once T26 production bootstrap issues its epoch"
    )]
    fn bound(
        authority: RuntimeEpochAuthority,
        hydration_lifecycle: Arc<EpochLifecycle>,
        runtime_invalidated: CancellationToken,
    ) -> Self {
        Self {
            inner: Arc::new(EpochCapabilityInner {
                authority: Some(authority),
                hydration_lifecycle,
                runtime_invalidated,
                owner_invalidated: CancellationToken::new(),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn unbound_test(runtime_invalidated: CancellationToken) -> Self {
        Self {
            inner: Arc::new(EpochCapabilityInner {
                authority: None,
                hydration_lifecycle: Arc::new(EpochLifecycle::new()),
                runtime_invalidated,
                owner_invalidated: CancellationToken::new(),
            }),
        }
    }

    pub(crate) fn authority(&self) -> Result<&RuntimeEpochAuthority> {
        self.inner
            .authority
            .as_ref()
            .ok_or_else(|| anyhow!("test-only post-commit epoch has no runtime authority"))
    }

    pub(crate) fn same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub(crate) fn is_unbound_test(&self) -> bool {
        self.inner.authority.is_none()
    }

    pub(crate) fn ensure_active(&self) -> Result<()> {
        if self.is_cancelled() {
            bail!("post-commit runtime epoch capability is invalidated");
        }
        Ok(())
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.inner.hydration_lifecycle.invalidated.is_cancelled()
            || self.inner.runtime_invalidated.is_cancelled()
            || self.inner.owner_invalidated.is_cancelled()
    }

    pub(crate) async fn cancelled(&self) {
        tokio::select! {
            _ = self.inner.hydration_lifecycle.invalidated.cancelled() => {}
            _ = self.inner.runtime_invalidated.cancelled() => {}
            _ = self.inner.owner_invalidated.cancelled() => {}
        }
    }

    /// Serialize admission side effects with Store hydration rollover.
    pub(crate) async fn claim_admission(&self) -> Result<OwnedMutexGuard<()>> {
        let guard = self
            .inner
            .hydration_lifecycle
            .admission_gate
            .clone()
            .lock_owned()
            .await;
        self.ensure_active()?;
        Ok(guard)
    }

    pub(crate) fn owner_cancellation(&self) -> &CancellationToken {
        &self.inner.owner_invalidated
    }

    pub(crate) fn invalidate(&self) {
        self.inner.owner_invalidated.cancel();
    }
}

#[derive(Debug)]
struct FeedState {
    published_through: u64,
    dispatcher_claimed: bool,
    invariant_failure: Option<String>,
    hydrated_epoch: Option<HydratedEpoch>,
}

#[derive(Debug)]
struct FeedInner {
    state: Mutex<FeedState>,
    changed: Notify,
}

/// Store-owned publication boundary invoked only after a successful SQLite
/// commit.  Clones share the same writer high-water and dispatcher claim.
#[derive(Clone, Debug)]
pub(super) struct PostCommitFeed {
    inner: Arc<FeedInner>,
}

impl PostCommitFeed {
    pub(super) fn new(initial_event_head: u64) -> Self {
        Self {
            inner: Arc::new(FeedInner {
                state: Mutex::new(FeedState {
                    published_through: initial_event_head,
                    dispatcher_claimed: false,
                    invariant_failure: None,
                    hydrated_epoch: None,
                }),
                changed: Notify::new(),
            }),
        }
    }

    /// Publish the exact event sequences returned by one committed
    /// `EventWriter` transaction.
    ///
    /// EventWriter's single-writer gate makes consecutive commits contiguous.
    /// The sequences themselves remain in `agent_events`; coalescing only the
    /// wake high-water keeps this path synchronous and O(1) after COMMIT.
    pub(super) fn publish_exact(&self, seqs: &[u64]) {
        if seqs.is_empty() {
            return;
        }

        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.invariant_failure.is_some() {
            self.inner.changed.notify_waiters();
            return;
        }

        let expected_first = match state.published_through.checked_add(1) {
            Some(value) => value,
            None => {
                state.invariant_failure =
                    Some("post-commit event sequence high-water overflowed".to_owned());
                self.inner.changed.notify_waiters();
                return;
            }
        };
        let contiguous = seqs.iter().copied().enumerate().all(|(offset, seq)| {
            u64::try_from(offset)
                .ok()
                .and_then(|offset| expected_first.checked_add(offset))
                == Some(seq)
        });
        if seqs.first().copied() != Some(expected_first) || !contiguous {
            state.invariant_failure = Some(format!(
                "post-commit receipt is not the exact next durable sequence range: \
                 published_through={}, receipt={seqs:?}",
                state.published_through
            ));
            self.inner.changed.notify_waiters();
            return;
        }
        state.published_through = *seqs
            .last()
            .expect("non-empty post-commit receipt has a last sequence");
        drop(state);
        self.inner.changed.notify_waiters();
    }

    pub(super) fn claim(&self) -> Result<PostCommitReceiver> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.dispatcher_claimed {
            bail!("a post-commit dispatcher already owns this personality-agent Store");
        }
        if let Some(reason) = state.invariant_failure.as_ref() {
            bail!("post-commit feed is failed: {reason}");
        }
        state.dispatcher_claimed = true;
        Ok(PostCommitReceiver { feed: self.clone() })
    }

    pub(super) fn published_through(&self) -> Result<u64> {
        self.snapshot()
    }

    pub(super) async fn record_hydrated_epoch(
        &self,
        lease: &ProcessGenerationLease,
        fence: &GenerationRecoveryFence,
    ) -> Result<()> {
        fence
            .validate_exact(lease, fence.fence_id())
            .map_err(|error| anyhow!("invalid hydrated post-commit epoch: {error}"))?;
        let previous = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state
                .hydrated_epoch
                .as_ref()
                .is_some_and(|epoch| epoch.lease == *lease && epoch.fence == *fence)
            {
                return Ok(());
            }
            state.hydrated_epoch.take()
        };
        if let Some(previous) = previous {
            previous.lifecycle.invalidate_and_wait().await;
        }
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.hydrated_epoch.is_some() {
            bail!("concurrent Store hydration attempted to replace the post-commit runtime epoch");
        }
        state.hydrated_epoch = Some(HydratedEpoch {
            lease: lease.clone(),
            fence: fence.clone(),
            lifecycle: Arc::new(EpochLifecycle::new()),
            issued: None,
        });
        Ok(())
    }

    #[allow(
        dead_code,
        reason = "used once T26 production bootstrap issues its epoch"
    )]
    pub(super) fn issue_epoch_capability(
        &self,
        authority: RuntimeEpochAuthority,
        runtime_invalidated: CancellationToken,
    ) -> Result<PostCommitEpochCapability> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let hydrated = state
            .hydrated_epoch
            .as_mut()
            .ok_or_else(|| anyhow!("post-commit runtime epoch requested before Store hydration"))?;
        if authority.lease() != &hydrated.lease || authority.fence() != &hydrated.fence {
            bail!("post-commit runtime authority does not match the Store hydration lease/fence");
        }
        if hydrated.issued.is_some() {
            bail!("post-commit runtime epoch capability was already issued for this hydration");
        }
        let capability = PostCommitEpochCapability::bound(
            authority,
            hydrated.lifecycle.clone(),
            runtime_invalidated,
        );
        hydrated.issued = Some(capability.clone());
        Ok(capability)
    }

    pub(super) fn validate_epoch_capability(
        &self,
        capability: &PostCommitEpochCapability,
    ) -> Result<()> {
        capability.ensure_active()?;
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let issued = state
            .hydrated_epoch
            .as_ref()
            .and_then(|epoch| epoch.issued.as_ref())
            .ok_or_else(|| anyhow!("Store has no issued post-commit runtime epoch capability"))?;
        if !issued.same_instance(capability) {
            bail!("post-commit runtime epoch capability belongs to another Store hydration");
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<u64> {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(reason) = state.invariant_failure.as_ref() {
            return Err(anyhow!("post-commit feed invariant failed: {reason}"));
        }
        Ok(state.published_through)
    }
}

/// Exclusive receiver capability for T26's one dispatcher task.
pub(crate) struct PostCommitReceiver {
    feed: PostCommitFeed,
}

impl PostCommitReceiver {
    pub(crate) fn published_through(&self) -> Result<u64> {
        self.feed.snapshot()
    }

    /// Wait for a high-water advance without a lost-notification window.
    pub(crate) async fn wait_for_advance(
        &self,
        after_seq: u64,
        cancel: &CancellationToken,
    ) -> Result<Option<u64>> {
        loop {
            let changed = self.feed.inner.changed.notified();
            let published_through = self.feed.snapshot()?;
            if published_through > after_seq {
                return Ok(Some(published_through));
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(None),
                _ = changed => {}
            }
        }
    }
}

impl Drop for PostCommitReceiver {
    fn drop(&mut self) {
        let mut state = self
            .feed
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.dispatcher_claimed = false;
        drop(state);
        self.feed.inner.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_receipts_coalesce_to_one_bounded_high_water() {
        let feed = PostCommitFeed::new(4);
        feed.publish_exact(&[5, 6]);
        feed.publish_exact(&[7]);
        assert_eq!(feed.published_through().unwrap(), 7);
    }

    #[test]
    fn a_gap_fails_the_feed_without_rewriting_durable_state() {
        let feed = PostCommitFeed::new(4);
        feed.publish_exact(&[6]);
        let error = feed.published_through().unwrap_err();
        assert!(format!("{error:#}").contains("not the exact next durable sequence range"));
    }

    #[test]
    fn only_one_dispatcher_can_claim_a_store_feed() {
        let feed = PostCommitFeed::new(0);
        let receiver = feed.claim().unwrap();
        assert!(feed.claim().is_err());
        drop(receiver);
        assert!(feed.claim().is_ok());
    }
}
