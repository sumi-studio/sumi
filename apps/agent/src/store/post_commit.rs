//! Durable post-commit publication shared by every `EventWriter` handle.
//!
//! `agent_events` is the canonical FIFO.  This feed deliberately keeps only a
//! monotonic high-water mark and a wake hint: a slow or stopped live dispatcher
//! cannot accumulate an unbounded in-memory copy of durable events, and a lost
//! wakeup is recovered by scanning the authenticated durable prefix.

use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow, bail};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct FeedState {
    published_through: u64,
    dispatcher_claimed: bool,
    invariant_failure: Option<String>,
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
