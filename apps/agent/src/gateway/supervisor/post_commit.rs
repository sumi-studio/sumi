//! T26-owned ordered bridge from durable EventWriter commits to T17 delivery.
//!
//! The Store's `agent_events` prefix is the one FIFO. EventWriter publishes a
//! monotonic post-COMMIT high-water, and this single task reads the exact rows
//! back in sequence order before invoking T17. No producer owns a delivery
//! callback, no in-memory queue can grow with an offline agent, and Session
//! waits on the same cumulative admission proof as maintenance/recovery work.

use std::{fmt, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use super::DeliveryEpoch;
use super::session::DurableEventAdmission;
use crate::{
    runtime::contracts::PersonalityAgentId,
    store::{PostCommitReceiver, Store},
};

const DEFAULT_DISPATCH_PAGE_SIZE: usize = 64;

#[async_trait]
pub(crate) trait PostCommitAdmissionTarget: Send + Sync + 'static {
    async fn admit_committed(
        &self,
        personality_agent_id: &PersonalityAgentId,
        seq: u64,
    ) -> Result<DurableEventAdmission>;
}

#[derive(Clone, Debug)]
enum DispatchState {
    Running {
        processed_through: u64,
        admission: DurableEventAdmission,
    },
    Failed {
        processed_through: u64,
        reason: String,
    },
    Stopped {
        processed_through: u64,
    },
}

/// Cloneable Session-side proof client. It cannot invoke T17 or advance the
/// durable cursor; only the dispatcher task owns those capabilities.
#[derive(Clone)]
pub(crate) struct PostCommitDispatcherClient {
    personality_agent_id: PersonalityAgentId,
    progress: watch::Receiver<DispatchState>,
}

impl fmt::Debug for PostCommitDispatcherClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostCommitDispatcherClient")
            .field("personality_agent_id", &self.personality_agent_id)
            .finish_non_exhaustive()
    }
}

impl PostCommitDispatcherClient {
    pub(crate) fn personality_agent_id(&self) -> &PersonalityAgentId {
        &self.personality_agent_id
    }

    pub(crate) async fn admission_for(
        &self,
        personality_agent_id: &PersonalityAgentId,
        seq: u64,
    ) -> Result<DurableEventAdmission> {
        if personality_agent_id != &self.personality_agent_id {
            bail!(
                "post-commit admission targets personality agent {}, got {personality_agent_id}",
                self.personality_agent_id
            );
        }
        if seq == 0 {
            bail!("post-commit admission sequence must be positive");
        }

        let mut progress = self.progress.clone();
        loop {
            let state = progress.borrow().clone();
            match state {
                DispatchState::Running {
                    processed_through,
                    admission,
                } if processed_through >= seq => return Ok(admission),
                DispatchState::Failed {
                    processed_through,
                    reason,
                } => {
                    bail!("post-commit dispatcher failed after seq {processed_through}: {reason}")
                }
                DispatchState::Stopped { processed_through } => {
                    bail!("post-commit dispatcher stopped after seq {processed_through}")
                }
                DispatchState::Running { .. } => {}
            }
            progress
                .changed()
                .await
                .context("post-commit dispatcher progress channel closed")?;
        }
    }
}

/// Lifecycle owner for the one post-commit dispatcher bound to a Store.
pub(crate) struct OrderedPostCommitDispatcher {
    store: Arc<Store>,
    client: PostCommitDispatcherClient,
    stop: CancellationToken,
    drain_through: watch::Sender<Option<u64>>,
    task: Option<JoinHandle<Result<()>>>,
}

impl fmt::Debug for OrderedPostCommitDispatcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OrderedPostCommitDispatcher")
            .field("personality_agent_id", self.client.personality_agent_id())
            .finish_non_exhaustive()
    }
}

impl OrderedPostCommitDispatcher {
    /// Start after the last sequence already owned by T24 catch-up.
    ///
    /// On normal bootstrap this is the Store head observed before producers
    /// start. Crash-recovery tests may pass an earlier authenticated remote
    /// cursor; the dispatcher then scans the retained durable rows without
    /// re-executing their producers.
    pub(crate) fn start<T>(
        store: Arc<Store>,
        target: T,
        start_after_seq: u64,
        cancel: CancellationToken,
    ) -> Result<Self>
    where
        T: PostCommitAdmissionTarget,
    {
        Self::start_with_page_size(
            store,
            target,
            start_after_seq,
            cancel,
            DEFAULT_DISPATCH_PAGE_SIZE,
        )
    }

    fn start_with_page_size<T>(
        store: Arc<Store>,
        target: T,
        start_after_seq: u64,
        cancel: CancellationToken,
        page_size: usize,
    ) -> Result<Self>
    where
        T: PostCommitAdmissionTarget,
    {
        if page_size == 0 {
            bail!("post-commit dispatcher page size must be positive");
        }
        let receiver = store.claim_post_commit_receiver()?;
        let published_through = receiver.published_through()?;
        if start_after_seq > published_through {
            bail!(
                "post-commit start cursor {start_after_seq} exceeds durable high-water {published_through}"
            );
        }
        let personality_agent_id = store.scope().personality_agent_id.clone();
        let initial = DispatchState::Running {
            processed_through: start_after_seq,
            admission: DurableEventAdmission::Deferred { after_epoch: None },
        };
        let (progress_tx, progress_rx) = watch::channel(initial);
        let stop = cancel.child_token();
        let task_stop = stop.clone();
        let (drain_through_tx, drain_through_rx) = watch::channel(None);
        let task_personality_agent_id = personality_agent_id.clone();
        let task_store = store.clone();
        let task = tokio::spawn(async move {
            run_dispatcher(
                task_store,
                Arc::new(target),
                receiver,
                task_personality_agent_id,
                start_after_seq,
                page_size,
                task_stop,
                drain_through_rx,
                progress_tx,
            )
            .await
        });
        Ok(Self {
            store,
            client: PostCommitDispatcherClient {
                personality_agent_id,
                progress: progress_rx,
            },
            stop,
            drain_through: drain_through_tx,
            task: Some(task),
        })
    }

    pub(crate) fn client(&self) -> PostCommitDispatcherClient {
        self.client.clone()
    }

    /// Stop after admitting every event that was durable when shutdown began.
    ///
    /// Commits after the captured high-water belong to the next runtime. Drop
    /// remains the emergency cancellation path; orderly T26 teardown must use
    /// this drain so an accepted commit cannot be abandoned behind shutdown.
    pub(crate) async fn shutdown(mut self) -> Result<()> {
        let through = self.store.post_commit_published_through()?;
        self.drain_through.send_replace(Some(through));
        let task = self
            .task
            .take()
            .expect("post-commit dispatcher task is owned until shutdown");
        flatten_join(task.await).context("post-commit dispatcher shutdown")
    }
}

impl Drop for OrderedPostCommitDispatcher {
    fn drop(&mut self) {
        // An un-awaited owner drop cannot leave the exclusive Store receiver
        // or an in-flight T17 fence alive indefinitely.
        self.stop.cancel();
    }
}

fn flatten_join(result: std::result::Result<Result<()>, JoinError>) -> Result<()> {
    result.map_err(|error| anyhow!("post-commit dispatcher task failed to join: {error}"))?
}

#[allow(clippy::too_many_arguments)]
async fn run_dispatcher(
    store: Arc<Store>,
    target: Arc<dyn PostCommitAdmissionTarget>,
    receiver: PostCommitReceiver,
    personality_agent_id: PersonalityAgentId,
    start_after_seq: u64,
    page_size: usize,
    stop: CancellationToken,
    mut drain_through: watch::Receiver<Option<u64>>,
    progress: watch::Sender<DispatchState>,
) -> Result<()> {
    let mut processed_through = start_after_seq;
    let mut cumulative_admission = DurableEventAdmission::Deferred { after_epoch: None };

    loop {
        let requested_drain = *drain_through.borrow();
        if requested_drain.is_some_and(|through| processed_through >= through) {
            progress.send_replace(DispatchState::Stopped { processed_through });
            return Ok(());
        }

        let published_through = receiver.published_through().map_err(|error| {
            publish_failure(&progress, processed_through, &error);
            error
        })?;
        if published_through <= processed_through {
            tokio::select! {
                biased;
                _ = stop.cancelled() => {
                    progress.send_replace(DispatchState::Stopped { processed_through });
                    return Ok(());
                }
                changed = drain_through.changed() => {
                    if changed.is_err() {
                        progress.send_replace(DispatchState::Stopped { processed_through });
                        return Ok(());
                    }
                }
                result = receiver.wait_for_advance(processed_through, &stop) => {
                    result.map_err(|error| {
                        publish_failure(&progress, processed_through, &error);
                        error
                    })?;
                }
            }
            continue;
        }

        let requested_drain = *drain_through.borrow();
        let dispatch_through = requested_drain
            .map(|through| through.min(published_through))
            .unwrap_or(published_through);

        'dispatch: while processed_through < dispatch_through {
            let page = store
                .committed_event_sequences(processed_through, dispatch_through, page_size)
                .await
                .map_err(|error| {
                    publish_failure(&progress, processed_through, &error);
                    error
                })?;
            if page.is_empty() {
                let error = anyhow!(
                    "durable post-commit FIFO ended after seq {processed_through} before dispatch high-water {dispatch_through}"
                );
                publish_failure(&progress, processed_through, &error);
                return Err(error);
            }

            for seq in page {
                // Shutdown may have captured its boundary after this page was
                // read. Never consume a sequence owned by the next runtime.
                if drain_through.borrow().is_some_and(|through| seq > through) {
                    break 'dispatch;
                }
                let expected = processed_through
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("post-commit dispatcher sequence exhausted"))?;
                if seq != expected {
                    let error = anyhow!(
                        "durable post-commit FIFO gap: expected seq {expected}, found {seq}"
                    );
                    publish_failure(&progress, processed_through, &error);
                    return Err(error);
                }

                let admission = tokio::select! {
                    biased;
                    _ = stop.cancelled() => {
                        progress.send_replace(DispatchState::Stopped { processed_through });
                        return Ok(());
                    }
                    result = target.admit_committed(&personality_agent_id, seq) => {
                        result.map_err(|error| {
                            publish_failure(&progress, processed_through, &error);
                            error
                        })?
                    }
                };
                cumulative_admission = combine_admission(cumulative_admission, admission);
                processed_through = seq;
                progress.send_replace(DispatchState::Running {
                    processed_through,
                    admission: cumulative_admission,
                });
            }
        }
    }
}

fn publish_failure(
    progress: &watch::Sender<DispatchState>,
    processed_through: u64,
    error: &anyhow::Error,
) {
    progress.send_replace(DispatchState::Failed {
        processed_through,
        reason: format!("{error:#}"),
    });
}

fn combine_admission(
    previous: DurableEventAdmission,
    current: DurableEventAdmission,
) -> DurableEventAdmission {
    match (previous, current) {
        (
            DurableEventAdmission::Deferred {
                after_epoch: Some(previous_epoch),
            },
            DurableEventAdmission::Enqueued { epoch },
        ) if epoch == previous_epoch => DurableEventAdmission::Deferred {
            after_epoch: Some(previous_epoch),
        },
        (_, DurableEventAdmission::Enqueued { epoch }) => DurableEventAdmission::Enqueued { epoch },
        (
            DurableEventAdmission::Enqueued { epoch },
            DurableEventAdmission::Deferred { after_epoch: None },
        ) => DurableEventAdmission::Deferred {
            after_epoch: Some(epoch),
        },
        (
            DurableEventAdmission::Deferred {
                after_epoch: Some(epoch),
            },
            DurableEventAdmission::Deferred { after_epoch: None },
        ) => DurableEventAdmission::Deferred {
            after_epoch: Some(epoch),
        },
        (_, deferred @ DurableEventAdmission::Deferred { .. }) => deferred,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::sync::Notify;

    use super::*;
    use crate::{
        gateway::test_personality_agent_id,
        store::{DurableEvent, EventBatch, EventWrite, EventWriter},
    };

    #[derive(Clone)]
    struct RecordingTarget {
        epoch: DeliveryEpoch,
        calls: Arc<Mutex<Vec<u64>>>,
        first_started: Arc<Notify>,
        release_first: Arc<Notify>,
    }

    #[async_trait]
    impl PostCommitAdmissionTarget for RecordingTarget {
        async fn admit_committed(
            &self,
            _personality_agent_id: &PersonalityAgentId,
            seq: u64,
        ) -> Result<DurableEventAdmission> {
            self.calls.lock().unwrap().push(seq);
            if seq == 1 {
                self.first_started.notify_one();
                self.release_first.notified().await;
            }
            Ok(DurableEventAdmission::Enqueued { epoch: self.epoch })
        }
    }

    #[derive(Clone, Default)]
    struct ImmediateTarget {
        calls: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait]
    impl PostCommitAdmissionTarget for ImmediateTarget {
        async fn admit_committed(
            &self,
            _personality_agent_id: &PersonalityAgentId,
            seq: u64,
        ) -> Result<DurableEventAdmission> {
            self.calls.lock().unwrap().push(seq);
            Ok(DurableEventAdmission::Deferred { after_epoch: None })
        }
    }

    #[derive(Clone)]
    struct FailingTarget;

    #[async_trait]
    impl PostCommitAdmissionTarget for FailingTarget {
        async fn admit_committed(
            &self,
            _personality_agent_id: &PersonalityAgentId,
            seq: u64,
        ) -> Result<DurableEventAdmission> {
            bail!("injected permanent dispatcher failure at seq {seq}")
        }
    }

    fn maintenance(kind: &str) -> EventBatch {
        EventBatch {
            writes: vec![EventWrite {
                event: Some(DurableEvent::memory_maintenance(kind).unwrap()),
                projections: Vec::new(),
            }],
            injected_commands: Vec::new(),
        }
    }

    #[tokio::test]
    async fn held_n_fences_n_plus_one_without_blocking_the_event_writer_gate() {
        let store = Arc::new(
            Store::session_test_store(test_personality_agent_id().as_str())
                .await
                .unwrap(),
        );
        let calls = Arc::new(Mutex::new(Vec::new()));
        let first_started = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let target = RecordingTarget {
            epoch: DeliveryEpoch::for_test("ordered"),
            calls: calls.clone(),
            first_started: first_started.clone(),
            release_first: release_first.clone(),
        };
        let cancel = CancellationToken::new();
        let dispatcher =
            OrderedPostCommitDispatcher::start(store.clone(), target, 0, cancel).unwrap();
        let client = dispatcher.client();
        let writer = EventWriter::new(store.clone());

        assert_eq!(writer.apply(maintenance("memory")).await.unwrap(), vec![1]);
        first_started.notified().await;

        // The dispatcher is blocked on seq 1, but post-COMMIT publication is
        // O(1) and never retains EventWriter's single-writer gate.
        assert_eq!(writer.apply(maintenance("session")).await.unwrap(), vec![2]);
        let waiting = {
            let client = client.clone();
            let personality_agent_id = store.scope().personality_agent_id.clone();
            tokio::spawn(async move {
                client
                    .admission_for(&personality_agent_id, 2)
                    .await
                    .unwrap()
            })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        assert_eq!(*calls.lock().unwrap(), vec![1]);

        release_first.notify_one();
        assert_eq!(
            waiting.await.unwrap(),
            DurableEventAdmission::Enqueued {
                epoch: DeliveryEpoch::for_test("ordered")
            }
        );
        assert_eq!(*calls.lock().unwrap(), vec![1, 2]);
        dispatcher.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_shutdown_drains_the_captured_published_high_water() {
        let store = Arc::new(
            Store::session_test_store("post-commit-drain")
                .await
                .unwrap(),
        );
        let calls = Arc::new(Mutex::new(Vec::new()));
        let first_started = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        let target = RecordingTarget {
            epoch: DeliveryEpoch::for_test("drain"),
            calls: calls.clone(),
            first_started: first_started.clone(),
            release_first: release_first.clone(),
        };
        let dispatcher =
            OrderedPostCommitDispatcher::start(store.clone(), target, 0, CancellationToken::new())
                .unwrap();
        let writer = EventWriter::new(store.clone());
        assert_eq!(writer.apply(maintenance("drain-1")).await.unwrap(), vec![1]);
        first_started.notified().await;
        assert_eq!(writer.apply(maintenance("drain-2")).await.unwrap(), vec![2]);

        let shutdown = tokio::spawn(dispatcher.shutdown());
        tokio::task::yield_now().await;
        assert!(
            !shutdown.is_finished(),
            "shutdown must not abandon a captured committed sequence"
        );
        release_first.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), shutdown)
            .await
            .expect("dispatcher drains promptly")
            .expect("shutdown task")
            .expect("drained dispatcher succeeds");
        assert_eq!(*calls.lock().unwrap(), vec![1, 2]);

        // The receiver claim is released only after the drain. A later
        // runtime can own commits made after that boundary.
        let next_target = ImmediateTarget::default();
        let next_calls = next_target.calls.clone();
        let next = OrderedPostCommitDispatcher::start(
            store.clone(),
            next_target,
            2,
            CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(
            writer.apply(maintenance("next-runtime")).await.unwrap(),
            vec![3]
        );
        next.client()
            .admission_for(&store.scope().personality_agent_id, 3)
            .await
            .unwrap();
        assert_eq!(*next_calls.lock().unwrap(), vec![3]);
        next.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn permanent_dispatcher_failure_does_not_strand_the_event_writer_gate() {
        let store = Arc::new(
            Store::session_test_store("post-commit-failure")
                .await
                .unwrap(),
        );
        let dispatcher = OrderedPostCommitDispatcher::start(
            store.clone(),
            FailingTarget,
            0,
            CancellationToken::new(),
        )
        .unwrap();
        let client = dispatcher.client();
        let writer = EventWriter::new(store.clone());
        assert_eq!(
            writer.apply(maintenance("failure-1")).await.unwrap(),
            vec![1]
        );
        let error = client
            .admission_for(&store.scope().personality_agent_id, 1)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("injected permanent dispatcher failure"),
            "{error:#}"
        );
        assert_eq!(
            writer.apply(maintenance("failure-2")).await.unwrap(),
            vec![2],
            "post-COMMIT dispatcher failure cannot retain EventWriter's gate"
        );
        assert!(dispatcher.shutdown().await.is_err());
    }

    #[tokio::test]
    async fn restart_scans_a_committed_row_after_its_live_wake_is_lost() {
        let root =
            std::env::temp_dir().join(format!("sumi-post-commit-restart-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&root).expect("create restart fixture root");
        let path = root.join("agent.db");
        let store = Arc::new(
            Store::session_test_file_store(&path, "post-commit-restart")
                .await
                .unwrap(),
        );
        let writer = EventWriter::new(store.clone());
        let mut producer_runs = 0u64;
        producer_runs += 1;
        assert_eq!(
            writer
                .apply(maintenance("committed-before-crash"))
                .await
                .unwrap(),
            vec![1]
        );

        // EventWriter's abrupt after-COMMIT failpoint sits before publication.
        // Closing this Store before any receiver exists exercises the same
        // durable state: the row survives while every in-memory wake dies.
        store.pool().close().await;
        drop(writer);
        drop(store);

        let reopened = Arc::new(
            Store::session_test_file_store(&path, "post-commit-restart")
                .await
                .unwrap(),
        );
        let target = ImmediateTarget::default();
        let calls = target.calls.clone();
        let dispatcher = OrderedPostCommitDispatcher::start(
            reopened.clone(),
            target,
            0,
            CancellationToken::new(),
        )
        .unwrap();
        dispatcher
            .client()
            .admission_for(&reopened.scope().personality_agent_id, 1)
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), vec![1]);
        assert_eq!(producer_runs, 1, "restart must not re-execute the producer");
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(reopened.pool())
            .await
            .unwrap();
        assert_eq!(rows, 1);

        dispatcher.shutdown().await.unwrap();
        reopened.pool().close().await;
        drop(reopened);
        tokio::fs::remove_dir_all(root)
            .await
            .expect("remove restart fixture");
    }

    #[test]
    fn deferred_epoch_barrier_is_sticky_until_a_replacement_epoch_admits() {
        let old = DeliveryEpoch::for_test("old");
        let replacement = DeliveryEpoch::for_test("replacement");
        let deferred = combine_admission(
            DurableEventAdmission::Enqueued { epoch: old },
            DurableEventAdmission::Deferred { after_epoch: None },
        );
        assert_eq!(
            deferred,
            DurableEventAdmission::Deferred {
                after_epoch: Some(old)
            }
        );
        assert_eq!(
            combine_admission(
                deferred,
                DurableEventAdmission::Enqueued { epoch: replacement }
            ),
            DurableEventAdmission::Enqueued { epoch: replacement }
        );
    }
}
