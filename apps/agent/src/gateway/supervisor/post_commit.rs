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
#[cfg(test)]
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use super::DeliveryEpoch;
use super::session::DurableEventAdmission;
use crate::{
    runtime::contracts::PersonalityAgentId,
    store::{PostCommitEpochCapability, PostCommitReceiver, Store},
};

const DEFAULT_DISPATCH_PAGE_SIZE: usize = 64;

#[async_trait]
pub(crate) trait PostCommitAdmissionTarget: Send + Sync + 'static {
    fn bind_post_commit_epoch(&self, epoch: &PostCommitEpochCapability) -> Result<()>;

    async fn admit_committed(
        &self,
        epoch: &PostCommitEpochCapability,
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
    epoch: PostCommitEpochCapability,
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

    pub(crate) fn epoch(&self) -> &PostCommitEpochCapability {
        &self.epoch
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
        self.epoch.ensure_active()?;

        let mut progress = self.progress.clone();
        loop {
            self.epoch.ensure_active()?;
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
            tokio::select! {
                biased;
                _ = self.epoch.cancelled() => {
                    bail!("post-commit runtime epoch invalidated before seq {seq} was admitted");
                }
                changed = progress.changed() => {
                    changed.context("post-commit dispatcher progress channel closed")?;
                }
            }
        }
    }
}

/// Lifecycle owner for the one post-commit dispatcher bound to a Store.
pub(crate) struct OrderedPostCommitDispatcher {
    store: Arc<Store>,
    client: PostCommitDispatcherClient,
    epoch: PostCommitEpochCapability,
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
    #[cfg(test)]
    pub(crate) fn start<T>(
        store: Arc<Store>,
        target: T,
        start_after_seq: u64,
        cancel: CancellationToken,
    ) -> Result<Self>
    where
        T: PostCommitAdmissionTarget,
    {
        Self::start_bound_with_page_size(
            store,
            target,
            start_after_seq,
            PostCommitEpochCapability::unbound_test(cancel),
            DEFAULT_DISPATCH_PAGE_SIZE,
        )
    }

    pub(crate) fn start_bound<T>(
        store: Arc<Store>,
        target: T,
        start_after_seq: u64,
        epoch: PostCommitEpochCapability,
    ) -> Result<Self>
    where
        T: PostCommitAdmissionTarget,
    {
        Self::start_bound_with_page_size(
            store,
            target,
            start_after_seq,
            epoch,
            DEFAULT_DISPATCH_PAGE_SIZE,
        )
    }

    fn start_bound_with_page_size<T>(
        store: Arc<Store>,
        target: T,
        start_after_seq: u64,
        epoch: PostCommitEpochCapability,
        page_size: usize,
    ) -> Result<Self>
    where
        T: PostCommitAdmissionTarget,
    {
        if page_size == 0 {
            bail!("post-commit dispatcher page size must be positive");
        }
        store.validate_post_commit_epoch(&epoch)?;
        target.bind_post_commit_epoch(&epoch)?;
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
        let (drain_through_tx, drain_through_rx) = watch::channel(None);
        let task_personality_agent_id = personality_agent_id.clone();
        let task_store = store.clone();
        let task_epoch = epoch.clone();
        let task = tokio::spawn(async move {
            run_dispatcher(
                task_store,
                Arc::new(target),
                receiver,
                task_epoch,
                task_personality_agent_id,
                start_after_seq,
                page_size,
                drain_through_rx,
                progress_tx,
            )
            .await
        });
        Ok(Self {
            store,
            client: PostCommitDispatcherClient {
                personality_agent_id,
                epoch: epoch.clone(),
                progress: progress_rx,
            },
            epoch,
            drain_through: drain_through_tx,
            task: Some(task),
        })
    }

    pub(crate) fn client(&self) -> PostCommitDispatcherClient {
        self.client.clone()
    }

    /// Stop after admitting every event committed before the drain capture.
    ///
    /// The caller must quiesce every EventWriter producer first. Capture takes
    /// EventWriter's gate, so a transaction already committed cannot remain
    /// unpublished on the other side of this boundary. Drop remains the
    /// emergency cancellation path.
    pub(crate) async fn shutdown(mut self) -> Result<()> {
        let through = self.store.capture_post_commit_drain_through().await?;
        self.drain_through.send_replace(Some(through));
        let task = self
            .task
            .take()
            .expect("post-commit dispatcher task is owned until shutdown");
        let result = flatten_join(task.await).context("post-commit dispatcher shutdown");
        self.epoch.invalidate();
        result
    }
}

impl Drop for OrderedPostCommitDispatcher {
    fn drop(&mut self) {
        // An un-awaited owner drop cannot leave the exclusive Store receiver
        // or an in-flight T17 fence alive indefinitely.
        self.epoch.invalidate();
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
    epoch: PostCommitEpochCapability,
    personality_agent_id: PersonalityAgentId,
    start_after_seq: u64,
    page_size: usize,
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

        let published_through = receiver.published_through().inspect_err(|error| {
            publish_failure(&progress, processed_through, error);
        })?;
        if published_through <= processed_through {
            tokio::select! {
                biased;
                _ = epoch.cancelled() => {
                    return Err(cancellation_failure(&progress, processed_through));
                }
                changed = drain_through.changed() => {
                    if changed.is_err() {
                        return Err(cancellation_failure(&progress, processed_through));
                    }
                }
                result = receiver.wait_for_advance(processed_through, epoch.owner_cancellation()) => {
                    result.inspect_err(|error| {
                        publish_failure(&progress, processed_through, error);
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
            let page = tokio::select! {
                biased;
                _ = epoch.cancelled() => {
                    return Err(cancellation_failure(&progress, processed_through));
                }
                result = store.committed_event_sequences(
                    processed_through,
                    dispatch_through,
                    page_size,
                ) => {
                    result.inspect_err(|error| {
                        publish_failure(&progress, processed_through, error);
                    })?
                }
            };
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

                let admission_guard = tokio::select! {
                    biased;
                    _ = epoch.cancelled() => {
                        return Err(cancellation_failure(&progress, processed_through));
                    }
                    guard = epoch.claim_admission() => {
                        guard.inspect_err(|error| {
                            publish_failure(&progress, processed_through, error);
                        })?
                    }
                };
                let admission = tokio::select! {
                    biased;
                    _ = epoch.cancelled() => {
                        return Err(cancellation_failure(&progress, processed_through));
                    }
                    result = target.admit_committed(&epoch, &personality_agent_id, seq) => {
                        result.inspect_err(|error| {
                            publish_failure(&progress, processed_through, error);
                        })?
                    }
                };
                drop(admission_guard);
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

fn cancellation_failure(
    progress: &watch::Sender<DispatchState>,
    processed_through: u64,
) -> anyhow::Error {
    let error = anyhow!("post-commit runtime epoch invalidated before orderly drain completed");
    publish_failure(progress, processed_through, &error);
    error
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
        runtime::{
            authority::RuntimeEpochAuthority,
            contracts::{
                GenerationRecoveryFence, ProcessGeneration, ProcessGenerationLease, RpcIdentity,
            },
        },
        store::{
            DurableEvent, EventBatch, EventWrite, EventWriter, HydrationOutcome,
            PostCommitPublishHook,
        },
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
        fn bind_post_commit_epoch(&self, _epoch: &PostCommitEpochCapability) -> Result<()> {
            Ok(())
        }

        async fn admit_committed(
            &self,
            _epoch: &PostCommitEpochCapability,
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
        fn bind_post_commit_epoch(&self, _epoch: &PostCommitEpochCapability) -> Result<()> {
            Ok(())
        }

        async fn admit_committed(
            &self,
            _epoch: &PostCommitEpochCapability,
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
        fn bind_post_commit_epoch(&self, _epoch: &PostCommitEpochCapability) -> Result<()> {
            Ok(())
        }

        async fn admit_committed(
            &self,
            _epoch: &PostCommitEpochCapability,
            _personality_agent_id: &PersonalityAgentId,
            seq: u64,
        ) -> Result<DurableEventAdmission> {
            bail!("injected permanent dispatcher failure at seq {seq}")
        }
    }

    #[derive(Clone)]
    struct CapabilityTarget {
        epoch: PostCommitEpochCapability,
        calls: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait]
    impl PostCommitAdmissionTarget for CapabilityTarget {
        fn bind_post_commit_epoch(&self, epoch: &PostCommitEpochCapability) -> Result<()> {
            if !self.epoch.same_instance(epoch) {
                bail!("injected target runtime epoch mismatch");
            }
            self.epoch.ensure_active()
        }

        async fn admit_committed(
            &self,
            epoch: &PostCommitEpochCapability,
            _personality_agent_id: &PersonalityAgentId,
            seq: u64,
        ) -> Result<DurableEventAdmission> {
            self.bind_post_commit_epoch(epoch)?;
            self.calls.lock().unwrap().push(seq);
            Ok(DurableEventAdmission::Deferred { after_epoch: None })
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
    async fn shutdown_capture_waits_for_commit_to_publish_barrier() {
        let store = Arc::new(
            Store::session_test_store("post-commit-publication-barrier")
                .await
                .unwrap(),
        );
        let target = ImmediateTarget::default();
        let calls = target.calls.clone();
        let dispatcher =
            OrderedPostCommitDispatcher::start(store.clone(), target, 0, CancellationToken::new())
                .unwrap();
        let writer = EventWriter::new(store.clone());
        let hook = PostCommitPublishHook::default();
        writer
            .set_post_commit_publish_hook(Some(hook.clone()))
            .await;
        let write =
            tokio::spawn(
                async move { writer.apply(maintenance("commit-before-publication")).await },
            );
        hook.committed.notified().await;

        let shutdown = tokio::spawn(dispatcher.shutdown());
        tokio::task::yield_now().await;
        assert!(
            !shutdown.is_finished(),
            "drain capture must serialize behind COMMIT-to-publication"
        );

        hook.allow_publication.notify_one();
        assert_eq!(write.await.unwrap().unwrap(), vec![1]);
        shutdown.await.unwrap().unwrap();
        assert_eq!(*calls.lock().unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn cancellation_before_shutdown_cannot_report_an_unreached_high_water_as_drained() {
        let store = Arc::new(
            Store::session_test_store("post-commit-cancel-before-shutdown")
                .await
                .unwrap(),
        );
        let target = ImmediateTarget::default();
        let calls = target.calls.clone();
        let cancel = CancellationToken::new();
        let dispatcher =
            OrderedPostCommitDispatcher::start(store.clone(), target, 0, cancel.clone()).unwrap();
        cancel.cancel();
        assert_eq!(
            EventWriter::new(store.clone())
                .apply(maintenance("committed-after-cancel"))
                .await
                .unwrap(),
            vec![1]
        );

        let error = dispatcher.shutdown().await.unwrap_err();
        assert!(
            format!("{error:#}").contains("before orderly drain completed"),
            "{error:#}"
        );
        assert!(
            calls.lock().unwrap().is_empty(),
            "cancelled dispatcher must not claim the captured sequence"
        );
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

    #[tokio::test]
    async fn drop_cancels_a_pool_blocked_page_read_and_releases_the_receiver_claim() {
        let store = Arc::new(
            Store::session_test_store("post-commit-drop-pool-read")
                .await
                .unwrap(),
        );
        let dispatcher = OrderedPostCommitDispatcher::start(
            store.clone(),
            ImmediateTarget::default(),
            0,
            CancellationToken::new(),
        )
        .unwrap();
        let connection = store.pool().acquire().await.unwrap();
        store.publish_test_committed_event_receipt(&[1]);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        drop(dispatcher);
        let receiver = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if let Ok(receiver) = store.claim_post_commit_receiver() {
                    break receiver;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("receiver claim is released while the SQLite pool remains occupied");
        drop(receiver);
        drop(connection);
    }

    fn runtime_authority(
        personality_agent_id: &PersonalityAgentId,
        generation: u64,
        suffix: &str,
    ) -> (
        RuntimeEpochAuthority,
        ProcessGenerationLease,
        GenerationRecoveryFence,
    ) {
        let generation = ProcessGeneration::from_wire(generation).unwrap();
        let rpc = RpcIdentity::from_wire(
            personality_agent_id.as_str(),
            generation.as_u64(),
            format!("nonce-{suffix}"),
        )
        .unwrap();
        let lease = ProcessGenerationLease::new(
            personality_agent_id.clone(),
            generation,
            format!("lease-{suffix}"),
        )
        .unwrap();
        let fence = GenerationRecoveryFence::new(&lease, format!("fence-{suffix}")).unwrap();
        (
            RuntimeEpochAuthority::new(rpc, lease.clone(), fence.clone()).unwrap(),
            lease,
            fence,
        )
    }

    #[tokio::test]
    async fn hydration_rollover_invalidates_the_old_dispatcher_before_target_side_effects() {
        let store = Arc::new(
            Store::session_test_store("post-commit-runtime-rollover")
                .await
                .unwrap(),
        );
        let paid = store.scope().personality_agent_id.clone();
        let (old_authority, old_lease, old_fence) = runtime_authority(&paid, 7, "old");
        assert!(matches!(
            store.hydrate(&old_lease, &old_fence).await.unwrap(),
            HydrationOutcome::Complete(_)
        ));
        let old_epoch = store
            .issue_post_commit_epoch(old_authority.clone(), CancellationToken::new())
            .unwrap();
        let rival_rpc = RpcIdentity::from_wire(paid.as_str(), 7, "nonce-rival-process").unwrap();
        let rival_authority =
            RuntimeEpochAuthority::new(rival_rpc, old_lease.clone(), old_fence.clone()).unwrap();
        assert!(
            store
                .issue_post_commit_epoch(rival_authority, CancellationToken::new())
                .is_err(),
            "one Store hydration may issue only one exact boot capability"
        );
        let other_store = Arc::new(Store::session_test_store(paid.as_str()).await.unwrap());
        assert!(matches!(
            other_store.hydrate(&old_lease, &old_fence).await.unwrap(),
            HydrationOutcome::Complete(_)
        ));
        assert!(
            OrderedPostCommitDispatcher::start_bound(
                other_store,
                CapabilityTarget {
                    epoch: old_epoch.clone(),
                    calls: Arc::new(Mutex::new(Vec::new())),
                },
                0,
                old_epoch.clone(),
            )
            .is_err(),
            "a capability issued by another Store hydration must fail before dispatcher start"
        );
        let old_calls = Arc::new(Mutex::new(Vec::new()));
        let old = OrderedPostCommitDispatcher::start_bound(
            store.clone(),
            CapabilityTarget {
                epoch: old_epoch.clone(),
                calls: old_calls.clone(),
            },
            0,
            old_epoch.clone(),
        )
        .unwrap();

        let (new_authority, new_lease, new_fence) = runtime_authority(&paid, 8, "new");
        let admission_guard = old_epoch.claim_admission().await.unwrap();
        let rollover = {
            let store = store.clone();
            let new_lease = new_lease.clone();
            let new_fence = new_fence.clone();
            tokio::spawn(async move { store.hydrate(&new_lease, &new_fence).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !old_epoch.is_cancelled() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("rollover invalidates the old capability");
        assert!(
            !rollover.is_finished(),
            "hydration rollover must wait for the old admission gate"
        );
        drop(admission_guard);
        assert!(matches!(
            rollover.await.unwrap().unwrap(),
            HydrationOutcome::Complete(_)
        ));
        assert!(old_epoch.is_cancelled());
        assert!(
            store
                .issue_post_commit_epoch(old_authority, CancellationToken::new())
                .is_err(),
            "the Store must reject authority from its previous hydration"
        );
        assert_eq!(
            EventWriter::new(store.clone())
                .apply(maintenance("after-rollover"))
                .await
                .unwrap(),
            vec![1]
        );
        assert!(old.client().admission_for(&paid, 1).await.is_err());
        assert!(old.shutdown().await.is_err());
        assert!(old_calls.lock().unwrap().is_empty());

        let new_epoch = store
            .issue_post_commit_epoch(new_authority, CancellationToken::new())
            .unwrap();
        let new_calls = Arc::new(Mutex::new(Vec::new()));
        let new = OrderedPostCommitDispatcher::start_bound(
            store.clone(),
            CapabilityTarget {
                epoch: new_epoch.clone(),
                calls: new_calls.clone(),
            },
            0,
            new_epoch,
        )
        .unwrap();
        new.client().admission_for(&paid, 1).await.unwrap();
        assert_eq!(*new_calls.lock().unwrap(), vec![1]);
        new.shutdown().await.unwrap();
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
