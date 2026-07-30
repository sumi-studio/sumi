//! Process-wide lifecycle state for one generation-fenced executor manager.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use super::protocol::RPC_BOOT_UNIQUENESS_EXHAUSTED_CODE;
use super::{ExecutorResponse, RpcError, RpcLifecycleTracker};
use crate::tools::ToolError;

const RETAINED_OUTCOME_CAPACITY: usize = 256;

#[derive(Clone, Debug, PartialEq)]
struct RetainedOutcome {
    request_id: String,
    result: Result<ExecutorResponse, RpcError>,
}

struct ActiveExecution {
    request_id: String,
    cancel: Option<CancellationToken>,
    cancel_waiters: Vec<oneshot::Sender<Result<ExecutorResponse, RpcError>>>,
}

#[derive(Default)]
struct ManagerRegistry {
    lifecycle: RpcLifecycleTracker,
    active: HashMap<String, ActiveExecution>,
    retained_outcomes: HashMap<String, RetainedOutcome>,
    retained_order: VecDeque<String>,
}

/// Result of admitting a manager-wide cancel request.
pub(super) enum CancelDecision {
    /// The target is an active cancellable execution and its token was fired.
    Accepted(oneshot::Receiver<Result<ExecutorResponse, RpcError>>),
    /// The target is already terminal or is an active non-cancellable operation.
    TooLate,
    /// No execution with this identity is known in the current manager epoch.
    Unknown,
}

/// One manager process owns exactly one registry and one admission boundary.
///
/// Connections are replaceable transports. Request/execution uniqueness,
/// cancellation, retained terminal outcomes, and concurrency admission remain
/// shared for the full process boot.
#[derive(Clone)]
pub(super) struct ExecutorManager {
    registry: Arc<Mutex<ManagerRegistry>>,
    admission: Arc<Semaphore>,
}

impl ExecutorManager {
    pub(super) fn new(operation_capacity: usize) -> Arc<Self> {
        Self::with_registry(operation_capacity, ManagerRegistry::default())
    }

    fn with_registry(operation_capacity: usize, registry: ManagerRegistry) -> Arc<Self> {
        assert!(operation_capacity > 0);
        Arc::new(Self {
            registry: Arc::new(Mutex::new(registry)),
            admission: Arc::new(Semaphore::new(operation_capacity)),
        })
    }

    #[cfg(test)]
    fn with_test_boot_uniqueness_budget(
        operation_capacity: usize,
        capacity: usize,
        cancel_reserve: usize,
    ) -> Arc<Self> {
        Self::with_registry(
            operation_capacity,
            ManagerRegistry {
                lifecycle: RpcLifecycleTracker::with_test_boot_uniqueness_budget(
                    capacity,
                    cancel_reserve,
                ),
                ..ManagerRegistry::default()
            },
        )
    }

    pub(super) async fn begin_execution(
        self: &Arc<Self>,
        request_id: String,
        execution_id: String,
        cancel: Option<CancellationToken>,
    ) -> Result<ExecutionLease, ToolError> {
        // Waiting operations remain bounded by the listener's connection
        // semaphore. Cancellation requests bypass this admission queue.
        let permit = self
            .admission
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ToolError::Protocol("executor admission is closed".to_owned()))?;
        {
            let mut registry = self.lock_registry()?;
            registry
                .lifecycle
                .begin_execution(&request_id, &execution_id)?;
            if registry
                .active
                .insert(
                    execution_id.clone(),
                    ActiveExecution {
                        request_id: request_id.clone(),
                        cancel: cancel.clone(),
                        cancel_waiters: Vec::new(),
                    },
                )
                .is_some()
            {
                return Err(ToolError::Protocol(
                    "executor active registry admitted a duplicate execution_id".to_owned(),
                ));
            }
        }
        Ok(ExecutionLease {
            manager: self.clone(),
            request_id,
            execution_id,
            cancel,
            _permit: permit,
            terminal: false,
        })
    }

    pub(super) fn reject_request(&self, request_id: &str) -> Result<(), ToolError> {
        let mut registry = self.lock_registry()?;
        registry.lifecycle.begin_request(request_id)?;
        registry.lifecycle.accept_terminal(request_id)
    }

    pub(super) fn accept_update(&self, request_id: &str) -> Result<(), ToolError> {
        self.lock_registry()?.lifecycle.accept_update(request_id)
    }

    pub(super) fn cancel_execution(
        &self,
        request_id: &str,
        execution_id: &str,
    ) -> Result<CancelDecision, ToolError> {
        let (decision, token) = {
            let mut registry = self.lock_registry()?;
            let cancellable = registry
                .active
                .get(execution_id)
                .is_some_and(|active| active.cancel.is_some());
            let decision = match registry.active.get(execution_id) {
                Some(_) if cancellable => {
                    registry.lifecycle.accept_cancel(request_id, execution_id)?;
                    registry.lifecycle.accept_terminal(request_id)?;
                    let (completion, completed) = oneshot::channel();
                    registry
                        .active
                        .get_mut(execution_id)
                        .expect("active execution was checked under the same lock")
                        .cancel_waiters
                        .push(completion);
                    CancelDecision::Accepted(completed)
                }
                Some(_) => {
                    registry.lifecycle.begin_request(request_id)?;
                    registry.lifecycle.accept_terminal(request_id)?;
                    CancelDecision::TooLate
                }
                None if registry.lifecycle.execution_is_completed(execution_id) => {
                    registry.lifecycle.begin_request(request_id)?;
                    registry.lifecycle.accept_terminal(request_id)?;
                    CancelDecision::TooLate
                }
                None => {
                    registry.lifecycle.begin_request(request_id)?;
                    registry.lifecycle.accept_terminal(request_id)?;
                    CancelDecision::Unknown
                }
            };
            let token = registry
                .active
                .get(execution_id)
                .and_then(|active| active.cancel.clone());
            (decision, token)
        };
        if matches!(decision, CancelDecision::Accepted(_))
            && let Some(token) = token
        {
            token.cancel();
        }
        Ok(decision)
    }

    fn complete_execution(
        &self,
        request_id: &str,
        execution_id: &str,
        result: Result<ExecutorResponse, RpcError>,
    ) -> Result<(), ToolError> {
        let waiters = {
            let mut registry = self.lock_registry()?;
            let active = registry.active.get(execution_id).ok_or_else(|| {
                ToolError::Protocol(
                    "executor completion referenced an unknown execution".to_owned(),
                )
            })?;
            if active.request_id != request_id {
                return Err(ToolError::Protocol(
                    "executor completion request identity mismatch".to_owned(),
                ));
            }
            registry.lifecycle.accept_terminal(request_id)?;
            let active = registry
                .active
                .remove(execution_id)
                .expect("active execution was checked under the same lock");
            retain_outcome(
                &mut registry,
                execution_id.to_owned(),
                RetainedOutcome {
                    request_id: request_id.to_owned(),
                    result: result.clone(),
                },
            );
            active.cancel_waiters
        };
        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
        Ok(())
    }

    fn abandon_execution(&self, request_id: &str, execution_id: &str) {
        let result = self.lock_registry().and_then(|mut registry| {
            let Some(active) = registry.active.remove(execution_id) else {
                return Ok(());
            };
            if active.request_id != request_id {
                registry.active.insert(execution_id.to_owned(), active);
                return Err(ToolError::Protocol(
                    "executor abandoned execution request identity mismatch".to_owned(),
                ));
            }
            if let Some(cancel) = active.cancel {
                cancel.cancel();
            }
            registry.lifecycle.accept_terminal(request_id)?;
            let terminal = Err(RpcError {
                code: "rpc_indeterminate".to_owned(),
                resource_limit: None,
            });
            retain_outcome(
                &mut registry,
                execution_id.to_owned(),
                RetainedOutcome {
                    request_id: request_id.to_owned(),
                    result: terminal.clone(),
                },
            );
            for waiter in active.cancel_waiters {
                let _ = waiter.send(terminal.clone());
            }
            Ok(())
        });
        if let Err(error) = result {
            tracing::error!(
                %error,
                request_id,
                execution_id,
                "failed to finalize abandoned executor operation"
            );
        }
    }

    fn lock_registry(&self) -> Result<std::sync::MutexGuard<'_, ManagerRegistry>, ToolError> {
        self.registry
            .lock()
            .map_err(|_| ToolError::Protocol("executor manager registry lock poisoned".to_owned()))
    }

    #[cfg(test)]
    pub(super) fn retained_outcome(
        &self,
        execution_id: &str,
    ) -> Option<Result<ExecutorResponse, RpcError>> {
        self.registry
            .lock()
            .ok()
            .and_then(|registry| registry.retained_outcomes.get(execution_id).cloned())
            .map(|outcome| outcome.result)
    }

    #[cfg(test)]
    pub(super) fn active_count(&self) -> usize {
        self.registry
            .lock()
            .map(|registry| registry.active.len())
            .unwrap_or(usize::MAX)
    }

    #[cfg(test)]
    fn tracked_identity_count(&self) -> usize {
        self.registry
            .lock()
            .map(|registry| registry.lifecycle.tracked_identity_count())
            .unwrap_or(usize::MAX)
    }
}

fn retain_outcome(registry: &mut ManagerRegistry, execution_id: String, outcome: RetainedOutcome) {
    if registry.retained_outcomes.contains_key(&execution_id) {
        registry
            .retained_order
            .retain(|known| known != &execution_id);
    }
    registry
        .retained_outcomes
        .insert(execution_id.clone(), outcome);
    registry.retained_order.push_back(execution_id);
    while registry.retained_order.len() > RETAINED_OUTCOME_CAPACITY {
        if let Some(expired) = registry.retained_order.pop_front() {
            registry.retained_outcomes.remove(&expired);
        }
    }
}

/// An admitted execution remains registered until one terminal result is
/// recorded. Dropping the connection task cancels cancellable work and records
/// an indeterminate terminal before releasing admission.
pub(super) struct ExecutionLease {
    manager: Arc<ExecutorManager>,
    request_id: String,
    execution_id: String,
    cancel: Option<CancellationToken>,
    _permit: OwnedSemaphorePermit,
    terminal: bool,
}

impl ExecutionLease {
    pub(super) fn cancellation_token(&self) -> Option<CancellationToken> {
        self.cancel.clone()
    }

    pub(super) fn complete(
        &mut self,
        result: Result<ExecutorResponse, RpcError>,
    ) -> Result<(), ToolError> {
        self.manager
            .complete_execution(&self.request_id, &self.execution_id, result)?;
        self.terminal = true;
        Ok(())
    }
}

impl Drop for ExecutionLease {
    fn drop(&mut self) {
        if !self.terminal {
            self.manager
                .abandon_execution(&self.request_id, &self.execution_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connections_share_uniqueness_cancellation_and_outcomes() {
        let manager = ExecutorManager::new(2);
        let cancel = CancellationToken::new();
        let mut lease = manager
            .begin_execution(
                "request-1".to_owned(),
                "execution-1".to_owned(),
                Some(cancel.clone()),
            )
            .await
            .unwrap();

        assert!(
            manager
                .begin_execution("request-2".to_owned(), "execution-1".to_owned(), None,)
                .await
                .is_err()
        );
        let CancelDecision::Accepted(completed) =
            manager.cancel_execution("cancel-1", "execution-1").unwrap()
        else {
            panic!("active execution was not cancellable");
        };
        assert!(cancel.is_cancelled());

        let terminal = Ok(ExecutorResponse::CancelAccepted {});
        lease.complete(terminal.clone()).unwrap();
        assert_eq!(completed.await.unwrap(), terminal);
        assert_eq!(manager.retained_outcome("execution-1"), Some(terminal));
        assert_eq!(manager.active_count(), 0);
        assert!(matches!(
            manager.cancel_execution("cancel-2", "execution-1").unwrap(),
            CancelDecision::TooLate
        ));
    }

    #[tokio::test]
    async fn dropped_execution_retains_indeterminate_outcome() {
        let manager = ExecutorManager::new(1);
        let cancel = CancellationToken::new();
        let lease = manager
            .begin_execution(
                "request-drop".to_owned(),
                "execution-drop".to_owned(),
                Some(cancel.clone()),
            )
            .await
            .unwrap();
        drop(lease);

        assert!(cancel.is_cancelled());
        assert_eq!(manager.active_count(), 0);
        assert!(matches!(
            manager.retained_outcome("execution-drop"),
            Some(Err(RpcError { code, .. })) if code == "rpc_indeterminate"
        ));
    }

    #[tokio::test]
    async fn replay_after_outcome_eviction_is_rejected_before_side_effects() {
        let manager = ExecutorManager::new(1);
        for index in 0..4_097 {
            let mut lease = manager
                .begin_execution(
                    format!("request-{index}"),
                    format!("execution-{index}"),
                    None,
                )
                .await
                .expect("admit unique execution");
            lease
                .complete(Ok(ExecutorResponse::CancelAccepted {}))
                .expect("complete execution");
        }

        assert_eq!(manager.retained_outcome("execution-0"), None);
        assert!(manager.retained_outcome("execution-4096").is_some());

        let mut side_effects = 0;
        let replay = manager
            .begin_execution("request-0".to_owned(), "execution-replay".to_owned(), None)
            .await;
        if replay.is_ok() {
            side_effects += 1;
        }
        assert!(matches!(
            replay,
            Err(ToolError::Protocol(message)) if message == "RPC request_id must be unique"
        ));
        assert_eq!(side_effects, 0);
        assert_eq!(manager.active_count(), 0);
    }

    #[tokio::test]
    async fn exhausted_boot_budget_rejects_mutation_but_reserves_active_cancel() {
        let manager = ExecutorManager::with_test_boot_uniqueness_budget(2, 4, 1);
        let cancel = CancellationToken::new();
        let mut active = manager
            .begin_execution(
                "request-active".to_owned(),
                "execution-active".to_owned(),
                Some(cancel.clone()),
            )
            .await
            .expect("admit active execution");
        let mut completed = manager
            .begin_execution(
                "request-completed".to_owned(),
                "execution-completed".to_owned(),
                None,
            )
            .await
            .expect("fill boot uniqueness budget");
        completed
            .complete(Ok(ExecutorResponse::CancelAccepted {}))
            .expect("complete second execution");
        drop(completed);
        assert_eq!(manager.tracked_identity_count(), 4);

        assert!(matches!(
            manager
                .begin_execution(
                    "request-completed".to_owned(),
                    "execution-replay-request".to_owned(),
                    None,
                )
                .await,
            Err(ToolError::Protocol(message)) if message == "RPC request_id must be unique"
        ));
        assert!(matches!(
            manager
                .begin_execution(
                    "request-new".to_owned(),
                    "execution-new".to_owned(),
                    None,
                )
                .await,
            Err(ToolError::Protocol(message))
                if message == RPC_BOOT_UNIQUENESS_EXHAUSTED_CODE
        ));
        assert!(matches!(
            manager
                .begin_execution(
                    "request-replay-execution".to_owned(),
                    "execution-completed".to_owned(),
                    None,
                )
                .await,
            Err(ToolError::Protocol(message)) if message == "RPC execution_id must be unique"
        ));
        assert_eq!(manager.tracked_identity_count(), 4);
        assert_eq!(manager.active_count(), 1);

        let CancelDecision::Accepted(cancelled) = manager
            .cancel_execution("request-cancel", "execution-active")
            .expect("cancel reserve admits active cancellation")
        else {
            panic!("active execution was not cancellable");
        };
        assert!(cancel.is_cancelled());
        assert_eq!(manager.tracked_identity_count(), 5);
        active
            .complete(Ok(ExecutorResponse::CancelAccepted {}))
            .expect("complete cancelled execution");
        assert_eq!(
            cancelled.await.expect("cancel completion"),
            Ok(ExecutorResponse::CancelAccepted {})
        );
        assert_eq!(manager.tracked_identity_count(), 5);
        assert!(manager.tracked_identity_count() <= 4 + 1);
    }
}
