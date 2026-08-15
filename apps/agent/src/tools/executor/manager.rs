//! Process-wide lifecycle state for one generation-fenced executor manager.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use super::protocol::RPC_BOOT_UNIQUENESS_EXHAUSTED_CODE;
use super::protocol::rpc_id_digest;
use super::{
    ExecutorResponse, RpcError, RpcLifecycleTracker, VerifiedCallAuthority,
    call_authority::{AuthorityReplayBinding, VerifiedAuthorityExpiry},
};
use crate::tools::ToolError;

const RETAINED_OUTCOME_CAPACITY: usize = 256;
const RETAINED_OUTCOME_BYTE_CAPACITY: usize = 2 * 1024 * 1024;
const CONSUMED_GRANT_CAPACITY: usize = 1_000_000;
pub(super) const RPC_REPLAY_OUTCOME_UNAVAILABLE_CODE: &str = "rpc_replay_outcome_unavailable";
pub(super) const RPC_CALL_AUTHORITY_REPLAY_CODE: &str = "executor_call_authority_replay";
pub(super) const RPC_CALL_AUTHORITY_CAPACITY_EXHAUSTED_CODE: &str =
    "executor_call_authority_capacity_exhausted";
pub(super) const RPC_CALL_AUTHORITY_STALE_CODE: &str = "executor_call_authority_stale";

#[derive(Clone, Debug, PartialEq)]
struct RetainedOutcome {
    request_id: String,
    result: Result<ExecutorResponse, RpcError>,
    authority_binding: Option<AuthorityReplayBinding>,
    encoded_bytes: usize,
}

#[derive(Clone, Copy)]
struct CompletedExecutionReceipt {
    request_id_digest: [u8; 32],
}

struct ActiveExecution {
    request_id: String,
    authority_binding: Option<AuthorityReplayBinding>,
    cancel: Option<CancellationToken>,
    cancel_waiters: Vec<oneshot::Sender<Result<ExecutorResponse, RpcError>>>,
}

#[derive(Default)]
struct ManagerRegistry {
    lifecycle: RpcLifecycleTracker,
    active: HashMap<String, ActiveExecution>,
    completed_execution_receipts: HashMap<[u8; 32], CompletedExecutionReceipt>,
    retained_outcomes: HashMap<String, RetainedOutcome>,
    retained_order: VecDeque<String>,
    retained_outcome_bytes: usize,
    consumed_grants: HashSet<[u8; 32]>,
}

pub(super) enum ExecutionRegistration {
    Pending(PendingExecution),
    Replay(Result<ExecutorResponse, RpcError>),
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
    consumed_grant_capacity: usize,
}

impl ExecutorManager {
    pub(super) fn new(operation_capacity: usize) -> Arc<Self> {
        Self::with_registry(operation_capacity, ManagerRegistry::default())
    }

    fn with_registry(operation_capacity: usize, registry: ManagerRegistry) -> Arc<Self> {
        Self::with_registry_and_grant_capacity(
            operation_capacity,
            registry,
            CONSUMED_GRANT_CAPACITY,
        )
    }

    fn with_registry_and_grant_capacity(
        operation_capacity: usize,
        registry: ManagerRegistry,
        consumed_grant_capacity: usize,
    ) -> Arc<Self> {
        assert!(operation_capacity > 0);
        assert!(consumed_grant_capacity > 0);
        Arc::new(Self {
            registry: Arc::new(Mutex::new(registry)),
            admission: Arc::new(Semaphore::new(operation_capacity)),
            consumed_grant_capacity,
        })
    }

    #[cfg(test)]
    fn with_test_consumed_grant_capacity(
        operation_capacity: usize,
        consumed_grant_capacity: usize,
    ) -> Arc<Self> {
        Self::with_registry_and_grant_capacity(
            operation_capacity,
            ManagerRegistry::default(),
            consumed_grant_capacity,
        )
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
        let ExecutionRegistration::Pending(mut pending) =
            self.register_execution(request_id, execution_id, cancel)?
        else {
            return Err(ToolError::Protocol(
                "executor replay requires the replay-aware service path".to_owned(),
            ));
        };
        let Some(permit) = pending.wait_for_admission().await? else {
            return Err(ToolError::Cancelled);
        };
        pending.promote(permit)
    }

    pub(super) fn register_execution(
        self: &Arc<Self>,
        request_id: String,
        execution_id: String,
        cancel: Option<CancellationToken>,
    ) -> Result<ExecutionRegistration, ToolError> {
        self.register_execution_inner(request_id, execution_id, cancel, None, None)
    }

    /// Consume the verified grant id in the same manager critical section that
    /// admits the request/execution identity. A failed admission cannot leave
    /// a request registered without consuming its grant, and reminting a new
    /// random token id cannot admit the same grant twice.
    pub(super) fn register_authorized_execution(
        self: &Arc<Self>,
        request_id: String,
        execution_id: String,
        cancel: Option<CancellationToken>,
        authority: VerifiedCallAuthority,
    ) -> Result<ExecutionRegistration, ToolError> {
        if authority.request_id() != request_id || authority.execution_id() != execution_id {
            return Err(ToolError::Protocol(
                "verified executor authority identity mismatch".to_owned(),
            ));
        }
        self.register_execution_inner(
            request_id,
            execution_id,
            cancel,
            Some(authority.replay_binding()),
            Some(authority.expiry()),
        )
    }

    fn register_execution_inner(
        self: &Arc<Self>,
        request_id: String,
        execution_id: String,
        cancel: Option<CancellationToken>,
        authority_binding: Option<AuthorityReplayBinding>,
        authority_expiry: Option<VerifiedAuthorityExpiry>,
    ) -> Result<ExecutionRegistration, ToolError> {
        let mut registry = self.lock_registry()?;
        let execution_digest = rpc_id_digest(&execution_id);
        if let Some(receipt) = registry.completed_execution_receipts.get(&execution_digest) {
            if receipt.request_id_digest != rpc_id_digest(&request_id) {
                return Err(ToolError::Protocol(
                    "RPC execution_id must be unique".to_owned(),
                ));
            }
            return match registry.retained_outcomes.get(&execution_id) {
                Some(outcome)
                    if outcome.request_id == request_id
                        && outcome.authority_binding == authority_binding =>
                {
                    Ok(ExecutionRegistration::Replay(outcome.result.clone()))
                }
                _ => Err(ToolError::Protocol(
                    RPC_REPLAY_OUTCOME_UNAVAILABLE_CODE.to_owned(),
                )),
            };
        }
        let grant_digest = authority_binding
            .as_ref()
            .map(|binding| rpc_id_digest(binding.grant_digest()));
        if let Some(grant_digest) = grant_digest {
            if registry.consumed_grants.contains(&grant_digest) {
                return Err(ToolError::Protocol(
                    RPC_CALL_AUTHORITY_REPLAY_CODE.to_owned(),
                ));
            }
            if registry.consumed_grants.len() >= self.consumed_grant_capacity {
                return Err(ToolError::Protocol(
                    RPC_CALL_AUTHORITY_CAPACITY_EXHAUSTED_CODE.to_owned(),
                ));
            }
        }
        registry
            .lifecycle
            .begin_execution(&request_id, &execution_id)?;
        if let Some(grant_digest) = grant_digest {
            registry.consumed_grants.insert(grant_digest);
        }
        if registry
            .active
            .insert(
                execution_id.clone(),
                ActiveExecution {
                    request_id: request_id.clone(),
                    authority_binding,
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
        drop(registry);
        Ok(ExecutionRegistration::Pending(PendingExecution {
            manager: self.clone(),
            request_id,
            execution_id,
            cancel,
            authority_expiry,
            registered: true,
        }))
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
            remember_execution_receipt(&mut registry, request_id, execution_id);
            retain_outcome(
                &mut registry,
                execution_id.to_owned(),
                RetainedOutcome {
                    request_id: request_id.to_owned(),
                    result: result.clone(),
                    authority_binding: active.authority_binding,
                    encoded_bytes: 0,
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
            remember_execution_receipt(&mut registry, request_id, execution_id);
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
                    authority_binding: active.authority_binding,
                    encoded_bytes: 0,
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

    #[cfg(test)]
    fn retained_outcome_bytes(&self) -> usize {
        self.registry
            .lock()
            .map(|registry| registry.retained_outcome_bytes)
            .unwrap_or(usize::MAX)
    }

    #[cfg(test)]
    fn retained_outcome_count(&self) -> usize {
        self.registry
            .lock()
            .map(|registry| registry.retained_outcomes.len())
            .unwrap_or(usize::MAX)
    }
}

fn retain_outcome(registry: &mut ManagerRegistry, execution_id: String, outcome: RetainedOutcome) {
    let Ok(encoded) = serde_json::to_vec(&outcome.result) else {
        tracing::error!(execution_id, "failed to encode retained executor outcome");
        return;
    };
    if encoded.len() > RETAINED_OUTCOME_BYTE_CAPACITY {
        return;
    }
    let Ok(result) = serde_json::from_slice(&encoded) else {
        tracing::error!(
            execution_id,
            "failed to normalize retained executor outcome"
        );
        return;
    };
    let outcome = RetainedOutcome {
        request_id: outcome.request_id,
        result,
        authority_binding: outcome.authority_binding,
        encoded_bytes: encoded.len(),
    };
    if registry.retained_outcomes.contains_key(&execution_id) {
        registry
            .retained_order
            .retain(|known| known != &execution_id);
        if let Some(previous) = registry.retained_outcomes.remove(&execution_id) {
            registry.retained_outcome_bytes = registry
                .retained_outcome_bytes
                .saturating_sub(previous.encoded_bytes);
        }
    }
    registry.retained_outcome_bytes = registry
        .retained_outcome_bytes
        .saturating_add(outcome.encoded_bytes);
    registry
        .retained_outcomes
        .insert(execution_id.clone(), outcome);
    registry.retained_order.push_back(execution_id);
    while registry.retained_order.len() > RETAINED_OUTCOME_CAPACITY
        || registry.retained_outcome_bytes > RETAINED_OUTCOME_BYTE_CAPACITY
    {
        if let Some(expired) = registry.retained_order.pop_front()
            && let Some(outcome) = registry.retained_outcomes.remove(&expired)
        {
            registry.retained_outcome_bytes = registry
                .retained_outcome_bytes
                .saturating_sub(outcome.encoded_bytes);
        }
    }
}

fn remember_execution_receipt(
    registry: &mut ManagerRegistry,
    request_id: &str,
    execution_id: &str,
) {
    registry.completed_execution_receipts.insert(
        rpc_id_digest(execution_id),
        CompletedExecutionReceipt {
            request_id_digest: rpc_id_digest(request_id),
        },
    );
}

pub(super) struct PendingExecution {
    manager: Arc<ExecutorManager>,
    request_id: String,
    execution_id: String,
    cancel: Option<CancellationToken>,
    authority_expiry: Option<VerifiedAuthorityExpiry>,
    registered: bool,
}

impl PendingExecution {
    pub(super) async fn wait_for_admission(
        &self,
    ) -> Result<Option<OwnedSemaphorePermit>, ToolError> {
        let admission = self.manager.admission.clone().acquire_owned();
        if let Some(cancel) = &self.cancel {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Ok(None),
                permit = admission => {
                    let permit = permit.map_err(|_| {
                        ToolError::Protocol("executor admission is closed".to_owned())
                    })?;
                    if cancel.is_cancelled() {
                        Ok(None)
                    } else {
                        Ok(Some(permit))
                    }
                }
            }
        } else {
            admission
                .await
                .map(Some)
                .map_err(|_| ToolError::Protocol("executor admission is closed".to_owned()))
        }
    }

    pub(super) fn promote(
        &mut self,
        permit: OwnedSemaphorePermit,
    ) -> Result<ExecutionLease, ToolError> {
        if self
            .cancel
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(ToolError::Cancelled);
        }
        self.registered = false;
        Ok(ExecutionLease {
            manager: self.manager.clone(),
            request_id: self.request_id.clone(),
            execution_id: self.execution_id.clone(),
            cancel: self.cancel.clone(),
            authority_expiry: self.authority_expiry.take(),
            _permit: permit,
            terminal: false,
        })
    }

    pub(super) fn complete(
        &mut self,
        result: Result<ExecutorResponse, RpcError>,
    ) -> Result<(), ToolError> {
        self.manager
            .complete_execution(&self.request_id, &self.execution_id, result)?;
        self.registered = false;
        Ok(())
    }
}

impl Drop for PendingExecution {
    fn drop(&mut self) {
        if self.registered {
            self.manager
                .abandon_execution(&self.request_id, &self.execution_id);
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
    authority_expiry: Option<VerifiedAuthorityExpiry>,
    _permit: OwnedSemaphorePermit,
    terminal: bool,
}

impl ExecutionLease {
    pub(super) fn cancellation_token(&self) -> Option<CancellationToken> {
        self.cancel.clone()
    }

    pub(super) fn authority_expiry(&self) -> Option<VerifiedAuthorityExpiry> {
        self.authority_expiry.clone()
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

    fn verified_authority(request_id: &str, execution_id: &str) -> VerifiedCallAuthority {
        verified_authority_for_grant(request_id, execution_id, "manager-grant")
    }

    fn verified_authority_for_grant(
        request_id: &str,
        execution_id: &str,
        grant_id: &str,
    ) -> VerifiedCallAuthority {
        use ed25519_dalek::SigningKey;

        use crate::{
            runtime::contracts::RpcIdentity,
            tools::executor::{
                ExecutorCallAuthorityVerifier, ExecutorOperation,
                call_authority::{
                    CallAuthorityPermitClaims, Ed25519CallAuthorityIssuer,
                    ExecutorAuthorityProvenance, ExecutorInvocationRoute,
                },
                call_authority_key_id,
            },
        };

        let identity = RpcIdentity::from_wire(
            "018f47a2-9b3c-7def-8abc-0123456789ab",
            7,
            "manager-authority",
        )
        .unwrap();
        let key = SigningKey::from_bytes(&[7; 32]);
        let issuer =
            Ed25519CallAuthorityIssuer::new(call_authority_key_id(), key.clone(), identity.clone())
                .unwrap();
        let operation = ExecutorOperation::ListDir {
            path: ".".to_owned(),
            execution_id: execution_id.to_owned(),
        };
        let signed = issuer
            .issue_for_test(
                request_id.to_owned(),
                operation.clone(),
                CallAuthorityPermitClaims {
                    grant_digest: crate::tools::executor::call_authority::test_grant_digest(
                        grant_id,
                    ),
                    bound_evidence_digest: "11".repeat(32),
                    action_digest: "33".repeat(32),
                    authorization_projection_digest: "22".repeat(32),
                    route: ExecutorInvocationRoute::Normal,
                    resolved_authority: ExecutorAuthorityProvenance::AgentOwn,
                },
            )
            .unwrap();
        ExecutorCallAuthorityVerifier::new(call_authority_key_id(), key.verifying_key(), identity)
            .unwrap()
            .verify(Some(&signed), request_id, &operation)
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn authority_consumption_and_execution_admission_are_one_manager_decision() {
        let manager = ExecutorManager::new(1);
        let authority = verified_authority("authorized-request", "authorized-execution");
        let registration = manager
            .register_authorized_execution(
                "authorized-request".to_owned(),
                "authorized-execution".to_owned(),
                None,
                authority.clone(),
            )
            .unwrap();
        let ExecutionRegistration::Pending(mut pending) = registration else {
            panic!("first exact authority must admit one execution");
        };
        let reminted_authority = verified_authority("authorized-request", "authorized-execution");
        assert!(matches!(
            manager.register_authorized_execution(
                "authorized-request".to_owned(),
                "authorized-execution".to_owned(),
                None,
                reminted_authority,
            ),
            Err(ToolError::Protocol(code)) if code == RPC_CALL_AUTHORITY_REPLAY_CODE
        ));
        let permit = pending.wait_for_admission().await.unwrap().unwrap();
        let mut execution = pending.promote(permit).unwrap();
        let result = Ok(ExecutorResponse::Listed {
            entries: vec!["alpha".to_owned()],
        });
        execution.complete(result.clone()).unwrap();
        assert!(matches!(
            manager
                .register_authorized_execution(
                    "authorized-request".to_owned(),
                    "authorized-execution".to_owned(),
                    None,
                    authority,
                )
                .unwrap(),
            ExecutionRegistration::Replay(replayed) if replayed == result
        ));
    }

    #[test]
    fn consumed_grant_replay_and_capacity_exhaustion_remain_distinct() {
        let manager = ExecutorManager::with_test_consumed_grant_capacity(2, 1);
        let pending = manager
            .register_authorized_execution(
                "capacity-request-a".to_owned(),
                "capacity-execution-a".to_owned(),
                None,
                verified_authority_for_grant(
                    "capacity-request-a",
                    "capacity-execution-a",
                    "capacity-grant-a",
                ),
            )
            .expect("first grant fills the test capacity");
        assert!(matches!(pending, ExecutionRegistration::Pending(_)));

        assert!(matches!(
            manager.register_authorized_execution(
                "capacity-request-b".to_owned(),
                "capacity-execution-b".to_owned(),
                None,
                verified_authority_for_grant(
                    "capacity-request-b",
                    "capacity-execution-b",
                    "capacity-grant-b",
                ),
            ),
            Err(ToolError::Protocol(code))
                if code == RPC_CALL_AUTHORITY_CAPACITY_EXHAUSTED_CODE
        ));
        assert!(matches!(
            manager.register_authorized_execution(
                "replay-request".to_owned(),
                "replay-execution".to_owned(),
                None,
                verified_authority_for_grant(
                    "replay-request",
                    "replay-execution",
                    "capacity-grant-a",
                ),
            ),
            Err(ToolError::Protocol(code)) if code == RPC_CALL_AUTHORITY_REPLAY_CODE
        ));
    }

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

    #[tokio::test]
    async fn pending_execution_cancels_before_admission_without_starting_work() {
        let manager = ExecutorManager::new(1);
        let mut running = manager
            .begin_execution(
                "request-running".to_owned(),
                "execution-running".to_owned(),
                None,
            )
            .await
            .expect("occupy operation permit");
        let cancel = CancellationToken::new();
        let ExecutionRegistration::Pending(mut pending) = manager
            .register_execution(
                "request-pending".to_owned(),
                "execution-pending".to_owned(),
                Some(cancel.clone()),
            )
            .expect("register pending execution")
        else {
            panic!("new execution unexpectedly replayed");
        };
        let CancelDecision::Accepted(completed) = manager
            .cancel_execution("request-cancel-pending", "execution-pending")
            .expect("cancel registered pending execution")
        else {
            panic!("pending execution was not cancellable");
        };
        assert!(cancel.is_cancelled());
        assert!(
            pending
                .wait_for_admission()
                .await
                .expect("wait for cancelled admission")
                .is_none()
        );
        let mut side_effects = 0;
        if pending
            .wait_for_admission()
            .await
            .expect("repeat cancelled admission")
            .is_some()
        {
            side_effects += 1;
        }
        let terminal = Ok(ExecutorResponse::CancelAccepted {});
        pending
            .complete(terminal.clone())
            .expect("complete pending");
        assert_eq!(completed.await.expect("cancel waiter"), terminal);
        assert_eq!(side_effects, 0);
        running
            .complete(Ok(ExecutorResponse::CancelAccepted {}))
            .expect("release running permit");
    }

    #[tokio::test]
    async fn retained_outcomes_are_bounded_by_count_and_encoded_bytes() {
        let manager = ExecutorManager::new(1);
        for index in 0..256 {
            let mut lease = manager
                .begin_execution(
                    format!("request-large-{index}"),
                    format!("execution-large-{index}"),
                    None,
                )
                .await
                .expect("admit large outcome");
            lease
                .complete(Ok(ExecutorResponse::Listed {
                    entries: vec!["x".repeat(16 * 1024)],
                }))
                .expect("complete large outcome");
        }
        assert!(manager.retained_outcome_count() < RETAINED_OUTCOME_CAPACITY);
        assert!(manager.retained_outcome_bytes() <= RETAINED_OUTCOME_BYTE_CAPACITY);

        assert!(matches!(
            manager.register_execution(
                "request-large-0".to_owned(),
                "execution-large-0".to_owned(),
                None,
            ),
            Err(ToolError::Protocol(code)) if code == RPC_REPLAY_OUTCOME_UNAVAILABLE_CODE
        ));
        assert!(matches!(
            manager
                .register_execution(
                    "request-large-255".to_owned(),
                    "execution-large-255".to_owned(),
                    None,
                )
                .expect("latest exact outcome remains replayable"),
            ExecutionRegistration::Replay(Ok(ExecutorResponse::Listed { .. }))
        ));
        assert_eq!(manager.active_count(), 0);
    }
}
