//! Runtime approval broker.
//!
//! `ApprovalBroker` sits between the tool execution loop and the durable
//! `approval_log`. It projects each `ToolCall` to a redacted review shape,
//! evaluates the deterministic `Policy`, optionally calls the `Reviewer` for
//! `AutoReview`/`StrictAutoReview`, and coordinates user decisions through a
//! `oneshot` wait per pending request.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::{Context, Result};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    agent::events::{self, ApprovalRequest},
    approval::{
        action::{CanonicalAction, SandboxSummary, SecretAwareActionProjector},
        policy::{ApprovalRule, Policy, PolicyDecision, ResolvedDecision, UserDecision},
        prompt::TrustedEnvironment,
        reviewer::{ReviewOutcome, ReviewRequest, Reviewer, ReviewerMode},
    },
    gateway::ApprovalDecision as GatewayApprovalDecision,
    provider::types::{PublicMessage, ToolCall},
};

/// Result of asking the broker whether a tool may start.
#[derive(Debug)]
pub enum ApprovalOutcome {
    Allowed,
    Denied {
        reason: String,
    },
    Pending {
        request: ApprovalRequest,
        receiver: oneshot::Receiver<WaiterResult>,
    },
}

/// Message delivered to a pending approval waiter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WaiterResult {
    Resolved(ResolvedDecision),
    Cancelled,
}

/// Per-pending-request state stored in the broker.
#[allow(dead_code)]
struct PendingEntry {
    action: CanonicalAction,
    tool_call_id: String,
    run_id: String,
    turn_id: String,
    sender: oneshot::Sender<WaiterResult>,
}

/// Runtime approval broker. Clone is cheap: all mutable state lives behind
/// `Arc` handles so the `Session` and the worker can share the same broker.
#[derive(Clone)]
pub struct ApprovalBroker {
    policy: Arc<RwLock<Policy>>,
    projector: Arc<SecretAwareActionProjector>,
    reviewer: Option<Arc<Reviewer>>,
    mode: ReviewerMode,
    headless: bool,
    trusted_env: TrustedEnvironment,
    pending: Arc<Mutex<HashMap<String, PendingEntry>>>,
}

impl std::fmt::Debug for ApprovalBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApprovalBroker")
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

#[allow(dead_code)]
impl ApprovalBroker {
    pub fn new(
        policy: Policy,
        projector: SecretAwareActionProjector,
        reviewer: Option<Arc<Reviewer>>,
        mode: ReviewerMode,
        headless: bool,
        trusted_env: TrustedEnvironment,
    ) -> Self {
        Self {
            policy: Arc::new(RwLock::new(policy)),
            projector: Arc::new(projector),
            reviewer,
            mode,
            headless,
            trusted_env,
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Build a headless broker with no reviewer: `NeedsApproval` actions are
    /// denied unless the policy already allows them.
    pub fn headless(policy: Policy, projector: SecretAwareActionProjector) -> Self {
        let workspace_root = policy.workspace_root().to_string_lossy().into_owned();
        Self::new(
            policy,
            projector,
            None,
            ReviewerMode::User,
            true,
            TrustedEnvironment {
                workspace_root,
                sandbox: SandboxSummary::workspace(),
                denied_paths: Vec::new(),
                denied_network_domains: Vec::new(),
                repo_visibility: None,
                git_status: None,
            },
        )
    }

    /// Evaluate a tool call and either allow, deny, or enter a pending
    /// one-shot approval wait.
    pub async fn start_request(
        &self,
        tool_call: &ToolCall,
        transcript: &[PublicMessage],
        run_id: &str,
        turn_id: &str,
        context_version: &str,
        cancel: CancellationToken,
    ) -> Result<ApprovalOutcome> {
        let action = match CanonicalAction::from_tool_call(
            self.policy
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .workspace_root()
                .to_path_buf(),
            &tool_call.name,
            &tool_call.arguments,
        ) {
            Ok(a) => a,
            Err(e) => {
                return Ok(ApprovalOutcome::Denied {
                    reason: format!("invalid tool call: {e}"),
                });
            }
        };

        let projection = self.projector.project(&action);
        if let crate::approval::action::ReviewProjection::InsufficientEvidence { ref reason } =
            projection
        {
            return Ok(ApprovalOutcome::Denied {
                reason: reason.clone(),
            });
        }

        let policy = self
            .policy
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let decision = policy.evaluate(&action);

        match decision {
            PolicyDecision::Forbidden { reason, .. } => Ok(ApprovalOutcome::Denied { reason }),
            PolicyDecision::Allow { .. } => {
                if self.mode == ReviewerMode::StrictAutoReview {
                    self.call_reviewer(&projection, transcript, turn_id, context_version, cancel)
                        .await
                } else {
                    Ok(ApprovalOutcome::Allowed)
                }
            }
            PolicyDecision::NeedsApproval { reason, .. } => {
                if self.mode == ReviewerMode::User {
                    if self.headless {
                        return Ok(ApprovalOutcome::Denied {
                            reason: format!("{reason} (headless User mode)"),
                        });
                    }
                    self.make_pending(tool_call, &action, &projection, run_id, turn_id, &reason)
                } else {
                    self.call_reviewer(&projection, transcript, turn_id, context_version, cancel)
                        .await
                }
            }
        }
    }

    /// Convert an external `ApprovalDecision` into a `ResolvedDecision`, update
    /// the in-memory policy for safe `ApproveAlways` rules, and notify the
    /// waiting worker. Returns `None` when the `request_id` is not pending
    /// (terminal/unknown no-op).
    pub fn resolve(
        &self,
        request_id: &str,
        decision: &GatewayApprovalDecision,
    ) -> Option<ResolvedDecision> {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        let entry = pending.remove(request_id)?;
        drop(pending);

        let user_decision = match user_decision_from_gateway(request_id, decision) {
            Ok(d) => d,
            Err(e) => {
                let _ = entry
                    .sender
                    .send(WaiterResult::Resolved(ResolvedDecision::Rejected {
                        reason: e.to_string(),
                    }));
                return Some(ResolvedDecision::Rejected {
                    reason: e.to_string(),
                });
            }
        };

        let resolved = {
            let policy = self
                .policy
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            policy.resolve(&entry.action, user_decision, &self.projector)
        };

        let mut resolved = resolved;
        if let ResolvedDecision::ApproveAlways(ref rule) = resolved {
            let mut guard = self.policy.write().unwrap_or_else(|e| e.into_inner());
            if let Ok(new_policy) = guard.clone().try_with_rule(rule.clone()) {
                *guard = new_policy;
            } else {
                // The candidate rule cannot be safely persisted; never claim an
                // `ApproveAlways` decision that would not actually be durable.
                resolved = ResolvedDecision::ApproveOnce;
            }
        }

        if entry
            .sender
            .send(WaiterResult::Resolved(resolved.clone()))
            .is_err()
        {
            // The waiter was dropped; the request is no longer live. Return the
            // decision anyway so callers can emit the durable resolution.
        }

        Some(resolved)
    }

    /// Cancel a specific pending request and notify its waiter.
    pub fn cancel(&self, request_id: &str) -> bool {
        let entry = self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(request_id);
        if let Some(entry) = entry {
            let _ = entry.sender.send(WaiterResult::Cancelled);
            true
        } else {
            false
        }
    }

    /// Cancel every pending approval. Used by abort/soft-steer.
    pub fn cancel_all(&self) -> Vec<(String, String)> {
        let entries: Vec<_> = self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .collect();
        let mut released = Vec::with_capacity(entries.len());
        for (request_id, entry) in entries {
            let _ = entry.sender.send(WaiterResult::Cancelled);
            released.push((request_id, entry.tool_call_id));
        }
        released
    }

    /// True if this `request_id` is currently pending.
    pub fn has_pending(&self, request_id: &str) -> bool {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(request_id)
    }

    /// True if any approval is currently pending.
    pub fn any_pending(&self) -> bool {
        !self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }

    /// Look up the `tool_call_id` for a pending request, if any.
    pub fn pending_tool_call_id(&self, request_id: &str) -> Option<String> {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(request_id)
            .map(|e| e.tool_call_id.clone())
    }

    fn make_pending(
        &self,
        tool_call: &ToolCall,
        action: &CanonicalAction,
        projection: &crate::approval::action::ReviewProjection,
        run_id: &str,
        turn_id: &str,
        reason: &str,
    ) -> Result<ApprovalOutcome> {
        let request_id = Uuid::now_v7().to_string();
        let (tx, rx) = oneshot::channel();

        let args_summary = self
            .projector
            .redact_arguments(&tool_call.arguments)
            .context("redact tool arguments for approval request")?;

        let request = ApprovalRequest {
            id: request_id.clone(),
            tool_call_id: tool_call.id.clone(),
            tool_name: tool_call.name.clone(),
            action: to_events_projection(projection)?,
            args_summary,
            reason: Some(reason.to_owned()),
            audit: None,
        };

        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                request_id,
                PendingEntry {
                    action: action.clone(),
                    tool_call_id: tool_call.id.clone(),
                    run_id: run_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    sender: tx,
                },
            );

        Ok(ApprovalOutcome::Pending {
            request,
            receiver: rx,
        })
    }

    async fn call_reviewer(
        &self,
        projection: &crate::approval::action::ReviewProjection,
        transcript: &[PublicMessage],
        turn_id: &str,
        context_version: &str,
        cancel: CancellationToken,
    ) -> Result<ApprovalOutcome> {
        let Some(reviewer) = self.reviewer.as_ref() else {
            return Ok(ApprovalOutcome::Denied {
                reason: "no reviewer configured".to_owned(),
            });
        };

        if !reviewer.is_trusted() {
            return Ok(ApprovalOutcome::Denied {
                reason: "reviewer trust domain is not allowed".to_owned(),
            });
        }

        let request = ReviewRequest {
            mode: self.mode,
            projection: projection.clone(),
            transcript: transcript.to_vec(),
            trusted_environment: self.trusted_env.clone(),
            policy_hash: self.policy.read().unwrap_or_else(|e| e.into_inner()).hash(),
            context_version: context_version.to_owned(),
            turn_id: Some(turn_id.to_owned()),
        };

        match reviewer.review(request, cancel).await {
            ReviewOutcome::Allow(_) => Ok(ApprovalOutcome::Allowed),
            ReviewOutcome::Deny(decision) => {
                let reason = decision.rationale.clone();
                Ok(ApprovalOutcome::Denied { reason })
            }
        }
    }
}

fn user_decision_from_gateway(
    request_id: &str,
    decision: &GatewayApprovalDecision,
) -> Result<UserDecision> {
    match decision {
        GatewayApprovalDecision::ApproveOnce => Ok(UserDecision::ApproveOnce),
        GatewayApprovalDecision::Deny => Ok(UserDecision::Deny),
        GatewayApprovalDecision::ApproveAlways { rule } => {
            let value = serde_json::to_value(rule).context("serialize deferred rule")?;
            let mut rule: ApprovalRule =
                serde_json::from_value(value).context("parse deferred approval rule")?;
            if rule.id.is_empty() {
                rule.id = request_id.to_owned();
            }
            Ok(UserDecision::ApproveAlways { rule })
        }
    }
}

fn to_events_projection(
    projection: &crate::approval::action::ReviewProjection,
) -> Result<events::ReviewProjection> {
    match projection {
        crate::approval::action::ReviewProjection::Reviewable(action) => {
            let value = serde_json::to_value(action).context("serialize reviewable action")?;
            Ok(events::ReviewProjection::Reviewable(value))
        }
        crate::approval::action::ReviewProjection::InsufficientEvidence { reason } => {
            Ok(events::ReviewProjection::InsufficientEvidence {
                reason: reason.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        approval::action::SecretDigestKey, gateway::ApprovalDecision as GatewayApprovalDecision,
        store::Redactor,
    };

    fn projector() -> SecretAwareActionProjector {
        SecretAwareActionProjector::new(Redactor::v1(), SecretDigestKey::fixture())
    }

    fn read_file_call(path: &str) -> ToolCall {
        ToolCall {
            id: "call-1".to_owned(),
            name: "read_file".to_owned(),
            arguments: serde_json::from_value(json!({"path": path})).unwrap(),
        }
    }

    fn bash_call(command: &str) -> ToolCall {
        ToolCall {
            id: "call-2".to_owned(),
            name: "bash".to_owned(),
            arguments: serde_json::from_value(json!({"command": command})).unwrap(),
        }
    }

    fn broker() -> ApprovalBroker {
        ApprovalBroker::headless(Policy::new("/workspace"), projector())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn workspace_read_is_allowed() {
        let broker = broker();
        let outcome = broker
            .start_request(
                &read_file_call("notes.txt"),
                &[],
                "run-1",
                "turn-1",
                "v1",
                CancellationToken::new(),
            )
            .await
            .expect("start_request");
        assert!(matches!(outcome, ApprovalOutcome::Allowed));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn workspace_escape_is_denied() {
        let broker = broker();
        let outcome = broker
            .start_request(
                &serde_json::from_value::<ToolCall>(json!({
                    "id": "call-3",
                    "name": "write_file",
                    "arguments": {"path": "/etc/passwd", "content": "x"}
                }))
                .unwrap(),
                &[],
                "run-1",
                "turn-1",
                "v1",
                CancellationToken::new(),
            )
            .await
            .expect("start_request");
        assert!(matches!(outcome, ApprovalOutcome::Denied { .. }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bash_in_headless_user_mode_is_denied() {
        let broker = broker();
        let outcome = broker
            .start_request(
                &bash_call("git status"),
                &[],
                "run-1",
                "turn-1",
                "v1",
                CancellationToken::new(),
            )
            .await
            .expect("start_request");
        assert!(matches!(outcome, ApprovalOutcome::Denied { .. }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_approval_can_be_resolved_once() {
        let mut broker = broker();
        broker.headless = false;
        let outcome = broker
            .start_request(
                &bash_call("git status"),
                &[],
                "run-1",
                "turn-1",
                "v1",
                CancellationToken::new(),
            )
            .await
            .expect("start_request");

        let ApprovalOutcome::Pending {
            request,
            mut receiver,
        } = outcome
        else {
            panic!("expected pending approval");
        };

        let resolved = broker
            .resolve(&request.id, &GatewayApprovalDecision::ApproveOnce)
            .expect("resolve pending");
        assert!(matches!(resolved, ResolvedDecision::ApproveOnce));
        assert!(!broker.any_pending());

        let waiter = receiver.try_recv().expect("waiter receives");
        assert_eq!(
            waiter,
            WaiterResult::Resolved(ResolvedDecision::ApproveOnce)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn approve_always_updates_policy_and_subsequent_call_is_allowed() {
        let mut broker = broker();
        broker.headless = false;
        let call = bash_call("ls /workspace");
        let outcome = broker
            .start_request(
                &call,
                &[],
                "run-1",
                "turn-1",
                "v1",
                CancellationToken::new(),
            )
            .await
            .expect("start_request");
        let ApprovalOutcome::Pending { request, .. } = outcome else {
            panic!("expected pending approval, got {outcome:?}");
        };
        let rule = serde_json::from_value::<crate::gateway::DeferredApprovalRule>(json!({
            "id": "rule-1",
            "tool": "bash",
            "literal_prefix": ["ls", "/workspace"],
            "effect": "allow",
            "workspace_only": true,
            "allowed_permissions": ["exec"],
            "allowed_network_domains": []
        }))
        .expect("deferred rule");
        let resolved = broker
            .resolve(
                &request.id,
                &GatewayApprovalDecision::ApproveAlways { rule },
            )
            .expect("resolve pending");
        assert!(matches!(resolved, ResolvedDecision::ApproveAlways(_)));
        let outcome = broker
            .start_request(
                &call,
                &[],
                "run-1",
                "turn-2",
                "v2",
                CancellationToken::new(),
            )
            .await
            .expect("start_request");
        assert!(matches!(outcome, ApprovalOutcome::Allowed));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_unknown_request_is_noop() {
        let broker = broker();
        assert!(
            broker
                .resolve("no-such-id", &GatewayApprovalDecision::ApproveOnce)
                .is_none()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancel_all_pending_notifies_waiters() {
        let mut broker = broker();
        broker.headless = false;
        let outcome = broker
            .start_request(
                &bash_call("git status"),
                &[],
                "run-1",
                "turn-1",
                "v1",
                CancellationToken::new(),
            )
            .await
            .expect("start_request");

        let ApprovalOutcome::Pending {
            request,
            mut receiver,
        } = outcome
        else {
            panic!("expected pending");
        };

        let cancelled = broker.cancel_all();
        assert_eq!(cancelled.len(), 1);
        assert_eq!(cancelled[0].1, "call-2");

        let waiter = receiver.try_recv().expect("waiter cancelled");
        assert_eq!(waiter, WaiterResult::Cancelled);
        assert!(!broker.has_pending(&request.id));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn approve_once_does_not_change_policy() {
        let mut broker = broker();
        broker.headless = false;
        let outcome = broker
            .start_request(
                &bash_call("git status"),
                &[],
                "run-1",
                "turn-1",
                "v1",
                CancellationToken::new(),
            )
            .await
            .expect("start_request");

        let ApprovalOutcome::Pending { request, .. } = outcome else {
            panic!("expected pending");
        };

        let _ = broker.resolve(&request.id, &GatewayApprovalDecision::ApproveOnce);
        // A second identical bash call must still require approval.
        let outcome2 = broker
            .start_request(
                &bash_call("git status"),
                &[],
                "run-1",
                "turn-1",
                "v1",
                CancellationToken::new(),
            )
            .await
            .expect("start_request");
        assert!(matches!(outcome2, ApprovalOutcome::Pending { .. }));
    }
}
