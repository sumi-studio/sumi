//! Runtime approval broker.
//!
//! `ApprovalBroker` sits between the tool execution loop and the durable
//! `approval_log`. It projects each `ToolCall` to a redacted review shape,
//! evaluates the deterministic `Policy`, optionally calls the `Reviewer` for
//! `AutoReview`/`StrictAutoReview`, and coordinates user decisions through a
//! `oneshot` wait per pending request.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio::sync::{RwLock, RwLockReadGuard, oneshot};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    agent::events::{self, ApprovalRequest},
    approval::{
        action::{CanonicalAction, SandboxSummary, SecretAwareActionProjector},
        policy::{
            ApprovalRule, Policy, PolicyDecision, ResolvedDecision, RuleValidationError,
            UserDecision,
        },
        prompt::TrustedEnvironment,
        reviewer::{
            AuditDecision, ReviewOutcome, ReviewRequest, Reviewer, ReviewerMode, RiskLevel,
            UserAuthorization,
        },
    },
    gateway::ApprovalDecision as GatewayApprovalDecision,
    provider::types::{PublicMessage, ToolCall},
};

/// Result of asking the broker whether a tool may start.
#[derive(Debug)]
pub enum ApprovalOutcome {
    Allowed {
        grant: ExecutableGrant,
    },
    Denied {
        reason: String,
        audit: Option<AuditDecision>,
    },
    Pending {
        pending: PendingApproval,
    },
}

type ApprovalClock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// Opaque, single-call authority to attempt a durable tool start.
///
/// The durable bridge revalidates this value immediately before committing
/// `ToolExecutionStart`. Its fields stay private so callers cannot mint or
/// widen authority by editing policy identity, deadline, or call bindings.
#[derive(Clone)]
pub(crate) struct ExecutableGrant {
    policy: Arc<RwLock<Policy>>,
    clock: ApprovalClock,
    policy_hash: String,
    valid_until: Option<DateTime<Utc>>,
    tool_call_id: String,
    tool_name: String,
    arguments_hash: [u8; 32],
    run_id: String,
    turn_id: String,
}

impl std::fmt::Debug for ExecutableGrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutableGrant")
            .field("tool_call_id", &self.tool_call_id)
            .field("run_id", &self.run_id)
            .field("turn_id", &self.turn_id)
            .field("valid_until", &self.valid_until)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GrantRevalidation {
    Valid,
    Reauthorize,
}

impl ExecutableGrant {
    pub(crate) async fn authorize(
        &self,
        tool_call_id: &str,
        tool_name: &str,
        arguments: &Value,
        run_id: &str,
        turn_id: &str,
    ) -> Result<(GrantRevalidation, GrantLease<'_>)> {
        let arguments_hash: [u8; 32] =
            Sha256::digest(serde_json::to_vec(arguments).context("serialize tool arguments")?)
                .into();
        anyhow::ensure!(
            self.tool_call_id == tool_call_id
                && self.tool_name == tool_name
                && self.arguments_hash == arguments_hash
                && self.run_id == run_id
                && self.turn_id == turn_id,
            "executable grant does not match ToolExecutionStart"
        );

        let now = (self.clock)();
        let policy = self.policy.read().await;
        if self.valid_until.is_some_and(|deadline| now >= deadline)
            || policy.hash_at(now) != self.policy_hash
        {
            return Ok((GrantRevalidation::Reauthorize, GrantLease::empty()));
        }
        Ok((
            GrantRevalidation::Valid,
            GrantLease {
                _policy: Some(policy),
            },
        ))
    }

    #[cfg(test)]
    async fn revalidate(
        &self,
        tool_call_id: &str,
        tool_name: &str,
        arguments: &Value,
        run_id: &str,
        turn_id: &str,
    ) -> Result<GrantRevalidation> {
        Ok(self
            .authorize(tool_call_id, tool_name, arguments, run_id, turn_id)
            .await?
            .0)
    }
}

/// A read lease over the verified policy. Holding this lease through the
/// EventWriter transaction linearizes policy replacement with tool start: a
/// replacement waits for the durable start to commit before becoming visible.
pub(crate) struct GrantLease<'a> {
    _policy: Option<RwLockReadGuard<'a, Policy>>,
}

impl<'a> GrantLease<'a> {
    pub(crate) fn empty() -> Self {
        Self { _policy: None }
    }
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

/// Immutable snapshot of a broker entry needed to durably close a request that
/// is no longer attached to a live run.
#[derive(Clone, Debug)]
pub struct PendingSummary {
    pub tool_call_id: String,
    pub tool_name: String,
}

/// Supplies the current runtime-captured environment for audit review.
/// Implementations must return metadata already collected by the runtime; the
/// broker never invokes tools or shells out while evaluating an approval.
pub trait TrustedEnvironmentProvider: Send + Sync {
    fn current(&self) -> Result<TrustedEnvironment>;
}

#[derive(Clone)]
struct FixedTrustedEnvironment(TrustedEnvironment);

impl TrustedEnvironmentProvider for FixedTrustedEnvironment {
    fn current(&self) -> Result<TrustedEnvironment> {
        Ok(self.0.clone())
    }
}

struct ReviewPolicySnapshot {
    policy_hash: String,
    cache_expires_at: Option<chrono::DateTime<Utc>>,
    grant_expires_at: Option<chrono::DateTime<Utc>>,
    context_version: String,
    run_id: String,
    turn_id: String,
}

/// The worker-side ownership of an unresolved approval request.
///
/// A request is only live while this value is retained by the caller.  This
/// makes dropping a completed `start_request` future (for example when a
/// simultaneously-ready steer or abort wins the Runner select) fail closed:
/// the broker entry is removed rather than retaining a receiver with no
/// worker left to observe it.
pub struct PendingApproval {
    request: ApprovalRequest,
    receiver: oneshot::Receiver<WaiterResult>,
    pending: Arc<Mutex<HashMap<String, PendingEntry>>>,
}

impl PendingApproval {
    pub fn request(&self) -> &ApprovalRequest {
        &self.request
    }

    pub(crate) fn receiver_mut(&mut self) -> &mut oneshot::Receiver<WaiterResult> {
        &mut self.receiver
    }
}

impl std::fmt::Debug for PendingApproval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingApproval")
            .field("request_id", &self.request.id)
            .field("tool_call_id", &self.request.tool_call_id)
            .finish_non_exhaustive()
    }
}

impl Drop for PendingApproval {
    fn drop(&mut self) {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.request.id);
    }
}

/// Runtime approval broker. Clone is cheap: all mutable state lives behind
/// `Arc` handles so the `Session` and the worker can share the same broker.
#[derive(Clone)]
pub struct ApprovalBroker {
    policy: Arc<RwLock<Policy>>,
    clock: ApprovalClock,
    projector: Arc<SecretAwareActionProjector>,
    reviewer: Option<Arc<Reviewer>>,
    mode: ReviewerMode,
    headless: bool,
    trusted_environment: Arc<dyn TrustedEnvironmentProvider>,
    pending: Arc<Mutex<HashMap<String, PendingEntry>>>,
    resolving: Arc<Mutex<HashSet<String>>>,
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
        Self::new_with_environment_provider(
            policy,
            projector,
            reviewer,
            mode,
            headless,
            Arc::new(FixedTrustedEnvironment(trusted_env)),
        )
    }

    /// Construct a broker with an injected runtime environment seam. The
    /// provider is queried for every reviewer request so cwd/repository state
    /// changes cannot reuse a decision cached for an earlier environment.
    pub fn new_with_environment_provider(
        policy: Policy,
        projector: SecretAwareActionProjector,
        reviewer: Option<Arc<Reviewer>>,
        mode: ReviewerMode,
        headless: bool,
        trusted_environment: Arc<dyn TrustedEnvironmentProvider>,
    ) -> Self {
        Self {
            policy: Arc::new(RwLock::new(policy)),
            clock: Arc::new(Utc::now),
            projector: Arc::new(projector),
            reviewer,
            mode,
            headless,
            trusted_environment,
            pending: Arc::new(Mutex::new(HashMap::new())),
            resolving: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_clock(
        mut self,
        clock: impl Fn() -> DateTime<Utc> + Send + Sync + 'static,
    ) -> Self {
        self.clock = Arc::new(clock);
        self
    }

    /// Replace the live policy authority and invalidate reviewer allows.
    ///
    /// Existing executable grants observe the same `RwLock`, so the durable
    /// start boundary detects a replacement instead of trusting its snapshot.
    pub(crate) async fn replace_policy(&self, policy: Policy) -> Result<()> {
        // Hold the write lock through validation and installation so concurrent
        // replacements cannot both validate against the same stale authority
        // version and then install a rollback out of order.
        anyhow::ensure!(
            policy.authority_binding().is_some(),
            "replacement policy must carry verified approval authority"
        );
        let mut current = self.policy.write().await;
        anyhow::ensure!(
            policy.workspace_root() == current.workspace_root(),
            "replacement policy changed workspace root"
        );
        match (current.authority_binding(), policy.authority_binding()) {
            (
                Some((
                    current_tenant,
                    current_personality_agent_id,
                    current_version,
                    current_digest,
                )),
                Some((next_tenant, next_personality_agent_id, next_version, next_digest)),
            ) => {
                anyhow::ensure!(
                    current_tenant == next_tenant
                        && current_personality_agent_id == next_personality_agent_id,
                    "replacement policy changed approval authority scope"
                );
                anyhow::ensure!(
                    next_version > current_version
                        || (next_version == current_version && next_digest == current_digest),
                    "replacement policy is not a strictly newer or exact replayed authority bundle"
                );
            }
            (Some(_), None) => {
                anyhow::bail!("replacement policy dropped verified approval authority scope");
            }
            (None, Some(_)) => {}
            (None, None) => unreachable!("replacement authority checked above"),
        }
        *current = policy;
        if let Some(reviewer) = self.reviewer.as_ref() {
            reviewer.clear_allow_cache();
        }
        Ok(())
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
        // One immutable policy snapshot owns action construction, evaluation,
        // and reviewer cache identity for this request.
        let policy = self.policy.read().await.clone();
        let evaluated_at = (self.clock)();
        let action = match CanonicalAction::from_tool_call(
            policy.workspace_root().to_path_buf(),
            &tool_call.name,
            &tool_call.arguments,
        ) {
            Ok(a) => a,
            Err(e) => {
                return Ok(ApprovalOutcome::Denied {
                    reason: format!("invalid tool call: {e}"),
                    audit: None,
                });
            }
        };

        let projection = self.projector.project(&action);
        let decision = policy.evaluate_at(&action, evaluated_at);
        let verified_cache_expiry = policy.review_cache_expires_at(evaluated_at);
        let review_policy = ReviewPolicySnapshot {
            policy_hash: policy.hash_at(evaluated_at),
            // An unsigned or unavailable policy must never seed or hit the
            // shared reviewer allow cache. A past expiry is the existing
            // cache contract's fail-closed disable switch; grant expiry is
            // kept separate so an explicit reviewer decision remains a
            // one-shot authorization under the current policy.
            cache_expires_at: verified_cache_expiry
                .or_else(|| Some(evaluated_at - chrono::Duration::seconds(1))),
            grant_expires_at: verified_cache_expiry,
            context_version: context_version.to_owned(),
            run_id: run_id.to_owned(),
            turn_id: turn_id.to_owned(),
        };

        match decision {
            PolicyDecision::Forbidden { reason, .. } => Ok(ApprovalOutcome::Denied {
                reason,
                audit: None,
            }),
            PolicyDecision::Allow { .. } => {
                if self.mode == ReviewerMode::StrictAutoReview {
                    let outcome = self
                        .call_reviewer(tool_call, &projection, transcript, &review_policy, cancel)
                        .await?;
                    self.reviewer_fallback(
                        outcome,
                        tool_call,
                        &action,
                        &projection,
                        run_id,
                        turn_id,
                    )
                } else {
                    self.allow(tool_call, run_id, turn_id, &review_policy)
                }
            }
            PolicyDecision::NeedsApproval { reason, .. } => {
                if self.mode == ReviewerMode::User {
                    if self.headless {
                        return Ok(ApprovalOutcome::Denied {
                            reason: format!("{reason} (headless User mode)"),
                            audit: None,
                        });
                    }
                    self.make_pending(
                        tool_call,
                        &action,
                        &projection,
                        run_id,
                        turn_id,
                        &reason,
                        None,
                    )
                } else {
                    let outcome = self
                        .call_reviewer(tool_call, &projection, transcript, &review_policy, cancel)
                        .await?;
                    self.reviewer_fallback(
                        outcome,
                        tool_call,
                        &action,
                        &projection,
                        run_id,
                        turn_id,
                    )
                }
            }
        }
    }

    fn allow(
        &self,
        tool_call: &ToolCall,
        run_id: &str,
        turn_id: &str,
        policy: &ReviewPolicySnapshot,
    ) -> Result<ApprovalOutcome> {
        let arguments = Value::Object(tool_call.arguments.as_object().clone());
        let arguments_hash: [u8; 32] =
            Sha256::digest(serde_json::to_vec(&arguments).context("serialize tool arguments")?)
                .into();
        Ok(ApprovalOutcome::Allowed {
            grant: ExecutableGrant {
                policy: self.policy.clone(),
                clock: self.clock.clone(),
                policy_hash: policy.policy_hash.clone(),
                valid_until: policy.grant_expires_at,
                tool_call_id: tool_call.id.clone(),
                tool_name: tool_call.name.clone(),
                arguments_hash,
                run_id: run_id.to_owned(),
                turn_id: turn_id.to_owned(),
            },
        })
    }

    /// Convert an external `ApprovalDecision` into a `ResolvedDecision`, update
    /// the in-memory policy for safe `ApproveAlways` rules, and notify the
    /// waiting worker. Returns `None` when the `request_id` is not pending
    /// (terminal/unknown no-op).
    pub async fn resolve(
        &self,
        request_id: &str,
        decision: &GatewayApprovalDecision,
    ) -> Option<ResolvedDecision> {
        let entry = {
            let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            pending.remove(request_id)?
        };
        self.resolving
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(request_id.to_owned());

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
            let policy = self.policy.read().await.clone();
            policy.resolve(&entry.action, user_decision, &self.projector)
        };

        let mut resolved = resolved;
        if let ResolvedDecision::ApproveAlways(ref rule) = resolved {
            let guard = self.policy.read().await;
            if let Err(error) = guard.clone().try_with_rule(rule.clone()) {
                let literal_prefix =
                    matches!(&error, RuleValidationError::BroadPrefix).then(|| {
                        rule.literal_prefix
                            .iter()
                            .map(|token| self.projector.redact_text(token))
                            .collect::<Vec<_>>()
                    });
                tracing::warn!(
                    rule_id = %rule.id,
                    tool = %rule.tool,
                    literal_prefix = ?literal_prefix,
                    %error,
                    "downgrading invalid ApproveAlways candidate to ApproveOnce"
                );
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

    /// Publish a rule only after the approval resolution and matching tool
    /// start have committed atomically. Until this point the decision is staged
    /// and must not affect authorization of another call.
    pub(crate) fn commit_resolution(
        &self,
        request_id: &str,
        resolved: &ResolvedDecision,
    ) -> Result<()> {
        let ResolvedDecision::ApproveAlways(_rule) = resolved else {
            self.finish_resolution(request_id);
            return Ok(());
        };
        // The authenticated user choice authorizes this execution once and
        // remains a durable proposal. Persistent authority begins only after
        // the control plane publishes and the agent verifies a signed
        // replacement bundle.
        self.finish_resolution(request_id);
        Ok(())
    }

    /// True while an authenticated decision owns the request but its durable
    /// resolution boundary has not yet completed.
    pub fn is_resolving(&self, request_id: &str) -> bool {
        self.resolving
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(request_id)
    }

    /// Release resolving ownership only after the matching durable resolution
    /// (and, for approvals, ToolExecutionStart) has committed.
    pub fn finish_resolution(&self, request_id: &str) {
        self.resolving
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(request_id);
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

    /// Return a snapshot of a pending request, if any.
    pub fn pending_summary(&self, request_id: &str) -> Option<PendingSummary> {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(request_id)
            .map(|entry| PendingSummary {
                tool_call_id: entry.tool_call_id.clone(),
                tool_name: entry.action.tool.clone(),
            })
    }

    #[allow(clippy::too_many_arguments)]
    fn make_pending(
        &self,
        tool_call: &ToolCall,
        action: &CanonicalAction,
        projection: &crate::approval::action::ReviewProjection,
        run_id: &str,
        turn_id: &str,
        reason: &str,
        audit: Option<AuditDecision>,
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
            audit: audit.as_ref().map(to_events_audit),
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
            pending: PendingApproval {
                request,
                receiver: rx,
                pending: self.pending.clone(),
            },
        })
    }

    fn reviewer_fallback(
        &self,
        outcome: ApprovalOutcome,
        tool_call: &ToolCall,
        action: &CanonicalAction,
        projection: &crate::approval::action::ReviewProjection,
        run_id: &str,
        turn_id: &str,
    ) -> Result<ApprovalOutcome> {
        match outcome {
            ApprovalOutcome::Denied { reason, audit } if !self.headless => self.make_pending(
                tool_call, action, projection, run_id, turn_id, &reason, audit,
            ),
            other => Ok(other),
        }
    }
}

/// Check that a reviewer `Allow` response is executable under the canonical
/// risk/authorization contract. This is the single authoritative boundary;
/// both cached and fresh `ReviewOutcome::Allow` values must pass it before an
/// `ApprovalOutcome::Allowed` is ever produced.
fn reviewer_allow_is_executable(decision: &AuditDecision) -> Result<(), String> {
    let Some(required) = decision.risk.minimum_authorization() else {
        return Err("critical risk must deny".to_owned());
    };
    if decision.authorization.rank() < required.rank() {
        return Err(format!(
            "{} risk requires {} authorization",
            decision.risk.as_str(),
            required.as_str()
        ));
    }
    Ok(())
}

impl ApprovalBroker {
    async fn call_reviewer(
        &self,
        tool_call: &ToolCall,
        projection: &crate::approval::action::ReviewProjection,
        transcript: &[PublicMessage],
        policy: &ReviewPolicySnapshot,
        cancel: CancellationToken,
    ) -> Result<ApprovalOutcome> {
        let Some(reviewer) = self.reviewer.as_ref() else {
            return Ok(ApprovalOutcome::Denied {
                reason: "no reviewer configured".to_owned(),
                audit: Some(synthetic_audit("no reviewer configured")),
            });
        };

        if !reviewer.is_trusted() {
            return Ok(ApprovalOutcome::Denied {
                reason: "reviewer trust domain is not allowed".to_owned(),
                audit: Some(synthetic_audit("reviewer trust domain is not allowed")),
            });
        }

        let trusted_environment = match self.trusted_environment.current() {
            Ok(environment) => environment,
            Err(_) => {
                return Ok(ApprovalOutcome::Denied {
                    reason: "trusted reviewer environment is unavailable".to_owned(),
                    audit: Some(synthetic_audit(
                        "trusted reviewer environment is unavailable",
                    )),
                });
            }
        };
        let environment_version =
            match trusted_environment_version(&policy.context_version, &trusted_environment) {
                Ok(version) => version,
                Err(_) => {
                    return Ok(ApprovalOutcome::Denied {
                        reason: "trusted reviewer environment cannot be versioned".to_owned(),
                        audit: Some(synthetic_audit(
                            "trusted reviewer environment cannot be versioned",
                        )),
                    });
                }
            };
        let request = ReviewRequest {
            mode: self.mode,
            projection: projection.clone(),
            transcript: transcript.to_vec(),
            trusted_environment,
            policy_hash: policy.policy_hash.clone(),
            policy_cache_expires_at: policy.cache_expires_at,
            context_version: environment_version,
            run_id: policy.run_id.clone(),
            turn_id: Some(policy.turn_id.clone()),
        };

        match reviewer.review(request, cancel).await {
            ReviewOutcome::Allow(decision) => match reviewer_allow_is_executable(&decision) {
                Ok(()) => self.allow(tool_call, &policy.run_id, &policy.turn_id, policy),
                Err(constraint) => Ok(ApprovalOutcome::Denied {
                    reason: format!("{constraint}: {}", decision.rationale),
                    audit: Some(decision),
                }),
            },
            ReviewOutcome::Deny(decision) => {
                let reason = decision.rationale.clone();
                Ok(ApprovalOutcome::Denied {
                    reason,
                    audit: Some(decision),
                })
            }
        }
    }
}

fn trusted_environment_version(
    context_version: &str,
    environment: &TrustedEnvironment,
) -> Result<String> {
    let environment = serde_json::to_vec(environment)
        .context("serialize trusted environment for reviewer cache version")?;
    Ok(format!(
        "{context_version}:environment:{:x}",
        Sha256::digest(environment)
    ))
}

fn synthetic_audit(reason: impl Into<String>) -> AuditDecision {
    AuditDecision {
        outcome: crate::approval::reviewer::AuditOutcome::Deny,
        risk: RiskLevel::High,
        authorization: UserAuthorization::Unknown,
        rationale: reason.into(),
    }
}

fn to_events_audit(decision: &AuditDecision) -> events::AuditDecision {
    events::AuditDecision {
        outcome: match decision.outcome {
            crate::approval::reviewer::AuditOutcome::Allow => events::AuditOutcome::Allow,
            crate::approval::reviewer::AuditOutcome::Deny => events::AuditOutcome::Deny,
        },
        risk: match decision.risk {
            RiskLevel::Low => events::RiskLevel::Low,
            RiskLevel::Medium => events::RiskLevel::Medium,
            RiskLevel::High => events::RiskLevel::High,
            RiskLevel::Critical => events::RiskLevel::Critical,
        },
        authorization: match decision.authorization {
            UserAuthorization::Unknown => events::UserAuthorization::Unknown,
            UserAuthorization::Low => events::UserAuthorization::Low,
            UserAuthorization::Medium => events::UserAuthorization::Medium,
            UserAuthorization::High => events::UserAuthorization::High,
        },
        rationale: decision.rationale.clone(),
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
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        approval::{
            action::{
                Permission, ReviewPath, ReviewPathComponent, ReviewProjection, ReviewToken,
                ReviewableAction, SandboxSummary, SecretDigestKey,
            },
            policy::{APPROVAL_POLICY_BUNDLE_SCHEMA_VERSION, ApprovalPolicyBundle},
            prompt::{ReviewerPrompt, TrustedEnvironment},
            reviewer::{
                ReviewRequest, Reviewer, ReviewerMode, ReviewerModelSpec, ReviewerTransport,
                ReviewerTransportError, ReviewerTrustSet,
            },
        },
        gateway::ApprovalDecision as GatewayApprovalDecision,
        runtime::contracts::PersonalityAgentId,
        store::Redactor,
    };

    fn projector() -> SecretAwareActionProjector {
        SecretAwareActionProjector::new(Redactor::v1(), SecretDigestKey::fixture())
    }

    fn personality_agent_id(value: &str) -> PersonalityAgentId {
        PersonalityAgentId::parse(value)
            .expect("test personality_agent_id must be a canonical UUIDv7")
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

    fn trusted_env() -> TrustedEnvironment {
        TrustedEnvironment {
            workspace_root: "/workspace".to_owned(),
            sandbox: SandboxSummary::workspace(),
            denied_paths: Vec::new(),
            denied_network_domains: Vec::new(),
            repo_visibility: None,
            git_status: None,
        }
    }

    #[derive(Clone)]
    struct StaticTransport {
        response: String,
    }

    #[async_trait]
    impl ReviewerTransport for StaticTransport {
        async fn complete(
            &self,
            _prompt: &ReviewerPrompt,
            _cancel: CancellationToken,
        ) -> Result<String, ReviewerTransportError> {
            Ok(self.response.clone())
        }
    }

    #[derive(Clone)]
    struct CountingTransport {
        response: String,
        calls: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct CapturingTransport {
        response: String,
        calls: Arc<AtomicUsize>,
        prompts: Arc<Mutex<Vec<ReviewerPrompt>>>,
    }

    #[async_trait]
    impl ReviewerTransport for CapturingTransport {
        async fn complete(
            &self,
            prompt: &ReviewerPrompt,
            _cancel: CancellationToken,
        ) -> Result<String, ReviewerTransportError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.prompts
                .lock()
                .expect("captured reviewer prompts")
                .push(prompt.clone());
            Ok(self.response.clone())
        }
    }

    struct MutableEnvironmentProvider {
        environment: Arc<Mutex<TrustedEnvironment>>,
    }

    impl TrustedEnvironmentProvider for MutableEnvironmentProvider {
        fn current(&self) -> Result<TrustedEnvironment> {
            Ok(self
                .environment
                .lock()
                .expect("mutable trusted environment")
                .clone())
        }
    }

    struct FailingEnvironmentProvider;

    impl TrustedEnvironmentProvider for FailingEnvironmentProvider {
        fn current(&self) -> Result<TrustedEnvironment> {
            Err(anyhow::anyhow!("fixture environment capture failure"))
        }
    }

    #[async_trait]
    impl ReviewerTransport for CountingTransport {
        async fn complete(
            &self,
            _prompt: &ReviewerPrompt,
            _cancel: CancellationToken,
        ) -> Result<String, ReviewerTransportError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.response.clone())
        }
    }

    fn allow_json() -> String {
        r#"{"outcome":"allow","risk":"low","authorization":"low","rationale":"ok"}"#.to_owned()
    }

    fn reviewer_broker(response: &str, mode: ReviewerMode) -> ApprovalBroker {
        let model = ReviewerModelSpec::new(
            "reviewer-model",
            "reviewer-provider",
            "https://reviewer.example.test/v1",
            "default",
            "reviewer-domain",
            "tenant-policy",
        );
        let trust = ReviewerTrustSet::new(model.clone(), Vec::new());
        let transport = Arc::new(StaticTransport {
            response: response.to_owned(),
        });
        let reviewer = Arc::new(Reviewer::new(
            model,
            trust,
            transport,
            Arc::new(projector()),
        ));
        ApprovalBroker::new(
            Policy::new("/workspace"),
            projector(),
            Some(reviewer),
            mode,
            false,
            trusted_env(),
        )
    }

    fn broker_with_counting_reviewer() -> (ApprovalBroker, Arc<Reviewer>, Arc<AtomicUsize>) {
        let model = ReviewerModelSpec::new(
            "reviewer-model",
            "reviewer-provider",
            "https://reviewer.example.test/v1",
            "default",
            "reviewer-domain",
            "tenant-policy",
        );
        let trust = ReviewerTrustSet::new(model.clone(), Vec::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let transport = Arc::new(CountingTransport {
            response: allow_json(),
            calls: calls.clone(),
        });
        let reviewer = Arc::new(Reviewer::new(
            model,
            trust,
            transport,
            Arc::new(projector()),
        ));
        let broker = ApprovalBroker::new(
            Policy::new("/workspace"),
            projector(),
            Some(reviewer.clone()),
            ReviewerMode::User,
            false,
            trusted_env(),
        );
        (broker, reviewer, calls)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unsigned_authority_never_seeds_or_hits_reviewer_allow_cache() {
        let (mut broker, reviewer, calls) = broker_with_counting_reviewer();
        broker.mode = ReviewerMode::AutoReview;
        let call = bash_call("git status");

        for turn_id in ["turn-1", "turn-2"] {
            assert!(matches!(
                broker
                    .start_request(
                        &call,
                        &[],
                        "run-1",
                        turn_id,
                        "context-1",
                        CancellationToken::new(),
                    )
                    .await
                    .expect("review without verified authority"),
                ApprovalOutcome::Allowed { .. }
            ));
        }

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            reviewer.allow_cache_entry_count(),
            0,
            "unsigned authority must not seed a shared allow cache"
        );
    }

    fn git_status_review_request(policy_hash: &str, context_version: &str) -> ReviewRequest {
        let projection = ReviewProjection::Reviewable(ReviewableAction {
            tool: "bash".to_owned(),
            operation: "exec".to_owned(),
            argv: vec![
                ReviewToken::Literal {
                    text: "git".to_owned(),
                },
                ReviewToken::Literal {
                    text: "status".to_owned(),
                },
            ],
            cwd: ReviewPath(vec![ReviewPathComponent::Literal {
                text: "/workspace".to_owned(),
            }]),
            affected_paths: Vec::new(),
            sandbox: SandboxSummary::workspace(),
            requested_permissions: vec![Permission::Exec],
            justification: None,
        });
        ReviewRequest {
            mode: ReviewerMode::AutoReview,
            projection,
            transcript: Vec::new(),
            trusted_environment: trusted_env(),
            policy_hash: policy_hash.to_owned(),
            policy_cache_expires_at: None,
            context_version: context_version.to_owned(),
            run_id: "run-1".to_owned(),
            turn_id: Some("turn-1".to_owned()),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn repo_state_change_rebuilds_prompt_and_invalidates_allow_cache() {
        let model = ReviewerModelSpec::new(
            "reviewer-model",
            "reviewer-provider",
            "https://reviewer.example.test/v1",
            "default",
            "reviewer-domain",
            "tenant-policy",
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let prompts = Arc::new(Mutex::new(Vec::new()));
        let trust = ReviewerTrustSet::new(model.clone(), Vec::new());
        let reviewer = Arc::new(Reviewer::new(
            model,
            trust,
            Arc::new(CapturingTransport {
                response: allow_json(),
                calls: calls.clone(),
                prompts: prompts.clone(),
            }),
            Arc::new(projector()),
        ));
        let environment = Arc::new(Mutex::new(trusted_env()));
        let broker = ApprovalBroker::new_with_environment_provider(
            Policy::new("/workspace"),
            projector(),
            Some(reviewer),
            ReviewerMode::AutoReview,
            false,
            Arc::new(MutableEnvironmentProvider {
                environment: environment.clone(),
            }),
        );
        let call = bash_call("git status");

        assert!(matches!(
            broker
                .start_request(
                    &call,
                    &[],
                    "run-1",
                    "turn-1",
                    "context-1",
                    CancellationToken::new(),
                )
                .await
                .expect("first review"),
            ApprovalOutcome::Allowed { .. }
        ));
        environment
            .lock()
            .expect("mutable trusted environment")
            .git_status = Some("M src/critical.rs".to_owned());
        assert!(matches!(
            broker
                .start_request(
                    &call,
                    &[],
                    "run-1",
                    "turn-2",
                    "context-1",
                    CancellationToken::new(),
                )
                .await
                .expect("review after repo change"),
            ApprovalOutcome::Allowed { .. }
        ));

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "repo change must miss cache"
        );
        let prompts = prompts.lock().expect("captured reviewer prompts");
        assert_eq!(prompts.len(), 2);
        assert!(
            !prompts[0]
                .messages
                .iter()
                .any(|message| message.content.contains("M src/critical.rs")),
            "first prompt must use the initial environment"
        );
        assert!(
            prompts[1]
                .messages
                .iter()
                .any(|message| message.content.contains("M src/critical.rs")),
            "cache version and prompt must use the same updated environment"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn environment_capture_failure_falls_back_without_reviewer_or_cache_reuse() {
        let model = ReviewerModelSpec::new(
            "reviewer-model",
            "reviewer-provider",
            "https://reviewer.example.test/v1",
            "default",
            "reviewer-domain",
            "tenant-policy",
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let trust = ReviewerTrustSet::new(model.clone(), Vec::new());
        let reviewer = Arc::new(Reviewer::new(
            model,
            trust,
            Arc::new(CountingTransport {
                response: allow_json(),
                calls: calls.clone(),
            }),
            Arc::new(projector()),
        ));
        let interactive = ApprovalBroker::new_with_environment_provider(
            Policy::new("/workspace"),
            projector(),
            Some(reviewer.clone()),
            ReviewerMode::AutoReview,
            false,
            Arc::new(FailingEnvironmentProvider),
        );

        let outcome = interactive
            .start_request(
                &bash_call("git status"),
                &[],
                "run-1",
                "turn-1",
                "context-1",
                CancellationToken::new(),
            )
            .await
            .expect("environment failure falls back to manual approval");
        let ApprovalOutcome::Pending { pending } = outcome else {
            panic!("interactive environment failure must request manual approval");
        };
        assert_eq!(
            pending.request().audit.as_ref().map(|audit| audit.risk),
            Some(events::RiskLevel::High)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(reviewer.allow_cache_entry_count(), 0);

        let headless = ApprovalBroker::new_with_environment_provider(
            Policy::new("/workspace"),
            projector(),
            Some(reviewer.clone()),
            ReviewerMode::AutoReview,
            true,
            Arc::new(FailingEnvironmentProvider),
        );
        assert!(matches!(
            headless
                .start_request(
                    &bash_call("git status"),
                    &[],
                    "run-2",
                    "turn-1",
                    "context-1",
                    CancellationToken::new(),
                )
                .await
                .expect("headless environment failure blocks"),
            ApprovalOutcome::Denied { audit: Some(audit), .. }
                if audit.risk == RiskLevel::High
                    && audit.authorization == UserAuthorization::Unknown
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(reviewer.allow_cache_entry_count(), 0);
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
        assert!(matches!(outcome, ApprovalOutcome::Allowed { .. }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn executable_grant_observes_live_policy_replacement() {
        let now = Utc::now();
        let authority = |version: u64, rules: Vec<ApprovalRule>| ApprovalPolicyBundle {
            schema_version: APPROVAL_POLICY_BUNDLE_SCHEMA_VERSION,
            tenant_id: "tenant-1".to_owned(),
            personality_agent_id: personality_agent_id("018f8a9e-65c0-7a5b-8d3c-1f2a3b4c5d6e"),
            version,
            issued_at: now - chrono::Duration::minutes(1),
            expires_at: now + chrono::Duration::hours(1),
            rules,
        };
        let broker = ApprovalBroker::headless(
            Policy::from_verified_bundle("/workspace", &authority(1, Vec::new()))
                .expect("initial verified policy"),
            projector(),
        );
        let call = read_file_call("notes.txt");
        let ApprovalOutcome::Allowed { grant } = broker
            .start_request(
                &call,
                &[],
                "run-1",
                "turn-1",
                "v1",
                CancellationToken::new(),
            )
            .await
            .expect("initial allow")
        else {
            panic!("workspace read should initially allow");
        };
        let replacement = Policy::from_verified_bundle(
            "/workspace",
            &authority(
                2,
                vec![ApprovalRule {
                    id: "replacement-identity".to_owned(),
                    tool: "bash".to_owned(),
                    literal_prefix: vec!["git".to_owned(), "status".to_owned()],
                    effect: crate::approval::RuleEffect::NeedsApproval,
                    workspace_only: true,
                    allowed_permissions: vec![Permission::Exec],
                    allowed_network_domains: Vec::new(),
                }],
            ),
        )
        .expect("valid replacement policy");
        broker
            .replace_policy(replacement)
            .await
            .expect("replace live policy");

        assert_eq!(
            grant
                .revalidate(
                    &call.id,
                    &call.name,
                    &Value::Object(call.arguments.as_object().clone()),
                    "run-1",
                    "turn-1",
                )
                .await
                .expect("grant revalidation"),
            GrantRevalidation::Reauthorize,
            "a live policy replacement must invalidate an already-issued grant"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn policy_replacement_waits_for_grant_start_authorization_lease() {
        let now = Utc::now();
        let bundle = |version: u64| ApprovalPolicyBundle {
            schema_version: APPROVAL_POLICY_BUNDLE_SCHEMA_VERSION,
            tenant_id: "tenant-1".to_owned(),
            personality_agent_id: personality_agent_id("018f8a9e-65c0-7a5b-8d3c-1f2a3b4c5d6e"),
            version,
            issued_at: now - chrono::Duration::minutes(1),
            expires_at: now + chrono::Duration::hours(1),
            rules: Vec::new(),
        };
        let broker = ApprovalBroker::headless(
            Policy::from_verified_bundle("/workspace", &bundle(1))
                .expect("initial verified policy"),
            projector(),
        );
        let call = read_file_call("notes.txt");
        let ApprovalOutcome::Allowed { grant } = broker
            .start_request(
                &call,
                &[],
                "run-1",
                "turn-1",
                "v1",
                CancellationToken::new(),
            )
            .await
            .expect("initial allow")
        else {
            panic!("workspace read should initially allow");
        };
        let (status, lease) = grant
            .authorize(
                &call.id,
                &call.name,
                &Value::Object(call.arguments.as_object().clone()),
                "run-1",
                "turn-1",
            )
            .await
            .expect("start authorization");
        assert_eq!(status, GrantRevalidation::Valid);

        let replacement = Policy::from_verified_bundle("/workspace", &bundle(2))
            .expect("replacement verified policy");
        let replacement_task = tokio::spawn({
            let broker = broker.clone();
            async move { broker.replace_policy(replacement).await }
        });
        tokio::task::yield_now().await;
        assert!(
            !replacement_task.is_finished(),
            "policy replacement must wait while a tool-start authorization lease is held"
        );

        drop(lease);
        replacement_task
            .await
            .expect("replacement task join")
            .expect("replacement after durable authorization");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replacement_rejects_a_different_verified_authority_provenance_or_owner() {
        let now = Utc::now();
        let bundle = |tenant_id: &str, personality_agent_id: PersonalityAgentId, version: u64| {
            ApprovalPolicyBundle {
                schema_version: APPROVAL_POLICY_BUNDLE_SCHEMA_VERSION,
                tenant_id: tenant_id.to_owned(),
                personality_agent_id,
                version,
                issued_at: now - chrono::Duration::minutes(1),
                expires_at: now + chrono::Duration::hours(1),
                rules: Vec::new(),
            }
        };
        let paid_a = personality_agent_id("018f8a9e-65c0-7a5b-8d3c-1f2a3b4c5d6e");
        let paid_b = personality_agent_id("018f8a9e-65c1-7b6c-9e4d-2a3b4c5d6e7f");
        let current =
            Policy::from_verified_bundle("/workspace", &bundle("tenant-a", paid_a.clone(), 1))
                .expect("current verified policy");
        let same_scope =
            Policy::from_verified_bundle("/workspace", &bundle("tenant-a", paid_a.clone(), 2))
                .expect("same-provenance and same-owner replacement policy");
        let tenant_replacement =
            Policy::from_verified_bundle("/workspace", &bundle("tenant-b", paid_a.clone(), 3))
                .expect("replacement with different event-time tenant provenance");
        let owner_replacement =
            Policy::from_verified_bundle("/workspace", &bundle("tenant-a", paid_b, 3))
                .expect("replacement with different PAID owner");
        let broker = ApprovalBroker::headless(current, projector());

        let unsigned = Policy::new("/workspace");
        let unsigned_error = broker
            .replace_policy(unsigned)
            .await
            .expect_err("unsigned replacement must fail closed");
        assert!(
            unsigned_error
                .to_string()
                .contains("verified approval authority")
        );

        broker
            .replace_policy(same_scope)
            .await
            .expect("same-scope policy replacement must remain allowed");

        let rollback =
            Policy::from_verified_bundle("/workspace", &bundle("tenant-a", paid_a.clone(), 1))
                .expect("rollback policy");
        let rollback_error = broker
            .replace_policy(rollback)
            .await
            .expect_err("authority bundle rollback must fail closed");
        assert!(rollback_error.to_string().contains("strictly newer"));

        let mut conflicting_bundle = bundle("tenant-a", paid_a, 2);
        conflicting_bundle.rules.push(ApprovalRule {
            id: "conflict".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["git".to_owned(), "status".to_owned()],
            effect: crate::approval::RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: Vec::new(),
        });
        let conflicting = Policy::from_verified_bundle("/workspace", &conflicting_bundle)
            .expect("same-version conflicting policy");
        let conflict_error = broker
            .replace_policy(conflicting)
            .await
            .expect_err("same-version conflicting authority bundle must fail closed");
        assert!(conflict_error.to_string().contains("strictly newer"));

        let tenant_error = broker
            .replace_policy(tenant_replacement)
            .await
            .expect_err("cross-tenant approval provenance replacement must fail closed");
        assert!(tenant_error.to_string().contains("authority scope"));

        let owner_error = broker
            .replace_policy(owner_replacement)
            .await
            .expect_err("cross-PAID owner replacement must fail closed");
        assert!(owner_error.to_string().contains("authority scope"));
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

        let ApprovalOutcome::Pending { mut pending } = outcome else {
            panic!("expected pending approval");
        };
        let request = pending.request().clone();

        let resolved = broker
            .resolve(&request.id, &GatewayApprovalDecision::ApproveOnce)
            .await
            .expect("resolve pending");
        assert!(matches!(resolved, ResolvedDecision::ApproveOnce));
        assert!(!broker.any_pending());

        let waiter = pending.receiver_mut().try_recv().expect("waiter receives");
        assert_eq!(
            waiter,
            WaiterResult::Resolved(ResolvedDecision::ApproveOnce)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_unconsumed_pending_outcome_removes_broker_waiter() {
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
        let request_id = match &outcome {
            ApprovalOutcome::Pending { pending } => pending.request().id.clone(),
            other => panic!("expected pending approval, got {other:?}"),
        };
        assert!(broker.has_pending(&request_id));

        // `Runner::evaluate_call` can receive a soft-steer or Abort in the
        // same poll in which `start_request` completes. Dropping this outcome
        // must therefore clean the broker entry instead of leaving a waiter
        // that no worker will ever consume.
        drop(outcome);
        assert!(
            !broker.has_pending(&request_id),
            "discarded pending outcome must not leak a broker waiter"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn approve_always_proposal_does_not_activate_without_signed_bundle() {
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
        let ApprovalOutcome::Pending { pending } = outcome else {
            panic!("expected pending approval, got {outcome:?}");
        };
        let request = pending.request();
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
            .await
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
        assert!(matches!(outcome, ApprovalOutcome::Pending { .. }));

        broker
            .commit_resolution(&request.id, &resolved)
            .expect("activate durably committed rule");
        let outcome = broker
            .start_request(
                &call,
                &[],
                "run-1",
                "turn-3",
                "v3",
                CancellationToken::new(),
            )
            .await
            .expect("start_request");
        assert!(matches!(outcome, ApprovalOutcome::Pending { .. }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_unknown_request_is_noop() {
        let broker = broker();
        assert!(
            broker
                .resolve("no-such-id", &GatewayApprovalDecision::ApproveOnce)
                .await
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

        let ApprovalOutcome::Pending { mut pending } = outcome else {
            panic!("expected pending");
        };
        let request = pending.request().clone();

        let cancelled = broker.cancel_all();
        assert_eq!(cancelled.len(), 1);
        assert_eq!(cancelled[0].1, "call-2");

        let waiter = pending.receiver_mut().try_recv().expect("waiter cancelled");
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

        let ApprovalOutcome::Pending { pending } = outcome else {
            panic!("expected pending");
        };
        let request = pending.request();

        let _ = broker
            .resolve(&request.id, &GatewayApprovalDecision::ApproveOnce)
            .await;
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

    #[tokio::test(flavor = "current_thread")]
    async fn approve_always_with_duplicate_rule_id_downgrades_to_once() {
        // Seed a policy that already contains a rule with id "rule-git-status".
        let base_rule = ApprovalRule {
            id: "rule-git-status".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["git".to_owned(), "status".to_owned()],
            effect: crate::approval::policy::RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![crate::approval::action::Permission::Exec],
            allowed_network_domains: vec![],
        };
        let policy = Policy::new("/workspace")
            .try_with_rule(base_rule)
            .expect("valid base rule");
        let mut broker = ApprovalBroker::headless(policy, projector());
        broker.headless = false;

        // A different action (git log) still requires user approval. The gateway
        // could return an ApproveAlways decision whose rule id collides with the
        // existing rule; the broker must downgrade to ApproveOnce.
        let call = bash_call("git log");
        let first = broker
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
        let ApprovalOutcome::Pending {
            pending: first_pending,
        } = first
        else {
            panic!("expected pending");
        };
        let first_rule = serde_json::from_value::<crate::gateway::DeferredApprovalRule>(json!({
            "id": "rule-git-status",
            "tool": "bash",
            "literal_prefix": ["git", "log"],
            "effect": "allow",
            "workspace_only": true,
            "allowed_permissions": ["exec"],
            "allowed_network_domains": []
        }))
        .expect("deferred rule");
        let first_resolved = broker
            .resolve(
                &first_pending.request().id,
                &GatewayApprovalDecision::ApproveAlways { rule: first_rule },
            )
            .await
            .expect("resolve pending");
        assert!(
            matches!(first_resolved, ResolvedDecision::ApproveOnce),
            "duplicate rule id must downgrade to ApproveOnce"
        );
        // The policy still contains only one rule.
        assert_eq!(broker.policy.read().await.rules().len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unsigned_approve_always_proposal_does_not_clear_allow_cache() {
        let (mut broker, reviewer, calls) = broker_with_counting_reviewer();
        broker.headless = false;

        let call = bash_call("git status");
        // Prime the reviewer allow cache with a decision for the current policy.
        let policy_hash = {
            let policy = broker.policy.read().await.clone();
            policy.hash()
        };
        let cache_request = git_status_review_request(&policy_hash, "v1");
        let outcome = reviewer
            .review(cache_request.clone(), CancellationToken::new())
            .await;
        assert!(matches!(outcome, ReviewOutcome::Allow(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(reviewer.allow_cache_entry_count(), 1);

        // Now start a pending request in User mode and resolve it ApproveAlways.
        let pending = broker
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
        let ApprovalOutcome::Pending {
            pending: pending_entry,
        } = pending
        else {
            panic!("expected pending");
        };
        let request = pending_entry.request().clone();
        let rule = serde_json::from_value::<crate::gateway::DeferredApprovalRule>(json!({
            "id": "rule-git-status",
            "tool": "bash",
            "literal_prefix": ["git", "status"],
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
            .await
            .expect("resolve pending");
        assert!(matches!(resolved, ResolvedDecision::ApproveAlways(_)));
        broker
            .commit_resolution(&request.id, &resolved)
            .expect("activate durable rule");

        assert_eq!(reviewer.allow_cache_entry_count(), 1);

        // No signed policy mutation occurred, so the cached decision remains.
        let re_review = reviewer
            .review(cache_request, CancellationToken::new())
            .await;
        assert!(matches!(re_review, ReviewOutcome::Allow(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn critical_allow_with_unknown_auth_is_denied() {
        let broker = reviewer_broker(
            r#"{"outcome":"allow","risk":"critical","authorization":"unknown","rationale":"user allowed"}"#,
            ReviewerMode::AutoReview,
        );
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
        let ApprovalOutcome::Pending { pending } = outcome else {
            panic!("critical reviewer rejection must fall back to pending");
        };
        assert!(
            pending
                .request()
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("critical risk"))
                && pending.request().audit.is_some()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn high_allow_with_unknown_auth_is_denied() {
        let broker = reviewer_broker(
            r#"{"outcome":"allow","risk":"high","authorization":"unknown","rationale":"user allowed"}"#,
            ReviewerMode::AutoReview,
        );
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
        let ApprovalOutcome::Pending { pending } = outcome else {
            panic!("high reviewer rejection must fall back to pending");
        };
        assert!(
            pending
                .request()
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("high authorization"))
                && pending.request().audit.is_some()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn high_allow_with_high_auth_is_allowed() {
        let broker = reviewer_broker(
            r#"{"outcome":"allow","risk":"high","authorization":"high","rationale":"explicit"}"#,
            ReviewerMode::AutoReview,
        );
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
        assert!(matches!(outcome, ApprovalOutcome::Allowed { .. }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn autoreview_deny_falls_back_to_pending_only_when_interactive() {
        let broker = reviewer_broker(
            r#"{"outcome":"deny","risk":"high","authorization":"unknown","rationale":"manual review required"}"#,
            ReviewerMode::AutoReview,
        );
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
            .expect("interactive review");
        let ApprovalOutcome::Pending { pending } = outcome else {
            panic!("interactive reviewer denial must fall back to pending");
        };
        assert!(
            pending.request().audit.is_some()
                && pending.request().reason.as_deref() == Some("manual review required")
        );

        let mut headless = reviewer_broker(
            r#"{"outcome":"deny","risk":"high","authorization":"unknown","rationale":"manual review required"}"#,
            ReviewerMode::AutoReview,
        );
        headless.headless = true;
        let outcome = headless
            .start_request(
                &bash_call("git status"),
                &[],
                "run-2",
                "turn-1",
                "v1",
                CancellationToken::new(),
            )
            .await
            .expect("headless review");
        assert!(matches!(
            outcome,
            ApprovalOutcome::Denied { reason, .. } if reason == "manual review required"
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn low_allow_with_low_auth_remains_allowed() {
        let broker = reviewer_broker(
            r#"{"outcome":"allow","risk":"low","authorization":"low","rationale":"harmless"}"#,
            ReviewerMode::StrictAutoReview,
        );
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
        assert!(matches!(outcome, ApprovalOutcome::Allowed { .. }));
    }

    fn test_decision(risk: RiskLevel, authorization: UserAuthorization) -> AuditDecision {
        AuditDecision {
            outcome: crate::approval::reviewer::AuditOutcome::Allow,
            risk,
            authorization,
            rationale: "test".to_owned(),
        }
    }

    #[test]
    fn reviewer_allow_is_executable_enforces_risk_authorization_contract() {
        assert!(
            reviewer_allow_is_executable(&test_decision(RiskLevel::Low, UserAuthorization::Low))
                .is_ok()
        );
        assert!(
            reviewer_allow_is_executable(&test_decision(
                RiskLevel::Low,
                UserAuthorization::Unknown
            ))
            .is_err()
        );
        assert!(
            reviewer_allow_is_executable(&test_decision(RiskLevel::Medium, UserAuthorization::Low))
                .is_err()
        );
        assert!(
            reviewer_allow_is_executable(&test_decision(
                RiskLevel::Medium,
                UserAuthorization::High
            ))
            .is_ok()
        );
        assert!(
            reviewer_allow_is_executable(&test_decision(RiskLevel::High, UserAuthorization::High))
                .is_ok()
        );
        assert!(
            reviewer_allow_is_executable(&test_decision(
                RiskLevel::High,
                UserAuthorization::Medium
            ))
            .is_err()
        );
        assert!(
            reviewer_allow_is_executable(&test_decision(
                RiskLevel::Critical,
                UserAuthorization::High
            ))
            .is_err()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn insufficient_evidence_falls_back_to_redacted_interactive_pending() {
        let mut broker = broker();
        broker.headless = false;

        let secret = "abcdef1234567890";
        let outcome = broker
            .start_request(
                &bash_call(&format!("curl \"https://$HOST/path?token={secret}\"")),
                &[],
                "run-1",
                "turn-1",
                "v1",
                CancellationToken::new(),
            )
            .await
            .expect("start_request");
        let ApprovalOutcome::Pending { pending } = outcome else {
            panic!("interactive insufficient evidence must become pending");
        };
        assert!(matches!(
            pending.request().action,
            events::ReviewProjection::InsufficientEvidence { .. }
        ));
        let public = serde_json::to_string(pending.request()).expect("serialize request");
        assert!(
            !public.contains(secret),
            "secret leaked in approval request"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn insufficient_evidence_remains_denied_headless() {
        let broker = broker();
        let outcome = broker
            .start_request(
                &bash_call("echo $TOKEN"),
                &[],
                "run-1",
                "turn-1",
                "v1",
                CancellationToken::new(),
            )
            .await
            .expect("start_request");
        assert!(matches!(outcome, ApprovalOutcome::Denied { .. }));
        assert!(!broker.any_pending());
    }
}
