//! Route-aware authorization over one already bound app operation.
//!
//! This broker deliberately receives no raw provider proposal and owns no app
//! action vocabulary. The exact app adapter has already resolved the proposal
//! to a sealed [`BoundToolInvocation`]. Policy, AutoReview, Human review, and
//! the later durable start all bind to that same immutable identity.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::Value;
use tokio::sync::{RwLock, oneshot};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    agent::{ApprovalRequest, ReviewProjection},
    approval::{
        authority::{
            AUTHORIZATION_EVIDENCE_VERSION_V1, ApprovalClock, AuthenticatedCurrentCallDecision,
            CurrentCallDecision, DENIAL_EVIDENCE_VERSION_V1, ExecutableGrant,
            ExecutionAuthorityProvenance, HumanAuthorizationContextV1, HumanDecisionEvidence,
            PolicyDecisionRecord, ToolExecutionAuthorizationEvidence, ToolExecutionDenialEvidence,
        },
        route_policy::{
            ElevatedPolicyEvaluation, NormalPolicyDecision, PolicyEvaluation, PolicySnapshot,
            RoutePolicy,
        },
        route_reviewer::{
            EscalationReviewEvidence, EscalationReviewOutcome, EscalationReviewRequest,
            EscalationReviewResult, EscalationReviewer, ExecutionReviewEvidence,
            ExecutionReviewOutcome, ExecutionReviewRequest, ExecutionReviewResult,
            ExecutionReviewer, REVIEW_TRANSCRIPT_SCHEMA_VERSION_V3, REVIEW_TRUNCATION_MARKER,
            ReviewerActionEvidence, ReviewerPolicyEvidence, ReviewerTerminalClass,
            ReviewerTranscript, ReviewerTranscriptEntry,
        },
    },
    provider::types::{PublicMessage, ToolInvocationRoute, UserContent},
    store::Redactor,
    tools::{BoundToolInvocation, SealedBoundToolInvocation},
};

const MAX_CONTEXT_MESSAGES: usize = 12;
const MAX_CONTEXT_TEXT_CHARS: usize = 4_000;
const MAX_CONTEXT_TOTAL_CHARS: usize = 24_000;
pub(crate) const MAX_NORMAL_ROUTE_AUTHORIZATION_ATTEMPTS: usize = 3;

pub(crate) const fn normal_reauthorization_exhausted(attempts: usize) -> bool {
    attempts >= MAX_NORMAL_ROUTE_AUTHORIZATION_ATTEMPTS
}

#[derive(Debug)]
pub(crate) enum RouteApprovalOutcome {
    Allowed {
        grant: ExecutableGrant,
    },
    Denied {
        reason: String,
        evidence: Box<ToolExecutionDenialEvidence>,
        bound: BoundToolInvocation,
    },
    Pending {
        pending: PendingApproval,
    },
}

#[derive(Debug)]
pub(crate) enum CurrentCallResolution {
    /// An authenticated command named this request but did not own its exact
    /// tenant/PA/Human scope. It must neither consume nor project the pending
    /// approval.
    Ignored,
    Approved {
        grant: ExecutableGrant,
        decision: HumanDecisionEvidence,
    },
    Denied {
        decision: HumanDecisionEvidence,
    },
    Rejected {
        decision: HumanDecisionEvidence,
        reason: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WaiterResult {
    /// The authenticated command path owns the non-cloneable resolution.
    /// This signal only wakes a waiter that did not observe that command.
    Resolved,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PendingApprovalRequest {
    pub id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub route: ToolInvocationRoute,
    pub proposal_digest: String,
    pub descriptor_digest: String,
    pub bound_evidence_digest: String,
    pub adapter_id: String,
    pub adapter_version: u32,
    pub descriptor: Value,
    pub review_projection: Value,
}

impl PendingApprovalRequest {
    pub(crate) fn from_bound(
        id: String,
        route: ToolInvocationRoute,
        bound: &BoundToolInvocation,
        redactor: &Redactor,
    ) -> Result<Self> {
        Ok(Self {
            id,
            tool_call_id: bound.tool_call_id.clone(),
            tool_name: bound.tool_name.clone(),
            route,
            proposal_digest: bound.proposal_digest.to_hex(),
            descriptor_digest: bound.descriptor_digest.to_hex(),
            bound_evidence_digest: bound.evidence_digest()?.to_hex(),
            adapter_id: bound.adapter.id.clone(),
            adapter_version: bound.adapter.version,
            descriptor: redactor.redact_value(
                &serde_json::to_value(&bound.descriptor)
                    .context("serialize approval descriptor")?,
            )?,
            review_projection: redactor
                .redact_value(&Value::Object(bound.review_projection.as_object().clone()))?,
        })
    }

    /// Build the Human-facing request without exposing provider review
    /// evidence or the private route/digest envelope. Exact binding stays in
    /// the authenticated command context and durable private evidence.
    pub(crate) fn public_request(&self) -> ApprovalRequest {
        ApprovalRequest {
            id: self.id.clone(),
            tool_call_id: self.tool_call_id.clone(),
            tool_name: self.tool_name.clone(),
            action: ReviewProjection::Reviewable(self.descriptor.clone()),
            args_summary: self.review_projection.clone(),
            reason: Some("This exact operation requires one-time approval.".to_owned()),
            audit: None,
        }
    }
}

struct PendingEntry {
    sealed: SealedBoundToolInvocation,
    scope: ApprovalPrincipalScope,
    run_id: String,
    turn_id: String,
    policy: PolicySnapshot,
    escalation_review: EscalationReviewEvidence,
    sender: oneshot::Sender<WaiterResult>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ApprovalPrincipalScope {
    pub tenant_id: String,
    pub personality_agent_id: String,
    pub human_principal_id: String,
}

impl ApprovalPrincipalScope {
    fn validate(&self) -> Result<()> {
        for (label, value) in [
            ("tenant", self.tenant_id.as_str()),
            ("personality agent", self.personality_agent_id.as_str()),
            ("Human principal", self.human_principal_id.as_str()),
        ] {
            if value.trim().is_empty() || value.chars().any(char::is_control) {
                anyhow::bail!("approval {label} identity is invalid");
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PendingSummary {
    pub tool_call_id: String,
    pub tool_name: String,
}

#[derive(Clone, Debug)]
pub(crate) struct DurablePendingApprovalEvidence {
    pub bound: BoundToolInvocation,
    pub policy: PolicySnapshot,
    pub escalation_review: EscalationReviewEvidence,
}

pub(crate) struct PendingApproval {
    request: Box<PendingApprovalRequest>,
    durable_evidence: DurablePendingApprovalEvidence,
    receiver: oneshot::Receiver<WaiterResult>,
    pending: Arc<Mutex<HashMap<String, PendingEntry>>>,
}

impl PendingApproval {
    pub(crate) fn request(&self) -> &PendingApprovalRequest {
        self.request.as_ref()
    }

    pub(crate) fn receiver_mut(&mut self) -> &mut oneshot::Receiver<WaiterResult> {
        &mut self.receiver
    }

    pub(crate) fn durable_evidence(&self) -> &DurablePendingApprovalEvidence {
        &self.durable_evidence
    }
}

impl std::fmt::Debug for PendingApproval {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingApproval")
            .field("request_id", &self.request.id)
            .field("tool_call_id", &self.request.tool_call_id)
            .finish_non_exhaustive()
    }
}

impl Drop for PendingApproval {
    fn drop(&mut self) {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.request.id);
    }
}

#[derive(Clone)]
pub(crate) struct RouteApprovalBroker {
    policy: Arc<RwLock<RoutePolicy>>,
    clock: ApprovalClock,
    redactor: Arc<Redactor>,
    execution_reviewer: Arc<ExecutionReviewer>,
    escalation_reviewer: Arc<EscalationReviewer>,
    pending: Arc<Mutex<HashMap<String, PendingEntry>>>,
    resolving: Arc<Mutex<HashSet<String>>>,
}

impl std::fmt::Debug for RouteApprovalBroker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RouteApprovalBroker")
            .field(
                "pending",
                &self.pending.lock().map(|value| value.len()).ok(),
            )
            .finish_non_exhaustive()
    }
}

impl RouteApprovalBroker {
    pub(crate) fn new(
        policy: RoutePolicy,
        redactor: Redactor,
        execution_reviewer: Arc<ExecutionReviewer>,
        escalation_reviewer: Arc<EscalationReviewer>,
    ) -> Self {
        Self {
            policy: Arc::new(RwLock::new(policy)),
            clock: Arc::new(Utc::now),
            redactor: Arc::new(redactor),
            execution_reviewer,
            escalation_reviewer,
            pending: Arc::new(Mutex::new(HashMap::new())),
            resolving: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_clock(
        mut self,
        clock: impl Fn() -> chrono::DateTime<Utc> + Send + Sync + 'static,
    ) -> Self {
        self.clock = Arc::new(clock);
        self
    }

    pub(crate) async fn replace_policy(&self, next: RoutePolicy) -> Result<()> {
        let now = (self.clock)();
        let mut current = self.policy.write().await;
        current.validate_replacement(&next, now)?;
        *current = next;
        Ok(())
    }

    /// Terminalize a Normal call whose exact grant repeatedly expired or was
    /// replaced before its durable start. Re-running AutoReview forever would
    /// make a sufficiently short-lived policy source a liveness failure. This
    /// is a foundation denial, never an escalation to Human review.
    pub(crate) async fn deny_reauthorization_exhausted(
        &self,
        sealed: SealedBoundToolInvocation,
        route: ToolInvocationRoute,
        attempts: usize,
    ) -> Result<RouteApprovalOutcome> {
        if route != ToolInvocationRoute::Normal {
            anyhow::bail!("only a Normal route may exhaust automatic reauthorization");
        }
        let bound = sealed.invocation();
        let now = (self.clock)();
        let snapshot = match self.policy.read().await.evaluate_normal(bound, now) {
            PolicyEvaluation::Ready { snapshot, .. }
            | PolicyEvaluation::Unavailable { snapshot, .. } => snapshot,
        };
        self.deny(
            bound,
            route,
            snapshot,
            PolicyDecisionRecord::Unavailable,
            None,
            None,
            format!(
                "authorization policy could not remain valid through durable start after {attempts} attempts"
            ),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn start_request(
        &self,
        sealed: SealedBoundToolInvocation,
        route: ToolInvocationRoute,
        transcript: &[PublicMessage],
        scope: ApprovalPrincipalScope,
        run_id: &str,
        turn_id: &str,
        cancel: CancellationToken,
    ) -> Result<RouteApprovalOutcome> {
        scope.validate()?;
        let bound = sealed.invocation();
        let now = (self.clock)();

        match route {
            ToolInvocationRoute::Normal => {
                match self.policy.read().await.evaluate_normal(bound, now) {
                    PolicyEvaluation::Unavailable {
                        snapshot, reason, ..
                    } => self.deny(
                        bound,
                        route,
                        snapshot,
                        PolicyDecisionRecord::Unavailable,
                        None,
                        None,
                        reason,
                    ),
                    PolicyEvaluation::Ready {
                        snapshot,
                        decision: NormalPolicyDecision::Deny { reason },
                    } => self.deny(
                        bound,
                        route,
                        snapshot,
                        PolicyDecisionRecord::Deny,
                        None,
                        None,
                        reason,
                    ),
                    PolicyEvaluation::Ready {
                        snapshot,
                        decision: NormalPolicyDecision::Allow,
                    } => self.allow(
                        sealed,
                        route,
                        run_id,
                        turn_id,
                        snapshot,
                        PolicyDecisionRecord::Allow,
                        ExecutionAuthorityProvenance::AgentOwn,
                        None,
                        None,
                        None,
                    ),
                    PolicyEvaluation::Ready {
                        snapshot,
                        decision: NormalPolicyDecision::Unmatched,
                    } => {
                        let (transcript, action, policy) = match review_inputs(
                            bound,
                            transcript,
                            route,
                            PolicyDecisionRecord::Unmatched,
                            &snapshot,
                            self.redactor.as_ref(),
                        ) {
                            Ok(inputs) => inputs,
                            Err(_) => {
                                let review = self.execution_reviewer.block_without_call(
                                    ReviewerTerminalClass::InsufficientEvidence,
                                );
                                let reason = review.decision.rationale.clone();
                                return self.deny(
                                    bound,
                                    route,
                                    snapshot,
                                    PolicyDecisionRecord::Unmatched,
                                    Some(review),
                                    None,
                                    reason,
                                );
                            }
                        };
                        let review = self
                            .execution_reviewer
                            .review(
                                ExecutionReviewRequest {
                                    transcript,
                                    action,
                                    policy,
                                },
                                cancel,
                            )
                            .await;
                        match review {
                            ExecutionReviewResult::Allow(review)
                                if review.decision.outcome == ExecutionReviewOutcome::Allow =>
                            {
                                self.allow(
                                    sealed,
                                    route,
                                    run_id,
                                    turn_id,
                                    snapshot,
                                    PolicyDecisionRecord::Unmatched,
                                    ExecutionAuthorityProvenance::AgentOwn,
                                    Some(review),
                                    None,
                                    None,
                                )
                            }
                            ExecutionReviewResult::Allow(review)
                            | ExecutionReviewResult::Block(review) => {
                                let reason = review.decision.rationale.clone();
                                self.deny(
                                    bound,
                                    route,
                                    snapshot,
                                    PolicyDecisionRecord::Unmatched,
                                    Some(review),
                                    None,
                                    reason,
                                )
                            }
                        }
                    }
                }
            }
            ToolInvocationRoute::Elevated => {
                let snapshot = match self.policy.read().await.evaluate_elevated(bound, now) {
                    ElevatedPolicyEvaluation::Unavailable {
                        snapshot, reason, ..
                    } => {
                        return self.deny(
                            bound,
                            route,
                            snapshot,
                            PolicyDecisionRecord::Unavailable,
                            None,
                            None,
                            reason,
                        );
                    }
                    ElevatedPolicyEvaluation::Deny { snapshot, reason } => {
                        return self.deny(
                            bound,
                            route,
                            snapshot,
                            PolicyDecisionRecord::Deny,
                            None,
                            None,
                            reason,
                        );
                    }
                    ElevatedPolicyEvaluation::Ready { snapshot } => snapshot,
                };
                let (transcript, action, policy) = match review_inputs(
                    bound,
                    transcript,
                    route,
                    PolicyDecisionRecord::ElevatedPreflight,
                    &snapshot,
                    self.redactor.as_ref(),
                ) {
                    Ok(inputs) => inputs,
                    Err(_) => {
                        let review = self
                            .escalation_reviewer
                            .block_without_call(ReviewerTerminalClass::InsufficientEvidence);
                        let reason = review.decision.rationale.clone();
                        return self.deny(
                            bound,
                            route,
                            snapshot,
                            PolicyDecisionRecord::ElevatedPreflight,
                            None,
                            Some(review),
                            reason,
                        );
                    }
                };
                let review = self
                    .escalation_reviewer
                    .review(
                        EscalationReviewRequest {
                            transcript,
                            action,
                            policy,
                        },
                        cancel,
                    )
                    .await;
                match review {
                    EscalationReviewResult::AskHuman(review)
                        if review.decision.outcome == EscalationReviewOutcome::AskHuman =>
                    {
                        self.make_pending(sealed, route, scope, run_id, turn_id, snapshot, review)
                    }
                    EscalationReviewResult::AskHuman(review)
                    | EscalationReviewResult::Block(review) => {
                        let reason = review.decision.rationale.clone();
                        self.deny(
                            bound,
                            route,
                            snapshot,
                            PolicyDecisionRecord::ElevatedPreflight,
                            None,
                            Some(review),
                            reason,
                        )
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn allow(
        &self,
        sealed: SealedBoundToolInvocation,
        route: ToolInvocationRoute,
        run_id: &str,
        turn_id: &str,
        policy: PolicySnapshot,
        policy_decision: PolicyDecisionRecord,
        resolved_authority: ExecutionAuthorityProvenance,
        execution_review: Option<ExecutionReviewEvidence>,
        escalation_review: Option<EscalationReviewEvidence>,
        human_decision: Option<HumanDecisionEvidence>,
    ) -> Result<RouteApprovalOutcome> {
        let bound = sealed.invocation();
        let authorization = ToolExecutionAuthorizationEvidence {
            evidence_version: AUTHORIZATION_EVIDENCE_VERSION_V1.to_owned(),
            grant_id: Uuid::now_v7().to_string(),
            tool_call_id: bound.tool_call_id.clone(),
            route,
            proposal_digest: bound.proposal_digest.to_hex(),
            descriptor_digest: bound.descriptor_digest.to_hex(),
            bound_evidence_digest: bound.evidence_digest()?.to_hex(),
            policy,
            policy_decision,
            resolved_authority,
            execution_review,
            escalation_review,
            human_decision,
        };
        authorization.validate(bound)?;
        Ok(RouteApprovalOutcome::Allowed {
            grant: ExecutableGrant::new(
                self.policy.clone(),
                self.clock.clone(),
                sealed,
                route,
                run_id.to_owned(),
                turn_id.to_owned(),
                authorization,
            )?,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn deny(
        &self,
        bound: &BoundToolInvocation,
        route: ToolInvocationRoute,
        policy: PolicySnapshot,
        policy_decision: PolicyDecisionRecord,
        execution_review: Option<ExecutionReviewEvidence>,
        escalation_review: Option<EscalationReviewEvidence>,
        reason: String,
    ) -> Result<RouteApprovalOutcome> {
        let denial = ToolExecutionDenialEvidence {
            evidence_version: DENIAL_EVIDENCE_VERSION_V1.to_owned(),
            tool_call_id: bound.tool_call_id.clone(),
            route,
            proposal_digest: bound.proposal_digest.to_hex(),
            descriptor_digest: bound.descriptor_digest.to_hex(),
            bound_evidence_digest: bound.evidence_digest()?.to_hex(),
            policy,
            policy_decision,
            execution_review,
            escalation_review,
            reason: non_empty_reason(reason),
        };
        denial.validate(bound)?;
        Ok(RouteApprovalOutcome::Denied {
            reason: denial.reason.clone(),
            evidence: Box::new(denial),
            bound: bound.clone(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn make_pending(
        &self,
        sealed: SealedBoundToolInvocation,
        route: ToolInvocationRoute,
        scope: ApprovalPrincipalScope,
        run_id: &str,
        turn_id: &str,
        policy: PolicySnapshot,
        escalation_review: EscalationReviewEvidence,
    ) -> Result<RouteApprovalOutcome> {
        let bound = sealed.invocation();
        let durable_evidence = DurablePendingApprovalEvidence {
            bound: bound.clone(),
            policy: policy.clone(),
            escalation_review: escalation_review.clone(),
        };
        let request_id = Uuid::now_v7().to_string();
        let request = PendingApprovalRequest::from_bound(
            request_id.clone(),
            route,
            bound,
            self.redactor.as_ref(),
        )?;
        let (sender, receiver) = oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                request_id,
                PendingEntry {
                    sealed,
                    scope,
                    run_id: run_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    policy,
                    escalation_review,
                    sender,
                },
            );
        Ok(RouteApprovalOutcome::Pending {
            pending: PendingApproval {
                request: Box::new(request),
                durable_evidence,
                receiver,
                pending: self.pending.clone(),
            },
        })
    }

    pub(crate) async fn resolve(
        &self,
        request_id: &str,
        command: AuthenticatedCurrentCallDecision,
    ) -> Option<CurrentCallResolution> {
        let entry = {
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let entry = pending.get(request_id)?;
            if command.tenant_id != entry.scope.tenant_id
                || command.personality_agent_id != entry.scope.personality_agent_id
                || command.human_principal_id != entry.scope.human_principal_id
            {
                return Some(CurrentCallResolution::Ignored);
            }
            pending
                .remove(request_id)
                .expect("pending entry observed under the same lock")
        };
        self.resolving
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(request_id.to_owned());

        let bound_evidence_digest = entry.sealed.evidence_digest().to_hex();
        let bound = entry.sealed.invocation();
        let human_context = HumanAuthorizationContextV1 {
            request_id,
            command_id: &command.command_id,
            command_seq: command.command_seq,
            tenant_id: &command.tenant_id,
            personality_agent_id: &command.personality_agent_id,
            human_principal_id: &command.human_principal_id,
            decision: command.decision,
            received_at: command.received_at,
            tool_call_id: &bound.tool_call_id,
            route: ToolInvocationRoute::Elevated,
            proposal_digest: &bound.proposal_digest.to_hex(),
            descriptor_digest: &bound.descriptor_digest.to_hex(),
            bound_evidence_digest: &bound_evidence_digest,
            policy_source_digest: &entry.policy.source_digest,
            run_id: &entry.run_id,
            turn_id: &entry.turn_id,
        };
        let human = HumanDecisionEvidence::from_context(human_context)
            .expect("fixed Human authorization context must serialize");
        let resolution = match command.decision {
            CurrentCallDecision::DenyOnce => CurrentCallResolution::Denied { decision: human },
            CurrentCallDecision::ApproveOnce => {
                let now = (self.clock)();
                if !self
                    .policy
                    .read()
                    .await
                    .snapshot_matches(&entry.policy, now)
                {
                    CurrentCallResolution::Rejected {
                        decision: human,
                        reason: "authorization policy changed while approval was pending"
                            .to_owned(),
                    }
                } else {
                    match self.allow(
                        entry.sealed,
                        ToolInvocationRoute::Elevated,
                        &entry.run_id,
                        &entry.turn_id,
                        entry.policy,
                        PolicyDecisionRecord::ElevatedPreflight,
                        ExecutionAuthorityProvenance::AgentOwnWithHumanConsent,
                        None,
                        Some(entry.escalation_review),
                        Some(human.clone()),
                    ) {
                        Ok(RouteApprovalOutcome::Allowed { grant }) => {
                            CurrentCallResolution::Approved {
                                grant,
                                decision: human,
                            }
                        }
                        Ok(_) => unreachable!("allow only returns Allowed"),
                        Err(error) => CurrentCallResolution::Rejected {
                            decision: human,
                            reason: error.to_string(),
                        },
                    }
                }
            }
        };

        let _ = entry.sender.send(WaiterResult::Resolved);
        Some(resolution)
    }

    pub(crate) fn pending_scope_matches(
        &self,
        request_id: &str,
        tenant_id: &str,
        personality_agent_id: &str,
        human_principal_id: &str,
    ) -> bool {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(request_id)
            .is_some_and(|entry| {
                tenant_id == entry.scope.tenant_id
                    && personality_agent_id == entry.scope.personality_agent_id
                    && human_principal_id == entry.scope.human_principal_id
            })
    }

    pub(crate) fn commit_resolution(&self, request_id: &str) {
        self.resolving
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(request_id);
    }

    pub(crate) fn is_resolving(&self, request_id: &str) -> bool {
        self.resolving
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains(request_id)
    }

    pub(crate) fn cancel(&self, request_id: &str) -> bool {
        let entry = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(request_id);
        if let Some(entry) = entry {
            let _ = entry.sender.send(WaiterResult::Cancelled);
            true
        } else {
            false
        }
    }

    pub(crate) fn cancel_all(&self) -> Vec<(String, String)> {
        let entries: Vec<_> = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .drain()
            .collect();
        entries
            .into_iter()
            .map(|(request_id, entry)| {
                let tool_call_id = entry.sealed.invocation().tool_call_id.clone();
                let _ = entry.sender.send(WaiterResult::Cancelled);
                (request_id, tool_call_id)
            })
            .collect()
    }

    pub(crate) fn has_pending(&self, request_id: &str) -> bool {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains_key(request_id)
    }

    pub(crate) fn any_pending(&self) -> bool {
        !self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_empty()
    }

    pub(crate) fn pending_tool_call_id(&self, request_id: &str) -> Option<String> {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(request_id)
            .map(|entry| entry.sealed.invocation().tool_call_id.clone())
    }

    pub(crate) fn pending_summary(&self, request_id: &str) -> Option<PendingSummary> {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(request_id)
            .map(|entry| PendingSummary {
                tool_call_id: entry.sealed.invocation().tool_call_id.clone(),
                tool_name: entry.sealed.invocation().tool_name.clone(),
            })
    }
}

fn review_inputs(
    bound: &BoundToolInvocation,
    transcript: &[PublicMessage],
    route: ToolInvocationRoute,
    decision: PolicyDecisionRecord,
    snapshot: &PolicySnapshot,
    redactor: &Redactor,
) -> Result<(
    ReviewerTranscript,
    ReviewerActionEvidence,
    ReviewerPolicyEvidence,
)> {
    let descriptor = redactor.redact_value(
        &serde_json::to_value(&bound.descriptor).context("serialize reviewer action descriptor")?,
    )?;
    let review_projection =
        redactor.redact_value(&Value::Object(bound.review_projection.as_object().clone()))?;
    Ok((
        bounded_user_transcript(transcript, redactor),
        ReviewerActionEvidence::new(route, descriptor, review_projection)?,
        ReviewerPolicyEvidence::from_snapshot(route, decision, snapshot),
    ))
}

fn bounded_user_transcript(
    transcript: &[PublicMessage],
    redactor: &Redactor,
) -> ReviewerTranscript {
    let users = transcript
        .iter()
        .filter_map(|message| {
            let PublicMessage::User(message) = message else {
                return None;
            };
            let text = message
                .content
                .iter()
                .filter_map(|content| match content {
                    UserContent::Text { text } => Some(text.as_str()),
                    UserContent::Image { .. } => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            (!text.is_empty()).then(|| redactor.redact_text(&text))
        })
        .collect::<Vec<_>>();
    let mut selected = Vec::<(usize, ReviewerTranscriptEntry)>::new();
    let mut remaining = MAX_CONTEXT_TOTAL_CHARS;

    if let Some(first) = users.first() {
        push_context_entry(&mut selected, &mut remaining, 0, first);
    }
    if users.len() > 1 {
        push_context_entry(
            &mut selected,
            &mut remaining,
            users.len() - 1,
            users.last().expect("non-empty users"),
        );
    }
    for index in (1..users.len().saturating_sub(1)).rev() {
        if selected.len() >= MAX_CONTEXT_MESSAGES || remaining == 0 {
            break;
        }
        push_context_entry(&mut selected, &mut remaining, index, &users[index]);
    }
    selected.sort_by_key(|(index, _)| *index);
    let omitted_entries = users.len().saturating_sub(selected.len());
    let mut entries = Vec::with_capacity(selected.len() + usize::from(omitted_entries != 0));
    for (position, (index, entry)) in selected.into_iter().enumerate() {
        if position == 1 && omitted_entries != 0 && index > 1 {
            entries.push(ReviewerTranscriptEntry::Omission {
                omitted_entries,
                marker: REVIEW_TRUNCATION_MARKER,
            });
        }
        entries.push(entry);
    }
    ReviewerTranscript {
        schema_version: REVIEW_TRANSCRIPT_SCHEMA_VERSION_V3,
        entries,
    }
}

fn push_context_entry(
    selected: &mut Vec<(usize, ReviewerTranscriptEntry)>,
    remaining: &mut usize,
    index: usize,
    text: &str,
) {
    let limit = (*remaining).min(MAX_CONTEXT_TEXT_CHARS);
    if limit == 0 {
        return;
    }
    if text.chars().count() > limit && limit < REVIEW_TRUNCATION_MARKER.chars().count() {
        return;
    }
    let (text, truncated) = truncate_context_text(text, limit);
    *remaining = (*remaining).saturating_sub(text.chars().count());
    selected.push((index, ReviewerTranscriptEntry::User { text, truncated }));
}

fn truncate_context_text(value: &str, limit: usize) -> (String, bool) {
    let characters = value.chars().count();
    if characters <= limit {
        return (value.to_owned(), false);
    }
    let marker_chars = REVIEW_TRUNCATION_MARKER.chars().count();
    let prefix_chars = limit.saturating_sub(marker_chars);
    let mut text = value.chars().take(prefix_chars).collect::<String>();
    text.push_str(REVIEW_TRUNCATION_MARKER);
    (text, true)
}

#[cfg(test)]
pub(crate) fn provider_review_inputs_for_test(
    bound: &BoundToolInvocation,
    transcript: &[PublicMessage],
    route: ToolInvocationRoute,
    decision: PolicyDecisionRecord,
    snapshot: &PolicySnapshot,
    redactor: &Redactor,
) -> Result<(
    ReviewerTranscript,
    ReviewerActionEvidence,
    ReviewerPolicyEvidence,
)> {
    review_inputs(bound, transcript, route, decision, snapshot, redactor)
}

fn non_empty_reason(reason: String) -> String {
    if reason.trim().is_empty() {
        "AutoReview blocked without a rationale".to_owned()
    } else {
        reason
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use chrono::{Duration, TimeZone};
    use serde_json::json;

    use super::*;
    use crate::{
        approval::{
            authority::{
                GrantRevalidation, executor_authorization_projection_digest, executor_grant_digest,
            },
            route_reviewer::{
                EscalationReviewerPrompt, EscalationReviewerTransport, ExecutionReviewerPrompt,
                ExecutionReviewerTransport, ReviewerBudgetV1, ReviewerModelSpec,
                ReviewerTransportError, ReviewerTrustSet,
            },
        },
        provider::types::{
            ApiProtocol, ProviderOrigin, PublicAssistantContent, PublicAssistantMessage,
            StopReason, ToolCall, ToolDefinition, ToolResultMessage, Usage, UserMessage,
            ValidatedToolArguments,
        },
        tools::{
            AdapterIdentity, AppActionDescriptor, BoundExecutionArguments, BoundToolAdapter,
            BoundToolCtx, BoundToolExecutionOutcome, CapabilityClass, DescribeError, ResourceScope,
            ReviewProjection, Tool, ToolBindCtx, ToolBinding, ToolCtx, ToolError, ToolOutput,
            ToolRegistryBuilder, ToolRisk, WorkspacePaths,
        },
    };

    struct BindingTool {
        capability: CapabilityClass,
    }

    #[async_trait]
    impl Tool for BindingTool {
        fn def(&self) -> ToolDefinition {
            ToolDefinition {
                name: "app_action".to_owned(),
                description: "bound test operation".to_owned(),
                parameters: json!({"type": "object"}),
            }
        }

        fn risk(&self) -> ToolRisk {
            ToolRisk::Mutating
        }

        fn bound_adapter(self: Arc<Self>) -> Option<Arc<dyn BoundToolAdapter>> {
            Some(self)
        }

        async fn execute(&self, _ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
            unreachable!("route broker tests never use the raw execution path")
        }
    }

    #[async_trait]
    impl BoundToolAdapter for BindingTool {
        fn identity(&self) -> AdapterIdentity {
            AdapterIdentity::new("test.app", 1).expect("valid adapter")
        }

        async fn bind(&self, ctx: ToolBindCtx<'_>) -> Result<ToolBinding, DescribeError> {
            let title = ctx
                .args
                .as_object()
                .get("title")
                .and_then(Value::as_str)
                .ok_or(DescribeError::InvalidArguments)?;
            Ok(ToolBinding::new(
                AppActionDescriptor::new(
                    "update_record",
                    self.capability.clone(),
                    vec![ResourceScope::resource("test", "record", title)],
                )?,
                ReviewProjection::from_value(json!({
                    "operation": "update_record",
                    "target": "record-1",
                    "summary": "replace title",
                    "title": title
                }))?,
                BoundExecutionArguments::from_value(Value::Object(ctx.args.as_object().clone()))?,
            ))
        }

        async fn execute(
            &self,
            _ctx: BoundToolCtx<'_>,
        ) -> Result<BoundToolExecutionOutcome, ToolError> {
            unreachable!("route broker tests never execute the app operation")
        }
    }

    struct ExecutionFake {
        model: ReviewerModelSpec,
        response: String,
        calls: AtomicUsize,
        prompts: Mutex<Vec<Value>>,
    }

    #[async_trait]
    impl ExecutionReviewerTransport for ExecutionFake {
        fn model_spec(&self) -> &ReviewerModelSpec {
            &self.model
        }

        async fn complete(
            &self,
            prompt: &ExecutionReviewerPrompt,
            _cancel: CancellationToken,
        ) -> std::result::Result<String, ReviewerTransportError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.prompts
                .lock()
                .expect("execution prompts")
                .push(serde_json::to_value(prompt).expect("serialize prompt"));
            Ok(self.response.clone())
        }
    }

    struct EscalationFake {
        model: ReviewerModelSpec,
        response: String,
        calls: AtomicUsize,
        prompts: Mutex<Vec<Value>>,
    }

    #[async_trait]
    impl EscalationReviewerTransport for EscalationFake {
        fn model_spec(&self) -> &ReviewerModelSpec {
            &self.model
        }

        async fn complete(
            &self,
            prompt: &EscalationReviewerPrompt,
            _cancel: CancellationToken,
        ) -> std::result::Result<String, ReviewerTransportError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.prompts
                .lock()
                .expect("escalation prompts")
                .push(serde_json::to_value(prompt).expect("serialize prompt"));
            Ok(self.response.clone())
        }
    }

    fn model() -> ReviewerModelSpec {
        ReviewerModelSpec::new(
            "reviewer-model",
            "test-provider",
            "https://reviewer.test/v1",
            "test-account",
            "test-trust-domain",
            "test-processing-policy",
        )
    }

    fn broker(
        execution_response: Value,
        escalation_response: Value,
    ) -> (RouteApprovalBroker, Arc<ExecutionFake>, Arc<EscalationFake>) {
        let execution_transport = Arc::new(ExecutionFake {
            model: model(),
            response: execution_response.to_string(),
            calls: AtomicUsize::new(0),
            prompts: Mutex::new(Vec::new()),
        });
        let escalation_transport = Arc::new(EscalationFake {
            model: model(),
            response: escalation_response.to_string(),
            calls: AtomicUsize::new(0),
            prompts: Mutex::new(Vec::new()),
        });
        let trust = ReviewerTrustSet::new(vec![model()]);
        let execution = Arc::new(
            ExecutionReviewer::new(
                model(),
                trust.clone(),
                execution_transport.clone(),
                ReviewerBudgetV1::execution(),
            )
            .expect("execution reviewer"),
        );
        let escalation = Arc::new(
            EscalationReviewer::new(
                model(),
                trust,
                escalation_transport.clone(),
                ReviewerBudgetV1::escalation(),
            )
            .expect("escalation reviewer"),
        );
        (
            RouteApprovalBroker::new(
                RoutePolicy::baseline_only_v1(),
                Redactor::v1(),
                execution,
                escalation,
            ),
            execution_transport,
            escalation_transport,
        )
    }

    fn scope() -> ApprovalPrincipalScope {
        ApprovalPrincipalScope {
            tenant_id: "tenant-1".to_owned(),
            personality_agent_id: "agent-1".to_owned(),
            human_principal_id: "human-1".to_owned(),
        }
    }

    fn user_message(text: impl Into<String>) -> PublicMessage {
        PublicMessage::User(UserMessage {
            content: vec![UserContent::Text { text: text.into() }],
            timestamp: Utc::now(),
        })
    }

    fn assistant_message(text: impl Into<String>) -> PublicMessage {
        PublicMessage::Assistant(PublicAssistantMessage {
            content: vec![PublicAssistantContent::Text {
                text: text.into(),
                wire_item_index: 0,
            }],
            model: "fixture".to_owned(),
            provider: "fixture".to_owned(),
            origin: ProviderOrigin {
                provider_instance_id: "fixture".to_owned(),
                protocol: ApiProtocol::OpenAiChatCompletions,
                model: "fixture".to_owned(),
            },
            usage: Usage::default(),
            stop_reason: StopReason::Stop,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: Utc::now(),
        })
    }

    fn tool_result_message(text: impl Into<String>) -> PublicMessage {
        PublicMessage::ToolResult(ToolResultMessage {
            tool_call_id: "prior-call".to_owned(),
            tool_name: "prior-tool".to_owned(),
            content: vec![UserContent::Text { text: text.into() }],
            details: Value::Null,
            is_error: false,
            timestamp: Utc::now(),
        })
    }

    async fn sealed(
        capability: CapabilityClass,
        route: ToolInvocationRoute,
    ) -> SealedBoundToolInvocation {
        sealed_with_title(capability, route, "new title").await
    }

    async fn sealed_with_title(
        capability: CapabilityClass,
        route: ToolInvocationRoute,
        title: &str,
    ) -> SealedBoundToolInvocation {
        let mut builder = ToolRegistryBuilder::default();
        builder
            .register(Arc::new(BindingTool { capability }))
            .expect("register bound tool");
        let registry = builder.build();
        let call = ToolCall {
            id: "tool-call-1".to_owned(),
            name: "app_action".to_owned(),
            route,
            arguments: serde_json::from_value::<ValidatedToolArguments>(json!({
                "title": title
            }))
            .expect("validated arguments"),
        };
        registry
            .bind(
                &call,
                "flow-1",
                &WorkspacePaths::new("/workspace").expect("workspace"),
            )
            .await
            .expect("bound invocation")
    }

    fn command(decision: CurrentCallDecision) -> AuthenticatedCurrentCallDecision {
        AuthenticatedCurrentCallDecision {
            command_id: Uuid::now_v7().to_string(),
            command_seq: 7,
            tenant_id: "tenant-1".to_owned(),
            personality_agent_id: "agent-1".to_owned(),
            human_principal_id: "human-1".to_owned(),
            decision,
            received_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn normal_baseline_read_skips_both_reviewers() {
        let (broker, execution, escalation) = broker(
            json!({"outcome":"block","risk":"high","rationale":"unused"}),
            json!({
                "outcome":"block",
                "risk":"high",
                "misunderstanding":null,
                "rationale":"unused"
            }),
        );
        let outcome = broker
            .start_request(
                sealed(CapabilityClass::Read, ToolInvocationRoute::Normal).await,
                ToolInvocationRoute::Normal,
                &[],
                scope(),
                "run-1",
                "turn-1",
                CancellationToken::new(),
            )
            .await
            .expect("route decision");
        let RouteApprovalOutcome::Allowed { grant } = outcome else {
            panic!("read should use the baseline Allow fast path")
        };
        assert_eq!(
            grant.evidence().policy_decision,
            PolicyDecisionRecord::Allow
        );
        assert_eq!(execution.calls.load(Ordering::Relaxed), 0);
        assert_eq!(escalation.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn expiring_policy_reauthorization_terminates_after_bounded_attempts() {
        let before_expiry = Utc
            .with_ymd_and_hms(2026, 8, 12, 0, 0, 0)
            .single()
            .expect("valid timestamp");
        let after_expiry = before_expiry + Duration::seconds(2);
        let expires_at = before_expiry + Duration::seconds(1);
        let source = crate::approval::route_policy::PolicySourceState::verified_overlay_v1(
            1,
            "signed-policy-1",
            expires_at,
            None,
            before_expiry,
        )
        .expect("valid policy source");
        let policy =
            RoutePolicy::verified_overlay_v1(source, BTreeMap::new()).expect("valid route policy");
        let ticks = Arc::new(AtomicUsize::new(0));
        let clock_ticks = ticks.clone();
        let (broker, execution, escalation) = broker(
            json!({"outcome":"block","risk":"high","rationale":"unused"}),
            json!({
                "outcome":"block",
                "risk":"high",
                "misunderstanding":null,
                "rationale":"unused"
            }),
        );
        let broker = broker.with_clock(move || {
            if clock_ticks.fetch_add(1, Ordering::SeqCst) % 2 == 0 {
                before_expiry
            } else {
                after_expiry
            }
        });
        broker
            .replace_policy(policy)
            .await
            .expect("install verified policy");
        ticks.store(0, Ordering::SeqCst);

        let mut sealed = sealed(CapabilityClass::Read, ToolInvocationRoute::Normal).await;
        let mut terminal = None;
        for attempt in 1..=MAX_NORMAL_ROUTE_AUTHORIZATION_ATTEMPTS {
            let outcome = broker
                .start_request(
                    sealed,
                    ToolInvocationRoute::Normal,
                    &[],
                    scope(),
                    "run-1",
                    "turn-1",
                    CancellationToken::new(),
                )
                .await
                .expect("route decision");
            let RouteApprovalOutcome::Allowed { grant } = outcome else {
                panic!("pre-expiry policy evaluation must allow the exact read")
            };
            let (status, lease, _, _) = grant
                .authorize(
                    "tool-call-1",
                    "app_action",
                    ToolInvocationRoute::Normal,
                    "run-1",
                    "turn-1",
                )
                .await
                .expect("grant revalidation");
            assert_eq!(status, GrantRevalidation::Reauthorize);
            drop(lease);
            sealed = grant.into_sealed_for_reauthorization();
            if normal_reauthorization_exhausted(attempt) {
                terminal = Some(
                    broker
                        .deny_reauthorization_exhausted(
                            sealed,
                            ToolInvocationRoute::Normal,
                            attempt,
                        )
                        .await
                        .expect("terminal denial"),
                );
                break;
            }
            assert!(
                attempt < MAX_NORMAL_ROUTE_AUTHORIZATION_ATTEMPTS,
                "the retry budget must not terminate early"
            );
        }

        let Some(RouteApprovalOutcome::Denied {
            evidence, reason, ..
        }) = terminal
        else {
            panic!("the bounded retry budget must end in a denial")
        };
        assert_eq!(evidence.error_code(), "policy_unavailable");
        assert_eq!(evidence.policy_decision, PolicyDecisionRecord::Unavailable);
        assert!(reason.contains("after 3 attempts"));
        assert_eq!(execution.calls.load(Ordering::Relaxed), 0);
        assert_eq!(escalation.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn normal_unmatched_uses_execution_review_and_seals_one_effect() {
        let (broker, execution, escalation) = broker(
            json!({"outcome":"allow","risk":"medium","rationale":"bounded and intended"}),
            json!({
                "outcome":"block",
                "risk":"high",
                "misunderstanding":null,
                "rationale":"unused"
            }),
        );
        let outcome = broker
            .start_request(
                sealed(CapabilityClass::Mutate, ToolInvocationRoute::Normal).await,
                ToolInvocationRoute::Normal,
                &[],
                scope(),
                "run-1",
                "turn-1",
                CancellationToken::new(),
            )
            .await
            .expect("route decision");
        let RouteApprovalOutcome::Allowed { grant } = outcome else {
            panic!("valid execution review should authorize Normal")
        };
        assert_eq!(execution.calls.load(Ordering::Relaxed), 1);
        assert_eq!(escalation.calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            grant.evidence().policy_decision,
            PolicyDecisionRecord::Unmatched
        );
        let (status, lease, bound, authorization) = grant
            .authorize(
                "tool-call-1",
                "app_action",
                ToolInvocationRoute::Normal,
                "run-1",
                "turn-1",
            )
            .await
            .expect("grant revalidation");
        assert_eq!(status, GrantRevalidation::Valid);
        let expected_authorization_digest =
            executor_authorization_projection_digest(&authorization, &bound)
                .expect("Executor-safe authorization projection digest");
        drop(lease);
        let authorized = grant.into_authorized_bound();
        assert_eq!(authorized.tool_call_id(), "tool-call-1");
        let (sealed, permit) = authorized
            .into_validated_parts_for_test()
            .expect("permit matches sealed invocation");
        assert_eq!(
            permit.grant_digest,
            executor_grant_digest(&authorization.grant_id).expect("opaque grant digest")
        );
        assert_eq!(
            permit.bound_evidence_digest,
            authorization.bound_evidence_digest
        );
        assert_eq!(permit.action_digest, authorization.descriptor_digest);
        assert_eq!(
            permit.authorization_projection_digest,
            expected_authorization_digest
        );
        assert_eq!(permit.route, ToolInvocationRoute::Normal);
        assert_eq!(
            permit.resolved_authority,
            ExecutionAuthorityProvenance::AgentOwn
        );
        assert_eq!(sealed.invocation().tool_call_id, "tool-call-1");
    }

    #[tokio::test]
    async fn elevated_review_only_creates_exact_current_call_human_request() {
        let (broker, execution, escalation) = broker(
            json!({"outcome":"block","risk":"high","rationale":"unused"}),
            json!({
                "outcome":"ask_human",
                "risk":"medium",
                "misunderstanding":null,
                "rationale":"clear exact target"
            }),
        );
        let outcome = broker
            .start_request(
                sealed(CapabilityClass::Mutate, ToolInvocationRoute::Elevated).await,
                ToolInvocationRoute::Elevated,
                &[],
                scope(),
                "run-1",
                "turn-1",
                CancellationToken::new(),
            )
            .await
            .expect("route decision");
        let RouteApprovalOutcome::Pending { mut pending } = outcome else {
            panic!("Escalation AskHuman should create a pending current-call request")
        };
        let request = pending.request().clone();
        assert_eq!(request.route, ToolInvocationRoute::Elevated);
        assert_eq!(request.tool_call_id, "tool-call-1");
        let public_request = request.public_request();
        let public_json = serde_json::to_string(&public_request).expect("public approval request");
        assert!(!public_json.contains("clear exact target"));
        assert!(!public_json.contains(&request.proposal_digest));
        assert!(!public_json.contains(&request.descriptor_digest));
        assert!(!public_json.contains(&request.bound_evidence_digest));
        assert!(!public_json.contains(&request.adapter_id));
        assert_eq!(execution.calls.load(Ordering::Relaxed), 0);
        assert_eq!(escalation.calls.load(Ordering::Relaxed), 1);

        let resolution = broker
            .resolve(&request.id, command(CurrentCallDecision::ApproveOnce))
            .await
            .expect("pending resolution");
        let CurrentCallResolution::Approved { grant, .. } = resolution else {
            panic!("exact current-call approval should authorize")
        };
        assert_eq!(
            grant.evidence().resolved_authority,
            ExecutionAuthorityProvenance::AgentOwnWithHumanConsent
        );
        let (status, lease, _, _) = grant
            .authorize(
                "tool-call-1",
                "app_action",
                ToolInvocationRoute::Elevated,
                "run-1",
                "turn-1",
            )
            .await
            .expect("approved elevated grant revalidation");
        assert_eq!(status, GrantRevalidation::Valid);
        drop(lease);
        let (_, permit) = grant
            .into_authorized_bound()
            .into_validated_parts_for_test()
            .expect("permit matches sealed invocation");
        assert_eq!(permit.route, ToolInvocationRoute::Elevated);
        assert_eq!(
            permit.resolved_authority,
            ExecutionAuthorityProvenance::AgentOwnWithHumanConsent
        );
        assert!(matches!(
            pending.receiver_mut().await.expect("waiter result"),
            WaiterResult::Resolved
        ));
    }

    #[tokio::test]
    async fn elevated_resolution_ignores_a_different_human_without_consuming_pending() {
        let (broker, _, _) = broker(
            json!({"outcome":"block","risk":"high","rationale":"unused"}),
            json!({
                "outcome":"ask_human",
                "risk":"medium",
                "misunderstanding":null,
                "rationale":"clear exact target"
            }),
        );
        let outcome = broker
            .start_request(
                sealed(CapabilityClass::Mutate, ToolInvocationRoute::Elevated).await,
                ToolInvocationRoute::Elevated,
                &[],
                scope(),
                "run-1",
                "turn-1",
                CancellationToken::new(),
            )
            .await
            .expect("route decision");
        let RouteApprovalOutcome::Pending { mut pending } = outcome else {
            panic!("Escalation AskHuman should create a pending current-call request")
        };
        let mut wrong_actor = command(CurrentCallDecision::ApproveOnce);
        wrong_actor.human_principal_id = "human-2".to_owned();
        assert!(!broker.pending_scope_matches(
            &pending.request().id,
            &wrong_actor.tenant_id,
            &wrong_actor.personality_agent_id,
            &wrong_actor.human_principal_id,
        ));

        let resolution = broker
            .resolve(&pending.request().id, wrong_actor)
            .await
            .expect("pending resolution");
        assert!(matches!(resolution, CurrentCallResolution::Ignored));
        assert!(broker.has_pending(&pending.request().id));
        assert!(broker.pending_scope_matches(
            &pending.request().id,
            "tenant-1",
            "agent-1",
            "human-1",
        ));
        assert!(matches!(
            pending.receiver_mut().try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        let resolution = broker
            .resolve(
                &pending.request().id,
                command(CurrentCallDecision::ApproveOnce),
            )
            .await
            .expect("owning principal still resolves pending approval");
        assert!(matches!(resolution, CurrentCallResolution::Approved { .. }));
        assert!(!broker.has_pending(&pending.request().id));
        assert!(matches!(
            pending.receiver_mut().await.expect("waiter result"),
            WaiterResult::Resolved
        ));
    }

    #[test]
    fn bounded_transcript_keeps_first_and_latest_user_turns_and_marks_omissions() {
        let mut transcript = (0..14)
            .map(|index| user_message(format!("user-turn<{index}>")))
            .collect::<Vec<_>>();
        transcript.insert(4, assistant_message("assistant-text-must-not-appear"));
        transcript.insert(9, tool_result_message("tool-result-must-not-appear"));
        let bounded = bounded_user_transcript(&transcript, &Redactor::v1());
        let encoded = serde_json::to_string(&bounded).expect("bounded transcript");

        assert!(encoded.contains("user-turn<0>"));
        assert!(encoded.contains("user-turn<13>"));
        assert!(!encoded.contains("user-turn<1>"));
        assert!(!encoded.contains("user-turn<2>"));
        assert!(encoded.contains("omitted_entries"));
        assert!(encoded.contains(REVIEW_TRUNCATION_MARKER));
        assert!(!encoded.contains("assistant-text-must-not-appear"));
        assert!(!encoded.contains("tool-result-must-not-appear"));
    }

    #[tokio::test]
    async fn route_reviewers_receive_user_intent_and_exact_action_without_non_user_text() {
        const INVITE_CODE_SENTINEL: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopq";
        const USER_SENTINEL: &str = "user-intent-review-sentinel";
        const IMAGE_SENTINEL: &str = "image-data-must-not-reach-reviewer";
        const ASSISTANT_SENTINEL: &str = "assistant-text-must-not-reach-reviewer";
        const TOOL_RESULT_SENTINEL: &str = "tool-result-must-not-reach-reviewer";
        assert_eq!(INVITE_CODE_SENTINEL.chars().count(), 43);
        let transcript = vec![
            PublicMessage::User(UserMessage {
                content: vec![
                    UserContent::Text {
                        text: USER_SENTINEL.to_owned(),
                    },
                    UserContent::Image {
                        data: IMAGE_SENTINEL.to_owned(),
                        mime_type: "image/png".to_owned(),
                    },
                ],
                timestamp: Utc::now(),
            }),
            assistant_message(ASSISTANT_SENTINEL),
            tool_result_message(TOOL_RESULT_SENTINEL),
        ];

        let (broker, execution, escalation) = broker(
            json!({"outcome":"allow","risk":"medium","rationale":"intrinsically safe"}),
            json!({
                "outcome":"ask_human",
                "risk":"medium",
                "misunderstanding":null,
                "rationale":"clear exact target"
            }),
        );
        let normal = broker
            .start_request(
                sealed_with_title(
                    CapabilityClass::Mutate,
                    ToolInvocationRoute::Normal,
                    INVITE_CODE_SENTINEL,
                )
                .await,
                ToolInvocationRoute::Normal,
                &transcript,
                scope(),
                "run-1",
                "turn-1",
                CancellationToken::new(),
            )
            .await
            .expect("normal review");
        assert!(matches!(normal, RouteApprovalOutcome::Allowed { .. }));

        let elevated = broker
            .start_request(
                sealed_with_title(
                    CapabilityClass::Mutate,
                    ToolInvocationRoute::Elevated,
                    INVITE_CODE_SENTINEL,
                )
                .await,
                ToolInvocationRoute::Elevated,
                &transcript,
                scope(),
                "run-2",
                "turn-2",
                CancellationToken::new(),
            )
            .await
            .expect("elevated review");
        let RouteApprovalOutcome::Pending { pending } = elevated else {
            panic!("Escalation reviewer should create the Human request")
        };
        let human_request = serde_json::to_string(&pending.request().public_request())
            .expect("Human approval request");
        assert!(
            human_request.contains(INVITE_CODE_SENTINEL),
            "the authenticated Human must retain the exact local projection"
        );

        let execution_prompts = execution.prompts.lock().expect("execution prompts");
        let escalation_prompts = escalation.prompts.lock().expect("escalation prompts");
        for prompt in execution_prompts.iter().chain(escalation_prompts.iter()) {
            let encoded = serde_json::to_string(prompt).expect("encode reviewer prompt");
            assert!(encoded.contains(INVITE_CODE_SENTINEL));
            assert!(encoded.contains(USER_SENTINEL));
            assert!(!encoded.contains(IMAGE_SENTINEL));
            assert!(!encoded.contains(ASSISTANT_SENTINEL));
            assert!(!encoded.contains(TOOL_RESULT_SENTINEL));
            for forbidden in ["context_version", "tenant-1", "agent-1", "human-1"] {
                assert!(
                    !encoded.contains(forbidden),
                    "leaked reviewer field: {forbidden}"
                );
            }
        }
    }
}
