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
use serde_json::{Value, json};
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
            EscalationObjectionOutcome, EscalationObjectionRequest, EscalationObjectionResponder,
            EscalationReviewEvidence, EscalationReviewOutcome, EscalationReviewRequest,
            EscalationReviewResult, EscalationReviewer, ExecutionReviewEvidence,
            ExecutionReviewOutcome, ExecutionReviewRequest, ExecutionReviewResult,
            ExecutionReviewer, REVIEW_NO_HUMAN_TURN_MARKER, REVIEW_TRANSCRIPT_SCHEMA_VERSION_V7,
            REVIEW_TRUNCATION_MARKER, ReviewerActionEvidence, ReviewerParticipants,
            ReviewerPolicyEvidence, ReviewerRejectedToolCallEvidence, ReviewerTerminalClass,
            ReviewerToolCallEvidence, ReviewerTranscript, ReviewerTranscriptEntry,
        },
    },
    provider::types::{PublicAssistantContent, PublicMessage, ToolInvocationRoute, UserContent},
    store::Redactor,
    tools::{BoundToolInvocation, SealedBoundToolInvocation},
};

const MAX_CONTEXT_USER_MESSAGES: usize = 12;
const MAX_CONTEXT_USER_TEXT_CHARS: usize = 4_000;
const MAX_CONTEXT_USER_TOTAL_CHARS: usize = 24_000;
const MAX_CONTEXT_ASSISTANT_MESSAGES: usize = 12;
const MAX_CONTEXT_ASSISTANT_TEXT_CHARS: usize = 4_000;
const MAX_CONTEXT_ASSISTANT_TOTAL_CHARS: usize = 16_000;
const MAX_CONTEXT_TOOL_CALLS: usize = 40;
const MAX_CONTEXT_TOOL_ARGUMENT_CHARS: usize = 2_000;
const MAX_CONTEXT_TOOL_TOTAL_CHARS: usize = 16_000;
const MAX_CONTEXT_TOOL_RESULTS: usize = 40;
const MAX_CONTEXT_TOOL_RESULT_CHARS: usize = 2_000;
const MAX_CONTEXT_TOOL_RESULT_TOTAL_CHARS: usize = 16_000;
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
    pub reviewer_objection: Option<String>,
    pub pa_reason: Option<String>,
    pub pa_objection_failure: Option<String>,
}

impl PendingApprovalRequest {
    pub(crate) fn from_bound(
        id: String,
        route: ToolInvocationRoute,
        bound: &BoundToolInvocation,
        redactor: &Redactor,
        reviewer_objection: Option<String>,
        pa_reason: Option<String>,
        pa_objection_failure: Option<String>,
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
            reviewer_objection,
            pa_reason,
            pa_objection_failure,
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
            reason: Some(
                match (
                    self.reviewer_objection.as_deref(),
                    self.pa_reason.as_deref(),
                    self.pa_objection_failure.as_deref(),
                ) {
                    (Some(objection), Some(pa_reason), _) => format!(
                        "The PA chose to proceed after AutoReview objected. Reviewer objection: {objection} PA reason: {pa_reason}"
                    ),
                    (Some(objection), None, Some(failure)) => format!(
                        "AutoReview objected, but the PA objection response could not be obtained ({failure}). The unchanged held call is shown to the Human for the final decision. Reviewer objection: {objection}"
                    ),
                    (Some(objection), None, None) => format!(
                        "The PA chose to proceed after AutoReview objected. Reviewer objection: {objection}"
                    ),
                    (None, _, _) => "This exact operation requires one-time approval.".to_owned(),
                },
            ),
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
    fn reviewer_participants(&self, redactor: &Redactor) -> Option<ReviewerParticipants> {
        let personality_agent_id = (!self.personality_agent_id.trim().is_empty())
            .then(|| redactor.redact_text(&self.personality_agent_id));
        personality_agent_id.map(|personality_agent_id| ReviewerParticipants {
            human_display_name: None,
            personality_agent_display_name: None,
            personality_agent_id: Some(personality_agent_id),
        })
    }
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
    escalation_objection_responder: Option<Arc<EscalationObjectionResponder>>,
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
        Self::with_shared_policy(
            Arc::new(RwLock::new(policy)),
            redactor,
            execution_reviewer,
            escalation_reviewer,
        )
    }

    pub(crate) fn with_escalation_objection_responder(
        mut self,
        responder: Arc<EscalationObjectionResponder>,
    ) -> Self {
        self.escalation_objection_responder = Some(responder);
        self
    }

    pub(crate) fn with_shared_policy(
        policy: Arc<RwLock<RoutePolicy>>,
        redactor: Redactor,
        execution_reviewer: Arc<ExecutionReviewer>,
        escalation_reviewer: Arc<EscalationReviewer>,
    ) -> Self {
        Self {
            policy,
            clock: Arc::new(Utc::now),
            redactor: Arc::new(redactor),
            execution_reviewer,
            escalation_reviewer,
            escalation_objection_responder: None,
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
                                    participants: scope
                                        .reviewer_participants(self.redactor.as_ref()),
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
                        return self
                            .make_pending(sealed, route, scope, run_id, turn_id, snapshot, review);
                    }
                };
                let review_request = EscalationReviewRequest {
                    participants: scope.reviewer_participants(self.redactor.as_ref()),
                    transcript,
                    action,
                    policy,
                };
                let review = self
                    .escalation_reviewer
                    .review(review_request.clone(), cancel.clone())
                    .await;
                let mut review = match review {
                    EscalationReviewResult::AskHuman(review)
                    | EscalationReviewResult::Block(review) => review,
                };
                if review.decision.outcome == EscalationReviewOutcome::AskHuman {
                    self.make_pending(sealed, route, scope, run_id, turn_id, snapshot, review)
                } else {
                    let Some(responder) = self.escalation_objection_responder.as_ref() else {
                        review.pa_objection_failure =
                            Some("PA objection-response channel is unavailable".to_owned());
                        return self
                            .make_pending(sealed, route, scope, run_id, turn_id, snapshot, review);
                    };
                    let response = Box::pin(responder.answer(
                        EscalationObjectionRequest {
                            review: review_request,
                            reviewer_objection: review.decision.rationale.clone(),
                        },
                        cancel,
                    ))
                    .await;
                    let answer = response.answer.clone();
                    review.pa_objection_response = Some(Box::new(response));
                    match answer.map(|answer| answer.outcome) {
                        Some(EscalationObjectionOutcome::Proceed) => self.make_pending(
                            sealed, route, scope, run_id, turn_id, snapshot, review,
                        ),
                        Some(EscalationObjectionOutcome::Withdraw) => self.deny(
                            bound,
                            route,
                            snapshot,
                            PolicyDecisionRecord::ElevatedPreflight,
                            None,
                            Some(review),
                            "The PA withdrew the held call after receiving the AutoReview objection"
                                .to_owned(),
                        ),
                        None => {
                            let terminal = review
                                .pa_objection_response
                                .as_ref()
                                .expect("objection response was recorded")
                                .budget
                                .terminal
                                .as_str();
                            review.pa_objection_failure = Some(format!(
                                "PA did not produce a valid objection answer (terminal: {terminal})"
                            ));
                            self.make_pending(
                                sealed, route, scope, run_id, turn_id, snapshot, review,
                            )
                        }
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
        let reviewer_objection =
            (escalation_review.decision.outcome == EscalationReviewOutcome::Block).then(|| {
                self.redactor
                    .redact_text(&escalation_review.decision.rationale)
            });
        let pa_reason = escalation_review
            .pa_objection_response
            .as_ref()
            .and_then(|response| response.answer.as_ref())
            .and_then(|answer| answer.reason.as_deref())
            .map(|reason| self.redactor.redact_text(reason));
        let pa_objection_failure = escalation_review
            .pa_objection_failure
            .as_deref()
            .map(|failure| self.redactor.redact_text(failure));
        let request = PendingApprovalRequest::from_bound(
            request_id.clone(),
            route,
            bound,
            self.redactor.as_ref(),
            reviewer_objection,
            pa_reason,
            pa_objection_failure,
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
        bounded_reviewer_transcript(transcript, redactor, &bound.tool_call_id)?,
        ReviewerActionEvidence::new(
            bound.tool_call_id.clone(),
            redactor.redact_text(&bound.tool_name),
            route,
            descriptor,
            review_projection,
        )?,
        ReviewerPolicyEvidence::from_snapshot(route, decision, snapshot),
    ))
}

fn bounded_reviewer_transcript(
    transcript: &[PublicMessage],
    redactor: &Redactor,
    pending_tool_call_id: &str,
) -> Result<ReviewerTranscript> {
    let mut users = Vec::<(usize, String)>::new();
    let mut assistants = Vec::<(usize, usize, String)>::new();
    let mut tools = Vec::<ReviewerToolCandidate>::new();
    let mut results = Vec::<(usize, String, ReviewerTranscriptEntry)>::new();
    let mut recorded_tool_calls = HashMap::<String, String>::new();
    let mut orphan_tool_results = Vec::<usize>::new();
    let mut ordinal = 0;
    let mut reached_pending_tool_call = false;

    for (turn_id, message) in transcript.iter().enumerate() {
        match message {
            PublicMessage::User(message) => {
                let text = message
                    .content
                    .iter()
                    .filter_map(|content| match content {
                        UserContent::Text { text } => Some(text.as_str()),
                        UserContent::Image { .. } => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                if !text.is_empty() {
                    users.push((ordinal, redactor.redact_text(&text)));
                    ordinal += 1;
                }
            }
            PublicMessage::Assistant(message) => {
                let message_ordinal = ordinal;
                ordinal += 1;
                let mut text_parts = Vec::new();
                for content in &message.content {
                    let entry = match content {
                        PublicAssistantContent::ToolCall { tool_call, .. }
                            if tool_call.id == pending_tool_call_id =>
                        {
                            reached_pending_tool_call = true;
                            break;
                        }
                        PublicAssistantContent::ToolCall { .. }
                        | PublicAssistantContent::RejectedToolCall { .. }
                            if reached_pending_tool_call =>
                        {
                            continue;
                        }
                        PublicAssistantContent::ToolCall { tool_call, .. } => {
                            let arguments = redactor.redact_value(&Value::Object(
                                tool_call.arguments.as_object().clone(),
                            ))?;
                            ReviewerTranscriptEntry::Assistant {
                                turn_id,
                                text: None,
                                text_truncated: false,
                                tool_calls: vec![ReviewerToolCallEvidence {
                                    id: tool_call.id.clone(),
                                    tool: tool_call.name.clone(),
                                    route: tool_call.route,
                                    arguments: capped_tool_arguments(arguments)?,
                                }],
                                rejected_tool_calls: Vec::new(),
                            }
                        }
                        PublicAssistantContent::RejectedToolCall { rejected, .. } => {
                            ReviewerTranscriptEntry::Assistant {
                                turn_id,
                                text: None,
                                text_truncated: false,
                                tool_calls: Vec::new(),
                                rejected_tool_calls: vec![ReviewerRejectedToolCallEvidence {
                                    id: rejected.id.clone(),
                                    tool: rejected.name.clone(),
                                    reason: rejected.error,
                                }],
                            }
                        }
                        PublicAssistantContent::Text { text, .. } => {
                            text_parts.push(text.as_str());
                            continue;
                        }
                        PublicAssistantContent::Thinking { .. } => continue,
                    };
                    let tool_call_id = match content {
                        PublicAssistantContent::ToolCall { tool_call, .. } => {
                            recorded_tool_calls
                                .insert(tool_call.id.clone(), tool_call.name.clone());
                            Some(tool_call.id.clone())
                        }
                        _ => None,
                    };
                    tools.push(ReviewerToolCandidate {
                        ordinal: message_ordinal,
                        tool_call_id,
                        entry,
                    });
                }
                let text = text_parts.join("\n\n");
                if !text.is_empty() {
                    assistants.push((message_ordinal, turn_id, redactor.redact_text(&text)));
                }
            }
            PublicMessage::ToolResult(result) => {
                let Some(tool) = recorded_tool_calls.get(&result.tool_call_id) else {
                    orphan_tool_results.push(ordinal);
                    ordinal += 1;
                    continue;
                };
                let content = reviewer_tool_result_content(result, redactor)?;
                let (content, truncated) =
                    truncate_context_text(&content, MAX_CONTEXT_TOOL_RESULT_CHARS);
                results.push((
                    ordinal,
                    result.tool_call_id.clone(),
                    ReviewerTranscriptEntry::ToolResult {
                        tool: tool.clone(),
                        tool_call_id: (!result.tool_call_id.is_empty())
                            .then(|| result.tool_call_id.clone()),
                        is_error: result.is_error,
                        content,
                        truncated,
                    },
                ));
                ordinal += 1;
            }
        }
    }

    let mut entries = select_user_entries(&users);
    entries.extend(select_assistant_entries(&assistants));
    entries.extend(select_tool_pair_entries(&tools, &results, ordinal)?);
    if !orphan_tool_results.is_empty() {
        entries.push((
            ordinal,
            ReviewerTranscriptEntry::OrphanToolResultOmission {
                omitted_orphan_tool_results: orphan_tool_results.len(),
                marker: REVIEW_TRUNCATION_MARKER,
            },
        ));
    }
    entries.sort_by_key(|(ordinal, _)| *ordinal);
    if users.is_empty() {
        entries.insert(
            0,
            (
                0,
                ReviewerTranscriptEntry::NoHumanTurn {
                    marker: REVIEW_NO_HUMAN_TURN_MARKER,
                },
            ),
        );
    }
    Ok(ReviewerTranscript {
        schema_version: REVIEW_TRANSCRIPT_SCHEMA_VERSION_V7,
        entries: entries.into_iter().map(|(_, entry)| entry).collect(),
    })
}

#[derive(Clone)]
struct ReviewerToolCandidate {
    ordinal: usize,
    tool_call_id: Option<String>,
    entry: ReviewerTranscriptEntry,
}

fn reviewer_tool_result_content(
    result: &crate::provider::types::ToolResultMessage,
    redactor: &Redactor,
) -> Result<String> {
    let mut parts = result
        .content
        .iter()
        .filter_map(|content| match content {
            UserContent::Text { text } => Some(redactor.redact_text(text)),
            UserContent::Image { .. } => None,
        })
        .collect::<Vec<_>>();
    if !result.details.is_null() {
        let details = redactor.redact_value(&result.details)?;
        parts.push(
            serde_json::to_string(&details)
                .context("serialize structured reviewer tool-result evidence")?,
        );
    }
    Ok(parts.join("\n"))
}

fn select_user_entries(users: &[(usize, String)]) -> Vec<(usize, ReviewerTranscriptEntry)> {
    let mut selected = Vec::<(usize, usize, ReviewerTranscriptEntry)>::new();
    let mut remaining = MAX_CONTEXT_USER_TOTAL_CHARS;

    for index in (0..users.len()).rev() {
        if selected.len() >= MAX_CONTEXT_USER_MESSAGES || remaining == 0 {
            break;
        }
        let (ordinal, text) = &users[index];
        push_user_entry(&mut selected, &mut remaining, index, *ordinal, text);
    }

    let selected_indices = selected
        .iter()
        .map(|(index, _, _)| *index)
        .collect::<HashSet<_>>();
    let omitted = users
        .iter()
        .enumerate()
        .filter(|(index, _)| !selected_indices.contains(index))
        .collect::<Vec<_>>();
    let mut entries = selected
        .into_iter()
        .map(|(_, ordinal, entry)| (ordinal, entry))
        .collect::<Vec<_>>();
    if let Some((_, (ordinal, _))) = omitted.first() {
        entries.push((
            *ordinal,
            ReviewerTranscriptEntry::UserOmission {
                omitted_user_turns: omitted.len(),
                marker: REVIEW_TRUNCATION_MARKER,
            },
        ));
    }
    entries
}

fn select_assistant_entries(
    assistants: &[(usize, usize, String)],
) -> Vec<(usize, ReviewerTranscriptEntry)> {
    let mut selected = Vec::<(usize, usize, ReviewerTranscriptEntry)>::new();
    let mut remaining = MAX_CONTEXT_ASSISTANT_TOTAL_CHARS;
    for index in (0..assistants.len()).rev() {
        if selected.len() >= MAX_CONTEXT_ASSISTANT_MESSAGES || remaining == 0 {
            break;
        }
        let (ordinal, turn_id, text) = &assistants[index];
        let limit = remaining.min(MAX_CONTEXT_ASSISTANT_TEXT_CHARS);
        if text.chars().count() > limit && limit < REVIEW_TRUNCATION_MARKER.chars().count() {
            continue;
        }
        let (text, text_truncated) = truncate_context_text(text, limit);
        remaining = remaining.saturating_sub(text.chars().count());
        selected.push((
            index,
            *ordinal,
            ReviewerTranscriptEntry::Assistant {
                turn_id: *turn_id,
                text: Some(text),
                text_truncated,
                tool_calls: Vec::new(),
                rejected_tool_calls: Vec::new(),
            },
        ));
    }

    let selected_indices = selected
        .iter()
        .map(|(index, _, _)| *index)
        .collect::<HashSet<_>>();
    let omitted = assistants
        .iter()
        .enumerate()
        .filter(|(index, _)| !selected_indices.contains(index))
        .collect::<Vec<_>>();
    let mut entries = selected
        .into_iter()
        .map(|(_, ordinal, entry)| (ordinal, entry))
        .collect::<Vec<_>>();
    if let Some((_, (ordinal, _, _))) = omitted.first() {
        entries.push((
            *ordinal,
            ReviewerTranscriptEntry::AssistantOmission {
                omitted_assistant_turns: omitted.len(),
                marker: REVIEW_TRUNCATION_MARKER,
            },
        ));
    }
    entries
}

fn push_user_entry(
    selected: &mut Vec<(usize, usize, ReviewerTranscriptEntry)>,
    remaining: &mut usize,
    index: usize,
    ordinal: usize,
    text: &str,
) {
    let limit = (*remaining).min(MAX_CONTEXT_USER_TEXT_CHARS);
    if limit == 0 {
        return;
    }
    if text.chars().count() > limit && limit < REVIEW_TRUNCATION_MARKER.chars().count() {
        return;
    }
    let (text, truncated) = truncate_context_text(text, limit);
    *remaining = (*remaining).saturating_sub(text.chars().count());
    selected.push((
        index,
        ordinal,
        ReviewerTranscriptEntry::User { text, truncated },
    ));
}

fn select_tool_pair_entries(
    tools: &[ReviewerToolCandidate],
    results: &[(usize, String, ReviewerTranscriptEntry)],
    omission_ordinal: usize,
) -> Result<Vec<(usize, ReviewerTranscriptEntry)>> {
    let result_by_tool_call_id = results
        .iter()
        .enumerate()
        .map(|(index, (_, tool_call_id, _))| (tool_call_id.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut selected_tools = Vec::<(usize, usize, ReviewerTranscriptEntry)>::new();
    let mut selected_results = Vec::<(usize, usize, ReviewerTranscriptEntry)>::new();
    let mut retained_result_indices = HashSet::new();
    let mut remaining_tool_chars = MAX_CONTEXT_TOOL_TOTAL_CHARS;
    let mut remaining_result_chars = MAX_CONTEXT_TOOL_RESULT_TOTAL_CHARS;

    for (index, candidate) in tools.iter().enumerate().rev() {
        if selected_tools.len() >= MAX_CONTEXT_TOOL_CALLS || remaining_tool_chars == 0 {
            break;
        }
        let tool_chars = serde_json::to_string(&candidate.entry)
            .context("serialize reviewer tool-call transcript entry")?
            .chars()
            .count();
        if tool_chars > remaining_tool_chars {
            break;
        }

        let paired_result = if let Some(tool_call_id) = candidate.tool_call_id.as_deref() {
            let Some(result_index) = result_by_tool_call_id.get(tool_call_id).copied() else {
                // A settled historical native call cannot be projected without its result.
                continue;
            };
            if retained_result_indices.contains(&result_index) {
                continue;
            }
            if selected_results.len() >= MAX_CONTEXT_TOOL_RESULTS || remaining_result_chars == 0 {
                break;
            }
            let (result_ordinal, _, result_entry) = &results[result_index];
            let result_chars = serde_json::to_string(result_entry)
                .context("serialize reviewer tool-result transcript entry")?
                .chars()
                .count();
            if result_chars > remaining_result_chars {
                break;
            }
            Some((
                result_index,
                *result_ordinal,
                result_entry.clone(),
                result_chars,
            ))
        } else {
            None
        };

        remaining_tool_chars -= tool_chars;
        selected_tools.push((index, candidate.ordinal, candidate.entry.clone()));
        if let Some((result_index, result_ordinal, result_entry, result_chars)) = paired_result {
            remaining_result_chars -= result_chars;
            retained_result_indices.insert(result_index);
            selected_results.push((result_index, result_ordinal, result_entry));
        }
    }

    let selected_tool_indices = selected_tools
        .iter()
        .map(|(index, _, _)| *index)
        .collect::<HashSet<_>>();
    let omitted_tool_calls = tools
        .iter()
        .enumerate()
        .filter(|(index, _)| !selected_tool_indices.contains(index))
        .count();
    let omitted_tool_results = results
        .iter()
        .enumerate()
        .filter(|(index, _)| !retained_result_indices.contains(index))
        .count();
    let mut entries = selected_tools
        .into_iter()
        .map(|(_, ordinal, entry)| (ordinal, entry))
        .collect::<Vec<_>>();
    entries.extend(
        selected_results
            .into_iter()
            .map(|(_, ordinal, entry)| (ordinal, entry)),
    );
    // Tool omission evidence follows all selected settled pairs so it can never
    // interrupt a native call/result sequence on provider wires.
    if omitted_tool_calls > 0 {
        entries.push((
            omission_ordinal,
            ReviewerTranscriptEntry::ToolCallOmission {
                omitted_tool_calls,
                marker: REVIEW_TRUNCATION_MARKER,
            },
        ));
    }
    if omitted_tool_results > 0 {
        entries.push((
            omission_ordinal,
            ReviewerTranscriptEntry::ToolResultOmission {
                omitted_tool_results,
                marker: REVIEW_TRUNCATION_MARKER,
            },
        ));
    }
    Ok(entries)
}

fn capped_tool_arguments(arguments: Value) -> Result<Value> {
    let encoded = serde_json::to_string(&arguments).context("serialize reviewer tool arguments")?;
    let encoded_chars = encoded.chars().count();
    if encoded_chars <= MAX_CONTEXT_TOOL_ARGUMENT_CHARS {
        return Ok(arguments);
    }

    let candidate = |prefix_chars: usize| {
        json!({
            "json_prefix": encoded.chars().take(prefix_chars).collect::<String>(),
            "omitted_characters": encoded_chars - prefix_chars,
            "marker": REVIEW_TRUNCATION_MARKER,
        })
    };
    let mut low = 0;
    let mut high = MAX_CONTEXT_TOOL_ARGUMENT_CHARS.min(encoded_chars);
    let mut best = candidate(0);
    while low <= high {
        let middle = low + (high - low) / 2;
        let value = candidate(middle);
        let value_chars = serde_json::to_string(&value)
            .context("serialize capped reviewer tool arguments")?
            .chars()
            .count();
        if value_chars <= MAX_CONTEXT_TOOL_ARGUMENT_CHARS {
            best = value;
            low = middle + 1;
        } else {
            high = middle.saturating_sub(1);
        }
    }
    Ok(best)
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
                GrantRevalidation, authorization_evidence_digest,
                executor_authorization_projection_digest, executor_grant_digest,
            },
            route_reviewer::{
                EscalationObjectionPrompt, EscalationObjectionResponder,
                EscalationObjectionResponderTransport, EscalationReviewerPrompt,
                EscalationReviewerTransport, ExecutionReviewerPrompt, ExecutionReviewerTransport,
                PersonalityAgentPromptContextHandle, ReviewerBudgetV1, ReviewerModelSpec,
                ReviewerTransportError, ReviewerTrustSet,
            },
        },
        provider::types::{
            ApiProtocol, PromptContext, ProviderOrigin, PublicAssistantContent,
            PublicAssistantMessage, RejectedToolCall, StopReason, ToolArgumentError, ToolCall,
            ToolDefinition, ToolResultMessage, Usage, UserMessage, ValidatedToolArguments,
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
            _tool_call_offset: usize,
            _cancel: CancellationToken,
        ) -> std::result::Result<
            crate::approval::route_reviewer::ReviewerTransportOutput,
            ReviewerTransportError,
        > {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.prompts
                .lock()
                .expect("execution prompts")
                .push(serde_json::to_value(prompt).expect("serialize prompt"));
            Ok(crate::approval::route_reviewer::ReviewerTransportOutput {
                text: self.response.clone(),
                tool_trace: Vec::new(),
            })
        }
    }

    struct EscalationFake {
        model: ReviewerModelSpec,
        response: String,
        calls: AtomicUsize,
        prompts: Mutex<Vec<Value>>,
    }

    struct ObjectionFake {
        model: ReviewerModelSpec,
        response: String,
        calls: AtomicUsize,
        prompts: Mutex<Vec<Value>>,
    }

    #[async_trait]
    impl EscalationObjectionResponderTransport for ObjectionFake {
        fn model_spec(&self) -> &ReviewerModelSpec {
            &self.model
        }

        async fn complete(
            &self,
            prompt: &EscalationObjectionPrompt,
            _cancel: CancellationToken,
        ) -> std::result::Result<
            crate::approval::route_reviewer::ReviewerTransportOutput,
            ReviewerTransportError,
        > {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.prompts
                .lock()
                .expect("objection prompts")
                .push(serde_json::to_value(prompt).expect("serialize objection prompt"));
            Ok(crate::approval::route_reviewer::ReviewerTransportOutput {
                text: self.response.clone(),
                tool_trace: Vec::new(),
            })
        }
    }

    #[async_trait]
    impl EscalationReviewerTransport for EscalationFake {
        fn model_spec(&self) -> &ReviewerModelSpec {
            &self.model
        }

        async fn complete(
            &self,
            prompt: &EscalationReviewerPrompt,
            _tool_call_offset: usize,
            _cancel: CancellationToken,
        ) -> std::result::Result<
            crate::approval::route_reviewer::ReviewerTransportOutput,
            ReviewerTransportError,
        > {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.prompts
                .lock()
                .expect("escalation prompts")
                .push(serde_json::to_value(prompt).expect("serialize prompt"));
            Ok(crate::approval::route_reviewer::ReviewerTransportOutput {
                text: self.response.clone(),
                tool_trace: Vec::new(),
            })
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

    fn with_objection_answer(
        broker: RouteApprovalBroker,
        response: Value,
    ) -> (RouteApprovalBroker, Arc<ObjectionFake>) {
        let transport = Arc::new(ObjectionFake {
            model: model(),
            response: response.to_string(),
            calls: AtomicUsize::new(0),
            prompts: Mutex::new(Vec::new()),
        });
        let responder = Arc::new(
            EscalationObjectionResponder::new(
                model(),
                transport.clone(),
                ReviewerBudgetV1::escalation(),
                PersonalityAgentPromptContextHandle::new(&PromptContext::new(
                    "test PA system prompt".to_owned(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                )),
            )
            .expect("objection responder"),
        );
        (
            broker.with_escalation_objection_responder(responder),
            transport,
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
        assistant_contents(vec![PublicAssistantContent::Text {
            text: text.into(),
            wire_item_index: 0,
        }])
    }

    fn assistant_contents(content: Vec<PublicAssistantContent>) -> PublicMessage {
        PublicMessage::Assistant(PublicAssistantMessage {
            content,
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

    fn prior_tool_call(
        name: impl Into<String>,
        route: ToolInvocationRoute,
        arguments: Value,
    ) -> PublicMessage {
        tool_call_message(Uuid::now_v7().to_string(), name, route, arguments)
    }

    fn tool_call_message(
        id: impl Into<String>,
        name: impl Into<String>,
        route: ToolInvocationRoute,
        arguments: Value,
    ) -> PublicMessage {
        assistant_contents(vec![PublicAssistantContent::ToolCall {
            tool_call: ToolCall {
                id: id.into(),
                name: name.into(),
                route,
                arguments: serde_json::from_value(arguments).expect("validated arguments"),
            },
            wire_item_index: 0,
        }])
    }

    fn rejected_tool_call(name: impl Into<String>, error: ToolArgumentError) -> PublicMessage {
        assistant_contents(vec![PublicAssistantContent::RejectedToolCall {
            rejected: RejectedToolCall {
                id: Uuid::now_v7().to_string(),
                name: name.into(),
                error,
            },
            wire_item_index: 0,
        }])
    }

    fn tool_result_message(text: impl Into<String>) -> PublicMessage {
        tool_result_for("prior-call", "prior-tool", text, Value::Null)
    }

    fn tool_result_for(
        tool_call_id: impl Into<String>,
        tool_name: impl Into<String>,
        text: impl Into<String>,
        details: Value,
    ) -> PublicMessage {
        PublicMessage::ToolResult(ToolResultMessage {
            tool_call_id: tool_call_id.into(),
            tool_name: tool_name.into(),
            content: vec![UserContent::Text { text: text.into() }],
            details,
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
    async fn escalation_objection_holds_same_call_and_pa_proceed_reaches_human_with_exchange() {
        let (broker, _, _) = broker(
            json!({"outcome":"block","risk":"high","rationale":"unused"}),
            json!({
                "outcome":"block",
                "risk":"critical",
                "misunderstanding":"target may be broader than intended",
                "rationale":"reviewer-objection-sentinel"
            }),
        );
        let (broker, responder) = with_objection_answer(
            broker,
            json!({
                "outcome":"proceed",
                "reason":"pa-reason-sentinel"
            }),
        );
        let sealed = sealed(CapabilityClass::Mutate, ToolInvocationRoute::Elevated).await;
        let original_call_id = sealed.invocation().tool_call_id.clone();
        let original_evidence_digest = sealed.evidence_digest().to_hex();
        let outcome = broker
            .start_request(
                sealed,
                ToolInvocationRoute::Elevated,
                &[user_message("Please ask me before changing the title")],
                scope(),
                "run-held",
                "turn-held",
                CancellationToken::new(),
            )
            .await
            .expect("held objection flow");
        let RouteApprovalOutcome::Pending { mut pending } = outcome else {
            panic!("PA proceed must send the unchanged held call to Human")
        };
        assert_eq!(pending.request().tool_call_id, original_call_id);
        assert_eq!(
            pending.request().bound_evidence_digest,
            original_evidence_digest,
            "the held call must not be rebound or resubmitted"
        );
        let public =
            serde_json::to_string(&pending.request().public_request()).expect("Human request");
        assert!(public.contains("reviewer-objection-sentinel"));
        assert!(public.contains("pa-reason-sentinel"));
        let review = &pending.durable_evidence().escalation_review;
        assert_eq!(review.decision.outcome, EscalationReviewOutcome::Block);
        assert_eq!(
            review
                .pa_objection_response
                .as_ref()
                .and_then(|response| response.answer.as_ref())
                .map(|answer| answer.outcome),
            Some(EscalationObjectionOutcome::Proceed)
        );
        assert_eq!(responder.calls.load(Ordering::Relaxed), 1);
        let held_bound = pending.durable_evidence().bound.clone();

        let resolution = broker
            .resolve(
                &pending.request().id,
                command(CurrentCallDecision::ApproveOnce),
            )
            .await
            .expect("Human resolves held call");
        let CurrentCallResolution::Approved { grant, .. } = resolution else {
            panic!("Human approval is final authority for the held call")
        };
        assert_eq!(grant.evidence().tool_call_id, original_call_id);
        assert_eq!(
            grant
                .evidence()
                .escalation_review
                .as_ref()
                .and_then(|review| review.pa_objection_response.as_ref())
                .and_then(|response| response.answer.as_ref())
                .map(|answer| answer.reason.as_deref()),
            Some(Some("pa-reason-sentinel"))
        );
        let baseline_digest = authorization_evidence_digest(grant.evidence(), &held_bound)
            .expect("held exchange digest");
        let mut changed_exchange = grant.evidence().clone();
        changed_exchange
            .escalation_review
            .as_mut()
            .and_then(|review| review.pa_objection_response.as_mut())
            .and_then(|response| response.answer.as_mut())
            .expect("PA response evidence")
            .reason = Some("different optional reason".to_owned());
        let changed_digest = authorization_evidence_digest(&changed_exchange, &held_bound)
            .expect("changed held exchange digest");
        assert_ne!(baseline_digest, changed_digest);
        assert!(matches!(
            pending.receiver_mut().await.expect("waiter result"),
            WaiterResult::Resolved
        ));
    }

    #[tokio::test]
    async fn escalation_objection_pa_withdraw_ends_held_call_once_without_human_request() {
        let (broker, _, escalation) = broker(
            json!({"outcome":"block","risk":"high","rationale":"unused"}),
            json!({
                "outcome":"block",
                "risk":"high",
                "misunderstanding":null,
                "rationale":"do not send this"
            }),
        );
        let (broker, responder) =
            with_objection_answer(broker, json!({"outcome":"withdraw","reason":null}));
        let outcome = broker
            .start_request(
                sealed(CapabilityClass::Mutate, ToolInvocationRoute::Elevated).await,
                ToolInvocationRoute::Elevated,
                &[user_message("consider changing the title")],
                scope(),
                "run-withdraw",
                "turn-withdraw",
                CancellationToken::new(),
            )
            .await
            .expect("withdraw objection flow");
        let RouteApprovalOutcome::Denied { evidence, .. } = outcome else {
            panic!("withdraw must end the held call without asking Human")
        };
        assert_eq!(evidence.error_code(), "escalation_review_blocked");
        assert_eq!(escalation.calls.load(Ordering::Relaxed), 1);
        assert_eq!(responder.calls.load(Ordering::Relaxed), 1);
        assert!(!broker.any_pending());
    }

    #[tokio::test]
    async fn failed_pa_objection_answer_is_visible_to_human_and_does_not_withdraw_held_call() {
        let (broker, _, _) = broker(
            json!({"outcome":"block","risk":"high","rationale":"unused"}),
            json!({
                "outcome":"block",
                "risk":"high",
                "misunderstanding":null,
                "rationale":"reviewer objection sentinel"
            }),
        );
        let (broker, responder) = with_objection_answer(broker, json!({"not":"a verdict"}));
        let outcome = broker
            .start_request(
                sealed(CapabilityClass::Mutate, ToolInvocationRoute::Elevated).await,
                ToolInvocationRoute::Elevated,
                &[user_message("still ask the Human")],
                scope(),
                "run-objection-failure",
                "turn-objection-failure",
                CancellationToken::new(),
            )
            .await
            .expect("objection failure must remain a Human decision");
        let RouteApprovalOutcome::Pending { pending } = outcome else {
            panic!("a missing PA objection answer must not withdraw the held call")
        };
        let public = pending.request().public_request();
        assert!(
            public
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("could not be obtained"))
        );
        let review = &pending.durable_evidence().escalation_review;
        assert!(
            review
                .pa_objection_failure
                .as_deref()
                .is_some_and(|failure| failure.contains("malformed_exhausted"))
        );
        assert_eq!(
            review
                .pa_objection_response
                .as_ref()
                .map(|response| response.budget.terminal),
            Some(ReviewerTerminalClass::MalformedExhausted)
        );
        assert_eq!(responder.calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn objection_lane_keeps_redaction_and_visible_user_omission_markers() {
        const SECRET: &str = "sk-objection-secret-must-not-reach-the-pa";
        let (broker, _, _) = broker(
            json!({"outcome":"block","risk":"high","rationale":"unused"}),
            json!({
                "outcome":"block",
                "risk":"high",
                "misunderstanding":null,
                "rationale":"reviewer objection sentinel"
            }),
        );
        let (broker, responder) =
            with_objection_answer(broker, json!({"outcome":"proceed","reason":null}));
        let mut transcript = (0..(MAX_CONTEXT_USER_MESSAGES + 1))
            .map(|index| user_message(format!("historical human turn {index}")))
            .collect::<Vec<_>>();
        transcript.push(user_message(format!("latest human secret: {SECRET}")));

        let outcome = broker
            .start_request(
                sealed(CapabilityClass::Mutate, ToolInvocationRoute::Elevated).await,
                ToolInvocationRoute::Elevated,
                &transcript,
                scope(),
                "run-objection-redaction",
                "turn-objection-redaction",
                CancellationToken::new(),
            )
            .await
            .expect("objection prompt is bounded and redacted");
        assert!(matches!(outcome, RouteApprovalOutcome::Pending { .. }));

        let prompts = responder.prompts.lock().expect("objection prompts");
        assert_eq!(prompts.len(), 1);
        let encoded = prompts[0].to_string();
        assert!(encoded.contains(REVIEW_TRUNCATION_MARKER));
        assert!(encoded.contains("[REDACTED:api_key]"));
        assert!(!encoded.contains(SECRET));
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
    fn bounded_transcript_omits_oldest_content_keeps_latest_human_and_marks_omissions() {
        let mut transcript = (0..14)
            .map(|index| user_message(format!("user-turn<{index}>")))
            .collect::<Vec<_>>();
        transcript.insert(4, assistant_message("assistant-text-must-not-appear"));
        transcript.insert(9, tool_result_message("tool-result-must-not-appear"));
        let bounded =
            bounded_reviewer_transcript(&transcript, &Redactor::v1(), "pending-call-not-present")
                .expect("bounded transcript");
        let value = serde_json::to_value(&bounded).expect("bounded transcript");
        let encoded = value.to_string();

        assert!(!encoded.contains("user-turn<0>"));
        assert!(encoded.contains("user-turn<13>"));
        assert!(!encoded.contains("user-turn<1>"));
        assert!(encoded.contains("user-turn<2>"));
        assert!(encoded.contains("omitted_user_turns"));
        assert!(encoded.contains(REVIEW_TRUNCATION_MARKER));
        assert!(encoded.contains("assistant-text-must-not-appear"));
        assert!(!encoded.contains("tool-result-must-not-appear"));
        assert!(encoded.contains("omitted_orphan_tool_results"));
        assert_eq!(
            value["entries"]
                .as_array()
                .expect("transcript entries")
                .iter()
                .find(|entry| entry["kind"] == "orphan_tool_result_omission")
                .expect("orphan omission marker")["omitted_orphan_tool_results"],
            1
        );
    }

    #[test]
    fn bounded_transcript_selects_settled_tool_pairs_atomically_under_result_pressure() {
        const SETTLED_PAIRS: usize = 12;
        let mut transcript = vec![user_message("Inspect the history, then perform the update")];
        for index in 0..SETTLED_PAIRS {
            let id = format!("large-call-{index}");
            let tool = format!("large_tool_{index}");
            transcript.push(tool_call_message(
                &id,
                &tool,
                ToolInvocationRoute::Normal,
                json!({"index": index}),
            ));
            transcript.push(tool_result_for(
                id,
                tool,
                format!(
                    "result-{index}:{}",
                    "x".repeat(MAX_CONTEXT_TOOL_RESULT_CHARS)
                ),
                Value::Null,
            ));
        }
        transcript.push(tool_call_message(
            "historical-call-without-result",
            "unfinished_history",
            ToolInvocationRoute::Normal,
            json!({}),
        ));
        transcript.push(tool_call_message(
            "pending-call",
            "pending_update",
            ToolInvocationRoute::Elevated,
            json!({"record":"target"}),
        ));

        let bounded = bounded_reviewer_transcript(&transcript, &Redactor::v1(), "pending-call")
            .expect("pair-bounded reviewer transcript");
        let mut selected_call_ids = HashSet::new();
        let mut selected_result_ids = HashSet::new();
        let mut omitted_tool_calls = 0;
        let mut omitted_tool_results = 0;
        let mut reached_tool_omission = false;
        for entry in &bounded.entries {
            match entry {
                ReviewerTranscriptEntry::Assistant { tool_calls, .. } => {
                    assert!(
                        !reached_tool_omission || tool_calls.is_empty(),
                        "native calls must precede tool omission evidence"
                    );
                    selected_call_ids.extend(tool_calls.iter().map(|call| call.id.clone()));
                }
                ReviewerTranscriptEntry::ToolResult {
                    tool_call_id: Some(tool_call_id),
                    ..
                } => {
                    assert!(
                        !reached_tool_omission,
                        "native results must precede tool omission evidence"
                    );
                    selected_result_ids.insert(tool_call_id.clone());
                }
                ReviewerTranscriptEntry::ToolCallOmission {
                    omitted_tool_calls: omitted,
                    ..
                } => {
                    reached_tool_omission = true;
                    omitted_tool_calls += *omitted;
                }
                ReviewerTranscriptEntry::ToolResultOmission {
                    omitted_tool_results: omitted,
                    ..
                } => {
                    reached_tool_omission = true;
                    omitted_tool_results += *omitted;
                }
                _ => {}
            }
        }

        assert!(!selected_call_ids.is_empty());
        assert!(selected_call_ids.len() < SETTLED_PAIRS);
        assert_eq!(selected_call_ids, selected_result_ids);
        assert!(selected_call_ids.contains("large-call-11"));
        assert!(!selected_call_ids.contains("large-call-0"));
        assert!(!selected_call_ids.contains("historical-call-without-result"));
        assert!(!selected_call_ids.contains("pending-call"));
        assert_eq!(
            omitted_tool_calls,
            SETTLED_PAIRS + 1 - selected_call_ids.len()
        );
        assert_eq!(
            omitted_tool_results,
            SETTLED_PAIRS - selected_result_ids.len()
        );
    }

    #[test]
    fn bounded_transcript_includes_redacted_capped_tool_history_with_separate_omissions() {
        const SECRET: &str = "tool-argument-secret-must-be-redacted";
        const SECRET_RESULT: &str = "tool-result-secret-must-be-redacted";
        const POST_PENDING_RESULT: &str = "post-pending-result-must-not-appear";
        let mut transcript = (0..14)
            .map(|index| user_message(format!("user-turn<{index}>")))
            .collect::<Vec<_>>();
        for index in 0..42 {
            let id = format!("prior-call-{index}");
            let tool = format!("prior_tool_{index}");
            transcript.push(tool_call_message(
                &id,
                &tool,
                ToolInvocationRoute::Normal,
                json!({"index": index}),
            ));
            transcript.push(tool_result_for(
                id,
                tool,
                format!("result-{index}"),
                json!({"index": index}),
            ));
        }
        transcript.push(rejected_tool_call(
            "broken_tool",
            ToolArgumentError::SchemaViolation,
        ));
        transcript.push(tool_call_message(
            "secret-call",
            "secret_tool",
            ToolInvocationRoute::Elevated,
            json!({"api_key": SECRET, "payload": "x".repeat(4_000)}),
        ));
        transcript.push(tool_result_for(
            "secret-call",
            "secret_tool",
            format!("api_key={SECRET_RESULT}\npayload={}", "y".repeat(4_000)),
            json!({"api_key": SECRET_RESULT}),
        ));
        transcript.push(tool_call_message(
            "pending-call",
            "pending-tool-must-not-appear",
            ToolInvocationRoute::Elevated,
            json!({"pending":"arguments-must-not-appear"}),
        ));
        transcript.push(prior_tool_call(
            "later-tool-must-not-appear",
            ToolInvocationRoute::Normal,
            json!({"later":"arguments-must-not-appear"}),
        ));
        transcript.push(tool_result_for(
            "pending-call",
            "pending-tool-must-not-appear",
            POST_PENDING_RESULT,
            Value::Null,
        ));

        let bounded = bounded_reviewer_transcript(&transcript, &Redactor::v1(), "pending-call")
            .expect("bounded reviewer transcript");
        let value = serde_json::to_value(&bounded).expect("serialize bounded transcript");
        let encoded = value.to_string();

        assert_eq!(value["schema_version"], REVIEW_TRANSCRIPT_SCHEMA_VERSION_V7);
        assert!(encoded.contains("secret_tool"));
        assert!(encoded.contains("\"route\":\"elevated\""));
        assert!(encoded.contains("[REDACTED:secret]"));
        assert!(!encoded.contains(SECRET));
        assert!(encoded.contains("json_prefix"));
        assert!(encoded.contains("omitted_characters"));
        assert!(encoded.contains(REVIEW_TRUNCATION_MARKER));
        assert!(encoded.contains("rejected_tool_call"));
        assert!(encoded.contains("schema_violation"));
        assert!(encoded.contains("omitted_user_turns"));
        assert!(encoded.contains("omitted_tool_calls"));
        assert!(encoded.contains("tool_result"));
        assert!(encoded.contains("omitted_tool_results"));
        assert!(encoded.contains("result-41"));
        assert!(encoded.contains("[REDACTED:secret]"));
        assert!(!encoded.contains(SECRET_RESULT));
        assert!(!encoded.contains("prior_tool_0"));
        assert!(!encoded.contains("pending-tool-must-not-appear"));
        assert!(!encoded.contains("later-tool-must-not-appear"));
        assert!(!encoded.contains("arguments-must-not-appear"));
        assert!(!encoded.contains(POST_PENDING_RESULT));

        let entries = value["entries"].as_array().expect("transcript entries");
        let selected_tool_calls = entries
            .iter()
            .map(|entry| {
                entry["tool_calls"].as_array().map_or(0, Vec::len)
                    + entry["rejected_tool_calls"].as_array().map_or(0, Vec::len)
            })
            .sum::<usize>();
        assert!(selected_tool_calls <= MAX_CONTEXT_TOOL_CALLS);
        let selected_tool_chars = entries
            .iter()
            .flat_map(|entry| {
                entry["tool_calls"].as_array().into_iter().flatten().chain(
                    entry["rejected_tool_calls"]
                        .as_array()
                        .into_iter()
                        .flatten(),
                )
            })
            .map(|entry| entry.to_string().chars().count())
            .sum::<usize>();
        assert!(selected_tool_chars <= MAX_CONTEXT_TOOL_TOTAL_CHARS);
        let selected_results = entries
            .iter()
            .filter(|entry| entry["kind"] == "tool_result")
            .collect::<Vec<_>>();
        assert!(selected_results.len() <= MAX_CONTEXT_TOOL_RESULTS);
        assert!(selected_results.iter().all(|entry| {
            entry["content"]
                .as_str()
                .expect("tool result content")
                .chars()
                .count()
                <= MAX_CONTEXT_TOOL_RESULT_CHARS
        }));
        assert!(
            selected_results
                .iter()
                .map(|entry| entry.to_string().chars().count())
                .sum::<usize>()
                <= MAX_CONTEXT_TOOL_RESULT_TOTAL_CHARS
        );
        assert!(
            entries
                .iter()
                .all(|entry| { entry["kind"] != "tool_result" || entry.get("text").is_none() })
        );
        let secret_arguments = entries
            .iter()
            .flat_map(|entry| entry["tool_calls"].as_array().into_iter().flatten())
            .find(|call| call["tool"] == "secret_tool")
            .and_then(|call| call.get("arguments"))
            .expect("latest tool arguments");
        assert!(secret_arguments.to_string().chars().count() <= MAX_CONTEXT_TOOL_ARGUMENT_CHARS);
    }

    #[test]
    fn bounded_current_turn_keeps_assistant_text_and_settled_sibling_result_before_pending_call() {
        let transcript = vec![
            user_message("Please inspect, then update the exact record"),
            assistant_contents(vec![
                PublicAssistantContent::Text {
                    text: "I checked the target and will now request the update".to_owned(),
                    wire_item_index: 0,
                },
                PublicAssistantContent::ToolCall {
                    tool_call: ToolCall {
                        id: "sibling-read".to_owned(),
                        name: "workspace_list".to_owned(),
                        route: ToolInvocationRoute::Normal,
                        arguments: serde_json::from_value(json!({"limit":1}))
                            .expect("sibling args"),
                    },
                    wire_item_index: 1,
                },
                PublicAssistantContent::ToolCall {
                    tool_call: ToolCall {
                        id: "pending-update".to_owned(),
                        name: "app_action".to_owned(),
                        route: ToolInvocationRoute::Elevated,
                        arguments: serde_json::from_value(json!({"title":"new"}))
                            .expect("pending args"),
                    },
                    wire_item_index: 2,
                },
            ]),
            tool_result_for(
                "sibling-read",
                "workspace_list",
                "exact target exists",
                json!({"count":1}),
            ),
        ];
        let bounded = bounded_reviewer_transcript(&transcript, &Redactor::v1(), "pending-update")
            .expect("bounded current turn");
        let encoded = serde_json::to_string(&bounded).expect("current turn evidence");
        assert!(encoded.contains("I checked the target"));
        assert!(encoded.contains("sibling-read"));
        assert!(encoded.contains("exact target exists"));
        assert!(!encoded.contains("pending-update"));
        assert!(!encoded.contains("\"title\":\"new\""));
    }

    #[test]
    fn bounded_transcript_redacts_human_assistant_tool_call_and_tool_result_lanes() {
        let transcript = vec![
            user_message("Human context api_key=human-secret-value"),
            assistant_contents(vec![
                PublicAssistantContent::Text {
                    text: "Assistant context access_token=assistant-secret-value".to_owned(),
                    wire_item_index: 0,
                },
                PublicAssistantContent::ToolCall {
                    tool_call: ToolCall {
                        id: "secret-read".to_owned(),
                        name: "workspace_list".to_owned(),
                        route: ToolInvocationRoute::Normal,
                        arguments: serde_json::from_value(
                            json!({"api_key":"tool-argument-secret-value"}),
                        )
                        .expect("secret args"),
                    },
                    wire_item_index: 1,
                },
            ]),
            tool_result_for(
                "secret-read",
                "workspace_list",
                "secret=tool-result-secret-value",
                json!({"refresh_token":"structured-result-secret-value"}),
            ),
        ];
        let bounded =
            bounded_reviewer_transcript(&transcript, &Redactor::v1(), "pending-call-not-present")
                .expect("redacted transcript");
        let encoded = serde_json::to_string(&bounded).expect("redacted evidence");
        for secret in [
            "human-secret-value",
            "assistant-secret-value",
            "tool-argument-secret-value",
            "tool-result-secret-value",
            "structured-result-secret-value",
        ] {
            assert!(!encoded.contains(secret), "secret leaked: {secret}");
        }
        assert!(encoded.matches("[REDACTED:secret]").count() >= 5);
    }

    #[tokio::test]
    async fn route_reviewers_receive_role_preserved_text_and_exact_action() {
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
            assert!(encoded.contains(ASSISTANT_SENTINEL));
            assert!(!encoded.contains(TOOL_RESULT_SENTINEL));
            assert!(encoded.contains("\"personality_agent_id\":\"agent-1\""));
            assert!(!encoded.contains("human_display_name"));
            assert!(!encoded.contains("personality_agent_display_name"));
            for forbidden in ["context_version", "tenant-1", "human-1"] {
                assert!(
                    !encoded.contains(forbidden),
                    "leaked reviewer field: {forbidden}"
                );
            }
        }
    }
}
