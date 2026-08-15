//! Exact-call execution authority sealed around an app-owned bound operation.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::{
    approval::{
        route_policy::{PolicySnapshot, RoutePolicy},
        route_reviewer::{
            EscalationReviewEvidence, EscalationReviewOutcome, ExecutionReviewEvidence,
            ExecutionReviewOutcome,
        },
    },
    provider::types::ToolInvocationRoute,
    tools::{BoundToolInvocation, SealedBoundToolInvocation},
};

pub const AUTHORIZATION_EVIDENCE_VERSION_V1: &str = "tool-execution-authorization-evidence/v1";
pub const DENIAL_EVIDENCE_VERSION_V1: &str = "tool-execution-denial-evidence/v1";
pub const HUMAN_DECISION_PROVENANCE_VERSION_V1: u8 = 1;
const AUTHORIZATION_DIGEST_DOMAIN: &[u8] = b"sumi-tool-authorization-evidence/v1\0";
const DENIAL_DIGEST_DOMAIN: &[u8] = b"sumi-tool-denial-evidence/v1\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionAuthorityProvenance {
    AgentOwn,
    AgentOwnWithHumanConsent,
    /// Reserved for a future authenticated connection-owner binding. The v1
    /// broker never constructs an executable grant with this provenance.
    HumanAccountOneShot,
}

impl ExecutionAuthorityProvenance {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AgentOwn => "agent_own",
            Self::AgentOwnWithHumanConsent => "agent_own_with_human_consent",
            Self::HumanAccountOneShot => "human_account_one_shot",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyDecisionRecord {
    Allow,
    Deny,
    Unmatched,
    ElevatedPreflight,
    Unavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentCallDecision {
    ApproveOnce,
    DenyOnce,
}

/// Authenticated facts supplied by the durable command path. The broker, not
/// the client, combines these facts with its private pending operation to
/// construct exact Human decision evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthenticatedCurrentCallDecision {
    pub command_id: String,
    pub command_seq: u64,
    pub tenant_id: String,
    pub personality_agent_id: String,
    pub human_principal_id: String,
    pub decision: CurrentCallDecision,
    pub received_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HumanAuthorizationContextV1<'a> {
    pub request_id: &'a str,
    pub command_id: &'a str,
    pub command_seq: u64,
    pub tenant_id: &'a str,
    pub personality_agent_id: &'a str,
    pub human_principal_id: &'a str,
    pub decision: CurrentCallDecision,
    pub received_at: DateTime<Utc>,
    pub tool_call_id: &'a str,
    pub route: ToolInvocationRoute,
    pub proposal_digest: &'a str,
    pub descriptor_digest: &'a str,
    pub bound_evidence_digest: &'a str,
    pub policy_source_digest: &'a str,
    pub run_id: &'a str,
    pub turn_id: &'a str,
}

impl PolicyDecisionRecord {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
            Self::Unmatched => "unmatched",
            Self::ElevatedPreflight => "elevated_preflight",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanDecisionEvidence {
    pub request_id: String,
    pub command_id: String,
    pub command_seq: u64,
    pub tenant_id: String,
    pub personality_agent_id: String,
    pub human_principal_id: String,
    pub provenance_version: u8,
    pub decision: CurrentCallDecision,
    pub received_at: DateTime<Utc>,
    pub authorization_context_digest: String,
}

impl HumanDecisionEvidence {
    pub fn from_context(context: HumanAuthorizationContextV1<'_>) -> Result<Self> {
        let authorization_context_digest = authorization_context_digest(&context)?;
        Ok(Self {
            request_id: context.request_id.to_owned(),
            command_id: context.command_id.to_owned(),
            command_seq: context.command_seq,
            tenant_id: context.tenant_id.to_owned(),
            personality_agent_id: context.personality_agent_id.to_owned(),
            human_principal_id: context.human_principal_id.to_owned(),
            provenance_version: HUMAN_DECISION_PROVENANCE_VERSION_V1,
            decision: context.decision,
            received_at: context.received_at,
            authorization_context_digest,
        })
    }

    pub fn validate_for(&self, context: HumanAuthorizationContextV1<'_>) -> Result<()> {
        if self.request_id != context.request_id
            || self.command_id != context.command_id
            || self.command_seq != context.command_seq
            || self.tenant_id != context.tenant_id
            || self.personality_agent_id != context.personality_agent_id
            || self.human_principal_id != context.human_principal_id
            || self.provenance_version != HUMAN_DECISION_PROVENANCE_VERSION_V1
            || self.decision != context.decision
            || self.received_at != context.received_at
            || self.authorization_context_digest != authorization_context_digest(&context)?
        {
            bail!("Human decision evidence does not match its exact authorization context");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolExecutionAuthorizationEvidence {
    pub evidence_version: String,
    pub grant_id: String,
    pub tool_call_id: String,
    pub route: ToolInvocationRoute,
    pub proposal_digest: String,
    pub descriptor_digest: String,
    pub bound_evidence_digest: String,
    pub policy: PolicySnapshot,
    pub policy_decision: PolicyDecisionRecord,
    pub resolved_authority: ExecutionAuthorityProvenance,
    pub execution_review: Option<ExecutionReviewEvidence>,
    pub escalation_review: Option<EscalationReviewEvidence>,
    pub human_decision: Option<HumanDecisionEvidence>,
}

impl ToolExecutionAuthorizationEvidence {
    pub fn validate(&self, bound: &BoundToolInvocation) -> Result<()> {
        validate_common_identity(
            &self.evidence_version,
            AUTHORIZATION_EVIDENCE_VERSION_V1,
            &self.tool_call_id,
            &self.proposal_digest,
            &self.descriptor_digest,
            &self.bound_evidence_digest,
            bound,
        )?;
        if self.grant_id.trim().is_empty() || self.policy.source_digest.trim().is_empty() {
            bail!("tool authorization evidence has an empty grant or policy identity");
        }
        match (
            self.route,
            self.policy_decision,
            self.resolved_authority,
            self.execution_review.as_ref(),
            self.escalation_review.as_ref(),
            self.human_decision.as_ref(),
        ) {
            (
                ToolInvocationRoute::Normal,
                PolicyDecisionRecord::Allow,
                ExecutionAuthorityProvenance::AgentOwn,
                None,
                None,
                None,
            ) => {}
            (
                ToolInvocationRoute::Normal,
                PolicyDecisionRecord::Unmatched,
                ExecutionAuthorityProvenance::AgentOwn,
                Some(review),
                None,
                None,
            ) if review.decision.outcome == ExecutionReviewOutcome::Allow => {}
            (
                ToolInvocationRoute::Elevated,
                PolicyDecisionRecord::ElevatedPreflight,
                ExecutionAuthorityProvenance::AgentOwnWithHumanConsent,
                None,
                Some(review),
                Some(human),
            ) if review.decision.outcome == EscalationReviewOutcome::AskHuman
                && human.decision == CurrentCallDecision::ApproveOnce
                && !human.request_id.trim().is_empty()
                && !human.command_id.trim().is_empty()
                && !human.authorization_context_digest.trim().is_empty() => {}
            _ => bail!("tool authorization evidence has an invalid route/decision tuple"),
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolExecutionDenialEvidence {
    pub evidence_version: String,
    pub tool_call_id: String,
    pub route: ToolInvocationRoute,
    pub proposal_digest: String,
    pub descriptor_digest: String,
    pub bound_evidence_digest: String,
    pub policy: PolicySnapshot,
    pub policy_decision: PolicyDecisionRecord,
    pub execution_review: Option<ExecutionReviewEvidence>,
    pub escalation_review: Option<EscalationReviewEvidence>,
    pub reason: String,
}

impl ToolExecutionDenialEvidence {
    pub fn validate(&self, bound: &BoundToolInvocation) -> Result<()> {
        validate_common_identity(
            &self.evidence_version,
            DENIAL_EVIDENCE_VERSION_V1,
            &self.tool_call_id,
            &self.proposal_digest,
            &self.descriptor_digest,
            &self.bound_evidence_digest,
            bound,
        )?;
        if self.reason.trim().is_empty() || self.policy.source_digest.trim().is_empty() {
            bail!("tool denial evidence has an empty reason or policy identity");
        }
        match (
            self.route,
            self.policy_decision,
            self.execution_review.as_ref(),
            self.escalation_review.as_ref(),
        ) {
            (
                ToolInvocationRoute::Normal | ToolInvocationRoute::Elevated,
                PolicyDecisionRecord::Deny | PolicyDecisionRecord::Unavailable,
                None,
                None,
            ) => {}
            (ToolInvocationRoute::Normal, PolicyDecisionRecord::Unmatched, Some(review), None)
                if review.decision.outcome == ExecutionReviewOutcome::Block => {}
            (
                ToolInvocationRoute::Elevated,
                PolicyDecisionRecord::ElevatedPreflight,
                None,
                Some(review),
            ) if review.decision.outcome == EscalationReviewOutcome::Block => {}
            _ => bail!("tool denial evidence has an invalid route/decision tuple"),
        }
        Ok(())
    }

    pub const fn error_code(&self) -> &'static str {
        match self.policy_decision {
            PolicyDecisionRecord::Deny => "policy_denied",
            PolicyDecisionRecord::Unavailable => "policy_unavailable",
            PolicyDecisionRecord::Unmatched => "execution_review_blocked",
            PolicyDecisionRecord::ElevatedPreflight => "escalation_review_blocked",
            PolicyDecisionRecord::Allow => "invalid_authorization_denial",
        }
    }
}

fn validate_common_identity(
    observed_version: &str,
    expected_version: &str,
    tool_call_id: &str,
    proposal_digest: &str,
    descriptor_digest: &str,
    bound_evidence_digest: &str,
    bound: &BoundToolInvocation,
) -> Result<()> {
    if observed_version != expected_version
        || tool_call_id != bound.tool_call_id
        || proposal_digest != bound.proposal_digest.to_hex()
        || descriptor_digest != bound.descriptor_digest.to_hex()
        || bound_evidence_digest != bound.evidence_digest()?.to_hex()
    {
        bail!("tool authority evidence does not match the bound invocation");
    }
    if bound.schema_version != crate::tools::bound::BOUND_TOOL_INVOCATION_SCHEMA_VERSION {
        bail!("unknown bound invocation schema version");
    }
    if bound.recompute_descriptor_digest()? != bound.descriptor_digest {
        bail!("bound invocation descriptor digest is invalid");
    }
    Ok(())
}

pub fn authorization_context_digest(value: &impl Serialize) -> Result<String> {
    let encoded = serde_json::to_vec(value).context("serialize Human authorization context")?;
    Ok(digest(AUTHORIZATION_DIGEST_DOMAIN, &encoded))
}

pub fn authorization_evidence_digest(
    evidence: &ToolExecutionAuthorizationEvidence,
    bound: &BoundToolInvocation,
) -> Result<String> {
    evidence.validate(bound)?;
    let encoded = serde_json::to_vec(evidence).context("serialize authorization evidence")?;
    Ok(digest(AUTHORIZATION_DIGEST_DOMAIN, &encoded))
}

pub fn denial_evidence_digest(
    evidence: &ToolExecutionDenialEvidence,
    bound: &BoundToolInvocation,
) -> Result<String> {
    evidence.validate(bound)?;
    let encoded = serde_json::to_vec(evidence).context("serialize denial evidence")?;
    Ok(digest(DENIAL_DIGEST_DOMAIN, &encoded))
}

fn digest(domain: &[u8], encoded: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update((encoded.len() as u64).to_be_bytes());
    digest.update(encoded);
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) type ApprovalClock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// Opaque one-call grant. It carries both durable evidence and the live
/// same-process registry seal, but only the commit barrier can turn it into an
/// execution permit.
pub(crate) struct ExecutableGrant {
    policy: Arc<RwLock<RoutePolicy>>,
    clock: ApprovalClock,
    sealed: SealedBoundToolInvocation,
    route: ToolInvocationRoute,
    run_id: String,
    turn_id: String,
    evidence: ToolExecutionAuthorizationEvidence,
}

impl std::fmt::Debug for ExecutableGrant {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutableGrant")
            .field("grant_id", &self.evidence.grant_id)
            .field("tool_call_id", &self.evidence.tool_call_id)
            .field("route", &self.route)
            .field("run_id", &self.run_id)
            .field("turn_id", &self.turn_id)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GrantRevalidation {
    Valid,
    Reauthorize,
}

pub(crate) struct GrantLease<'a> {
    _policy: RwLockReadGuard<'a, RoutePolicy>,
}

impl ExecutableGrant {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        policy: Arc<RwLock<RoutePolicy>>,
        clock: ApprovalClock,
        sealed: SealedBoundToolInvocation,
        route: ToolInvocationRoute,
        run_id: String,
        turn_id: String,
        evidence: ToolExecutionAuthorizationEvidence,
    ) -> Result<Self> {
        evidence.validate(sealed.invocation())?;
        Ok(Self {
            policy,
            clock,
            sealed,
            route,
            run_id,
            turn_id,
            evidence,
        })
    }

    pub(crate) async fn authorize(
        &self,
        tool_call_id: &str,
        tool_name: &str,
        route: ToolInvocationRoute,
        run_id: &str,
        turn_id: &str,
    ) -> Result<(
        GrantRevalidation,
        Option<GrantLease<'_>>,
        BoundToolInvocation,
        ToolExecutionAuthorizationEvidence,
    )> {
        let bound = self.sealed.invocation();
        if tool_call_id != bound.tool_call_id
            || tool_name != bound.tool_name
            || route != self.route
            || run_id != self.run_id
            || turn_id != self.turn_id
            || self.evidence.route != route
        {
            bail!("executable grant does not match ToolExecutionStart");
        }
        self.evidence.validate(bound)?;
        let now = (self.clock)();
        let policy = self.policy.read().await;
        if !policy.snapshot_matches(&self.evidence.policy, now) {
            return Ok((
                GrantRevalidation::Reauthorize,
                None,
                bound.clone(),
                self.evidence.clone(),
            ));
        }
        Ok((
            GrantRevalidation::Valid,
            Some(GrantLease { _policy: policy }),
            bound.clone(),
            self.evidence.clone(),
        ))
    }

    pub(crate) const fn route(&self) -> ToolInvocationRoute {
        self.route
    }

    pub(crate) fn into_authorized_bound(self) -> AuthorizedBoundInvocation {
        AuthorizedBoundInvocation {
            sealed: self.sealed,
        }
    }

    #[cfg(test)]
    pub(crate) fn evidence(&self) -> &ToolExecutionAuthorizationEvidence {
        &self.evidence
    }
}

/// Same-process execution permit released only after the durable start
/// transaction commits. Its live registry seal is owned and consumed exactly
/// once; serializable evidence cannot recreate this value.
pub(crate) struct AuthorizedBoundInvocation {
    sealed: SealedBoundToolInvocation,
}

impl AuthorizedBoundInvocation {
    pub(crate) fn into_sealed(self) -> SealedBoundToolInvocation {
        self.sealed
    }

    pub(crate) fn tool_call_id(&self) -> &str {
        &self.sealed.invocation().tool_call_id
    }

    pub(crate) fn tool_name(&self) -> &str {
        &self.sealed.invocation().tool_name
    }
}
