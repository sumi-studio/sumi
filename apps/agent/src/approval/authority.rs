//! Exact-call execution authority sealed around an app-owned bound operation.

use std::{future::Future, sync::Arc};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::{
    approval::{
        route_policy::{PolicySnapshot, PolicySourceState, RoutePolicy},
        route_reviewer::{
            EscalationReviewEvidence, EscalationReviewOutcome, ExecutionReviewEvidence,
            ExecutionReviewOutcome, RiskLevel,
        },
    },
    provider::types::ToolInvocationRoute,
    tools::{
        AuthorizedBoundRegistryAccess, BoundToolInvocation, DescribeError,
        SealedBoundToolInvocation,
    },
};

pub const AUTHORIZATION_EVIDENCE_VERSION_V1: &str = "tool-execution-authorization-evidence/v1";
pub const DENIAL_EVIDENCE_VERSION_V1: &str = "tool-execution-denial-evidence/v1";
pub const HUMAN_DECISION_PROVENANCE_VERSION_V1: u8 = 1;
const AUTHORIZATION_DIGEST_DOMAIN: &[u8] = b"sumi-tool-authorization-evidence/v1\0";
const DENIAL_DIGEST_DOMAIN: &[u8] = b"sumi-tool-denial-evidence/v1\0";
const EXECUTOR_AUTHORIZATION_PROJECTION_DIGEST_DOMAIN: &[u8] =
    b"sumi-executor-authorization-projection/v1\0";
const EXECUTOR_GRANT_DIGEST_DOMAIN: &[u8] = b"sumi-executor-grant/v1\0";
const REVIEWER_READ_AUTHORITY_DOMAIN: &[u8] = b"sumi-reviewer-read-authority/v1\0";
const EXECUTOR_AUTHORIZATION_PROJECTION_VERSION: u8 = 1;

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

/// Deliberately narrow authorization evidence that may influence an Executor
/// token. Exact arguments, resource identifiers, principals, Human command
/// identities, reviewer free-form text, and digests derived from those hidden
/// values stay inside the runtime's already-validated grant.
#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct ExecutorAuthorizationProjection {
    version: u8,
    route: ToolInvocationRoute,
    policy_source: ExecutorPolicySourceProjection,
    policy_decision: PolicyDecisionRecord,
    resolved_authority: ExecutionAuthorityProvenance,
    execution_review: Option<ExecutorExecutionReviewProjection>,
    escalation_review: Option<ExecutorEscalationReviewProjection>,
    human_decision: Option<ExecutorHumanDecisionProjection>,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum ExecutorPolicySourceProjection {
    BaselineOnly,
    VerifiedOverlay,
    RequiredUnavailable,
}

#[derive(Clone, Copy, Serialize)]
#[serde(deny_unknown_fields)]
struct ExecutorExecutionReviewProjection {
    outcome: ExecutionReviewOutcome,
    risk: RiskLevel,
}

#[derive(Clone, Copy, Serialize)]
#[serde(deny_unknown_fields)]
struct ExecutorEscalationReviewProjection {
    outcome: EscalationReviewOutcome,
    risk: RiskLevel,
}

#[derive(Clone, Copy, Serialize)]
#[serde(deny_unknown_fields)]
struct ExecutorHumanDecisionProjection {
    provenance_version: u8,
    decision: CurrentCallDecision,
}

fn executor_authorization_projection(
    evidence: &ToolExecutionAuthorizationEvidence,
) -> ExecutorAuthorizationProjection {
    ExecutorAuthorizationProjection {
        version: EXECUTOR_AUTHORIZATION_PROJECTION_VERSION,
        route: evidence.route,
        policy_source: match &evidence.policy.source {
            PolicySourceState::BaselineOnly { .. } => ExecutorPolicySourceProjection::BaselineOnly,
            PolicySourceState::VerifiedOverlay { .. } => {
                ExecutorPolicySourceProjection::VerifiedOverlay
            }
            PolicySourceState::RequiredUnavailable { .. } => {
                ExecutorPolicySourceProjection::RequiredUnavailable
            }
        },
        policy_decision: evidence.policy_decision,
        resolved_authority: evidence.resolved_authority,
        execution_review: evidence.execution_review.as_ref().map(|review| {
            ExecutorExecutionReviewProjection {
                outcome: review.decision.outcome,
                risk: review.decision.risk,
            }
        }),
        escalation_review: evidence.escalation_review.as_ref().map(|review| {
            ExecutorEscalationReviewProjection {
                outcome: review.decision.outcome,
                risk: review.decision.risk,
            }
        }),
        human_decision: evidence.human_decision.as_ref().map(|decision| {
            ExecutorHumanDecisionProjection {
                provenance_version: decision.provenance_version,
                decision: decision.decision,
            }
        }),
    }
}

pub(crate) fn executor_authorization_projection_digest(
    evidence: &ToolExecutionAuthorizationEvidence,
    bound: &BoundToolInvocation,
) -> Result<String> {
    evidence.validate(bound)?;
    let encoded = serde_json::to_vec(&executor_authorization_projection(evidence))
        .context("serialize Executor-safe authorization projection")?;
    Ok(digest(
        EXECUTOR_AUTHORIZATION_PROJECTION_DIGEST_DOMAIN,
        &encoded,
    ))
}

pub(crate) fn executor_grant_digest(grant_id: &str) -> Result<String> {
    if grant_id.trim().is_empty() {
        bail!("tool authorization evidence has an empty grant identity");
    }
    Ok(digest(EXECUTOR_GRANT_DIGEST_DOMAIN, grant_id.as_bytes()))
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
    executor_authorization_projection_digest: String,
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
        let executor_authorization_projection_digest =
            executor_authorization_projection_digest(&evidence, sealed.invocation())?;
        Ok(Self {
            policy,
            clock,
            sealed,
            route,
            run_id,
            turn_id,
            evidence,
            executor_authorization_projection_digest,
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test(sealed: SealedBoundToolInvocation, grant_id: impl Into<String>) -> Self {
        use crate::approval::route_policy::{NormalPolicyDecision, PolicyEvaluation};

        let policy = RoutePolicy::baseline_only_v1();
        let now = Utc::now();
        let policy_snapshot = match policy.evaluate_normal(sealed.invocation(), now) {
            PolicyEvaluation::Ready {
                snapshot,
                decision: NormalPolicyDecision::Allow,
            } => snapshot,
            _ => panic!("test execution grant requires baseline-readable bound authority"),
        };
        let bound = sealed.invocation();
        let evidence = ToolExecutionAuthorizationEvidence {
            evidence_version: AUTHORIZATION_EVIDENCE_VERSION_V1.to_owned(),
            grant_id: grant_id.into(),
            tool_call_id: bound.tool_call_id.clone(),
            route: ToolInvocationRoute::Normal,
            proposal_digest: bound.proposal_digest.to_hex(),
            descriptor_digest: bound.descriptor_digest.to_hex(),
            bound_evidence_digest: bound
                .evidence_digest()
                .expect("test bound evidence digest")
                .to_hex(),
            policy: policy_snapshot,
            policy_decision: PolicyDecisionRecord::Allow,
            resolved_authority: ExecutionAuthorityProvenance::AgentOwn,
            execution_review: None,
            escalation_review: None,
            human_decision: None,
        };
        Self::new(
            Arc::new(RwLock::new(policy)),
            Arc::new(Utc::now),
            sealed,
            ToolInvocationRoute::Normal,
            "test-run".to_owned(),
            "test-turn".to_owned(),
            evidence,
        )
        .expect("valid test execution grant")
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
        let Self {
            sealed,
            evidence,
            executor_authorization_projection_digest,
            ..
        } = self;
        let permit = CommittedExecutionPermit {
            grant_digest: executor_grant_digest(&evidence.grant_id)
                .expect("validated grant identity must remain digestible"),
            bound_evidence_digest: evidence.bound_evidence_digest,
            action_digest: evidence.descriptor_digest,
            authorization_projection_digest: executor_authorization_projection_digest,
            route: evidence.route,
            resolved_authority: evidence.resolved_authority,
        };
        AuthorizedBoundInvocation { sealed, permit }
    }

    /// Reauthorization consumes the stale grant without manufacturing a
    /// post-COMMIT execution permit.
    pub(crate) fn into_sealed_for_reauthorization(self) -> SealedBoundToolInvocation {
        self.sealed
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
    permit: CommittedExecutionPermit,
}

impl AuthorizedBoundInvocation {
    pub(crate) fn sealed(&self) -> &SealedBoundToolInvocation {
        &self.sealed
    }

    /// Open the opaque pair only at the registry execution seam, after proving
    /// that the post-COMMIT permit still names this exact sealed invocation.
    pub(crate) fn into_registry_parts(
        self,
        _access: AuthorizedBoundRegistryAccess,
    ) -> Result<(SealedBoundToolInvocation, CommittedExecutionPermit), DescribeError> {
        self.permit.validate_for(self.sealed.invocation())?;
        Ok((self.sealed, self.permit))
    }

    #[cfg(test)]
    pub(crate) fn into_validated_parts_for_test(
        self,
    ) -> Result<(SealedBoundToolInvocation, CommittedExecutionPermitParts), DescribeError> {
        self.permit.validate_for(self.sealed.invocation())?;
        Ok((self.sealed, self.permit.into_executor_parts_for_test()))
    }

    pub(crate) fn tool_call_id(&self) -> &str {
        &self.sealed.invocation().tool_call_id
    }

    pub(crate) fn tool_name(&self) -> &str {
        &self.sealed.invocation().tool_name
    }

    /// Mint process-local authority for one already-bound reviewer Read.
    ///
    /// Reviewer reads are not PA tool executions and do not create ordinary
    /// `tool_executions` rows. Their bounded, redacted trace is committed as
    /// part of the enclosing review evidence instead. This constructor is
    /// deliberately incapable of authorizing any other capability or route.
    pub(crate) fn for_reviewer_read(
        sealed: SealedBoundToolInvocation,
        policy: &PolicySnapshot,
        policy_decision: PolicyDecisionRecord,
    ) -> Result<Self> {
        let bound = sealed.invocation();
        if bound.descriptor.capability != crate::tools::CapabilityClass::Read
            || !matches!(
                policy_decision,
                PolicyDecisionRecord::Allow | PolicyDecisionRecord::Unmatched
            )
        {
            bail!("reviewer authority is restricted to policy-admitted reads");
        }
        policy.validate()?;
        let bound_evidence_digest = bound.evidence_digest()?.to_hex();
        let encoded = serde_json::to_vec(&serde_json::json!({
            "origin": "reviewer",
            "route": ToolInvocationRoute::Normal,
            "tool_call_id": bound.tool_call_id,
            "bound_evidence_digest": bound_evidence_digest,
            "policy_source_digest": policy.source_digest,
            "policy_decision": policy_decision,
            "resolved_authority": ExecutionAuthorityProvenance::AgentOwn,
        }))?;
        let authority_digest = digest(REVIEWER_READ_AUTHORITY_DOMAIN, &encoded);
        let permit = CommittedExecutionPermit {
            grant_digest: authority_digest.clone(),
            bound_evidence_digest,
            action_digest: bound.descriptor_digest.to_hex(),
            authorization_projection_digest: authority_digest,
            route: ToolInvocationRoute::Normal,
            resolved_authority: ExecutionAuthorityProvenance::AgentOwn,
        };
        Ok(Self { sealed, permit })
    }

    #[cfg(test)]
    pub(crate) fn for_test(sealed: SealedBoundToolInvocation) -> Self {
        let permit = CommittedExecutionPermit::for_test(sealed.invocation());
        Self { sealed, permit }
    }

    #[cfg(test)]
    pub(crate) fn swap_permits_for_test(left: Self, right: Self) -> (Self, Self) {
        let Self {
            sealed: left_sealed,
            permit: left_permit,
        } = left;
        let Self {
            sealed: right_sealed,
            permit: right_permit,
        } = right;
        (
            Self {
                sealed: left_sealed,
                permit: right_permit,
            },
            Self {
                sealed: right_sealed,
                permit: left_permit,
            },
        )
    }
}

/// Move-only, process-local authority released only by the durable start
/// commit barrier. Durable evidence can be audited after restart, but cannot
/// recreate this value or mint another executor token.
pub(crate) struct CommittedExecutionPermit {
    grant_digest: String,
    bound_evidence_digest: String,
    action_digest: String,
    authorization_projection_digest: String,
    route: ToolInvocationRoute,
    resolved_authority: ExecutionAuthorityProvenance,
}

/// One begun effect. It must survive the effect attempt and can produce at
/// most one success receipt.
pub(crate) struct CommittedEffectStart {
    _private: (),
}

/// A successful begun effect together with its result. This receipt is neither
/// cloneable nor constructible outside this authority module. Keeping the
/// result inside the receipt prevents an adapter from constructing a successful
/// bound outcome after a failed or cancelled effect future.
pub(crate) struct CommittedEffectReceipt<T> {
    value: T,
}

/// Executor-only continuation after one effect start. It cannot be converted
/// back into a local permit or begun a second time.
pub(crate) struct ExecutorCommittedExecutionPermit {
    permit: CommittedExecutionPermit,
}

/// Opaque executor effect start. The signing continuation is released only to
/// the exact result-producing future supplied to `complete`, so it cannot be
/// split from the receipt path and paired with another operation's result.
pub(crate) struct ExecutorCommittedEffectStart {
    permit: CommittedExecutionPermit,
}

pub(crate) struct CommittedExecutionPermitParts {
    pub grant_digest: String,
    pub bound_evidence_digest: String,
    pub action_digest: String,
    pub authorization_projection_digest: String,
    pub route: ToolInvocationRoute,
    pub resolved_authority: ExecutionAuthorityProvenance,
}

impl CommittedExecutionPermit {
    fn validate_for(&self, bound: &BoundToolInvocation) -> Result<(), DescribeError> {
        let bound_evidence_digest = bound
            .evidence_digest()
            .map_err(|_| DescribeError::ExecutionPermitMismatch)?
            .to_hex();
        if self.bound_evidence_digest != bound_evidence_digest
            || self.action_digest != bound.descriptor_digest.to_hex()
        {
            return Err(DescribeError::ExecutionPermitMismatch);
        }
        Ok(())
    }

    pub(crate) fn begin_local_effect(self) -> CommittedEffectStart {
        CommittedEffectStart { _private: () }
    }

    pub(crate) fn begin_executor_effect(self) -> ExecutorCommittedEffectStart {
        ExecutorCommittedEffectStart { permit: self }
    }

    /// Third closed effect shape: one Messaging send whose attachment bytes
    /// come from the Workspace through the executor. It consumes the permit
    /// exactly once, yields exactly one signing continuation for exactly one
    /// executor source operation, keeps the root effect unsettled while the
    /// runtime transfers those sources into exact-scoped uploads and commits
    /// the message, and mints the only receipt after the message commit. It
    /// cannot be combined with `begin_local_effect` or `begin_executor_effect`
    /// because all three consume `self`.
    pub(crate) fn begin_messaging_workspace_send_effect(
        self,
    ) -> MessagingWorkspaceSendEffectStart {
        MessagingWorkspaceSendEffectStart { permit: self }
    }

    #[cfg(test)]
    pub(crate) fn into_executor_parts_for_test(self) -> CommittedExecutionPermitParts {
        self.begin_executor_effect()
            .into_permit_for_test()
            .into_executor_parts()
    }

    #[cfg(test)]
    pub(crate) fn for_test(bound: &BoundToolInvocation) -> Self {
        Self {
            grant_digest: digest(
                EXECUTOR_GRANT_DIGEST_DOMAIN,
                format!("test-grant-{}", bound.tool_call_id).as_bytes(),
            ),
            bound_evidence_digest: bound
                .evidence_digest()
                .expect("test bound invocation digest")
                .to_hex(),
            action_digest: bound.descriptor_digest.to_hex(),
            authorization_projection_digest: "00".repeat(32),
            route: ToolInvocationRoute::Normal,
            resolved_authority: ExecutionAuthorityProvenance::AgentOwn,
        }
    }

    #[cfg(test)]
    pub(crate) fn executor_fixture(
        grant_id: &str,
        route: ToolInvocationRoute,
        resolved_authority: ExecutionAuthorityProvenance,
    ) -> Self {
        Self {
            grant_digest: digest(EXECUTOR_GRANT_DIGEST_DOMAIN, grant_id.as_bytes()),
            bound_evidence_digest: "11".repeat(32),
            action_digest: "33".repeat(32),
            authorization_projection_digest: "22".repeat(32),
            route,
            resolved_authority,
        }
    }
}

impl ExecutorCommittedExecutionPermit {
    pub(crate) fn into_executor_parts(self) -> CommittedExecutionPermitParts {
        let CommittedExecutionPermit {
            grant_digest,
            bound_evidence_digest,
            action_digest,
            authorization_projection_digest,
            route,
            resolved_authority,
        } = self.permit;
        CommittedExecutionPermitParts {
            grant_digest,
            bound_evidence_digest,
            action_digest,
            authorization_projection_digest,
            route,
            resolved_authority,
        }
    }
}

impl ExecutorCommittedEffectStart {
    /// Give the executor-only continuation to exactly one effect future and
    /// mint a receipt only when that same future succeeds.
    pub(crate) async fn complete<F, Fut, T, E>(
        self,
        effect: F,
    ) -> Result<CommittedEffectReceipt<T>, E>
    where
        F: FnOnce(ExecutorCommittedExecutionPermit) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        effect(ExecutorCommittedExecutionPermit {
            permit: self.permit,
        })
        .await
        .map(|value| CommittedEffectReceipt { value })
    }

    #[cfg(test)]
    pub(crate) fn into_permit_for_test(self) -> ExecutorCommittedExecutionPermit {
        ExecutorCommittedExecutionPermit {
            permit: self.permit,
        }
    }
}

/// Opaque composite effect start for a Messaging Workspace send (see
/// [`CommittedExecutionPermit::begin_messaging_workspace_send_effect`]).
pub(crate) struct MessagingWorkspaceSendEffectStart {
    permit: CommittedExecutionPermit,
}

/// The one signing continuation the composite effect may spend on its single
/// executor source operation. It is move-only and cannot be split, cloned, or
/// converted back into a local or generic executor start.
pub(crate) struct MessagingSourceSigningContinuation {
    permit: ExecutorCommittedExecutionPermit,
}

impl MessagingSourceSigningContinuation {
    /// Spend the continuation on exactly one executor operation signing.
    pub(crate) fn into_executor_permit(self) -> ExecutorCommittedExecutionPermit {
        self.permit
    }
}

impl MessagingWorkspaceSendEffectStart {
    /// Give the single source-signing continuation to exactly one effect
    /// future. The future owns the executor source transfer, every upload,
    /// and the message write; a receipt exists only if that whole future
    /// succeeds. Intermediate steps cannot construct one.
    pub(crate) async fn complete<F, Fut, T, E>(
        self,
        effect: F,
    ) -> Result<CommittedEffectReceipt<T>, E>
    where
        F: FnOnce(MessagingSourceSigningContinuation) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        effect(MessagingSourceSigningContinuation {
            permit: ExecutorCommittedExecutionPermit {
                permit: self.permit,
            },
        })
        .await
        .map(|value| CommittedEffectReceipt { value })
    }
}

impl CommittedEffectStart {
    /// Run the effect future and mint a receipt only for its successful result.
    /// An error consumes both this start and the underlying permit authority.
    pub(crate) async fn complete<F, Fut, T, E>(
        self,
        effect: F,
    ) -> Result<CommittedEffectReceipt<T>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        effect().await.map(|value| CommittedEffectReceipt { value })
    }
}

impl<T> CommittedEffectReceipt<T> {
    pub(crate) fn map<U>(self, map: impl FnOnce(T) -> U) -> CommittedEffectReceipt<U> {
        CommittedEffectReceipt {
            value: map(self.value),
        }
    }

    pub(crate) fn try_map<U, E>(
        self,
        map: impl FnOnce(T) -> Result<U, E>,
    ) -> Result<CommittedEffectReceipt<U>, E> {
        map(self.value).map(|value| CommittedEffectReceipt { value })
    }

    #[cfg(test)]
    pub(crate) fn into_inner(self) -> T {
        self.value
    }

    pub(crate) fn into_parts(self) -> (T, CommittedEffectReceipt<()>) {
        (self.value, CommittedEffectReceipt { value: () })
    }
}

#[cfg(test)]
mod executor_projection_tests {
    use super::*;
    use crate::approval::{
        route_policy::{NormalPolicyDecision, PolicyEvaluation, PolicySourceState},
        route_reviewer::{
            EscalationReviewDecision, ExecutionReviewDecision, ExecutionReviewEvidence,
            ReviewerBudgetEvidence, ReviewerTerminalClass, ReviewerToolTrace,
        },
    };

    fn hidden_elevated_evidence(hidden: &str) -> ToolExecutionAuthorizationEvidence {
        ToolExecutionAuthorizationEvidence {
            evidence_version: AUTHORIZATION_EVIDENCE_VERSION_V1.to_owned(),
            grant_id: format!("grant-{hidden}"),
            tool_call_id: format!("call-{hidden}"),
            route: ToolInvocationRoute::Elevated,
            proposal_digest: format!("proposal-{hidden}"),
            descriptor_digest: format!("descriptor-{hidden}"),
            bound_evidence_digest: format!("bound-{hidden}"),
            policy: PolicySnapshot {
                source: PolicySourceState::RequiredUnavailable {
                    baseline_version: format!("baseline-{hidden}"),
                    reason: format!("private-policy-{hidden}"),
                    minimum_version: 7,
                },
                source_digest: format!("private-source-digest-{hidden}"),
                evaluated_at: Utc::now(),
                valid_until: None,
                bundle_version: Some(7),
            },
            policy_decision: PolicyDecisionRecord::ElevatedPreflight,
            resolved_authority: ExecutionAuthorityProvenance::AgentOwnWithHumanConsent,
            execution_review: None,
            escalation_review: Some(EscalationReviewEvidence {
                reviewer_version: format!("private-reviewer-{hidden}"),
                prompt_version: format!("private-prompt-{hidden}"),
                schema_version: format!("private-schema-{hidden}"),
                model_id: format!("private-model-{hidden}"),
                model_binding_digest: format!("private-model-binding-digest-{hidden}"),
                budget: ReviewerBudgetEvidence {
                    version: format!("private-budget-{hidden}"),
                    digest: format!("private-budget-digest-{hidden}"),
                    attempts: u8::try_from(hidden.len()).unwrap(),
                    terminal: ReviewerTerminalClass::ValidDecision,
                },
                tool_trace: Vec::new(),
                decision: EscalationReviewDecision {
                    outcome: EscalationReviewOutcome::AskHuman,
                    risk: RiskLevel::High,
                    misunderstanding: Some(format!("private-misunderstanding-{hidden}")),
                    rationale: format!("private-rationale-{hidden}"),
                },
            }),
            human_decision: Some(HumanDecisionEvidence {
                request_id: format!("request-{hidden}"),
                command_id: format!("command-{hidden}"),
                command_seq: 41,
                tenant_id: format!("tenant-{hidden}"),
                personality_agent_id: format!("agent-{hidden}"),
                human_principal_id: format!("human-{hidden}"),
                provenance_version: HUMAN_DECISION_PROVENANCE_VERSION_V1,
                decision: CurrentCallDecision::ApproveOnce,
                received_at: Utc::now(),
                authorization_context_digest: format!("context-{hidden}"),
            }),
        }
    }

    #[test]
    fn reviewer_tool_trace_changes_the_durable_authorization_evidence_digest() {
        let bound = BoundToolInvocation::test_fixture(
            "review-trace-digest",
            crate::tools::CapabilityClass::Mutate,
        );
        let policy = RoutePolicy::baseline_only_v1();
        let snapshot = match policy.evaluate_normal(&bound, Utc::now()) {
            PolicyEvaluation::Ready {
                snapshot,
                decision: NormalPolicyDecision::Unmatched,
            } => snapshot,
            outcome => panic!("fixture mutate policy must be unmatched: {outcome:?}"),
        };
        let review = ExecutionReviewEvidence {
            reviewer_version: "execution-reviewer/v6".to_owned(),
            prompt_version: "execution-review-prompt/v6".to_owned(),
            schema_version: "execution-review-schema/v6".to_owned(),
            model_id: "fixture".to_owned(),
            model_binding_digest: "fixture-binding".to_owned(),
            budget: ReviewerBudgetEvidence {
                version: "reviewer-budget/v1".to_owned(),
                digest: "fixture-budget".to_owned(),
                attempts: 1,
                terminal: ReviewerTerminalClass::ValidDecision,
            },
            tool_trace: Vec::new(),
            decision: ExecutionReviewDecision {
                outcome: ExecutionReviewOutcome::Allow,
                risk: RiskLevel::Low,
                rationale: "verified".to_owned(),
            },
        };
        let mut evidence = ToolExecutionAuthorizationEvidence {
            evidence_version: AUTHORIZATION_EVIDENCE_VERSION_V1.to_owned(),
            grant_id: "grant-review-trace".to_owned(),
            tool_call_id: bound.tool_call_id.clone(),
            route: ToolInvocationRoute::Normal,
            proposal_digest: bound.proposal_digest.to_hex(),
            descriptor_digest: bound.descriptor_digest.to_hex(),
            bound_evidence_digest: bound.evidence_digest().unwrap().to_hex(),
            policy: snapshot,
            policy_decision: PolicyDecisionRecord::Unmatched,
            resolved_authority: ExecutionAuthorityProvenance::AgentOwn,
            execution_review: Some(review),
            escalation_review: None,
            human_decision: None,
        };
        let without_trace = authorization_evidence_digest(&evidence, &bound).unwrap();
        evidence
            .execution_review
            .as_mut()
            .unwrap()
            .tool_trace
            .push(ReviewerToolTrace {
                tool: "workspace_invitation_list".to_owned(),
                arguments: serde_json::json!({}),
                result_digest: "ab".repeat(32),
                is_error: false,
                elapsed_ms: 4,
            });
        let with_trace = authorization_evidence_digest(&evidence, &bound).unwrap();
        assert_ne!(without_trace, with_trace);
    }

    #[test]
    fn executor_projection_excludes_principals_raw_evidence_and_free_form_review_text() {
        let first = hidden_elevated_evidence("hidden-alpha");
        let second = hidden_elevated_evidence("hidden-beta-longer");

        let first_projection =
            serde_json::to_value(executor_authorization_projection(&first)).unwrap();
        let second_projection =
            serde_json::to_value(executor_authorization_projection(&second)).unwrap();
        assert_eq!(
            first_projection, second_projection,
            "excluded private values must not influence Executor authorization"
        );
        let serialized = serde_json::to_string(&first_projection).unwrap();
        for forbidden in [
            "hidden-alpha",
            "grant_id",
            "tool_call_id",
            "proposal_digest",
            "descriptor_digest",
            "bound_evidence_digest",
            "source_digest",
            "evaluated_at",
            "valid_until",
            "bundle_version",
            "baseline_version",
            "private-policy",
            "reviewer_version",
            "prompt_version",
            "schema_version",
            "model_id",
            "model_binding_digest",
            "budget",
            "rationale",
            "misunderstanding",
            "request_id",
            "command_id",
            "tenant_id",
            "personality_agent_id",
            "human_principal_id",
            "authorization_context_digest",
        ] {
            assert!(
                !serialized.contains(forbidden),
                "Executor projection leaked {forbidden}"
            );
        }

        let mut different_safe_facts = first.clone();
        different_safe_facts
            .escalation_review
            .as_mut()
            .expect("fixture escalation review")
            .decision
            .risk = RiskLevel::Low;
        assert_ne!(
            first_projection,
            serde_json::to_value(executor_authorization_projection(&different_safe_facts)).unwrap(),
            "typed authorization facts must remain bound into the projection"
        );
    }
}

#[cfg(test)]
mod effect_type_tests {
    use super::{
        CommittedEffectReceipt, CommittedEffectStart, CommittedExecutionPermit,
        ExecutorCommittedEffectStart, ExecutorCommittedExecutionPermit,
    };

    // These assignments lock the ownership boundary at compile time. Changing
    // either receiver to a borrow would make it possible to begin both routes
    // or begin one route twice from the same post-COMMIT permit.
    const _: fn(CommittedExecutionPermit) -> CommittedEffectStart =
        CommittedExecutionPermit::begin_local_effect;
    const _: fn(CommittedExecutionPermit) -> ExecutorCommittedEffectStart =
        CommittedExecutionPermit::begin_executor_effect;

    trait AmbiguousIfClone<A> {
        fn marker() {}
    }
    struct CloneImplemented;
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: ?Sized + Clone> AmbiguousIfClone<CloneImplemented> for T {}

    trait AmbiguousIfSerialize<A> {
        fn marker() {}
    }
    struct SerializeImplemented;
    impl<T: ?Sized> AmbiguousIfSerialize<()> for T {}
    impl<T: ?Sized + serde::Serialize> AmbiguousIfSerialize<SerializeImplemented> for T {}

    const _: fn() = || {
        let _ = <CommittedExecutionPermit as AmbiguousIfClone<_>>::marker;
        let _ = <CommittedEffectStart as AmbiguousIfClone<_>>::marker;
        let _ = <CommittedEffectReceipt<()> as AmbiguousIfClone<_>>::marker;
        let _ = <ExecutorCommittedEffectStart as AmbiguousIfClone<_>>::marker;
        let _ = <ExecutorCommittedExecutionPermit as AmbiguousIfClone<_>>::marker;
        let _ = <CommittedExecutionPermit as AmbiguousIfSerialize<_>>::marker;
        let _ = <ExecutorCommittedEffectStart as AmbiguousIfSerialize<_>>::marker;
        let _ = <ExecutorCommittedExecutionPermit as AmbiguousIfSerialize<_>>::marker;
    };

    // A successful receipt is only available through the result-coupled
    // completion future. This helper is compiled (but need not run) so a
    // signature regression cannot silently decouple receipt creation from the
    // effect result.
    #[allow(dead_code)]
    async fn receipt_requires_success(
        start: CommittedEffectStart,
        result: Result<(), ()>,
    ) -> Result<CommittedEffectReceipt<()>, ()> {
        start.complete(|| std::future::ready(result)).await
    }

    #[tokio::test]
    async fn failed_effect_consumes_start_without_minting_a_receipt() {
        let permit = CommittedExecutionPermit::executor_fixture(
            "grant-failed-effect",
            crate::provider::types::ToolInvocationRoute::Normal,
            super::ExecutionAuthorityProvenance::AgentOwn,
        );
        let start = permit.begin_local_effect();
        let result: Result<CommittedEffectReceipt<()>, &'static str> =
            start.complete(|| std::future::ready(Err("failed"))).await;
        assert_eq!(result.err(), Some("failed"));
    }
}
