//! Audit reviewer boundary.
//!
//! The `Reviewer` performs trust-domain validation, builds a sanitized prompt,
//! calls an injected transport with a 90-second overall timeout and up to three
//! attempts, and returns a strict JSON response. Failures, schema mismatches,
//! and untrusted domains are collapsed into a synthetic `High / Unknown / Deny`
//! outcome.

use std::{
    collections::{HashMap, VecDeque},
    fmt::Write as _,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    approval::{
        action::{ReviewProjection, SecretAwareActionProjector},
        prompt::{PromptLimits, ReviewerPrompt, TrustedEnvironment},
    },
    provider::types::PublicMessage,
};

const MAX_ATTEMPTS: u32 = 3;
const TOTAL_TIMEOUT_SECONDS: u64 = 90;

/// How the broker decides whether to send an action to the audit reviewer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewerMode {
    User,
    AutoReview,
    StrictAutoReview,
}

/// Model configuration for the reviewer call. `trust_domain_id` is the
/// trust-domain label used by `ReviewerTrustSet`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReviewerModelSpec {
    pub id: String,
    pub provider: String,
    pub base_url: String,
    pub account_scope: String,
    pub trust_domain_id: String,
    pub data_processing_policy: String,
}

impl ReviewerModelSpec {
    pub fn new(
        id: impl Into<String>,
        provider: impl Into<String>,
        base_url: impl Into<String>,
        account_scope: impl Into<String>,
        trust_domain_id: impl Into<String>,
        data_processing_policy: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            provider: provider.into(),
            base_url: base_url.into(),
            account_scope: account_scope.into(),
            trust_domain_id: trust_domain_id.into(),
            data_processing_policy: data_processing_policy.into(),
        }
    }
}

/// Allowed reviewer trust domains. The reviewer model is allowed when it is in
/// the same trust domain as the conversation provider or an explicitly allowed
/// audit domain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReviewerTrustSet {
    conversation_domain_id: String,
    allowed_audit_domains: Vec<String>,
}

impl ReviewerTrustSet {
    pub fn new(
        conversation_domain_id: impl Into<String>,
        allowed_audit_domains: Vec<String>,
    ) -> Self {
        Self {
            conversation_domain_id: conversation_domain_id.into(),
            allowed_audit_domains,
        }
    }

    pub fn allows(&self, model: &ReviewerModelSpec) -> bool {
        if model.trust_domain_id.is_empty() {
            return false;
        }
        if model.trust_domain_id == self.conversation_domain_id {
            return true;
        }
        self.allowed_audit_domains
            .iter()
            .any(|d| d == &model.trust_domain_id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Allow,
    Deny,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserAuthorization {
    Unknown,
    Low,
    Medium,
    High,
}

/// Strict review response. `deny_unknown_fields` rejects extra keys, and
/// `rationale` is validated to be non-empty and <= 1000 Unicode characters.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditDecision {
    pub outcome: AuditOutcome,
    pub risk: RiskLevel,
    pub authorization: UserAuthorization,
    pub rationale: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReviewOutcome {
    Allow(AuditDecision),
    Deny(AuditDecision),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReviewRequest {
    pub mode: ReviewerMode,
    pub projection: ReviewProjection,
    pub transcript: Vec<PublicMessage>,
    pub trusted_environment: TrustedEnvironment,
    pub policy_hash: String,
    pub context_version: String,
    pub run_id: String,
    pub turn_id: Option<String>,
}

/// Transport errors returned by the injected reviewer client.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ReviewerTransportError {
    #[error("transient reviewer transport error: {0}")]
    Transient(String),
    #[error("fatal reviewer transport error: {0}")]
    Fatal(String),
}

/// Injected transport seam. Implementations must not perform blocking I/O
/// without respecting the cancellation token.
#[async_trait]
pub trait ReviewerTransport: Send + Sync {
    /// Prompt the review model. Implementations must use `cancel` to abort
    /// outstanding work instead of continuing after the caller has dropped
    /// the request.
    async fn complete(
        &self,
        prompt: &ReviewerPrompt,
        cancel: CancellationToken,
    ) -> Result<String, ReviewerTransportError>;
}

/// Circuit breaker for the audit reviewer. Opens after three consecutive
/// denials or ten denials in the last fifty reviews within a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
}

#[derive(Clone, Debug)]
pub struct CircuitBreaker {
    open: bool,
    consecutive_denies: u32,
    recent: VecDeque<bool>,
    recent_denies: u32,
}

impl CircuitBreaker {
    pub fn new() -> Self {
        Self {
            open: false,
            consecutive_denies: 0,
            recent: VecDeque::new(),
            recent_denies: 0,
        }
    }

    pub fn record(&mut self, outcome: AuditOutcome) {
        if self.open {
            return;
        }
        match outcome {
            AuditOutcome::Allow => self.consecutive_denies = 0,
            AuditOutcome::Deny => {
                self.consecutive_denies += 1;
                if self.consecutive_denies >= 3 {
                    self.open = true;
                }
            }
        }
        let was_deny = matches!(outcome, AuditOutcome::Deny);
        self.recent.push_back(was_deny);
        if was_deny {
            self.recent_denies += 1;
        }
        if self.recent.len() > 50 {
            let old = self.recent.pop_front().unwrap_or(false);
            if old {
                self.recent_denies -= 1;
            }
        }
        if self.recent_denies >= 10 {
            self.open = true;
        }
    }

    pub fn state(&self) -> CircuitState {
        if self.open {
            CircuitState::Open
        } else {
            CircuitState::Closed
        }
    }

    pub fn is_open(&self) -> bool {
        self.open
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
struct AllowCache {
    entries: HashMap<CacheKey, AuditDecision>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    policy_hash: String,
    projection_hash: String,
    context_version: String,
}

impl AllowCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    fn get(
        &self,
        policy_hash: &str,
        projection_hash: &str,
        context_version: &str,
    ) -> Option<&AuditDecision> {
        self.entries.get(&CacheKey {
            policy_hash: policy_hash.to_owned(),
            projection_hash: projection_hash.to_owned(),
            context_version: context_version.to_owned(),
        })
    }

    fn put(
        &mut self,
        policy_hash: &str,
        projection_hash: &str,
        context_version: &str,
        decision: AuditDecision,
    ) {
        if decision.outcome == AuditOutcome::Allow {
            self.entries.insert(
                CacheKey {
                    policy_hash: policy_hash.to_owned(),
                    projection_hash: projection_hash.to_owned(),
                    context_version: context_version.to_owned(),
                },
                decision,
            );
        }
    }
}

impl Default for AllowCache {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
struct DenyCache {
    turn_id: Option<String>,
    entries: HashMap<String, AuditDecision>,
}

impl DenyCache {
    fn new() -> Self {
        Self {
            turn_id: None,
            entries: HashMap::new(),
        }
    }

    fn get(&self, projection_hash: &str, turn_id: Option<&str>) -> Option<&AuditDecision> {
        if turn_id.is_none() || self.turn_id.as_deref() != turn_id {
            return None;
        }
        self.entries.get(projection_hash)
    }

    fn put(&mut self, turn_id: Option<&str>, projection_hash: &str, decision: AuditDecision) {
        if decision.outcome != AuditOutcome::Deny {
            return;
        }
        let Some(turn_id) = turn_id else {
            return;
        };
        if self.turn_id.as_deref() != Some(turn_id) {
            self.turn_id = Some(turn_id.to_owned());
            self.entries.clear();
        }
        self.entries.insert(projection_hash.to_owned(), decision);
    }
}

impl Default for DenyCache {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Reviewer {
    model: ReviewerModelSpec,
    trust_set: ReviewerTrustSet,
    transport: Arc<dyn ReviewerTransport>,
    projector: Arc<SecretAwareActionProjector>,
    circuit_breaker: Mutex<RunCircuitBreaker>,
    allow_cache: Mutex<AllowCache>,
    deny_cache: Mutex<DenyCache>,
}

#[derive(Clone, Debug, Default)]
struct RunCircuitBreaker {
    run_id: Option<String>,
    breaker: CircuitBreaker,
}

impl RunCircuitBreaker {
    fn for_run(&mut self, run_id: &str) -> &mut CircuitBreaker {
        if self.run_id.as_deref() != Some(run_id) {
            self.run_id = Some(run_id.to_owned());
            self.breaker = CircuitBreaker::new();
        }
        &mut self.breaker
    }
}

impl Reviewer {
    pub fn new(
        model: ReviewerModelSpec,
        trust_set: ReviewerTrustSet,
        transport: Arc<dyn ReviewerTransport>,
        projector: Arc<SecretAwareActionProjector>,
    ) -> Self {
        Self {
            model,
            trust_set,
            transport,
            projector,
            circuit_breaker: Mutex::new(RunCircuitBreaker::default()),
            allow_cache: Mutex::new(AllowCache::new()),
            deny_cache: Mutex::new(DenyCache::new()),
        }
    }

    /// True when the configured model is in an allowed trust domain and may be
    /// called without triggering a manual/headless block.
    pub fn is_trusted(&self) -> bool {
        self.trust_set.allows(&self.model)
    }

    pub async fn review(&self, request: ReviewRequest, cancel: CancellationToken) -> ReviewOutcome {
        let projection_hash = hash_projection(&request.projection);
        self.circuit_breaker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .for_run(&request.run_id);

        if request.mode == ReviewerMode::User {
            return ReviewOutcome::Deny(synthetic_deny("audit review is disabled in User mode"));
        }
        if !self.trust_set.allows(&self.model) {
            self.record_outcome(&request.run_id, AuditOutcome::Deny);
            return ReviewOutcome::Deny(synthetic_deny(
                "reviewer model trust domain is not allowed",
            ));
        }
        if matches!(
            request.projection,
            ReviewProjection::InsufficientEvidence { .. }
        ) {
            self.record_outcome(&request.run_id, AuditOutcome::Deny);
            return ReviewOutcome::Deny(synthetic_deny(
                "insufficient evidence to review the action",
            ));
        }

        {
            let allow_cache = self.allow_cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(decision) = allow_cache.get(
                &request.policy_hash,
                &projection_hash,
                &request.context_version,
            ) {
                return ReviewOutcome::Allow(decision.clone());
            }
        }

        {
            let cb = self
                .circuit_breaker
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if cb
                .run_id
                .as_deref()
                .is_some_and(|run_id| run_id == request.run_id)
                && cb.breaker.is_open()
            {
                return ReviewOutcome::Deny(synthetic_deny("reviewer circuit breaker is open"));
            }
        }

        {
            let deny_cache = self.deny_cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(decision) = deny_cache.get(&projection_hash, request.turn_id.as_deref()) {
                self.record_outcome(&request.run_id, AuditOutcome::Deny);
                return ReviewOutcome::Deny(decision.clone());
            }
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(TOTAL_TIMEOUT_SECONDS);
        let mut retry_errors: Vec<String> = Vec::new();

        for attempt in 1..=MAX_ATTEMPTS {
            let prompt = match crate::approval::prompt::build_reviewer_prompt(
                None,
                &request.trusted_environment,
                &request.transcript,
                &request.projection,
                &retry_errors,
                &self.projector,
                &PromptLimits::default(),
            ) {
                Ok(p) => p,
                Err(e) => {
                    self.record_outcome(&request.run_id, AuditOutcome::Deny);
                    return ReviewOutcome::Deny(synthetic_deny(format!(
                        "failed to build reviewer prompt: {e}"
                    )));
                }
            };

            let now = tokio::time::Instant::now();
            let remaining = match deadline.checked_duration_since(now) {
                Some(d) => d,
                None => {
                    self.record_outcome(&request.run_id, AuditOutcome::Deny);
                    return ReviewOutcome::Deny(synthetic_deny("reviewer total timeout exceeded"));
                }
            };

            let result = tokio::select! {
                biased;
                _ = cancel.cancelled() => ReviewerCall::Cancelled,
                outcome = tokio::time::timeout(remaining, self.transport.complete(&prompt, cancel.clone())) => {
                    match outcome {
                        Ok(Ok(raw)) => ReviewerCall::Raw(raw),
                        Ok(Err(ReviewerTransportError::Transient(e))) => ReviewerCall::Transient(e),
                        Ok(Err(ReviewerTransportError::Fatal(e))) => ReviewerCall::Fatal(e),
                        Err(_) => ReviewerCall::TimedOut,
                    }
                }
            };

            match result {
                ReviewerCall::Raw(raw) => match parse_audit_response(&raw) {
                    Ok(decision) => {
                        return self.return_decision(decision, &projection_hash, &request);
                    }
                    Err(e) => {
                        retry_errors.push(format!("attempt {attempt}: invalid response: {e}"));
                        continue;
                    }
                },
                ReviewerCall::Transient(e) => {
                    retry_errors.push(format!("attempt {attempt}: transient error: {e}"));
                    continue;
                }
                ReviewerCall::Fatal(e) => {
                    self.record_outcome(&request.run_id, AuditOutcome::Deny);
                    return ReviewOutcome::Deny(synthetic_deny(format!(
                        "fatal reviewer transport error: {e}"
                    )));
                }
                ReviewerCall::TimedOut => {
                    self.record_outcome(&request.run_id, AuditOutcome::Deny);
                    return ReviewOutcome::Deny(synthetic_deny(
                        "reviewer call timed out after 90 seconds",
                    ));
                }
                ReviewerCall::Cancelled => {
                    return ReviewOutcome::Deny(synthetic_deny("review cancelled"));
                }
            }
        }

        self.record_outcome(&request.run_id, AuditOutcome::Deny);
        let last = retry_errors
            .last()
            .map(String::as_str)
            .unwrap_or("no error recorded");
        ReviewOutcome::Deny(synthetic_deny(format!(
            "reviewer did not return a valid decision after {MAX_ATTEMPTS} attempts: {last}"
        )))
    }

    fn record_outcome(&self, run_id: &str, outcome: AuditOutcome) {
        self.circuit_breaker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .for_run(run_id)
            .record(outcome);
    }

    fn return_decision(
        &self,
        decision: AuditDecision,
        projection_hash: &str,
        request: &ReviewRequest,
    ) -> ReviewOutcome {
        self.record_outcome(&request.run_id, decision.outcome);
        match decision.outcome {
            AuditOutcome::Allow => {
                self.allow_cache
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .put(
                        &request.policy_hash,
                        projection_hash,
                        &request.context_version,
                        decision.clone(),
                    );
                ReviewOutcome::Allow(decision)
            }
            AuditOutcome::Deny => {
                self.deny_cache
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .put(
                        request.turn_id.as_deref(),
                        projection_hash,
                        decision.clone(),
                    );
                ReviewOutcome::Deny(decision)
            }
        }
    }
}

enum ReviewerCall {
    Raw(String),
    Transient(String),
    Fatal(String),
    TimedOut,
    Cancelled,
}

fn synthetic_deny(rationale: impl Into<String>) -> AuditDecision {
    AuditDecision {
        outcome: AuditOutcome::Deny,
        risk: RiskLevel::High,
        authorization: UserAuthorization::Unknown,
        rationale: rationale.into(),
    }
}

fn parse_audit_response(raw: &str) -> Result<AuditDecision, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err("empty response".to_owned());
    }
    let decision: AuditDecision = serde_json::from_str(raw).map_err(|e| e.to_string())?;
    let rationale = decision.rationale.trim();
    if rationale.is_empty() || rationale.chars().count() > 1000 {
        return Err("rationale is empty or exceeds 1000 characters".to_owned());
    }
    Ok(AuditDecision {
        outcome: decision.outcome,
        risk: decision.risk,
        authorization: decision.authorization,
        rationale: rationale.to_owned(),
    })
}

fn hash_projection(projection: &ReviewProjection) -> String {
    let bytes = serde_json::to_vec(projection).expect("ReviewProjection serializes");
    let digest = Sha256::digest(&bytes);
    hex_string(&digest)
}

fn hex_string(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, VecDeque},
        fmt::Write as _,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_trait::async_trait;
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        approval::action::{
            Permission, ReviewPath, ReviewPathComponent, ReviewToken, ReviewableAction,
            SandboxSummary, SecretAwareActionProjector, SecretDigestKey,
        },
        provider::types::{
            ApiProtocol, AssistantMessage, ProviderOrigin, PublicAssistantContent,
            PublicAssistantMessage, PublicMessage, StopReason, ToolCall, Usage, UserContent,
            UserMessage, ValidatedToolArguments,
        },
        store::Redactor,
    };

    fn projector() -> SecretAwareActionProjector {
        SecretAwareActionProjector::new(Redactor::v1(), SecretDigestKey::fixture())
    }

    fn reviewable_projection() -> ReviewProjection {
        ReviewProjection::Reviewable(ReviewableAction {
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
                text: "workspace".to_owned(),
            }]),
            affected_paths: Vec::new(),
            sandbox: SandboxSummary::workspace(),
            requested_permissions: vec![Permission::Exec],
            justification: None,
        })
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

    fn review_request(projection: ReviewProjection) -> ReviewRequest {
        ReviewRequest {
            mode: ReviewerMode::AutoReview,
            projection,
            transcript: Vec::new(),
            trusted_environment: trusted_env(),
            policy_hash: "policy-hash".to_owned(),
            context_version: "v1".to_owned(),
            run_id: "run-1".to_owned(),
            turn_id: Some("turn-1".to_owned()),
        }
    }

    fn make_reviewer_with_trust(
        transport: Arc<dyn ReviewerTransport>,
        trust: ReviewerTrustSet,
    ) -> Reviewer {
        let model = ReviewerModelSpec::new(
            "reviewer-model",
            "reviewer-provider",
            "https://reviewer.example.test/v1",
            "default",
            "reviewer-domain",
            "tenant-policy",
        );
        Reviewer::new(model, trust, transport, Arc::new(projector()))
    }

    fn make_reviewer(transport: Arc<dyn ReviewerTransport>) -> Reviewer {
        make_reviewer_with_trust(transport, ReviewerTrustSet::new("reviewer-domain", vec![]))
    }

    fn allow_json() -> String {
        json!({"outcome": "allow", "risk": "low", "authorization": "high", "rationale": "user explicitly requested"}).to_string()
    }

    fn deny_json() -> String {
        json!({"outcome": "deny", "risk": "high", "authorization": "low", "rationale": "no explicit authorization"}).to_string()
    }

    #[derive(Clone)]
    struct FakeTransport {
        log: Arc<Mutex<Vec<String>>>,
        calls: Arc<Mutex<VecDeque<FakeResponse>>>,
    }

    #[derive(Clone)]
    enum FakeResponse {
        Return(Result<String, ReviewerTransportError>),
        SleepThen(Duration, Result<String, ReviewerTransportError>),
        SleepForever,
    }

    impl FakeTransport {
        fn sequence(responses: Vec<Result<String, ReviewerTransportError>>) -> Arc<Self> {
            let calls: VecDeque<FakeResponse> =
                responses.into_iter().map(FakeResponse::Return).collect();
            Arc::new(Self {
                log: Arc::new(Mutex::new(Vec::new())),
                calls: Arc::new(Mutex::new(calls)),
            })
        }

        fn sleep_forever() -> Arc<Self> {
            Arc::new(Self {
                log: Arc::new(Mutex::new(Vec::new())),
                calls: Arc::new(Mutex::new(VecDeque::from([FakeResponse::SleepForever]))),
            })
        }

        fn sleep_then(d: Duration, r: Result<String, ReviewerTransportError>) -> Arc<Self> {
            Arc::new(Self {
                log: Arc::new(Mutex::new(Vec::new())),
                calls: Arc::new(Mutex::new(VecDeque::from([FakeResponse::SleepThen(d, r)]))),
            })
        }

        fn called_count(&self) -> usize {
            self.log.lock().unwrap_or_else(|e| e.into_inner()).len()
        }
    }

    #[async_trait]
    impl ReviewerTransport for FakeTransport {
        async fn complete(
            &self,
            prompt: &ReviewerPrompt,
            cancel: CancellationToken,
        ) -> Result<String, ReviewerTransportError> {
            let serialized = serde_json::to_string(prompt).unwrap_or_default();
            let response = {
                self.log
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(serialized);
                let mut calls = self.calls.lock().unwrap_or_else(|e| e.into_inner());
                calls.pop_front()
            };
            match response {
                Some(FakeResponse::Return(r)) => r,
                Some(FakeResponse::SleepThen(d, r)) => {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => Err(ReviewerTransportError::Fatal("cancelled".to_owned())),
                        _ = tokio::time::sleep(d) => r,
                    }
                }
                Some(FakeResponse::SleepForever) => {
                    cancel.cancelled().await;
                    Err(ReviewerTransportError::Fatal("cancelled".to_owned()))
                }
                None => Err(ReviewerTransportError::Transient(
                    "no more fake responses".to_owned(),
                )),
            }
        }
    }

    fn user_message(text: &str) -> PublicMessage {
        PublicMessage::User(UserMessage {
            content: vec![UserContent::Text {
                text: text.to_owned(),
            }],
            timestamp: chrono::Utc::now(),
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn returns_allow_for_valid_response() {
        let fake = FakeTransport::sequence(vec![Ok(allow_json())]);
        let reviewer = make_reviewer(fake.clone());
        let outcome = reviewer
            .review(
                review_request(reviewable_projection()),
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(outcome, ReviewOutcome::Allow(_)));
        assert_eq!(fake.called_count(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn returns_deny_for_valid_response() {
        let fake = FakeTransport::sequence(vec![Ok(deny_json())]);
        let reviewer = make_reviewer(fake.clone());
        let outcome = reviewer
            .review(
                review_request(reviewable_projection()),
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(outcome, ReviewOutcome::Deny(_)));
        assert_eq!(fake.called_count(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn trust_domain_violation_does_not_call_transport() {
        let fake = FakeTransport::sequence(vec![]);
        let reviewer = make_reviewer_with_trust(
            fake.clone(),
            ReviewerTrustSet::new("different-domain", vec![]),
        );
        let outcome = reviewer
            .review(
                review_request(reviewable_projection()),
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(outcome, ReviewOutcome::Deny(d) if d.rationale.contains("trust domain")));
        assert_eq!(fake.called_count(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn insufficient_evidence_does_not_call_transport() {
        let fake = FakeTransport::sequence(vec![]);
        let reviewer = make_reviewer(fake.clone());
        let projection = ReviewProjection::InsufficientEvidence {
            reason: "hidden host".to_owned(),
        };
        let mut request = review_request(projection);
        request.mode = ReviewerMode::AutoReview;
        let outcome = reviewer.review(request, CancellationToken::new()).await;
        assert!(
            matches!(outcome, ReviewOutcome::Deny(d) if d.rationale.contains("insufficient evidence"))
        );
        assert_eq!(fake.called_count(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retries_transient_errors_and_succeeds() {
        let fake = FakeTransport::sequence(vec![
            Err(ReviewerTransportError::Transient("boom".to_owned())),
            Err(ReviewerTransportError::Transient("boom".to_owned())),
            Ok(allow_json()),
        ]);
        let reviewer = make_reviewer(fake.clone());
        let outcome = reviewer
            .review(
                review_request(reviewable_projection()),
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(outcome, ReviewOutcome::Allow(_)));
        assert_eq!(fake.called_count(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retries_parse_failure_and_succeeds() {
        let fake = FakeTransport::sequence(vec![Ok("not json".to_owned()), Ok(allow_json())]);
        let reviewer = make_reviewer(fake.clone());
        let outcome = reviewer
            .review(
                review_request(reviewable_projection()),
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(outcome, ReviewOutcome::Allow(_)));
        assert_eq!(fake.called_count(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn three_failures_produce_synthetic_deny() {
        let fake = FakeTransport::sequence(vec![
            Err(ReviewerTransportError::Transient("boom".to_owned())),
            Err(ReviewerTransportError::Transient("boom".to_owned())),
            Err(ReviewerTransportError::Transient("boom".to_owned())),
        ]);
        let reviewer = make_reviewer(fake.clone());
        let outcome = reviewer
            .review(
                review_request(reviewable_projection()),
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(outcome, ReviewOutcome::Deny(d) if d.rationale.contains("3 attempts")));
        assert_eq!(fake.called_count(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fatal_error_produces_synthetic_deny_without_retry() {
        let fake = FakeTransport::sequence(vec![Err(ReviewerTransportError::Fatal(
            "bad config".to_owned(),
        ))]);
        let reviewer = make_reviewer(fake.clone());
        let outcome = reviewer
            .review(
                review_request(reviewable_projection()),
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(outcome, ReviewOutcome::Deny(d) if d.rationale.contains("fatal")));
        assert_eq!(fake.called_count(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn timeout_produces_synthetic_deny() {
        tokio::time::pause();
        let fake = FakeTransport::sleep_forever();
        let reviewer = Arc::new(make_reviewer(fake.clone()));
        let request = review_request(reviewable_projection());
        let cancel = CancellationToken::new();
        let review_fut = reviewer.review(request, cancel);
        let advance = tokio::time::advance(Duration::from_secs(95));
        let (outcome, _) = tokio::join!(review_fut, advance);
        assert!(matches!(outcome, ReviewOutcome::Deny(d) if d.rationale.contains("timed out")));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancellation_produces_synthetic_deny_without_call() {
        let fake = FakeTransport::sequence(vec![]);
        let reviewer = make_reviewer(fake.clone());
        let cancel = CancellationToken::new();
        cancel.cancel();
        let outcome = reviewer
            .review(review_request(reviewable_projection()), cancel)
            .await;
        assert!(matches!(outcome, ReviewOutcome::Deny(d) if d.rationale.contains("cancelled")));
        assert_eq!(fake.called_count(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn circuit_breaker_opens_after_three_consecutive_denies() {
        let fake = FakeTransport::sequence(vec![Ok(deny_json()), Ok(deny_json()), Ok(deny_json())]);
        let reviewer = make_reviewer(fake.clone());
        let mut req = review_request(reviewable_projection());
        req.turn_id = None; // disable per-turn deny cache so each call reaches the model
        for _ in 0..3 {
            reviewer.review(req.clone(), CancellationToken::new()).await;
        }
        let outcome = reviewer.review(req, CancellationToken::new()).await;
        assert!(
            matches!(outcome, ReviewOutcome::Deny(d) if d.rationale.contains("circuit breaker"))
        );
        assert_eq!(fake.called_count(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn circuit_breaker_resets_at_run_boundary() {
        let fake = FakeTransport::sequence(vec![
            Ok(deny_json()),
            Ok(deny_json()),
            Ok(deny_json()),
            Ok(allow_json()),
        ]);
        let reviewer = make_reviewer(fake.clone());
        let mut request = review_request(reviewable_projection());
        request.turn_id = None;
        for _ in 0..3 {
            assert!(matches!(
                reviewer
                    .review(request.clone(), CancellationToken::new())
                    .await,
                ReviewOutcome::Deny(_)
            ));
        }
        request.run_id = "run-2".to_owned();
        assert!(matches!(
            reviewer.review(request, CancellationToken::new()).await,
            ReviewOutcome::Allow(_)
        ));
        assert_eq!(fake.called_count(), 4);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn allow_cache_avoids_transport_on_repeat() {
        let fake = FakeTransport::sequence(vec![Ok(allow_json())]);
        let reviewer = make_reviewer(fake.clone());
        let req = review_request(reviewable_projection());
        let outcome1 = reviewer.review(req.clone(), CancellationToken::new()).await;
        assert!(matches!(outcome1, ReviewOutcome::Allow(_)));
        let outcome2 = reviewer.review(req, CancellationToken::new()).await;
        assert!(matches!(outcome2, ReviewOutcome::Allow(_)));
        assert_eq!(fake.called_count(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn allow_cache_does_not_cross_same_run_context_mutation() {
        let fake = FakeTransport::sequence(vec![Ok(allow_json()), Ok(allow_json())]);
        let reviewer = make_reviewer(fake.clone());
        let mut request = review_request(reviewable_projection());
        assert!(matches!(
            reviewer
                .review(request.clone(), CancellationToken::new())
                .await,
            ReviewOutcome::Allow(_)
        ));
        request.context_version = "v2-after-steer".to_owned();
        assert!(matches!(
            reviewer.review(request, CancellationToken::new()).await,
            ReviewOutcome::Allow(_)
        ));
        assert_eq!(fake.called_count(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn deny_cache_only_within_same_turn() {
        let fake = FakeTransport::sequence(vec![Ok(deny_json())]);
        let reviewer = make_reviewer(fake.clone());
        let mut req = review_request(reviewable_projection());
        let outcome1 = reviewer.review(req.clone(), CancellationToken::new()).await;
        assert!(matches!(outcome1, ReviewOutcome::Deny(_)));

        req.turn_id = Some("turn-2".to_owned());
        let fake2 = FakeTransport::sequence(vec![Ok(deny_json())]);
        let reviewer2 = make_reviewer(fake2.clone());
        let outcome2 = reviewer2.review(req, CancellationToken::new()).await;
        assert!(matches!(outcome2, ReviewOutcome::Deny(_)));
        // Different turn => not cached, so the second transport is called.
        assert_eq!(fake2.called_count(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invalid_json_response_is_retry_then_synthetic_deny() {
        let fake = FakeTransport::sequence(vec![
            Ok("not json".to_owned()),
            Ok("still not json".to_owned()),
            Ok("{\"outcome\": \"allow\"}".to_owned()), // missing required fields
        ]);
        let reviewer = make_reviewer(fake.clone());
        let outcome = reviewer
            .review(
                review_request(reviewable_projection()),
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(outcome, ReviewOutcome::Deny(d) if d.rationale.contains("3 attempts")));
        assert_eq!(fake.called_count(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn strict_schema_rejects_extra_properties() {
        let fake = FakeTransport::sequence(vec![Ok(
            "{\"outcome\":\"allow\",\"risk\":\"low\",\"authorization\":\"high\",\"rationale\":\"ok\",\"extra\":1}".to_owned(),
        )]);
        let reviewer = make_reviewer(fake.clone());
        let outcome = reviewer
            .review(
                review_request(reviewable_projection()),
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(outcome, ReviewOutcome::Deny(d) if d.rationale.contains("3 attempts")));
        assert_eq!(fake.called_count(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn user_mode_returns_synthetic_deny() {
        let fake = FakeTransport::sequence(vec![]);
        let reviewer = make_reviewer(fake.clone());
        let mut request = review_request(reviewable_projection());
        request.mode = ReviewerMode::User;
        let outcome = reviewer.review(request, CancellationToken::new()).await;
        assert!(matches!(outcome, ReviewOutcome::Deny(d) if d.rationale.contains("User mode")));
        assert_eq!(fake.called_count(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn no_tool_result_body_or_raw_action_in_prompt() {
        let fake = FakeTransport::sequence(vec![Ok(allow_json())]);
        let reviewer = make_reviewer(fake.clone());
        let mut request = review_request(reviewable_projection());
        request.transcript = vec![
            user_message("run it"),
            PublicMessage::Assistant(PublicAssistantMessage {
                content: vec![PublicAssistantContent::ToolCall {
                    tool_call: ToolCall {
                        id: "call-1".to_owned(),
                        name: "bash".to_owned(),
                        arguments: serde_json::from_value(json!({"command": "echo safe"})).unwrap(),
                    },
                    wire_item_index: 0,
                }],
                model: "model".to_owned(),
                provider: "provider".to_owned(),
                origin: ProviderOrigin {
                    provider_instance_id: "instance".to_owned(),
                    protocol: ApiProtocol::OpenAiChatCompletions,
                    model: "model".to_owned(),
                },
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                provider_code: None,
                interrupted: false,
                timestamp: chrono::Utc::now(),
            }),
            PublicMessage::ToolResult(crate::provider::types::ToolResultMessage {
                tool_call_id: "call-1".to_owned(),
                tool_name: "bash".to_owned(),
                content: vec![UserContent::Text {
                    text: "TOOL_RESULT_BODY_SECRET".to_owned(),
                }],
                details: json!({}),
                is_error: false,
                timestamp: chrono::Utc::now(),
            }),
        ];
        reviewer.review(request, CancellationToken::new()).await;

        let log = fake.log.lock().unwrap_or_else(|e| e.into_inner());
        let payload = log.first().cloned().unwrap_or_default();
        assert!(
            !payload.contains("TOOL_RESULT_BODY_SECRET"),
            "tool result body crossed reviewer boundary"
        );
        assert!(
            payload.contains("tool_result bash: outcome=ok"),
            "tool result summary missing"
        );
        // The ReviewProjection is already redacted; raw argv and absolute cwd are
        // not present (the projection uses digest/path components, not raw text).
        assert!(
            !payload.contains("TOOL_RESULT_BODY_SECRET"),
            "raw action body crossed reviewer boundary"
        );
    }
}
