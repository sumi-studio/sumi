//! ADR 0013's two fail-closed AutoReview boundaries.
//!
//! The two reviewers deliberately have separate request, prompt, transport,
//! decision, evidence, and result types. This module consumes only already
//! redacted, app-owned evidence; it never receives raw execution arguments.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

use crate::provider::{
    ModelSpec, ProtocolCompat, RequestOptions,
    model::{ChatStructuredOutputMode, StructuredOutputSchema},
    retry, stream,
    types::{
        AssistantContent, ContextMessage, Message, PromptContext, ProviderEvent, StopReason,
        UserContent, UserMessage,
    },
};

const MAX_COMPILED_ATTEMPTS: u8 = 2;
const MAX_COMPILED_TOTAL: Duration = Duration::from_secs(30);
const MAX_REVIEW_REQUEST_BYTES: usize = 256 * 1024;

pub const REVIEWER_BUDGET_VERSION_V1: &str = "reviewer-budget/v1";
pub const EXECUTION_REVIEWER_VERSION_V1: &str = "execution-reviewer/v1";
pub const EXECUTION_PROMPT_VERSION_V1: &str = "execution-review-prompt/v1";
pub const EXECUTION_SCHEMA_VERSION_V1: &str = "execution-review-schema/v1";
pub const ESCALATION_REVIEWER_VERSION_V1: &str = "escalation-reviewer/v1";
pub const ESCALATION_PROMPT_VERSION_V1: &str = "escalation-review-prompt/v1";
pub const ESCALATION_SCHEMA_VERSION_V1: &str = "escalation-review-schema/v1";

const EXECUTION_SYSTEM_PROMPT: &str = include_str!("../../prompts/approval/execution-review.md");
const ESCALATION_SYSTEM_PROMPT: &str = include_str!("../../prompts/approval/escalation-review.md");

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct ReviewerBudgetV1 {
    pub max_attempts: u8,
    #[serde(with = "duration_millis")]
    pub attempt_timeout: Duration,
    #[serde(with = "duration_millis")]
    pub total_timeout: Duration,
}

impl ReviewerBudgetV1 {
    pub const fn execution() -> Self {
        Self {
            max_attempts: 2,
            attempt_timeout: Duration::from_secs(10),
            total_timeout: Duration::from_secs(15),
        }
    }

    pub const fn escalation() -> Self {
        Self {
            max_attempts: 2,
            attempt_timeout: Duration::from_secs(15),
            total_timeout: Duration::from_secs(20),
        }
    }

    pub fn compile(self) -> Result<CompiledReviewerBudget, ReviewerNotReady> {
        if self.max_attempts == 0 || self.max_attempts > MAX_COMPILED_ATTEMPTS {
            return Err(ReviewerNotReady::InvalidBudget(
                "max_attempts must be between 1 and 2".to_owned(),
            ));
        }
        if self.attempt_timeout.is_zero()
            || self.total_timeout.is_zero()
            || self.attempt_timeout > self.total_timeout
            || self.total_timeout > MAX_COMPILED_TOTAL
        {
            return Err(ReviewerNotReady::InvalidBudget(
                "timeouts must be non-zero, attempt <= total, and total <= 30s".to_owned(),
            ));
        }
        let encoded = serde_json::to_vec(&self).map_err(|error| {
            ReviewerNotReady::InvalidBudget(format!("budget serialization failed: {error}"))
        })?;
        let mut digest = Sha256::new();
        digest.update(b"sumi-reviewer-budget/v1\0");
        digest.update((encoded.len() as u64).to_be_bytes());
        digest.update(encoded);
        Ok(CompiledReviewerBudget {
            budget: self,
            digest: hex(&digest.finalize()),
        })
    }
}

#[derive(Clone, Debug)]
pub struct CompiledReviewerBudget {
    budget: ReviewerBudgetV1,
    digest: String,
}

impl CompiledReviewerBudget {
    pub fn evidence(
        &self,
        attempts: u8,
        terminal: ReviewerTerminalClass,
    ) -> ReviewerBudgetEvidence {
        ReviewerBudgetEvidence {
            version: REVIEWER_BUDGET_VERSION_V1.to_owned(),
            digest: self.digest.clone(),
            attempts,
            terminal,
        }
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReviewerNotReady {
    #[error("reviewer budget is invalid: {0}")]
    InvalidBudget(String),
    #[error("reviewer model is outside the configured trust bindings")]
    UntrustedModel,
    #[error("{reviewer} reviewer model does not support structured output")]
    StructuredOutputUnsupported { reviewer: &'static str },
    #[error("{left} and {right} must use distinct provider-instance/account/model identities")]
    CollapsedModelIdentity {
        left: &'static str,
        right: &'static str,
    },
    #[error("{left} and {right} must use distinct normalized provider base endpoints")]
    CollapsedProviderOrigin {
        left: &'static str,
        right: &'static str,
    },
    #[error("{left} and {right} must use distinct credential sources")]
    CollapsedCredentialSource {
        left: &'static str,
        right: &'static str,
    },
}

#[derive(Clone, Debug)]
pub struct IndependentReviewerModels {
    execution: ModelSpec,
    escalation: ModelSpec,
}

impl IndependentReviewerModels {
    pub fn new(
        conversation: &ModelSpec,
        execution: ModelSpec,
        escalation: ModelSpec,
    ) -> Result<Self, ReviewerNotReady> {
        require_structured_output("Execution", &execution)?;
        require_structured_output("Escalation", &escalation)?;
        require_independent_pair(
            "conversation",
            conversation,
            "Execution reviewer",
            &execution,
        )?;
        require_independent_pair(
            "conversation",
            conversation,
            "Escalation reviewer",
            &escalation,
        )?;
        require_independent_pair(
            "Execution reviewer",
            &execution,
            "Escalation reviewer",
            &escalation,
        )?;
        Ok(Self {
            execution,
            escalation,
        })
    }

    pub fn into_parts(self) -> (ModelSpec, ModelSpec, ReviewerTrustSet) {
        let execution_model = ReviewerModelSpec::from_provider(&self.execution);
        let escalation_model = ReviewerModelSpec::from_provider(&self.escalation);
        let trust = ReviewerTrustSet::new(vec![execution_model.clone(), escalation_model.clone()]);
        (self.execution, self.escalation, trust)
    }
}

fn require_independent_pair(
    left_name: &'static str,
    left: &ModelSpec,
    right_name: &'static str,
    right: &ModelSpec,
) -> Result<(), ReviewerNotReady> {
    require_distinct_identity(left_name, left, right_name, right)?;
    if left.provider_origin_id() == right.provider_origin_id() {
        return Err(ReviewerNotReady::CollapsedProviderOrigin {
            left: left_name,
            right: right_name,
        });
    }
    if left.api_key_env == right.api_key_env {
        return Err(ReviewerNotReady::CollapsedCredentialSource {
            left: left_name,
            right: right_name,
        });
    }
    Ok(())
}

fn require_structured_output(
    reviewer: &'static str,
    spec: &ModelSpec,
) -> Result<(), ReviewerNotReady> {
    let supported = match &spec.compat {
        ProtocolCompat::Chat(compat) => {
            compat.structured_output != ChatStructuredOutputMode::Unsupported
        }
        ProtocolCompat::Responses(_) | ProtocolCompat::Anthropic(_) => true,
    };
    if supported {
        Ok(())
    } else {
        Err(ReviewerNotReady::StructuredOutputUnsupported { reviewer })
    }
}

fn require_distinct_identity(
    left_name: &'static str,
    left: &ModelSpec,
    right_name: &'static str,
    right: &ModelSpec,
) -> Result<(), ReviewerNotReady> {
    let left_identity = (
        left.provider_instance_id(),
        left.account_scope.as_str(),
        left.id.as_str(),
    );
    let right_identity = (
        right.provider_instance_id(),
        right.account_scope.as_str(),
        right.id.as_str(),
    );
    if left_identity == right_identity {
        Err(ReviewerNotReady::CollapsedModelIdentity {
            left: left_name,
            right: right_name,
        })
    } else {
        Ok(())
    }
}

/// Complete reviewer identity. A friendly trust-domain label alone is not a
/// credential: provider endpoint, account scope, and processing policy are all
/// bound by trusted runtime configuration.
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

    pub fn from_provider(spec: &ModelSpec) -> Self {
        Self {
            id: spec.id.clone(),
            provider: spec.provider.clone(),
            base_url: spec.base_url.clone(),
            account_scope: spec.account_scope.clone(),
            trust_domain_id: spec.provider_instance_id(),
            data_processing_policy: "same-provider-account".to_owned(),
        }
    }

    fn is_complete(&self) -> bool {
        [
            &self.id,
            &self.provider,
            &self.base_url,
            &self.account_scope,
            &self.trust_domain_id,
            &self.data_processing_policy,
        ]
        .into_iter()
        .all(|value| !value.trim().is_empty())
    }

    fn binding_digest(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"sumi-reviewer-model-binding/v1\0");
        for field in [
            &self.id,
            &self.provider,
            &self.base_url,
            &self.account_scope,
            &self.trust_domain_id,
            &self.data_processing_policy,
        ] {
            digest.update((field.len() as u64).to_be_bytes());
            digest.update(field.as_bytes());
        }
        hex(&digest.finalize())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReviewerTrustSet {
    allowed: Vec<ReviewerModelSpec>,
}

impl ReviewerTrustSet {
    pub fn new(allowed_reviewer_models: Vec<ReviewerModelSpec>) -> Self {
        Self {
            allowed: allowed_reviewer_models,
        }
    }

    pub fn allows(&self, model: &ReviewerModelSpec) -> bool {
        model.is_complete()
            && self
                .allowed
                .iter()
                .any(|allowed| allowed.is_complete() && allowed == model)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewerTerminalClass {
    ValidDecision,
    CriticalPositiveBlocked,
    TransientExhausted,
    MalformedExhausted,
    AttemptTimeout,
    Cancelled,
    FatalTransport,
    EmptyResponse,
    ToolCallResponse,
    InsufficientEvidence,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewerBudgetEvidence {
    pub version: String,
    pub digest: String,
    pub attempts: u8,
    pub terminal: ReviewerTerminalClass,
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
pub enum ExecutionReviewOutcome {
    Allow,
    Block,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionReviewDecision {
    pub outcome: ExecutionReviewOutcome,
    pub risk: RiskLevel,
    pub rationale: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationReviewOutcome {
    AskHuman,
    Block,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EscalationReviewDecision {
    pub outcome: EscalationReviewOutcome,
    pub risk: RiskLevel,
    pub misunderstanding: Option<String>,
    pub rationale: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionReviewRequest {
    pub action_digest: String,
    pub redacted_evidence: Value,
    pub bounded_context: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EscalationReviewRequest {
    pub action_digest: String,
    pub redacted_evidence: Value,
    pub bounded_context: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExecutionReviewOutputSchema(StructuredOutputSchema);

impl ExecutionReviewOutputSchema {
    fn v1() -> Self {
        Self(StructuredOutputSchema {
            name: "sumi_execution_review_v1".to_owned(),
            description: "Sumi Execution AutoReview decision".to_owned(),
            schema: json!({
                "type": "object",
                "properties": {
                    "outcome": {"type": "string", "enum": ["allow", "block"]},
                    "risk": {"type": "string", "enum": ["low", "medium", "high", "critical"]},
                    "rationale": {"type": "string"}
                },
                "required": ["outcome", "risk", "rationale"],
                "additionalProperties": false
            }),
        })
    }

    fn provider_schema(&self) -> &StructuredOutputSchema {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct EscalationReviewOutputSchema(StructuredOutputSchema);

impl EscalationReviewOutputSchema {
    fn v1() -> Self {
        Self(StructuredOutputSchema {
            name: "sumi_escalation_review_v1".to_owned(),
            description: "Sumi Escalation AutoReview decision".to_owned(),
            schema: json!({
                "type": "object",
                "properties": {
                    "outcome": {"type": "string", "enum": ["ask_human", "block"]},
                    "risk": {"type": "string", "enum": ["low", "medium", "high", "critical"]},
                    "misunderstanding": {"type": ["string", "null"]},
                    "rationale": {"type": "string"}
                },
                "required": ["outcome", "risk", "misunderstanding", "rationale"],
                "additionalProperties": false
            }),
        })
    }

    fn provider_schema(&self) -> &StructuredOutputSchema {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionReviewerPrompt {
    #[serde(skip)]
    pub system: &'static str,
    #[serde(skip)]
    pub output_schema: ExecutionReviewOutputSchema,
    pub prompt_version: &'static str,
    pub schema_version: &'static str,
    pub request: ExecutionReviewRequest,
    pub retry_validation_code: Option<ReviewerValidationCode>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EscalationReviewerPrompt {
    #[serde(skip)]
    pub system: &'static str,
    #[serde(skip)]
    pub output_schema: EscalationReviewOutputSchema,
    pub prompt_version: &'static str,
    pub schema_version: &'static str,
    pub request: EscalationReviewRequest,
    pub retry_validation_code: Option<ReviewerValidationCode>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewerValidationCode {
    InvalidJson,
    SchemaMismatch,
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReviewerTransportError {
    #[error("transient reviewer transport failure: {0}")]
    Transient(String),
    #[error("fatal reviewer transport failure: {0}")]
    Fatal(String),
    #[error("reviewer transport was cancelled")]
    Cancelled,
    #[error("reviewer returned an empty response")]
    Empty,
    #[error("reviewer returned a tool call")]
    ToolCall,
}

#[async_trait]
pub trait ExecutionReviewerTransport: Send + Sync {
    fn model_spec(&self) -> &ReviewerModelSpec;

    async fn complete(
        &self,
        prompt: &ExecutionReviewerPrompt,
        cancel: CancellationToken,
    ) -> Result<String, ReviewerTransportError>;
}

#[async_trait]
pub trait EscalationReviewerTransport: Send + Sync {
    fn model_spec(&self) -> &ReviewerModelSpec;

    async fn complete(
        &self,
        prompt: &EscalationReviewerPrompt,
        cancel: CancellationToken,
    ) -> Result<String, ReviewerTransportError>;
}

/// Production transport for Execution AutoReview. Its concrete type stays
/// separate from the escalation transport even when both use one provider.
pub struct ProviderExecutionReviewerTransport {
    spec: ModelSpec,
    model: ReviewerModelSpec,
}

impl ProviderExecutionReviewerTransport {
    pub fn new(spec: ModelSpec) -> Self {
        let model = ReviewerModelSpec::from_provider(&spec);
        Self { spec, model }
    }
}

#[async_trait]
impl ExecutionReviewerTransport for ProviderExecutionReviewerTransport {
    fn model_spec(&self) -> &ReviewerModelSpec {
        &self.model
    }

    async fn complete(
        &self,
        prompt: &ExecutionReviewerPrompt,
        cancel: CancellationToken,
    ) -> Result<String, ReviewerTransportError> {
        complete_provider_review(
            &self.spec,
            prompt.system,
            prompt.output_schema.provider_schema(),
            prompt,
            cancel,
        )
        .await
    }
}

pub struct ProviderEscalationReviewerTransport {
    spec: ModelSpec,
    model: ReviewerModelSpec,
}

impl ProviderEscalationReviewerTransport {
    pub fn new(spec: ModelSpec) -> Self {
        let model = ReviewerModelSpec::from_provider(&spec);
        Self { spec, model }
    }
}

#[async_trait]
impl EscalationReviewerTransport for ProviderEscalationReviewerTransport {
    fn model_spec(&self) -> &ReviewerModelSpec {
        &self.model
    }

    async fn complete(
        &self,
        prompt: &EscalationReviewerPrompt,
        cancel: CancellationToken,
    ) -> Result<String, ReviewerTransportError> {
        complete_provider_review(
            &self.spec,
            prompt.system,
            prompt.output_schema.provider_schema(),
            prompt,
            cancel,
        )
        .await
    }
}

async fn complete_provider_review(
    spec: &ModelSpec,
    system: &str,
    output_schema: &StructuredOutputSchema,
    prompt: &impl Serialize,
    cancel: CancellationToken,
) -> Result<String, ReviewerTransportError> {
    let (context, options) = build_provider_review_request(spec, system, output_schema, prompt)?;
    let mut events = stream(spec.clone(), context, options, cancel.clone());
    loop {
        let Some(event) = events.recv().await else {
            return Err(ReviewerTransportError::Transient(
                "provider ended without a terminal event".to_owned(),
            ));
        };
        if let Some(terminal) = classify_provider_review_terminal(event, cancel.is_cancelled()) {
            return terminal;
        }
    }
}

fn classify_provider_review_terminal(
    event: ProviderEvent,
    cancelled: bool,
) -> Option<Result<String, ReviewerTransportError>> {
    let (kind, reason, output) = match event {
        ProviderEvent::Done { reason, output } => ("done", reason, output),
        ProviderEvent::Error { reason, output } => ("error", reason, output),
        _ => return None,
    };
    if cancelled || reason == StopReason::Aborted {
        return Some(Err(ReviewerTransportError::Cancelled));
    }
    if reason != output.message.stop_reason {
        return Some(Err(ReviewerTransportError::Fatal(format!(
            "provider {kind} terminal reason disagrees with its message"
        ))));
    }
    match reason {
        StopReason::Stop if kind == "done" => Some(extract_provider_review_text(output.message)),
        StopReason::ToolUse => Some(Err(ReviewerTransportError::ToolCall)),
        StopReason::Length => Some(Err(ReviewerTransportError::Fatal(
            "provider truncated the reviewer response".to_owned(),
        ))),
        StopReason::Error => Some(Err(classify_provider_review_error(&output.message))),
        StopReason::Stop => Some(Err(ReviewerTransportError::Fatal(
            "provider emitted an error terminal with a success reason".to_owned(),
        ))),
        StopReason::Aborted => unreachable!("aborted terminals return above"),
    }
}

fn extract_provider_review_text(
    message: crate::provider::types::AssistantMessage,
) -> Result<String, ReviewerTransportError> {
    let mut parts = Vec::new();
    for content in message.content {
        match content {
            AssistantContent::Text { text, .. } => parts.push(text),
            AssistantContent::Thinking { .. } => {}
            AssistantContent::ToolCall { .. } | AssistantContent::RejectedToolCall { .. } => {
                return Err(ReviewerTransportError::ToolCall);
            }
        }
    }
    let text = parts.join("").trim().to_owned();
    if text.is_empty() {
        Err(ReviewerTransportError::Empty)
    } else {
        Ok(text)
    }
}

fn classify_provider_review_error(
    message: &crate::provider::types::AssistantMessage,
) -> ReviewerTransportError {
    let detail = match (&message.provider_code, &message.error_message) {
        (Some(code), Some(error)) => format!("{code}: {error}"),
        (Some(code), None) => code.clone(),
        (None, Some(error)) => error.clone(),
        (None, None) => "provider returned an unclassified error".to_owned(),
    };
    if retry::is_retryable(message) {
        ReviewerTransportError::Transient(detail)
    } else {
        ReviewerTransportError::Fatal(detail)
    }
}

fn build_provider_review_request(
    spec: &ModelSpec,
    system: &str,
    output_schema: &StructuredOutputSchema,
    prompt: &impl Serialize,
) -> Result<(PromptContext, RequestOptions), ReviewerTransportError> {
    let evidence = serde_json::to_string(prompt).map_err(|error| {
        ReviewerTransportError::Fatal(format!("reviewer prompt serialization failed: {error}"))
    })?;
    let context = PromptContext::new(
        system.to_owned(),
        Vec::new(),
        vec![ContextMessage::Synthetic {
            message: Message::User(UserMessage {
                content: vec![UserContent::Text { text: evidence }],
                timestamp: Utc::now(),
            }),
        }],
        Vec::new(),
        Vec::new(),
    );
    let options = RequestOptions {
        max_tokens: Some(spec.max_output_tokens.min(2_048)),
        structured_output: Some(output_schema.clone()),
        ..RequestOptions::default()
    };
    Ok((context, options))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionReviewEvidence {
    pub reviewer_version: String,
    pub prompt_version: String,
    pub schema_version: String,
    pub model_id: String,
    pub model_binding_digest: String,
    pub budget: ReviewerBudgetEvidence,
    pub decision: ExecutionReviewDecision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionReviewResult {
    Allow(ExecutionReviewEvidence),
    Block(ExecutionReviewEvidence),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EscalationReviewEvidence {
    pub reviewer_version: String,
    pub prompt_version: String,
    pub schema_version: String,
    pub model_id: String,
    pub model_binding_digest: String,
    pub budget: ReviewerBudgetEvidence,
    pub decision: EscalationReviewDecision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EscalationReviewResult {
    AskHuman(EscalationReviewEvidence),
    Block(EscalationReviewEvidence),
}

pub struct ExecutionReviewer {
    model: ReviewerModelSpec,
    transport: Arc<dyn ExecutionReviewerTransport>,
    budget: CompiledReviewerBudget,
}

impl ExecutionReviewer {
    pub fn new(
        model: ReviewerModelSpec,
        trust: ReviewerTrustSet,
        transport: Arc<dyn ExecutionReviewerTransport>,
        budget: ReviewerBudgetV1,
    ) -> Result<Self, ReviewerNotReady> {
        if !trust.allows(&model) {
            return Err(ReviewerNotReady::UntrustedModel);
        }
        if transport.model_spec() != &model {
            return Err(ReviewerNotReady::UntrustedModel);
        }
        Ok(Self {
            model,
            transport,
            budget: budget.compile()?,
        })
    }

    pub async fn review(
        &self,
        request: ExecutionReviewRequest,
        cancel: CancellationToken,
    ) -> ExecutionReviewResult {
        if !review_request_is_bounded(&request) {
            return execution_synthetic_block(self, 0, ReviewerTerminalClass::InsufficientEvidence);
        }
        let mut retry_validation_code = None;
        let started = Instant::now();
        let mut attempts = 0;
        loop {
            attempts += 1;
            let prompt = ExecutionReviewerPrompt {
                system: EXECUTION_SYSTEM_PROMPT,
                output_schema: ExecutionReviewOutputSchema::v1(),
                prompt_version: EXECUTION_PROMPT_VERSION_V1,
                schema_version: EXECUTION_SCHEMA_VERSION_V1,
                request: request.clone(),
                retry_validation_code,
            };
            let attempt = run_attempt(
                &*self.transport,
                &prompt,
                cancel.clone(),
                self.budget.budget.attempt_timeout,
                self.budget
                    .budget
                    .total_timeout
                    .saturating_sub(started.elapsed()),
            )
            .await;
            match attempt {
                AttemptOutcome::Response(raw) => match parse_execution_decision(&raw) {
                    Ok(mut decision) => {
                        let terminal = if decision.risk == RiskLevel::Critical
                            && decision.outcome == ExecutionReviewOutcome::Allow
                        {
                            decision.outcome = ExecutionReviewOutcome::Block;
                            ReviewerTerminalClass::CriticalPositiveBlocked
                        } else {
                            ReviewerTerminalClass::ValidDecision
                        };
                        return execution_result(self, decision, attempts, terminal);
                    }
                    Err(code) if attempts < self.budget.budget.max_attempts => {
                        retry_validation_code = Some(code);
                    }
                    Err(_) => {
                        return execution_synthetic_block(
                            self,
                            attempts,
                            ReviewerTerminalClass::MalformedExhausted,
                        );
                    }
                },
                AttemptOutcome::RetryTransient
                    if attempts < self.budget.budget.max_attempts
                        && started.elapsed() < self.budget.budget.total_timeout => {}
                AttemptOutcome::RetryTransient => {
                    return execution_synthetic_block(
                        self,
                        attempts,
                        ReviewerTerminalClass::TransientExhausted,
                    );
                }
                AttemptOutcome::Terminal(class) => {
                    return execution_synthetic_block(self, attempts, class);
                }
            }
        }
    }

    pub fn block_without_call(&self, terminal: ReviewerTerminalClass) -> ExecutionReviewEvidence {
        execution_synthetic_block(self, 0, terminal).into_evidence()
    }
}

pub struct EscalationReviewer {
    model: ReviewerModelSpec,
    transport: Arc<dyn EscalationReviewerTransport>,
    budget: CompiledReviewerBudget,
}

impl EscalationReviewer {
    pub fn new(
        model: ReviewerModelSpec,
        trust: ReviewerTrustSet,
        transport: Arc<dyn EscalationReviewerTransport>,
        budget: ReviewerBudgetV1,
    ) -> Result<Self, ReviewerNotReady> {
        if !trust.allows(&model) {
            return Err(ReviewerNotReady::UntrustedModel);
        }
        if transport.model_spec() != &model {
            return Err(ReviewerNotReady::UntrustedModel);
        }
        Ok(Self {
            model,
            transport,
            budget: budget.compile()?,
        })
    }

    pub async fn review(
        &self,
        request: EscalationReviewRequest,
        cancel: CancellationToken,
    ) -> EscalationReviewResult {
        if !review_request_is_bounded(&request) {
            return escalation_synthetic_block(
                self,
                0,
                ReviewerTerminalClass::InsufficientEvidence,
            );
        }
        let mut retry_validation_code = None;
        let started = Instant::now();
        let mut attempts = 0;
        loop {
            attempts += 1;
            let prompt = EscalationReviewerPrompt {
                system: ESCALATION_SYSTEM_PROMPT,
                output_schema: EscalationReviewOutputSchema::v1(),
                prompt_version: ESCALATION_PROMPT_VERSION_V1,
                schema_version: ESCALATION_SCHEMA_VERSION_V1,
                request: request.clone(),
                retry_validation_code,
            };
            let attempt = run_attempt(
                &*self.transport,
                &prompt,
                cancel.clone(),
                self.budget.budget.attempt_timeout,
                self.budget
                    .budget
                    .total_timeout
                    .saturating_sub(started.elapsed()),
            )
            .await;
            match attempt {
                AttemptOutcome::Response(raw) => match parse_escalation_decision(&raw) {
                    Ok(mut decision) => {
                        let terminal = if decision.risk == RiskLevel::Critical
                            && decision.outcome == EscalationReviewOutcome::AskHuman
                        {
                            decision.outcome = EscalationReviewOutcome::Block;
                            ReviewerTerminalClass::CriticalPositiveBlocked
                        } else {
                            ReviewerTerminalClass::ValidDecision
                        };
                        return escalation_result(self, decision, attempts, terminal);
                    }
                    Err(code) if attempts < self.budget.budget.max_attempts => {
                        retry_validation_code = Some(code);
                    }
                    Err(_) => {
                        return escalation_synthetic_block(
                            self,
                            attempts,
                            ReviewerTerminalClass::MalformedExhausted,
                        );
                    }
                },
                AttemptOutcome::RetryTransient
                    if attempts < self.budget.budget.max_attempts
                        && started.elapsed() < self.budget.budget.total_timeout => {}
                AttemptOutcome::RetryTransient => {
                    return escalation_synthetic_block(
                        self,
                        attempts,
                        ReviewerTerminalClass::TransientExhausted,
                    );
                }
                AttemptOutcome::Terminal(class) => {
                    return escalation_synthetic_block(self, attempts, class);
                }
            }
        }
    }

    pub fn block_without_call(&self, terminal: ReviewerTerminalClass) -> EscalationReviewEvidence {
        escalation_synthetic_block(self, 0, terminal).into_evidence()
    }
}

impl ExecutionReviewResult {
    fn into_evidence(self) -> ExecutionReviewEvidence {
        match self {
            Self::Allow(evidence) | Self::Block(evidence) => evidence,
        }
    }
}

impl EscalationReviewResult {
    fn into_evidence(self) -> EscalationReviewEvidence {
        match self {
            Self::AskHuman(evidence) | Self::Block(evidence) => evidence,
        }
    }
}

#[derive(Debug)]
enum AttemptOutcome {
    Response(String),
    RetryTransient,
    Terminal(ReviewerTerminalClass),
}

#[async_trait]
trait AttemptTransport<P>: Send + Sync {
    async fn call(
        &self,
        prompt: &P,
        cancel: CancellationToken,
    ) -> Result<String, ReviewerTransportError>;
}

#[async_trait]
impl<T: ExecutionReviewerTransport + ?Sized> AttemptTransport<ExecutionReviewerPrompt> for T {
    async fn call(
        &self,
        prompt: &ExecutionReviewerPrompt,
        cancel: CancellationToken,
    ) -> Result<String, ReviewerTransportError> {
        self.complete(prompt, cancel).await
    }
}

#[async_trait]
impl<T: EscalationReviewerTransport + ?Sized> AttemptTransport<EscalationReviewerPrompt> for T {
    async fn call(
        &self,
        prompt: &EscalationReviewerPrompt,
        cancel: CancellationToken,
    ) -> Result<String, ReviewerTransportError> {
        self.complete(prompt, cancel).await
    }
}

async fn run_attempt<P>(
    transport: &(impl AttemptTransport<P> + ?Sized),
    prompt: &P,
    cancel: CancellationToken,
    attempt_timeout: Duration,
    remaining_total: Duration,
) -> AttemptOutcome {
    if cancel.is_cancelled() {
        return AttemptOutcome::Terminal(ReviewerTerminalClass::Cancelled);
    }
    let deadline = attempt_timeout.min(remaining_total);
    if deadline.is_zero() {
        return AttemptOutcome::Terminal(ReviewerTerminalClass::AttemptTimeout);
    }
    let attempt_cancel = cancel.child_token();
    let response = tokio::select! {
        _ = cancel.cancelled() => {
            attempt_cancel.cancel();
            return AttemptOutcome::Terminal(ReviewerTerminalClass::Cancelled);
        }
        response = timeout(deadline, transport.call(prompt, attempt_cancel.clone())) => response,
    };
    match response {
        Err(_) => {
            attempt_cancel.cancel();
            AttemptOutcome::Terminal(ReviewerTerminalClass::AttemptTimeout)
        }
        Ok(Err(ReviewerTransportError::Transient(_))) => AttemptOutcome::RetryTransient,
        Ok(Err(ReviewerTransportError::Fatal(_))) => {
            AttemptOutcome::Terminal(ReviewerTerminalClass::FatalTransport)
        }
        Ok(Err(ReviewerTransportError::Cancelled)) => {
            AttemptOutcome::Terminal(ReviewerTerminalClass::Cancelled)
        }
        Ok(Err(ReviewerTransportError::Empty)) => {
            AttemptOutcome::Terminal(ReviewerTerminalClass::EmptyResponse)
        }
        Ok(Err(ReviewerTransportError::ToolCall)) => {
            AttemptOutcome::Terminal(ReviewerTerminalClass::ToolCallResponse)
        }
        Ok(Ok(raw)) if raw.trim().is_empty() => {
            AttemptOutcome::Terminal(ReviewerTerminalClass::EmptyResponse)
        }
        Ok(Ok(raw)) => AttemptOutcome::Response(raw),
    }
}

fn parse_decision<T: DeserializeOwned>(raw: &str) -> Result<T, ReviewerValidationCode> {
    let value: Value =
        serde_json::from_str(raw).map_err(|_| ReviewerValidationCode::InvalidJson)?;
    serde_json::from_value(value).map_err(|_| ReviewerValidationCode::SchemaMismatch)
}

fn parse_execution_decision(raw: &str) -> Result<ExecutionReviewDecision, ReviewerValidationCode> {
    let decision = parse_decision::<ExecutionReviewDecision>(raw)?;
    if !valid_rationale(&decision.rationale) {
        return Err(ReviewerValidationCode::SchemaMismatch);
    }
    Ok(decision)
}

fn parse_escalation_decision(
    raw: &str,
) -> Result<EscalationReviewDecision, ReviewerValidationCode> {
    let value: Value =
        serde_json::from_str(raw).map_err(|_| ReviewerValidationCode::InvalidJson)?;
    if !value
        .as_object()
        .is_some_and(|object| object.contains_key("misunderstanding"))
    {
        return Err(ReviewerValidationCode::SchemaMismatch);
    }
    let decision = serde_json::from_value::<EscalationReviewDecision>(value)
        .map_err(|_| ReviewerValidationCode::SchemaMismatch)?;
    if !valid_rationale(&decision.rationale)
        || decision
            .misunderstanding
            .as_ref()
            .is_some_and(|value| value.chars().count() > 1000)
    {
        return Err(ReviewerValidationCode::SchemaMismatch);
    }
    Ok(decision)
}

fn valid_rationale(rationale: &str) -> bool {
    !rationale.trim().is_empty() && rationale.chars().count() <= 1000
}

fn review_request_is_bounded(request: &impl Serialize) -> bool {
    serde_json::to_vec(request).is_ok_and(|encoded| encoded.len() <= MAX_REVIEW_REQUEST_BYTES)
}

fn execution_result(
    reviewer: &ExecutionReviewer,
    mut decision: ExecutionReviewDecision,
    attempts: u8,
    terminal: ReviewerTerminalClass,
) -> ExecutionReviewResult {
    let allow = decision.outcome == ExecutionReviewOutcome::Allow;
    if decision.risk == RiskLevel::Critical {
        decision.outcome = ExecutionReviewOutcome::Block;
    }
    let evidence = ExecutionReviewEvidence {
        reviewer_version: EXECUTION_REVIEWER_VERSION_V1.to_owned(),
        prompt_version: EXECUTION_PROMPT_VERSION_V1.to_owned(),
        schema_version: EXECUTION_SCHEMA_VERSION_V1.to_owned(),
        model_id: reviewer.model.id.clone(),
        model_binding_digest: reviewer.model.binding_digest(),
        budget: reviewer.budget.evidence(attempts, terminal),
        decision,
    };
    if allow && evidence.decision.outcome == ExecutionReviewOutcome::Allow {
        ExecutionReviewResult::Allow(evidence)
    } else {
        ExecutionReviewResult::Block(evidence)
    }
}

fn execution_synthetic_block(
    reviewer: &ExecutionReviewer,
    attempts: u8,
    terminal: ReviewerTerminalClass,
) -> ExecutionReviewResult {
    ExecutionReviewResult::Block(ExecutionReviewEvidence {
        reviewer_version: EXECUTION_REVIEWER_VERSION_V1.to_owned(),
        prompt_version: EXECUTION_PROMPT_VERSION_V1.to_owned(),
        schema_version: EXECUTION_SCHEMA_VERSION_V1.to_owned(),
        model_id: reviewer.model.id.clone(),
        model_binding_digest: reviewer.model.binding_digest(),
        budget: reviewer.budget.evidence(attempts, terminal),
        decision: ExecutionReviewDecision {
            outcome: ExecutionReviewOutcome::Block,
            risk: RiskLevel::High,
            rationale: "reviewer failed closed".to_owned(),
        },
    })
}

fn escalation_result(
    reviewer: &EscalationReviewer,
    mut decision: EscalationReviewDecision,
    attempts: u8,
    terminal: ReviewerTerminalClass,
) -> EscalationReviewResult {
    let ask_human = decision.outcome == EscalationReviewOutcome::AskHuman;
    if decision.risk == RiskLevel::Critical {
        decision.outcome = EscalationReviewOutcome::Block;
    }
    let evidence = EscalationReviewEvidence {
        reviewer_version: ESCALATION_REVIEWER_VERSION_V1.to_owned(),
        prompt_version: ESCALATION_PROMPT_VERSION_V1.to_owned(),
        schema_version: ESCALATION_SCHEMA_VERSION_V1.to_owned(),
        model_id: reviewer.model.id.clone(),
        model_binding_digest: reviewer.model.binding_digest(),
        budget: reviewer.budget.evidence(attempts, terminal),
        decision,
    };
    if ask_human && evidence.decision.outcome == EscalationReviewOutcome::AskHuman {
        EscalationReviewResult::AskHuman(evidence)
    } else {
        EscalationReviewResult::Block(evidence)
    }
}

fn escalation_synthetic_block(
    reviewer: &EscalationReviewer,
    attempts: u8,
    terminal: ReviewerTerminalClass,
) -> EscalationReviewResult {
    EscalationReviewResult::Block(EscalationReviewEvidence {
        reviewer_version: ESCALATION_REVIEWER_VERSION_V1.to_owned(),
        prompt_version: ESCALATION_PROMPT_VERSION_V1.to_owned(),
        schema_version: ESCALATION_SCHEMA_VERSION_V1.to_owned(),
        model_id: reviewer.model.id.clone(),
        model_binding_digest: reviewer.model.binding_digest(),
        budget: reviewer.budget.evidence(attempts, terminal),
        decision: EscalationReviewDecision {
            outcome: EscalationReviewOutcome::Block,
            risk: RiskLevel::High,
            misunderstanding: None,
            rationale: "reviewer failed closed".to_owned(),
        },
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

mod duration_millis {
    use std::time::Duration;

    use serde::Serializer;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u128(duration.as_millis())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, VecDeque},
        sync::{LazyLock, Mutex},
    };

    use super::*;

    struct ExecutionTransport(Mutex<VecDeque<Result<String, ReviewerTransportError>>>);

    #[async_trait]
    impl ExecutionReviewerTransport for ExecutionTransport {
        fn model_spec(&self) -> &ReviewerModelSpec {
            &FIXTURE_REVIEWER_MODEL
        }

        async fn complete(
            &self,
            _prompt: &ExecutionReviewerPrompt,
            _cancel: CancellationToken,
        ) -> Result<String, ReviewerTransportError> {
            self.0
                .lock()
                .unwrap()
                .pop_front()
                .expect("fixture response")
        }
    }

    struct EscalationTransport(Mutex<VecDeque<Result<String, ReviewerTransportError>>>);

    #[async_trait]
    impl EscalationReviewerTransport for EscalationTransport {
        fn model_spec(&self) -> &ReviewerModelSpec {
            &FIXTURE_REVIEWER_MODEL
        }

        async fn complete(
            &self,
            _prompt: &EscalationReviewerPrompt,
            _cancel: CancellationToken,
        ) -> Result<String, ReviewerTransportError> {
            self.0
                .lock()
                .unwrap()
                .pop_front()
                .expect("fixture response")
        }
    }

    struct RecordingExecutionTransport {
        responses: Mutex<VecDeque<Result<String, ReviewerTransportError>>>,
        prompts: Mutex<Vec<ExecutionReviewerPrompt>>,
    }

    #[async_trait]
    impl ExecutionReviewerTransport for RecordingExecutionTransport {
        fn model_spec(&self) -> &ReviewerModelSpec {
            &FIXTURE_REVIEWER_MODEL
        }

        async fn complete(
            &self,
            prompt: &ExecutionReviewerPrompt,
            _cancel: CancellationToken,
        ) -> Result<String, ReviewerTransportError> {
            self.prompts.lock().unwrap().push(prompt.clone());
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("fixture response")
        }
    }

    struct RecordingEscalationTransport {
        responses: Mutex<VecDeque<Result<String, ReviewerTransportError>>>,
        prompts: Mutex<Vec<EscalationReviewerPrompt>>,
    }

    #[async_trait]
    impl EscalationReviewerTransport for RecordingEscalationTransport {
        fn model_spec(&self) -> &ReviewerModelSpec {
            &FIXTURE_REVIEWER_MODEL
        }

        async fn complete(
            &self,
            prompt: &EscalationReviewerPrompt,
            _cancel: CancellationToken,
        ) -> Result<String, ReviewerTransportError> {
            self.prompts.lock().unwrap().push(prompt.clone());
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("fixture response")
        }
    }

    fn execution_request() -> ExecutionReviewRequest {
        ExecutionReviewRequest {
            action_digest: "digest".to_owned(),
            redacted_evidence: serde_json::json!({"operation":"write"}),
            bounded_context: serde_json::json!([]),
        }
    }

    fn escalation_request() -> EscalationReviewRequest {
        EscalationReviewRequest {
            action_digest: "digest".to_owned(),
            redacted_evidence: serde_json::json!({"operation":"write"}),
            bounded_context: serde_json::json!([]),
        }
    }

    static FIXTURE_REVIEWER_MODEL: LazyLock<ReviewerModelSpec> = LazyLock::new(|| {
        ReviewerModelSpec::new(
            "reviewer",
            "fixture-provider",
            "https://reviewer.invalid",
            "fixture-account",
            "fixture-trust-domain",
            "fixture-no-training",
        )
    });

    fn reviewer_model() -> ReviewerModelSpec {
        FIXTURE_REVIEWER_MODEL.clone()
    }

    fn reviewer_trust(model: &ReviewerModelSpec) -> ReviewerTrustSet {
        ReviewerTrustSet::new(vec![model.clone()])
    }

    fn distinct_model(preset: &str, id: &str, account_scope: &str) -> ModelSpec {
        let mut spec = ModelSpec::preset(preset).expect("fixture preset");
        spec.id = id.to_owned();
        spec.account_scope = account_scope.to_owned();
        spec
    }

    fn provider_terminal(
        event_kind: &str,
        reason: StopReason,
        text: Option<&str>,
        provider_code: Option<&str>,
    ) -> ProviderEvent {
        let message = crate::provider::types::AssistantMessage {
            content: text
                .map(|text| {
                    vec![AssistantContent::Text {
                        text: text.to_owned(),
                        wire_item_index: 0,
                    }]
                })
                .unwrap_or_default(),
            model: "reviewer".to_owned(),
            provider: "fixture".to_owned(),
            origin: crate::provider::types::ProviderOrigin {
                provider_instance_id: "fixture-instance".to_owned(),
                protocol: crate::provider::types::ApiProtocol::OpenAiChatCompletions,
                model: "reviewer".to_owned(),
            },
            usage: crate::provider::types::Usage::default(),
            stop_reason: reason,
            error_message: provider_code.map(|code| format!("fixture error {code}")),
            provider_code: provider_code.map(str::to_owned),
            interrupted: reason == StopReason::Aborted,
            timestamp: Utc::now(),
        };
        let output = crate::provider::types::ProviderOutput {
            message,
            provider_context: Vec::new(),
        };
        match event_kind {
            "done" => ProviderEvent::Done { reason, output },
            "error" => ProviderEvent::Error { reason, output },
            other => panic!("unknown terminal fixture {other}"),
        }
    }

    #[test]
    fn compiled_budget_caps_are_enforced() {
        assert!(ReviewerBudgetV1::execution().compile().is_ok());
        assert!(ReviewerBudgetV1::escalation().compile().is_ok());
        assert!(matches!(
            ReviewerBudgetV1 {
                max_attempts: 3,
                attempt_timeout: Duration::from_secs(1),
                total_timeout: Duration::from_secs(2),
            }
            .compile(),
            Err(ReviewerNotReady::InvalidBudget(_))
        ));
    }

    #[test]
    fn reviewer_models_are_explicit_structured_and_independent() {
        let conversation = distinct_model("opencode-go", "conversation", "conversation-account");
        let execution = distinct_model("kimi-k3", "execution", "execution-account");
        let escalation = distinct_model("glm-5.2", "escalation", "escalation-account");
        let (_, _, trust) =
            IndependentReviewerModels::new(&conversation, execution.clone(), escalation.clone())
                .expect("three independent reviewer identities")
                .into_parts();

        assert!(!trust.allows(&ReviewerModelSpec::from_provider(&conversation)));
        assert!(trust.allows(&ReviewerModelSpec::from_provider(&execution)));
        assert!(trust.allows(&ReviewerModelSpec::from_provider(&escalation)));

        let conversation = distinct_model("kimi-k3", "conversation", "conversation-account");
        let error = IndependentReviewerModels::new(&conversation, conversation.clone(), escalation)
            .expect_err("Execution reviewer must not collapse to conversation model");
        assert!(matches!(
            error,
            ReviewerNotReady::CollapsedModelIdentity {
                left: "conversation",
                right: "Execution reviewer"
            }
        ));

        let execution = distinct_model("glm-5.2", "execution", "execution-account");
        let error = IndependentReviewerModels::new(&conversation, execution.clone(), execution)
            .expect_err("the two reviewers must not collapse to one identity");
        assert!(matches!(
            error,
            ReviewerNotReady::CollapsedModelIdentity {
                left: "Execution reviewer",
                right: "Escalation reviewer"
            }
        ));

        let conversation = distinct_model("opencode-go", "conversation", "conversation-account");
        let execution = distinct_model("kimi-k3", "execution", "execution-account");
        let same_origin = distinct_model("kimi-k3", "escalation", "escalation-account");
        assert!(matches!(
            IndependentReviewerModels::new(&conversation, execution.clone(), same_origin),
            Err(ReviewerNotReady::CollapsedProviderOrigin {
                left: "Execution reviewer",
                right: "Escalation reviewer"
            })
        ));

        let mut renamed_same_endpoint =
            distinct_model("glm-5.2", "escalation", "escalation-account");
        renamed_same_endpoint
            .base_url
            .clone_from(&execution.base_url);
        assert!(matches!(
            IndependentReviewerModels::new(&conversation, execution.clone(), renamed_same_endpoint,),
            Err(ReviewerNotReady::CollapsedProviderOrigin {
                left: "Execution reviewer",
                right: "Escalation reviewer"
            })
        ));

        let mut shared_credential = distinct_model("glm-5.2", "escalation", "escalation-account");
        shared_credential
            .api_key_env
            .clone_from(&execution.api_key_env);
        assert!(matches!(
            IndependentReviewerModels::new(&conversation, execution, shared_credential),
            Err(ReviewerNotReady::CollapsedCredentialSource {
                left: "Execution reviewer",
                right: "Escalation reviewer"
            })
        ));
    }

    #[test]
    fn structured_output_incompatible_reviewer_fails_startup() {
        let conversation =
            distinct_model("openai-responses", "conversation", "conversation-account");
        let unsupported = ModelSpec::preset("opencode-go").expect("OpenCode fixture preset");
        let escalation = distinct_model("glm-5.2", "escalation", "escalation-account");

        assert!(matches!(
            IndependentReviewerModels::new(&conversation, unsupported, escalation),
            Err(ReviewerNotReady::StructuredOutputUnsupported {
                reviewer: "Execution"
            })
        ));
    }

    #[test]
    fn provider_review_terminal_rejects_truncation_abort_and_permanent_errors() {
        let valid_json = r#"{"outcome":"allow","risk":"low","rationale":"complete"}"#;

        assert!(matches!(
            classify_provider_review_terminal(
                provider_terminal("done", StopReason::Length, Some(valid_json), None),
                false,
            ),
            Some(Err(ReviewerTransportError::Fatal(message)))
                if message.contains("truncated")
        ));
        assert!(matches!(
            classify_provider_review_terminal(
                provider_terminal("done", StopReason::Aborted, Some(valid_json), None),
                false,
            ),
            Some(Err(ReviewerTransportError::Cancelled))
        ));
        assert!(matches!(
            classify_provider_review_terminal(
                provider_terminal(
                    "error",
                    StopReason::Error,
                    Some(valid_json),
                    Some("invalid_provider_request"),
                ),
                false,
            ),
            Some(Err(ReviewerTransportError::Fatal(message)))
                if message.contains("invalid_provider_request")
        ));
        assert_eq!(
            classify_provider_review_terminal(
                provider_terminal("done", StopReason::Stop, Some(valid_json), None),
                false,
            ),
            Some(Ok(valid_json.to_owned()))
        );
    }

    #[test]
    fn reviewer_model_and_transport_must_match_the_trusted_binding() {
        let declared = ReviewerModelSpec::new(
            "reviewer",
            "fixture-provider",
            "https://different-endpoint.invalid",
            "fixture-account",
            "fixture-trust-domain",
            "fixture-no-training",
        );
        let trust = ReviewerTrustSet::new(vec![declared.clone()]);
        let result = ExecutionReviewer::new(
            declared,
            trust,
            Arc::new(ExecutionTransport(Mutex::new(VecDeque::new()))),
            ReviewerBudgetV1::execution(),
        );
        assert!(matches!(result, Err(ReviewerNotReady::UntrustedModel)));
    }

    #[tokio::test]
    async fn oversized_evidence_blocks_without_contacting_the_reviewer() {
        let model = reviewer_model();
        let transport = Arc::new(ExecutionTransport(Mutex::new(VecDeque::from([Ok(
            r#"{"outcome":"allow","risk":"low","rationale":"must remain unused"}"#.to_owned(),
        )]))));
        let reviewer = ExecutionReviewer::new(
            model.clone(),
            reviewer_trust(&model),
            transport.clone(),
            ReviewerBudgetV1::execution(),
        )
        .unwrap();
        let mut request = execution_request();
        request.bounded_context = serde_json::json!(["x".repeat(MAX_REVIEW_REQUEST_BYTES)]);

        let ExecutionReviewResult::Block(evidence) =
            reviewer.review(request, CancellationToken::new()).await
        else {
            panic!("oversized evidence must fail closed")
        };
        assert_eq!(evidence.budget.attempts, 0);
        assert_eq!(
            evidence.budget.terminal,
            ReviewerTerminalClass::InsufficientEvidence
        );
        assert_eq!(transport.0.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn execution_retries_only_malformed_or_transient_and_never_prompts_human() {
        let model = reviewer_model();
        let transport = Arc::new(RecordingExecutionTransport {
            responses: Mutex::new(VecDeque::from([
                Ok("not-json".to_owned()),
                Ok(r#"{"outcome":"allow","risk":"low","rationale":"bounded"}"#.to_owned()),
            ])),
            prompts: Mutex::new(Vec::new()),
        });
        let reviewer = ExecutionReviewer::new(
            model.clone(),
            reviewer_trust(&model),
            transport.clone(),
            ReviewerBudgetV1::execution(),
        )
        .unwrap();
        let ExecutionReviewResult::Allow(evidence) = reviewer
            .review(execution_request(), CancellationToken::new())
            .await
        else {
            panic!("second valid response should allow")
        };
        assert_eq!(evidence.budget.attempts, 2);
        assert_eq!(
            evidence.budget.terminal,
            ReviewerTerminalClass::ValidDecision
        );
        let prompts = transport.prompts.lock().unwrap();
        assert_eq!(prompts.len(), 2);
        for (index, prompt) in prompts.iter().enumerate() {
            assert_eq!(prompt.output_schema, ExecutionReviewOutputSchema::v1());
            assert_eq!(
                prompt.retry_validation_code,
                if index == 0 {
                    None
                } else {
                    Some(ReviewerValidationCode::InvalidJson)
                }
            );
            let (_, options) = build_provider_review_request(
                &ModelSpec::preset("openai-responses").unwrap(),
                prompt.system,
                prompt.output_schema.provider_schema(),
                prompt,
            )
            .unwrap();
            assert_eq!(
                options.structured_output.as_ref(),
                Some(prompt.output_schema.provider_schema())
            );
        }
    }

    #[tokio::test]
    async fn semantic_schema_mismatch_uses_the_single_bounded_retry() {
        let model = reviewer_model();
        let transport = Arc::new(RecordingEscalationTransport {
            responses: Mutex::new(VecDeque::from([
                Ok(
                    r#"{"outcome":"ask_human","risk":"low","misunderstanding":null,"rationale":""}"#
                        .to_owned(),
                ),
                Ok(
                    r#"{"outcome":"ask_human","risk":"low","misunderstanding":null,"rationale":"request matches the exact action"}"#
                        .to_owned(),
                ),
            ])),
            prompts: Mutex::new(Vec::new()),
        });
        let reviewer = EscalationReviewer::new(
            model.clone(),
            reviewer_trust(&model),
            transport.clone(),
            ReviewerBudgetV1::escalation(),
        )
        .unwrap();
        let EscalationReviewResult::AskHuman(evidence) = reviewer
            .review(escalation_request(), CancellationToken::new())
            .await
        else {
            panic!("a valid bounded retry should permit a Human prompt")
        };
        assert_eq!(evidence.budget.attempts, 2);
        assert_eq!(
            evidence.budget.terminal,
            ReviewerTerminalClass::ValidDecision
        );
        let prompts = transport.prompts.lock().unwrap();
        assert_eq!(prompts.len(), 2);
        for (index, prompt) in prompts.iter().enumerate() {
            assert_eq!(prompt.output_schema, EscalationReviewOutputSchema::v1());
            assert_eq!(
                prompt.retry_validation_code,
                if index == 0 {
                    None
                } else {
                    Some(ReviewerValidationCode::SchemaMismatch)
                }
            );
            let (_, options) = build_provider_review_request(
                &ModelSpec::preset("anthropic").unwrap(),
                prompt.system,
                prompt.output_schema.provider_schema(),
                prompt,
            )
            .unwrap();
            assert_eq!(
                options.structured_output.as_ref(),
                Some(prompt.output_schema.provider_schema())
            );
        }
        assert_ne!(
            ExecutionReviewOutputSchema::v1().provider_schema().schema,
            EscalationReviewOutputSchema::v1().provider_schema().schema
        );
    }

    #[test]
    fn escalation_schema_requires_the_nullable_misunderstanding_member() {
        assert_eq!(
            parse_escalation_decision(
                r#"{"outcome":"block","risk":"low","rationale":"missing member"}"#
            ),
            Err(ReviewerValidationCode::SchemaMismatch)
        );
    }

    #[test]
    fn auto_review_schemas_stay_inside_the_kimi_mfjs_strict_subset_we_use() {
        for schema in [
            ExecutionReviewOutputSchema::v1()
                .provider_schema()
                .schema
                .clone(),
            EscalationReviewOutputSchema::v1()
                .provider_schema()
                .schema
                .clone(),
        ] {
            let root = schema.as_object().expect("review schema root is an object");
            assert_eq!(
                root.keys().map(String::as_str).collect::<BTreeSet<_>>(),
                BTreeSet::from(["additionalProperties", "properties", "required", "type"])
            );
            assert_eq!(root.get("type"), Some(&json!("object")));
            assert_eq!(root.get("additionalProperties"), Some(&json!(false)));

            let properties = root["properties"]
                .as_object()
                .expect("review object properties");
            let required = root["required"]
                .as_array()
                .expect("review object required fields")
                .iter()
                .map(|field| field.as_str().expect("required field name"))
                .collect::<BTreeSet<_>>();
            assert_eq!(
                required,
                properties
                    .keys()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>()
            );

            for property in properties.values() {
                let property = property.as_object().expect("review scalar property");
                assert!(property.keys().all(|key| key == "type" || key == "enum"));
                assert!(
                    matches!(
                        property.get("type"),
                        Some(Value::String(kind)) if kind == "string"
                    ) || matches!(
                        property.get("type"),
                        Some(Value::Array(kinds))
                            if kinds == &vec![json!("string"), json!("null")]
                    )
                );
                if let Some(values) = property.get("enum") {
                    let values = values.as_array().expect("review string enum");
                    assert!(!values.is_empty());
                    assert!(values.iter().all(Value::is_string));
                }
            }
        }
    }

    #[tokio::test]
    async fn fatal_and_critical_positive_fail_closed_without_retry() {
        let model = reviewer_model();
        let fatal = ExecutionReviewer::new(
            model.clone(),
            reviewer_trust(&model),
            Arc::new(ExecutionTransport(Mutex::new(VecDeque::from([
                Err(ReviewerTransportError::Fatal("bad auth".to_owned())),
                Ok(r#"{"outcome":"allow","risk":"low","rationale":"unused"}"#.to_owned()),
            ])))),
            ReviewerBudgetV1::execution(),
        )
        .unwrap();
        let ExecutionReviewResult::Block(evidence) = fatal
            .review(execution_request(), CancellationToken::new())
            .await
        else {
            panic!("fatal must block")
        };
        assert_eq!(evidence.budget.attempts, 1);
        assert_eq!(
            evidence.budget.terminal,
            ReviewerTerminalClass::FatalTransport
        );

        let critical = EscalationReviewer::new(
            model.clone(),
            reviewer_trust(&model),
            Arc::new(EscalationTransport(Mutex::new(VecDeque::from([Ok(
                r#"{"outcome":"ask_human","risk":"critical","misunderstanding":null,"rationale":"too risky"}"#.to_owned(),
            )])))),
            ReviewerBudgetV1::escalation(),
        )
        .unwrap();
        let EscalationReviewResult::Block(evidence) = critical
            .review(escalation_request(), CancellationToken::new())
            .await
        else {
            panic!("critical positive must be locally blocked")
        };
        assert_eq!(
            evidence.budget.terminal,
            ReviewerTerminalClass::CriticalPositiveBlocked
        );
    }
}
