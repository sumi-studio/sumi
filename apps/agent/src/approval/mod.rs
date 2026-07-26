//! Tool execution approval broker and deterministic policy.

// T23: remove these module-wide allows once gateway/persistence integration
// wires all action and policy exports; they currently suppress skeleton code.
#[allow(dead_code, unused_imports)]
pub mod action;
// T23: remove these module-wide allows once gateway/persistence integration
// wires all action and policy exports; they currently suppress skeleton code.
#[allow(dead_code, unused_imports)]
pub mod policy;

#[allow(unused_imports)]
pub use action::{
    CanonicalAction, Permission, RedactedText, ReviewPath, ReviewPathComponent, ReviewProjection,
    ReviewToken, ReviewableAction, SandboxSummary, SecretAwareActionProjector, SecretDigestKey,
};
#[allow(unused_imports)]
pub use policy::{
    ApprovalRule, Policy, PolicyDecision, ResolvedDecision, RuleEffect, RuleValidationError,
    UserDecision,
};

/// Runtime-internal approval broker. T23 will wire persistence and gateway
/// integration; this skeleton exposes the pure T22 projection/evaluation seam.
#[allow(dead_code)]
pub struct ApprovalBroker {
    policy: Policy,
    projector: SecretAwareActionProjector,
}

#[allow(dead_code)]
impl ApprovalBroker {
    pub fn new(policy: Policy, projector: SecretAwareActionProjector) -> Self {
        Self { policy, projector }
    }

    pub fn project(&self, action: &CanonicalAction) -> ReviewProjection {
        self.projector.project(action)
    }

    pub fn evaluate(&self, action: &CanonicalAction) -> PolicyDecision {
        self.policy.evaluate(action)
    }

    pub fn resolve(&self, action: &CanonicalAction, decision: UserDecision) -> ResolvedDecision {
        self.policy.resolve(action, decision, &self.projector)
    }
}

use crate::{
    agent::{ApprovalRequest, ApprovalResolution},
    gateway::ApprovalDecision,
    tools::ToolRisk,
};

/// Runtime approval broker interface.
///
/// Implementations evaluate a concrete tool call and return an
/// `ApprovalResolution` that the `InjectedRunDriver` enforces before executing
/// any tool that is not read-only.
pub trait RuntimeApprovalBroker: Send + Sync {
    fn request(&self, request: &ApprovalRequest, risk: ToolRisk) -> ApprovalResolution;
}

/// Production fail-closed broker.
///
/// Read-only tools are approved for a single execution. All mutating and
/// exec-risk tools are denied until an interactive or policy broker replaces
/// this stub. This keeps T26 production fail-closed while still allowing the
/// policy `ApprovalBroker` to be composed and tested.
pub struct FailClosedApprovalBroker;

impl FailClosedApprovalBroker {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FailClosedApprovalBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeApprovalBroker for FailClosedApprovalBroker {
    fn request(&self, _request: &ApprovalRequest, risk: ToolRisk) -> ApprovalResolution {
        match risk {
            ToolRisk::ReadOnly => ApprovalResolution::Decision(ApprovalDecision::ApproveOnce),
            ToolRisk::Mutating | ToolRisk::Exec => {
                ApprovalResolution::Decision(ApprovalDecision::Deny)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::json;

    use crate::store::Redactor;

    use super::*;

    fn projector() -> SecretAwareActionProjector {
        SecretAwareActionProjector::new(Redactor::v1(), SecretDigestKey::fixture())
    }

    fn args(value: serde_json::Value) -> crate::provider::types::ValidatedToolArguments {
        serde_json::from_value(value).expect("valid args")
    }

    fn default_broker() -> ApprovalBroker {
        ApprovalBroker::new(Policy::new("/workspace"), projector())
    }

    #[test]
    fn adversarial_table() {
        // D3 workspace read fast path
        let read = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "read_file",
            &args(json!({"path": "notes.txt"})),
        )
        .expect("read_file");
        assert!(default_broker().evaluate(&read).is_allow());

        // Workspace escape through write_file
        let escape = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "write_file",
            &args(json!({"path": "/etc/passwd", "content": "x"})),
        )
        .expect("write_file");
        assert!(default_broker().evaluate(&escape).is_forbidden());

        // Multi-segment: strictest result dominates (Allow + Forbidden -> Forbidden)
        let multi = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command": "git status && rm -rf /"})),
        )
        .expect("bash");
        let p = Policy::new("/workspace")
            .try_with_rule(ApprovalRule {
                id: "git-status".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["git".to_owned(), "status".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();
        let broker = ApprovalBroker::new(p, projector());
        let decision = broker.evaluate(&multi);
        assert!(
            decision.is_forbidden(),
            "forbidden segment must dominate: {decision:?}"
        );

        // Quoted separators do not split segments
        let quoted = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command": "echo \"a && b\""})),
        )
        .expect("bash");
        let p2 = Policy::new("/workspace")
            .try_with_rule(ApprovalRule {
                id: "echo".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["echo".to_owned(), "a && b".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();
        let broker2 = ApprovalBroker::new(p2, projector());
        assert!(broker2.evaluate(&quoted).is_allow());

        // Nested/dynamic constructs require approval
        let dynamic = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command": "echo $(date)"})),
        )
        .expect("bash");
        assert!(!default_broker().evaluate(&dynamic).is_allow());

        // Broad prefixes downgrade ApproveAlways rather than persisting it.
        let broad_action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command": "echo hello"})),
        )
        .expect("bash");
        let broad = ApprovalRule {
            id: "broad".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["echo".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        let resolved =
            default_broker().resolve(&broad_action, UserDecision::ApproveAlways { rule: broad });
        assert!(matches!(resolved, ResolvedDecision::ApproveOnce));

        // A forbidden action is rejected rather than downgraded to executable ApproveOnce.
        let forbidden_rule = ApprovalRule {
            id: "git-status".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["git".to_owned(), "status".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        let resolved = default_broker().resolve(
            &multi,
            UserDecision::ApproveAlways {
                rule: forbidden_rule,
            },
        );
        assert!(matches!(resolved, ResolvedDecision::Rejected { .. }));

        // Signed URL and Authorization secret handling + ApproveAlways downgrade
        let curl_secret = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command": "curl -H \"Authorization: Bearer abcdef1234567890\" https://example.com"})),
        )
        .expect("bash");
        let projection = default_broker().project(&curl_secret);
        let ReviewProjection::Reviewable(review) = projection else {
            panic!("expected reviewable projection");
        };
        let argv_text = serde_json::to_string(&review.argv).unwrap();
        assert!(!argv_text.contains("abcdef1234567890"));
        assert!(argv_text.contains("bearer_token"));

        let signed = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(
                json!({"command": "curl \"https://example.com?X-Amz-Signature=abcdef1234567890\""}),
            ),
        )
        .expect("bash");
        let ReviewProjection::Reviewable(signed_review) = default_broker().project(&signed) else {
            panic!("expected reviewable projection");
        };
        let signed_text = serde_json::to_string(&signed_review.argv).unwrap();
        assert!(!signed_text.contains("abcdef1234567890"));
        assert!(signed_text.contains("signature"));

        // Insufficient evidence when redaction removes the network destination
        let hidden_host = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command": "curl https://sk-abcdefghijklmnop.example.com"})),
        )
        .expect("bash");
        assert!(matches!(
            default_broker().project(&hidden_host),
            ReviewProjection::InsufficientEvidence { .. }
        ));
    }
}
