//! Tool execution approval broker and deterministic policy.

pub mod action;
pub mod broker;
pub mod policy;
pub mod prompt;
pub mod reviewer;

#[allow(unused_imports)]
pub use action::{
    CanonicalAction, Permission, RedactedText, ReviewPath, ReviewPathComponent, ReviewProjection,
    ReviewToken, ReviewableAction, SandboxSummary, SecretAwareActionProjector, SecretDigestKey,
};
pub use broker::{ApprovalBroker, ApprovalOutcome, WaiterResult};
#[allow(unused_imports)]
pub use policy::{
    ApprovalRule, Policy, PolicyDecision, ResolvedDecision, RuleEffect, RuleValidationError,
    UserDecision,
};
#[allow(unused_imports)]
pub use prompt::{PromptLimits, ReviewerMessage, ReviewerPrompt, ReviewerRole, TrustedEnvironment};
#[allow(unused_imports)]
pub use reviewer::{
    AuditDecision, AuditOutcome, CircuitBreaker, CircuitState, ReviewOutcome, ReviewRequest,
    Reviewer, ReviewerMode, ReviewerModelSpec, ReviewerTransport, ReviewerTransportError,
    ReviewerTrustSet, RiskLevel, UserAuthorization,
};

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

    #[test]
    fn adversarial_table() {
        let policy = Policy::new("/workspace");

        // D3 workspace read fast path
        let read = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "read_file",
            &args(json!({"path": "notes.txt"})),
        )
        .expect("read_file");
        assert!(policy.evaluate(&read).is_allow());

        // Workspace escape through write_file
        let escape = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "write_file",
            &args(json!({"path": "/etc/passwd", "content": "x"})),
        )
        .expect("write_file");
        assert!(policy.evaluate(&escape).is_forbidden());

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
        let decision = p.evaluate(&multi);
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
        assert!(p2.evaluate(&quoted).is_allow());

        // Nested/dynamic constructs require approval
        let dynamic = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command": "echo $(date)"})),
        )
        .expect("bash");
        assert!(!policy.evaluate(&dynamic).is_allow());

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
        let resolved = policy.resolve(
            &broad_action,
            UserDecision::ApproveAlways { rule: broad },
            &projector(),
        );
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
        let resolved = policy.resolve(
            &multi,
            UserDecision::ApproveAlways {
                rule: forbidden_rule,
            },
            &projector(),
        );
        assert!(matches!(resolved, ResolvedDecision::Rejected { .. }));

        // Signed URL and Authorization secret handling + ApproveAlways downgrade
        let curl_secret = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command": "curl -H \"Authorization: Bearer abcdef1234567890\" https://example.com"})),
        )
        .expect("bash");
        let projection = projector().project(&curl_secret);
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
        let ReviewProjection::Reviewable(signed_review) = projector().project(&signed) else {
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
            projector().project(&hidden_host),
            ReviewProjection::InsufficientEvidence { .. }
        ));
    }
}
