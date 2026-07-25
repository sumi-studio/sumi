//! Deterministic approval policy and `ApproveAlways` candidate validation.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::action::{
    BASH_TOOL_NAME, CanonicalAction, Permission, SecretAwareActionProjector, shell,
    text_contains_secret_material,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleEffect {
    Allow,
    NeedsApproval,
    Forbidden,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRule {
    pub id: String,
    pub tool: String,
    pub literal_prefix: Vec<String>,
    pub effect: RuleEffect,
    pub workspace_only: bool,
    pub allowed_permissions: Vec<Permission>,
    pub allowed_network_domains: Vec<String>,
}

impl ApprovalRule {
    pub fn matches(
        &self,
        action: &CanonicalAction,
        tokens: &[String],
        workspace_root: &Path,
    ) -> bool {
        if self.tool != action.tool {
            return false;
        }
        if !tokens.starts_with(&self.literal_prefix) {
            return false;
        }
        if !action.sandbox.workspace_only || action.sandbox.network_allowed {
            return false;
        }
        if path_check(&action.cwd, &action.cwd, workspace_root) != PathCheck::InsideWorkspace {
            return false;
        }
        if self.workspace_only
            && path_check_all(action, workspace_root) != PathCheck::InsideWorkspace
        {
            return false;
        }
        if !action
            .requested_permissions
            .iter()
            .all(|p| self.allowed_permissions.contains(p))
        {
            return false;
        }
        if action.requested_permissions.contains(&Permission::Network)
            || action
                .requested_permissions
                .contains(&Permission::DomainMutation)
        {
            // T22 does not implement domain extraction; a domain-constrained
            // rule therefore cannot match until a future task.
            return false;
        }
        if !self.allowed_network_domains.is_empty() {
            // Domain metadata without a network operation is still not
            // meaningful until T23's domain extractor exists.
            return false;
        }
        true
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyDecision {
    Allow {
        matched_rules: Vec<String>,
    },
    NeedsApproval {
        matched_rules: Vec<String>,
        reason: String,
    },
    Forbidden {
        matched_rules: Vec<String>,
        reason: String,
    },
}

impl PolicyDecision {
    pub fn is_allow(&self) -> bool {
        matches!(self, Self::Allow { .. })
    }
    pub fn is_forbidden(&self) -> bool {
        matches!(self, Self::Forbidden { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UserDecision {
    ApproveOnce,
    ApproveAlways { rule: ApprovalRule },
    Deny,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedDecision {
    ApproveOnce,
    ApproveAlways(ApprovalRule),
    Deny,
    /// The requested approval cannot authorize this action. In particular, a
    /// deterministic `Forbidden` result must never be silently downgraded to
    /// an executable one-shot approval.
    Rejected {
        reason: String,
    },
}

/// Reason a rule cannot be loaded into an in-memory policy.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RuleValidationError {
    #[error("rule literal prefix is too broad to be safely persisted")]
    BroadPrefix,
    #[error("rule contains secret material and cannot be persisted")]
    SecretMaterial,
}

#[derive(Clone)]
pub struct Policy {
    workspace_root: PathBuf,
    rules: Vec<ApprovalRule>,
}

impl Policy {
    pub fn new(workspace_root: impl Into<PathBuf>) -> Self {
        Self {
            workspace_root: workspace_root.into(),
            rules: Vec::new(),
        }
    }

    /// Load `rule` after validating that its literal prefix is narrow enough
    /// to be safely persisted. Broad or otherwise invalid rules are rejected
    /// with a typed error rather than silently accepted.
    pub fn try_with_rule(mut self, rule: ApprovalRule) -> Result<Self, RuleValidationError> {
        if is_broad_prefix(&rule.literal_prefix) {
            return Err(RuleValidationError::BroadPrefix);
        }
        if rule_contains_secret_material(&rule) {
            return Err(RuleValidationError::SecretMaterial);
        }
        self.rules.push(rule);
        Ok(self)
    }

    pub fn evaluate(&self, action: &CanonicalAction) -> PolicyDecision {
        if action.validate().is_err() {
            return PolicyDecision::Forbidden {
                matched_rules: Vec::new(),
                reason: "canonical action failed invariant validation".to_owned(),
            };
        }
        if let Some(decision) = validate_action_context(action, &self.workspace_root) {
            return decision;
        }

        if action.tool == BASH_TOOL_NAME && action.operation == "exec" {
            let command = action.argv.first().map(String::as_str).unwrap_or("");
            let segments = shell::segment_command(command);
            if segments.is_empty() {
                return PolicyDecision::NeedsApproval {
                    matched_rules: Vec::new(),
                    reason: "empty bash command".to_owned(),
                };
            }
            let mut overall = PolicyDecision::Allow {
                matched_rules: Vec::new(),
            };
            for segment in segments {
                let decision = self.evaluate_bash_segment(action, &segment);
                overall = combine(overall, decision);
            }
            overall
        } else {
            self.evaluate_non_bash(action)
        }
    }

    pub fn resolve(
        &self,
        action: &CanonicalAction,
        decision: UserDecision,
        projector: &SecretAwareActionProjector,
    ) -> ResolvedDecision {
        match decision {
            UserDecision::Deny => ResolvedDecision::Deny,
            UserDecision::ApproveOnce => match self.evaluate(action) {
                PolicyDecision::Forbidden { reason, .. } => ResolvedDecision::Rejected { reason },
                _ => ResolvedDecision::ApproveOnce,
            },
            UserDecision::ApproveAlways { rule } => {
                self.evaluate_approve_always(action, rule, projector)
            }
        }
    }

    fn evaluate_bash_segment(
        &self,
        action: &CanonicalAction,
        segment: &shell::Segment,
    ) -> PolicyDecision {
        let tokens = shell::tokenize_command(&segment.raw);
        if tokens.is_empty() || segment.is_dynamic() {
            return PolicyDecision::NeedsApproval {
                matched_rules: Vec::new(),
                reason: "dynamic or unparseable shell construct".to_owned(),
            };
        }

        let eff = match shell::effective_command(&tokens, 1) {
            Some(e) if !e.tokens.is_empty() => e,
            _ => {
                return PolicyDecision::NeedsApproval {
                    matched_rules: Vec::new(),
                    reason: "unparseable shell wrapper".to_owned(),
                };
            }
        };

        let command = &eff.tokens[0];
        if is_privilege_escalation_command(command)
            || action
                .requested_permissions
                .contains(&Permission::PrivilegeEscalation)
        {
            return PolicyDecision::Forbidden {
                matched_rules: Vec::new(),
                reason: "privilege escalation".to_owned(),
            };
        }

        match bash_path_check(eff.tokens, &action.cwd, &self.workspace_root) {
            PathCheck::WorkspaceEscape => {
                return PolicyDecision::Forbidden {
                    matched_rules: Vec::new(),
                    reason: "shell path escapes workspace".to_owned(),
                };
            }
            PathCheck::InternalState => {
                return PolicyDecision::NeedsApproval {
                    matched_rules: Vec::new(),
                    reason: "shell path touches internal state".to_owned(),
                };
            }
            PathCheck::InsideWorkspace => {}
        }

        if eff.leading_assignments > 0 || eff.had_generic_wrapper {
            return PolicyDecision::NeedsApproval {
                matched_rules: Vec::new(),
                reason: "shell wrapper or environment assignment".to_owned(),
            };
        }

        if has_unmodeled_command_form(eff.tokens)
            || has_unmodeled_option_payload(eff.tokens)
            || has_embedded_execution_payload(eff.tokens)
        {
            // ssh option payloads such as ProxyCommand can execute arbitrary
            // local commands, so they must not be downgraded to ordinary
            // network one-shot approvals.
            if shell::canonicalize_command_name(&shell::command_basename(command), &["ssh"])
                .is_some_and(|family| family == "ssh")
            {
                return PolicyDecision::Forbidden {
                    matched_rules: Vec::new(),
                    reason: "unmodeled shell wrapper or option payload".to_owned(),
                };
            }
            return PolicyDecision::NeedsApproval {
                matched_rules: Vec::new(),
                reason: "unmodeled shell wrapper or option payload".to_owned(),
            };
        }

        if is_directory_state_command(command) {
            return PolicyDecision::NeedsApproval {
                matched_rules: Vec::new(),
                reason: "shell directory state mutation".to_owned(),
            };
        }

        let mut effects: Vec<(RuleEffect, String)> = Vec::new();
        for rule in &self.rules {
            if rule.matches(action, &tokens, &self.workspace_root) {
                effects.push((rule.effect, rule.id.clone()));
            }
        }

        if effects.is_empty() {
            PolicyDecision::NeedsApproval {
                matched_rules: Vec::new(),
                reason: "bash command requires explicit approval rule".to_owned(),
            }
        } else {
            from_effects(effects)
        }
    }

    fn evaluate_non_bash(&self, action: &CanonicalAction) -> PolicyDecision {
        if action
            .requested_permissions
            .contains(&Permission::PrivilegeEscalation)
        {
            return PolicyDecision::Forbidden {
                matched_rules: Vec::new(),
                reason: "privilege escalation".to_owned(),
            };
        }

        match path_check_all(action, &self.workspace_root) {
            PathCheck::WorkspaceEscape => {
                return PolicyDecision::Forbidden {
                    matched_rules: Vec::new(),
                    reason: "workspace escape".to_owned(),
                };
            }
            PathCheck::InternalState => {
                return PolicyDecision::NeedsApproval {
                    matched_rules: Vec::new(),
                    reason: "internal state path".to_owned(),
                };
            }
            PathCheck::InsideWorkspace => {}
        }

        if action.requested_permissions.contains(&Permission::Network)
            || action
                .requested_permissions
                .contains(&Permission::DomainMutation)
        {
            // Network/domain mutations can only be allowed by an explicit rule.
            let effects: Vec<_> = self
                .rules
                .iter()
                .filter(|r| r.matches(action, &action.argv, &self.workspace_root))
                .map(|r| (r.effect, r.id.clone()))
                .collect();
            if effects.is_empty() {
                return PolicyDecision::NeedsApproval {
                    matched_rules: Vec::new(),
                    reason: "network or domain mutation requires an approval rule".to_owned(),
                };
            }
            return from_effects(effects);
        }

        let default = default_non_bash_decision(action);
        let effects: Vec<_> = self
            .rules
            .iter()
            .filter(|r| r.matches(action, &action.argv, &self.workspace_root))
            .map(|r| (r.effect, r.id.clone()))
            .collect();
        if effects.is_empty() {
            default
        } else {
            from_effects(effects)
        }
    }

    fn evaluate_approve_always(
        &self,
        action: &CanonicalAction,
        candidate: ApprovalRule,
        projector: &SecretAwareActionProjector,
    ) -> ResolvedDecision {
        if let PolicyDecision::Forbidden { reason, .. } = self.evaluate(action) {
            return ResolvedDecision::Rejected { reason };
        }

        if action_contains_secret(projector, action) || rule_contains_secret(projector, &candidate)
        {
            return ResolvedDecision::ApproveOnce;
        }
        let with_candidate = match self.clone().try_with_rule(candidate.clone()) {
            Ok(p) => p,
            Err(_) => return ResolvedDecision::ApproveOnce,
        };
        let decision = with_candidate.evaluate(action);
        match decision {
            PolicyDecision::Allow { .. } => ResolvedDecision::ApproveAlways(candidate),
            PolicyDecision::NeedsApproval { .. } => ResolvedDecision::ApproveOnce,
            PolicyDecision::Forbidden { reason, .. } => ResolvedDecision::Rejected { reason },
        }
    }
}

pub(crate) fn is_broad_prefix(prefix: &[String]) -> bool {
    if prefix.is_empty() || prefix.len() < 2 {
        return true;
    }

    let Some(eff) = shell::effective_command(prefix, 0) else {
        return true;
    };
    if eff.tokens.is_empty()
        || eff.leading_assignments > 0
        || eff.had_generic_wrapper
        || eff.tokens.len() < 2
    {
        return true;
    }

    let command = shell::command_basename(&eff.tokens[0]);
    if is_privilege_escalation_command(&command)
        || has_unmodeled_command_form(eff.tokens)
        || has_unmodeled_option_payload(eff.tokens)
        || has_control_option_payload(eff.tokens)
        || has_embedded_execution_payload(eff.tokens)
        || shell::is_network_command(eff.tokens)
    {
        return true;
    }

    const SAFE_SELF_CONTAINED_FLAGS: [&str; 2] = ["--help", "--version"];
    if let Some(last) = eff.tokens.last()
        && last.starts_with('-')
        && !last.contains('=')
        && !SAFE_SELF_CONTAINED_FLAGS.contains(&last.as_str())
        && !(matches!(command.as_str(), "popd" | "pushd") && last == "-n")
    {
        return true;
    }

    false
}

fn rule_contains_secret(projector: &SecretAwareActionProjector, rule: &ApprovalRule) -> bool {
    projector.text_contains_secret(&rule.id)
        || projector.text_contains_secret(&rule.tool)
        || rule
            .literal_prefix
            .iter()
            .any(|token| projector.text_contains_secret(token))
        || projector.text_contains_secret(&rule.literal_prefix.join(" "))
        || rule
            .allowed_network_domains
            .iter()
            .any(|domain| projector.text_contains_secret(domain))
}

fn rule_contains_secret_material(rule: &ApprovalRule) -> bool {
    text_contains_secret_material(&rule.id)
        || text_contains_secret_material(&rule.tool)
        || rule
            .literal_prefix
            .iter()
            .any(|token| text_contains_secret_material(token))
        || text_contains_secret_material(&rule.literal_prefix.join(" "))
        || rule
            .allowed_network_domains
            .iter()
            .any(|domain| text_contains_secret_material(domain))
}

fn action_contains_secret(
    projector: &SecretAwareActionProjector,
    action: &CanonicalAction,
) -> bool {
    projector.text_contains_secret(&action.tool)
        || projector.text_contains_secret(&action.operation)
        || action
            .argv
            .iter()
            .any(|token| projector.text_contains_secret(token))
        || projector.text_contains_secret(&action.cwd.to_string_lossy())
        || action
            .affected_paths
            .iter()
            .any(|path| projector.text_contains_secret(&path.to_string_lossy()))
        || action
            .justification
            .as_deref()
            .is_some_and(|text| projector.text_contains_secret(text))
}

fn is_privilege_escalation_command(command: &str) -> bool {
    const PRIVILEGE_FAMILIES: &[&str] = &[
        "sudo",
        "su",
        "doas",
        "pkexec",
        "runuser",
        "newgrp",
        "sg",
        "unshare",
        "nsenter",
        "chroot",
        "setpriv",
        "runcon",
        "mount",
        "umount",
        "losetup",
        "mkfs",
        "mkswap",
        "swapon",
        "swapoff",
        "fdisk",
        "sfdisk",
        "parted",
        "partprobe",
        "blockdev",
        "hdparm",
        "dd",
        "fsck",
        "e2fsck",
        "tune2fs",
        "resize2fs",
        "debugfs",
        "wipefs",
        "fusermount",
    ];
    let base = shell::command_basename(command);
    shell::canonicalize_command_name(&base, PRIVILEGE_FAMILIES).is_some()
}

fn is_directory_state_command(command: &str) -> bool {
    const DIRECTORY_FAMILIES: &[&str] = &["cd", "pushd", "popd"];
    let base = shell::command_basename(command);
    shell::canonicalize_command_name(&base, DIRECTORY_FAMILIES).is_some()
}

fn is_unmodeled_command(command: &str) -> bool {
    const UNMODELED_FAMILIES: &[&str] = &[
        "command", "if", "then", "else", "elif", "fi", "for", "while", "until", "do", "done",
        "case", "esac", "select", "function", "coproc", "source", ".", "builtin", "eval", "trap",
        "export", "alias", "set", "unset", "readonly", "declare", "typeset", "local", "bash", "sh",
        "dash", "zsh", "ksh", "csh", "tcsh", "fish", "python", "python3", "python2", "perl",
        "ruby", "php", "php7", "php8", "lua", "lua5.1", "lua5.2", "lua5.3", "lua5.4", "node",
        "nodejs", "awk", "gawk", "nawk", "mawk", "env", "xargs", "timeout", "nice", "nohup",
        "setsid", "stdbuf", "chrt", "busybox", "make", "ninja", "cargo", "go", "npm", "yarn",
        "pnpm", "cmake", "meson", "just", "sed", "ed", "ex", "vi", "vim", "script", "expect",
        "tclsh", "wish", "gdb", "lldb", "sqlite3", "parallel", "socat", "pytest", "py.test",
        "rustup",
    ];
    let base = shell::command_basename(command);
    shell::canonicalize_command_name(&base, UNMODELED_FAMILIES).is_some()
}

fn has_unmodeled_command_form(tokens: &[String]) -> bool {
    let Some(command) = tokens.first() else {
        return true;
    };
    let command = shell::command_basename(command);

    // Canon §9.4 explicitly treats a limited `npm test` prefix as eligible
    // for durable approval, while npm's other script/package operations stay
    // fail-closed. Keep this token-aware exception out of the basename list.
    if command == "npm"
        && tokens
            .get(1)
            .is_some_and(|subcommand| subcommand.eq_ignore_ascii_case("test"))
    {
        return false;
    }

    is_unmodeled_command(&command)
}

fn has_unmodeled_option_payload(tokens: &[String]) -> bool {
    for token in tokens.iter().skip(1) {
        if let Some((_name, value)) = token.split_once('=')
            && token.starts_with('-')
            && value.contains('=')
        {
            return true;
        }
    }
    false
}

fn has_control_option_payload(tokens: &[String]) -> bool {
    tokens.windows(2).skip(1).any(|window| {
        matches!(window[0].as_str(), "-c" | "-o")
            && window[1].split(',').any(|item| {
                item.split_once('=')
                    .is_some_and(|(_name, value)| token_looks_like_path(value))
            })
    })
}

fn git_config_key_is_command_executing(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    let parts: Vec<&str> = lower.split('.').collect();
    if parts.first().copied().unwrap_or("") == "alias" {
        return true;
    }
    if parts.first().copied().unwrap_or("") == "submodule"
        && parts.last().copied().unwrap_or("") == "command"
    {
        return true;
    }
    if lower == "gpg.program" || lower == "core.fsmonitor" {
        return true;
    }
    matches!(
        parts.last().copied().unwrap_or(""),
        "pager"
            | "editor"
            | "hookspath"
            | "sshcommand"
            | "external"
            | "cmd"
            | "tool"
            | "clean"
            | "smudge"
            | "helper"
            | "command"
    )
}

fn git_config_option_key(tokens: &[String], i: usize) -> Option<(String, usize)> {
    let token = tokens.get(i)?;
    let lower = token.to_ascii_lowercase();
    if matches!(lower.as_str(), "-c" | "--config" | "--config-env") {
        let next = tokens.get(i + 1)?;
        let key = next.split_once('=').map(|(k, _)| k).unwrap_or(next);
        return Some((key.to_owned(), 2));
    }
    if let Some(rest) = lower.strip_prefix("-c") {
        let key = rest.split_once('=').map(|(k, _)| k).unwrap_or(rest);
        if !key.is_empty() {
            return Some((key.to_owned(), 1));
        }
    }
    if let Some(rest) = lower.strip_prefix("--config=") {
        let key = rest.split_once('=').map(|(k, _)| k).unwrap_or(rest);
        if !key.is_empty() {
            return Some((key.to_owned(), 1));
        }
    }
    if let Some(rest) = lower.strip_prefix("--config-env=") {
        let key = rest.split_once('=').map(|(k, _)| k).unwrap_or(rest);
        if !key.is_empty() {
            return Some((key.to_owned(), 1));
        }
    }
    None
}

/// Reject command-execution payloads embedded in another program's options.
/// T22 intentionally does not attempt to model every tool-specific option
/// grammar; an option whose name advertises command/exec/checkpoint/filter
/// semantics is therefore not eligible for persistence.
fn has_embedded_execution_payload(tokens: &[String]) -> bool {
    let Some(command) = tokens.first() else {
        return false;
    };
    let command = shell::command_basename(command);
    const EMBEDDED_FAMILIES: &[&str] = &["find", "git", "rsync", "ssh", "tar"];
    let command = shell::canonicalize_command_name(&command, EMBEDDED_FAMILIES).unwrap_or(&command);
    if command == "find"
        && tokens.iter().skip(1).any(|token| {
            let lower = token.to_ascii_lowercase();
            matches!(
                lower.as_str(),
                "-exec" | "-execdir" | "-delete" | "-ok" | "-okdir"
            ) || lower.starts_with("-exec=")
                || lower.starts_with("-execdir=")
        })
    {
        return true;
    }
    if command == "git" {
        let mut i = 1usize;
        while i < tokens.len() {
            if let Some((key, consumed)) = git_config_option_key(tokens, i) {
                if git_config_key_is_command_executing(&key) {
                    return true;
                }
                i += consumed;
                continue;
            }
            let lower = tokens[i].to_ascii_lowercase();
            if matches!(
                lower.as_str(),
                "hooks"
                    | "hook"
                    | "commit"
                    | "merge"
                    | "rebase"
                    | "am"
                    | "checkout"
                    | "switch"
                    | "pull"
                    | "push"
                    | "worktree"
            ) {
                return true;
            }
            if matches!(
                lower.as_str(),
                "-C" | "--git-dir" | "--work-tree" | "--exec-path"
            ) {
                if tokens.get(i + 1).is_some_and(|t| is_dotgit_hooks_path(t)) {
                    return true;
                }
                i += 2;
                continue;
            }
            if lower.starts_with("--git-dir=")
                || lower.starts_with("--work-tree=")
                || lower.starts_with("--exec-path=")
            {
                if lower
                    .split_once('=')
                    .is_some_and(|(_, value)| is_dotgit_hooks_path(value))
                {
                    return true;
                }
                i += 1;
                continue;
            }
            if is_dotgit_hooks_path(&tokens[i]) {
                return true;
            }
            i += 1;
        }
    }
    if command == "rsync"
        && tokens.iter().skip(1).any(|token| {
            let lower = token.to_ascii_lowercase();
            token.starts_with("-e") && token.len() > 2
                || lower == "-e"
                || lower == "--rsh"
                || lower.starts_with("-e=")
                || lower.starts_with("--rsh=")
        })
    {
        return true;
    }
    if command == "tar"
        && tokens.iter().skip(1).any(|token| {
            let lower = token.to_ascii_lowercase();
            token.starts_with("-I") && token.len() > 2
                || lower == "-i"
                || lower == "--use-compress-program"
                || lower.starts_with("-i=")
                || lower.starts_with("--use-compress-program=")
                || lower == "--to-command"
                || lower.starts_with("--to-command=")
        })
    {
        return true;
    }
    if command == "ssh" {
        for window in tokens.windows(2) {
            let option_token = if window[0] == "-o"
                || window[0] == "--option"
                || (window[1].starts_with("-o") && window[1].len() > 2)
                || window[1].starts_with("-o=")
                || window[1].starts_with("--option=")
            {
                &window[1]
            } else {
                continue;
            };
            let lower = option_token.to_ascii_lowercase();
            let after_prefix = lower
                .strip_prefix("-o=")
                .or_else(|| lower.strip_prefix("--option="))
                .or_else(|| lower.strip_prefix("-o"))
                .unwrap_or(&lower);
            if let Some((key, _)) = after_prefix.split_once('=')
                && matches!(
                    key,
                    "proxycommand" | "localcommand" | "remotecommand" | "match"
                )
            {
                return true;
            }
        }
    }
    tokens.iter().skip(1).any(|token| {
        let lower = token.to_ascii_lowercase();
        let option = lower
            .split_once('=')
            .map_or(lower.as_str(), |(name, _)| name);
        option.starts_with("--")
            && (option.contains("exec")
                || option.contains("command")
                || option.contains("checkpoint")
                || option == "--rsync-path"
                || option.contains("filter"))
    })
}

fn is_dotgit_hooks_path(token: &str) -> bool {
    let parts: Vec<&str> = token.split('/').collect();
    parts.windows(2).any(|window| {
        window[0].eq_ignore_ascii_case(".git") && window[1].eq_ignore_ascii_case("hooks")
    })
}

fn bash_path_check(tokens: &[String], cwd: &Path, workspace: &Path) -> PathCheck {
    let mut worst = PathCheck::InsideWorkspace;
    for path in option_path_values(tokens) {
        worst = worst.max(path_check(Path::new(&path), cwd, workspace));
        if worst == PathCheck::WorkspaceEscape {
            return worst;
        }
    }
    worst
}

/// Extract path values from option assignments before applying path policy.
/// Treating `--file=/etc/passwd` as one relative pathname would otherwise
/// incorrectly place it under the workspace. Long `--name=value` and short
/// glued options whose value is visibly path-like are intentionally handled
/// without trying to model every command's option grammar.
fn option_path_values(tokens: &[String]) -> Vec<String> {
    let mut paths = Vec::new();
    let mut i = 1usize;
    while i < tokens.len() {
        let token = &tokens[i];
        if (token == "-c" || token == "-o") && i + 1 < tokens.len() {
            for item in tokens[i + 1].split(',') {
                if let Some((_name, value)) = item.split_once('=') {
                    if token_looks_like_path(value) {
                        paths.push(value.to_owned());
                    }
                } else if token_looks_like_path(item) {
                    paths.push(item.to_owned());
                }
            }
            i += 2;
            continue;
        }

        if let Some((name, value)) = token.split_once('=')
            && name.starts_with('-')
            && token_looks_like_path(value)
        {
            paths.push(value.to_owned());
            i += 1;
            continue;
        }

        if token.starts_with('-') && !token.starts_with("--") {
            if let Some((idx, c)) = token.char_indices().nth(1) {
                let value_start = idx + c.len_utf8();
                if value_start < token.len() {
                    let value = &token[value_start..];
                    if token_looks_like_path(value) {
                        paths.push(value.to_owned());
                    }
                }
            }
        } else if token_looks_like_path(token) {
            paths.push(token.clone());
        }
        i += 1;
    }
    paths
}

fn token_looks_like_path(token: &str) -> bool {
    token == "/"
        || token.starts_with('/')
        || token.starts_with("..")
        || token == ".."
        || token.starts_with("./")
        || (token.contains('/') && !token.contains("://"))
        || INTERNAL_STATE_MARKERS
            .iter()
            .any(|m| token == *m || token.strip_prefix(m).is_some_and(|s| s.starts_with('/')))
}

fn default_non_bash_decision(action: &CanonicalAction) -> PolicyDecision {
    match action.tool.as_str() {
        "read_file" | "list_dir" | "glob" | "grep" => PolicyDecision::Allow {
            matched_rules: Vec::new(),
        },
        "write_file" | "edit_file" => PolicyDecision::Allow {
            matched_rules: Vec::new(),
        },
        "delete" => PolicyDecision::NeedsApproval {
            matched_rules: Vec::new(),
            reason: "workspace delete requires approval".to_owned(),
        },
        _ => PolicyDecision::NeedsApproval {
            matched_rules: Vec::new(),
            reason: format!("tool '{}' requires explicit approval rule", action.tool),
        },
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum PathCheck {
    InsideWorkspace,
    InternalState,
    WorkspaceEscape,
}

const INTERNAL_STATE_MARKERS: &[&str] = &[".sumi", "agent.db", "secrets", ".git", ".git/hooks"];

fn non_bash_read_covers_internal_state(
    action: &CanonicalAction,
    workspace: &Path,
) -> Option<PolicyDecision> {
    match action.tool.as_str() {
        "glob" => {
            let pattern = action.argv.get(1).map(String::as_str).unwrap_or("");
            if glob_pattern_covers_internal_state(pattern) {
                return Some(PolicyDecision::NeedsApproval {
                    matched_rules: Vec::new(),
                    reason: "recursive or pattern read may include internal state".to_owned(),
                });
            }
        }
        "grep" => {
            if let Some(path) = action.affected_paths.first()
                && grep_path_is_ancestor_of_internal_state(path, &action.cwd, workspace)
            {
                return Some(PolicyDecision::NeedsApproval {
                    matched_rules: Vec::new(),
                    reason: "recursive read may descend into internal state".to_owned(),
                });
            }
        }
        "list_dir" => {
            if let Some(path) = action.affected_paths.first()
                && resolve_components(path, &action.cwd) == workspace_components(workspace)
            {
                return Some(PolicyDecision::NeedsApproval {
                    matched_rules: Vec::new(),
                    reason: "list_dir on workspace root may expose internal state".to_owned(),
                });
            }
        }
        _ => {}
    }
    None
}

fn glob_pattern_covers_internal_state(pattern: &str) -> bool {
    let components: Vec<&str> = pattern.split('/').filter(|c| !c.is_empty()).collect();
    if components.contains(&"**") {
        return true;
    }
    for marker in INTERNAL_STATE_MARKERS {
        let marker_comps: Vec<&str> = marker.split('/').collect();
        if components.windows(marker_comps.len()).any(|window| {
            window
                .iter()
                .zip(marker_comps.iter())
                .all(|(pc, mc)| glob_component_matches(pc, mc))
        }) {
            return true;
        }
    }
    false
}

fn glob_component_matches(component: &str, marker: &str) -> bool {
    let c = component.as_bytes();
    let m = marker.as_bytes();
    let mut i = 0usize;
    let mut j = 0usize;
    let mut star = None;
    let mut match_after_star = 0usize;
    while j < m.len() {
        if i < c.len() && (c[i] == m[j] || c[i] == b'?') {
            i += 1;
            j += 1;
        } else if i < c.len() && c[i] == b'*' {
            star = Some(i);
            match_after_star = j;
            i += 1;
        } else if let Some(star_idx) = star {
            i = star_idx + 1;
            match_after_star += 1;
            j = match_after_star;
        } else {
            return false;
        }
    }
    while i < c.len() && c[i] == b'*' {
        i += 1;
    }
    i == c.len()
}

fn grep_path_is_ancestor_of_internal_state(path: &Path, cwd: &Path, workspace: &Path) -> bool {
    let comps = resolve_components(path, cwd);
    let workspace_comps = workspace_components(workspace);
    if !starts_with(&comps, &workspace_comps) {
        return false;
    }
    let relative = &comps[workspace_comps.len()..];
    for marker in INTERNAL_STATE_MARKERS {
        let marker_comps: Vec<_> = marker.split('/').map(std::ffi::OsString::from).collect();
        if relative.len() < marker_comps.len()
            && relative
                .iter()
                .zip(marker_comps.iter())
                .all(|(a, b)| a == b)
        {
            return true;
        }
    }
    false
}

fn validate_action_context(action: &CanonicalAction, workspace: &Path) -> Option<PolicyDecision> {
    if !action.cwd.is_absolute() {
        return Some(PolicyDecision::Forbidden {
            matched_rules: Vec::new(),
            reason: "relative working directory".to_owned(),
        });
    }

    match path_check_all(action, workspace) {
        PathCheck::WorkspaceEscape => {
            return Some(PolicyDecision::Forbidden {
                matched_rules: Vec::new(),
                reason: "workspace escape".to_owned(),
            });
        }
        PathCheck::InternalState => {
            return Some(PolicyDecision::NeedsApproval {
                matched_rules: Vec::new(),
                reason: "internal state path".to_owned(),
            });
        }
        PathCheck::InsideWorkspace => {
            if let Some(decision) = non_bash_read_covers_internal_state(action, workspace) {
                return Some(decision);
            }
        }
    }

    if !action.sandbox.workspace_only || action.sandbox.network_allowed {
        return Some(PolicyDecision::Forbidden {
            matched_rules: Vec::new(),
            reason: "sandbox summary is broader than the default policy".to_owned(),
        });
    }

    None
}

fn path_check_all(action: &CanonicalAction, workspace: &Path) -> PathCheck {
    let mut worst = PathCheck::InsideWorkspace;
    worst = worst.max(path_check(&action.cwd, &action.cwd, workspace));
    for p in &action.affected_paths {
        worst = worst.max(path_check(p, &action.cwd, workspace));
    }
    worst
}

fn path_check(path: &Path, cwd: &Path, workspace: &Path) -> PathCheck {
    if path
        .as_os_str()
        .to_str()
        .is_some_and(|s| s.starts_with("artifact://"))
    {
        return PathCheck::InsideWorkspace;
    }

    let comps = resolve_components(path, cwd);
    let workspace_comps = workspace_components(workspace);

    if !starts_with(&comps, &workspace_comps) {
        return PathCheck::WorkspaceEscape;
    }

    if comps.len() > workspace_comps.len()
        && INTERNAL_STATE_MARKERS.iter().any(|marker| {
            let marker_comps: Vec<_> = marker.split('/').map(std::ffi::OsString::from).collect();
            comps[workspace_comps.len()..]
                .windows(marker_comps.len())
                .any(|window| window.iter().zip(marker_comps.iter()).all(|(a, b)| a == b))
        })
    {
        return PathCheck::InternalState;
    }

    PathCheck::InsideWorkspace
}

fn resolve_components(path: &Path, cwd: &Path) -> Vec<std::ffi::OsString> {
    let mut comps: Vec<std::ffi::OsString> = Vec::new();
    if !path.is_absolute() {
        for c in cwd.components() {
            if let std::path::Component::Normal(s) = c {
                comps.push(s.to_os_string());
            }
        }
    }
    for c in path.components() {
        match c {
            std::path::Component::Normal(s) => comps.push(s.to_os_string()),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                comps.pop();
            }
            std::path::Component::RootDir => comps.clear(),
            std::path::Component::Prefix(_) => {}
        }
    }
    comps
}

fn workspace_components(workspace: &Path) -> Vec<std::ffi::OsString> {
    resolve_components(workspace, Path::new("/"))
}

fn starts_with(comps: &[std::ffi::OsString], prefix: &[std::ffi::OsString]) -> bool {
    comps.len() >= prefix.len() && comps.iter().zip(prefix.iter()).all(|(a, b)| a == b)
}

fn from_effects(effects: Vec<(RuleEffect, String)>) -> PolicyDecision {
    let mut effect = RuleEffect::Allow;
    let mut ids = Vec::new();
    for (e, id) in effects {
        if e > effect {
            effect = e;
        }
        ids.push(id);
    }
    ids.sort();
    ids.dedup();
    let reason = format!("matched rules: {}", ids.join(", "));
    match effect {
        RuleEffect::Allow => PolicyDecision::Allow { matched_rules: ids },
        RuleEffect::NeedsApproval => PolicyDecision::NeedsApproval {
            matched_rules: ids,
            reason,
        },
        RuleEffect::Forbidden => PolicyDecision::Forbidden {
            matched_rules: ids,
            reason,
        },
    }
}

fn combine(a: PolicyDecision, b: PolicyDecision) -> PolicyDecision {
    let (ea, ra, ma) = destructure(a);
    let (eb, rb, mb) = destructure(b);
    let effect = ea.max(eb);
    let mut matched = ma;
    matched.extend(mb);
    matched.sort();
    matched.dedup();
    let reason = if effect == RuleEffect::Forbidden {
        if ea == RuleEffect::Forbidden { ra } else { rb }
    } else if effect == RuleEffect::NeedsApproval {
        if ea == RuleEffect::NeedsApproval {
            ra
        } else {
            rb
        }
    } else {
        String::new()
    };
    match effect {
        RuleEffect::Allow => PolicyDecision::Allow {
            matched_rules: matched,
        },
        RuleEffect::NeedsApproval => PolicyDecision::NeedsApproval {
            matched_rules: matched,
            reason,
        },
        RuleEffect::Forbidden => PolicyDecision::Forbidden {
            matched_rules: matched,
            reason,
        },
    }
}

fn destructure(d: PolicyDecision) -> (RuleEffect, String, Vec<String>) {
    match d {
        PolicyDecision::Allow { matched_rules } => {
            (RuleEffect::Allow, String::new(), matched_rules)
        }
        PolicyDecision::NeedsApproval {
            matched_rules,
            reason,
        } => (RuleEffect::NeedsApproval, reason, matched_rules),
        PolicyDecision::Forbidden {
            matched_rules,
            reason,
        } => (RuleEffect::Forbidden, reason, matched_rules),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::provider::types::ValidatedToolArguments;
    use crate::store::Redactor;

    fn policy() -> Policy {
        Policy::new("/workspace")
    }

    fn projector() -> SecretAwareActionProjector {
        SecretAwareActionProjector::new(
            Redactor::v1(),
            super::super::action::SecretDigestKey::fixture(),
        )
    }

    fn args(value: serde_json::Value) -> ValidatedToolArguments {
        serde_json::from_value(value).expect("valid args")
    }

    #[test]
    fn workspace_read_is_allowed_by_default() {
        let action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "read_file",
            &args(json!({"path":"notes.txt"})),
        )
        .unwrap();
        let decision = policy().evaluate(&action);
        assert!(decision.is_allow());
    }

    #[test]
    fn workspace_write_outside_workspace_is_forbidden() {
        let action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "write_file",
            &args(json!({"path":"/etc/passwd","content":"x"})),
        )
        .unwrap();
        assert!(policy().evaluate(&action).is_forbidden());
    }

    #[test]
    fn bash_is_needs_approval_by_default() {
        let action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command":"printf done"})),
        )
        .unwrap();
        assert!(matches!(
            policy().evaluate(&action),
            PolicyDecision::NeedsApproval { .. }
        ));
    }

    #[test]
    fn strictest_applies_across_segments() {
        let action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command":"git status && rm -rf /"})),
        )
        .unwrap();
        let p = policy()
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
        let decision = p.evaluate(&action);
        assert!(decision.is_forbidden());
    }

    #[test]
    fn quoted_separators_are_not_split() {
        let action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command":"echo \"a && b\""})),
        )
        .unwrap();
        let p = policy()
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
        assert!(p.evaluate(&action).is_allow());
    }

    #[test]
    fn dynamic_construct_is_needs_approval() {
        let action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command":"echo $(date)"})),
        )
        .unwrap();
        let p = policy()
            .try_with_rule(ApprovalRule {
                id: "echo".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["echo".to_owned(), "$(date)".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();
        assert!(matches!(
            p.evaluate(&action),
            PolicyDecision::NeedsApproval { .. }
        ));
    }

    #[test]
    fn broad_prefix_rejects_approve_always() {
        // An arbitrary one-token prefix that would match and Allow this action
        // if it were not too broad.
        let action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command":"printf done"})),
        )
        .unwrap();
        let candidate = ApprovalRule {
            id: "printf".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["printf".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        let resolved = policy().resolve(
            &action,
            UserDecision::ApproveAlways { rule: candidate },
            &projector(),
        );
        assert!(matches!(resolved, ResolvedDecision::ApproveOnce));

        // Shell/interpreter/wrapper families stay broad even with additional tokens.
        let action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command":"bash script.sh"})),
        )
        .unwrap();
        let candidate = ApprovalRule {
            id: "bash-script".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["bash".to_owned(), "script.sh".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        let resolved = policy().resolve(
            &action,
            UserDecision::ApproveAlways { rule: candidate },
            &projector(),
        );
        assert!(matches!(resolved, ResolvedDecision::ApproveOnce));
    }

    #[test]
    fn approve_always_secret_downgrade() {
        let action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command":"curl -H \"Authorization: Bearer abcdef1234567890\" https://example.com"})),
        )
        .unwrap();
        let candidate = ApprovalRule {
            id: "curl".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec![
                "curl".to_owned(),
                "-H".to_owned(),
                "Authorization: Bearer abcdef1234567890".to_owned(),
                "https://example.com".to_owned(),
            ],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec, Permission::Network],
            allowed_network_domains: vec![],
        };
        let resolved = policy().resolve(
            &action,
            UserDecision::ApproveAlways { rule: candidate },
            &projector(),
        );
        assert!(matches!(resolved, ResolvedDecision::ApproveOnce));
    }

    #[test]
    fn approve_always_secret_downgrade_non_network() {
        let action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command":"printf \"Authorization: Bearer abcdef1234567890\""})),
        )
        .unwrap();
        assert!(!action.requested_permissions.contains(&Permission::Network));

        let tokens = shell::tokenize_command(&action.argv[0]);
        let candidate = ApprovalRule {
            id: "printf-auth".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec![
                "printf".to_owned(),
                "Authorization: Bearer abcdef1234567890".to_owned(),
            ],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        assert!(candidate.matches(&action, &tokens, &PathBuf::from("/workspace")));
        assert!(projector().text_contains_secret(&candidate.literal_prefix[1]));

        let resolved = policy().resolve(
            &action,
            UserDecision::ApproveAlways { rule: candidate },
            &projector(),
        );
        assert!(matches!(resolved, ResolvedDecision::ApproveOnce));
    }

    #[test]
    fn approve_always_conflict_downgrade() {
        let action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command":"git status"})),
        )
        .unwrap();
        let p = policy()
            .try_with_rule(ApprovalRule {
                id: "existing".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["git".to_owned(), "status".to_owned()],
                effect: RuleEffect::NeedsApproval,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();
        let candidate = ApprovalRule {
            id: "candidate".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["git".to_owned(), "status".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        let resolved = p.resolve(
            &action,
            UserDecision::ApproveAlways { rule: candidate },
            &projector(),
        );
        assert!(matches!(resolved, ResolvedDecision::ApproveOnce));
    }

    #[test]
    fn valid_approve_always_is_allowed() {
        let action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command":"git status"})),
        )
        .unwrap();
        let candidate = ApprovalRule {
            id: "git-status".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["git".to_owned(), "status".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        let resolved = policy().resolve(
            &action,
            UserDecision::ApproveAlways { rule: candidate },
            &projector(),
        );
        assert!(matches!(resolved, ResolvedDecision::ApproveAlways(_)));
    }

    #[test]
    fn workspace_delete_is_not_allowed_by_default() {
        let action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "delete",
            &args(json!({"path": "notes.txt"})),
        )
        .unwrap();
        let decision = policy().evaluate(&action);
        assert!(
            matches!(decision, PolicyDecision::NeedsApproval { .. }),
            "expected delete to require approval, got {decision:?}"
        );

        let edit_action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "edit_file",
            &args(json!({"path": "notes.txt", "old_string": "a", "new_string": "b"})),
        )
        .unwrap();
        assert!(policy().evaluate(&edit_action).is_allow());
    }

    #[test]
    fn empty_allowed_permissions_does_not_match_exec() {
        let action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command": "echo hi"})),
        )
        .unwrap();
        let rule = ApprovalRule {
            id: "empty".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["echo".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![],
            allowed_network_domains: vec![],
        };
        let tokens = vec!["echo".to_owned(), "hi".to_owned()];
        assert!(!rule.matches(&action, &tokens, &PathBuf::from("/workspace")));
    }

    #[test]
    fn network_action_cannot_be_allowed_without_verifiable_domain() {
        let action = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command": "curl https://example.com"})),
        )
        .unwrap();
        assert!(action.requested_permissions.contains(&Permission::Network));
        let rule = ApprovalRule {
            id: "curl".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["curl".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec, Permission::Network],
            allowed_network_domains: vec![],
        };
        let tokens = vec!["curl".to_owned(), "https://example.com".to_owned()];
        assert!(!rule.matches(&action, &tokens, &PathBuf::from("/workspace")));
    }

    fn bash(command: &str) -> CanonicalAction {
        CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "bash",
            &args(json!({"command": command})),
        )
        .unwrap()
    }

    #[test]
    fn glued_redirection_cannot_allow_or_persist() {
        let action = bash("cat file>../secret.txt");
        let rule = ApprovalRule {
            id: "cat-file".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["cat".to_owned(), "file".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        let p = policy().try_with_rule(rule.clone()).unwrap();
        assert!(
            matches!(p.evaluate(&action), PolicyDecision::NeedsApproval { .. }),
            "glued redirection must require approval"
        );

        let resolved = p.resolve(&action, UserDecision::ApproveAlways { rule }, &projector());
        assert!(matches!(resolved, ResolvedDecision::ApproveOnce));
    }

    #[test]
    fn redirection_descriptor_heredoc_and_input_cannot_allow() {
        let p = policy()
            .try_with_rule(ApprovalRule {
                id: "cat-file".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cat".to_owned(), "file".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "cat-heredoc".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cat".to_owned(), "<<EOF".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "cat-lt".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cat".to_owned(), "<".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "cat-gt".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cat".to_owned(), ">".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();
        for command in [
            "cat file > ../secret.txt",
            "cat file 2>/dev/null",
            "cat <<EOF",
            "cat < file",
            "cat > file",
            "cat file >&2",
        ] {
            let action = bash(command);
            assert!(
                matches!(p.evaluate(&action), PolicyDecision::NeedsApproval { .. }),
                "{command} must require approval"
            );
        }
    }

    #[test]
    fn quoted_or_escaped_redirect_and_tilde_are_literal() {
        let p = policy()
            .try_with_rule(ApprovalRule {
                id: "echo-gt".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["echo".to_owned(), ">".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "echo-spaced".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["echo".to_owned(), "a > b".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "echo-glued".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["echo".to_owned(), "a>b".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "echo-tilde".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["echo".to_owned(), "~".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "echo-tilde-foo".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["echo".to_owned(), "~foo".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();
        for command in [
            "echo \">\"",
            "echo '>'",
            "echo \\>",
            "echo \"a > b\"",
            "echo 'a > b'",
            "echo a\\>b",
            "echo \"~\"",
            "echo '~'",
            "echo \\~foo",
        ] {
            let action = bash(command);
            assert!(
                p.evaluate(&action).is_allow(),
                "{command} should be allowed when quoted/escaped"
            );
        }
    }

    #[test]
    fn tilde_expansion_is_unverifiable() {
        let p = policy()
            .try_with_rule(ApprovalRule {
                id: "echo-home".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["echo".to_owned(), "~/file".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "echo-user".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["echo".to_owned(), "~user/file".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "echo-plus".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["echo".to_owned(), "~+".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "echo-minus".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["echo".to_owned(), "~-".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "echo-plus-n".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["echo".to_owned(), "~+1".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();
        for command in [
            "echo ~/file",
            "echo ~user/file",
            "echo ~+",
            "echo ~-",
            "echo ~+1",
            "export PATH=~foo",
        ] {
            let action = bash(command);
            assert!(
                matches!(p.evaluate(&action), PolicyDecision::NeedsApproval { .. }),
                "{command} must require approval"
            );
        }
    }

    #[test]
    fn special_shell_parameters_are_unverifiable() {
        let second_tokens = ["$$", "$?", "$!", "$0", "$9", "$@", "$*", "$#", "$-"];
        let mut p = policy();
        for (i, token) in second_tokens.iter().enumerate() {
            p = p
                .try_with_rule(ApprovalRule {
                    id: format!("echo-special-{i}"),
                    tool: "bash".to_owned(),
                    literal_prefix: vec!["echo".to_owned(), (*token).to_owned()],
                    effect: RuleEffect::Allow,
                    workspace_only: true,
                    allowed_permissions: vec![Permission::Exec],
                    allowed_network_domains: vec![],
                })
                .unwrap();
        }
        for command in [
            "echo $$", "echo $?", "echo $!", "echo $0", "echo $9", "echo $@", "echo $*", "echo $#",
            "echo $-",
        ] {
            let action = bash(command);
            assert!(
                matches!(p.evaluate(&action), PolicyDecision::NeedsApproval { .. }),
                "{command} must require approval"
            );
        }
    }

    #[test]
    fn directory_state_commands_are_unverifiable() {
        let p = policy()
            .try_with_rule(ApprovalRule {
                id: "cd-sumi".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cd".to_owned(), ".sumi".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "pushd-sumi".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["pushd".to_owned(), ".sumi".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "popd-n".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["popd".to_owned(), "-n".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();
        for command in ["cd .sumi", "pushd .sumi", "popd -n"] {
            let action = bash(command);
            assert!(
                matches!(p.evaluate(&action), PolicyDecision::NeedsApproval { .. }),
                "{command} must require approval"
            );
        }
    }

    #[test]
    fn cd_then_command_cannot_allow_through_separate_rules() {
        let p = policy()
            .try_with_rule(ApprovalRule {
                id: "cd-sumi".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cd".to_owned(), ".sumi".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "cat-config".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cat".to_owned(), "config".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();
        let action = bash("cd .sumi; cat config");
        assert!(matches!(
            p.evaluate(&action),
            PolicyDecision::NeedsApproval { .. }
        ));
    }

    #[test]
    fn bash_workspace_escape_is_forbidden_and_internal_state_needs_approval() {
        let p = policy()
            .try_with_rule(ApprovalRule {
                id: "cat-escape".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cat".to_owned(), "../secret.txt".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "cat-sumi".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cat".to_owned(), ".sumi/config".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "cat-sumi-dir".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cat".to_owned(), ".sumi".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();

        let escape = bash("cat ../secret.txt");
        assert!(matches!(
            p.evaluate(&escape),
            PolicyDecision::Forbidden { .. }
        ));

        let internal = bash("cat .sumi/config");
        assert!(matches!(
            p.evaluate(&internal),
            PolicyDecision::NeedsApproval { .. }
        ));

        let internal_dir = bash("cat .sumi");
        assert!(matches!(
            p.evaluate(&internal_dir),
            PolicyDecision::NeedsApproval { .. }
        ));
    }

    #[test]
    fn decision_reasons_do_not_leak_raw_path_tokens() {
        let error = match policy().try_with_rule(ApprovalRule {
            id: "cat-escape".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["cat".to_owned(), "../sk-abcdef123456".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        }) {
            Ok(_) => panic!("secret-bearing prefixes must not be persisted"),
            Err(error) => error,
        };
        assert_eq!(error, RuleValidationError::SecretMaterial);
        let p = policy()
            .try_with_rule(ApprovalRule {
                id: "cat-sumi".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cat".to_owned(), ".sumi/config".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "cat-file".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cat".to_owned(), "file".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();

        let escape = bash("cat ../sk-abcdef123456");
        let decision = p.evaluate(&escape);
        let text = format!("{:?}", decision);
        assert!(!text.contains("sk-abcdef123456"), "secret leaked: {text}");
        assert!(!text.contains("../sk-abcdef123456"), "path leaked: {text}");
        assert!(matches!(decision, PolicyDecision::Forbidden { .. }));

        let internal = bash("cat .sumi/config");
        let decision = p.evaluate(&internal);
        let text = format!("{:?}", decision);
        assert!(!text.contains(".sumi/config"), "path leaked: {text}");
        assert!(matches!(decision, PolicyDecision::NeedsApproval { .. }));

        let redirect = bash("cat file > ../sk-abcdef123456");
        let decision = p.evaluate(&redirect);
        let text = format!("{:?}", decision);
        assert!(!text.contains("sk-abcdef123456"), "secret leaked: {text}");
        assert!(!text.contains("../sk-abcdef123456"), "path leaked: {text}");
        assert!(matches!(decision, PolicyDecision::NeedsApproval { .. }));
    }

    #[test]
    fn broad_rule_ingestion_is_rejected_and_valid_rule_evaluates() {
        let broad = ApprovalRule {
            id: "broad".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["bash".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        assert!(matches!(
            Policy::new("/workspace").try_with_rule(broad),
            Err(RuleValidationError::BroadPrefix)
        ));

        let valid = ApprovalRule {
            id: "git-status".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["git".to_owned(), "status".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        let action = bash("git status");
        assert!(
            Policy::new("/workspace")
                .try_with_rule(valid)
                .unwrap()
                .evaluate(&action)
                .is_allow()
        );
    }

    #[test]
    fn adversarial_exec_wrapper_and_option_paths() {
        let p = policy();

        // Counterexample 1: exec bash -c must be fail-closed for ApproveAlways.
        let safe_exec = bash("exec bash -c 'echo safe'");
        let exec_bash_c_rule = ApprovalRule {
            id: "exec-bash-c".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["exec".to_owned(), "bash".to_owned(), "-c".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        assert!(
            matches!(
                p.clone().try_with_rule(exec_bash_c_rule.clone()),
                Err(RuleValidationError::BroadPrefix)
            ),
            "exec bash -c is an opaque interpreter and must not be persisted"
        );
        let resolved = p.resolve(
            &safe_exec,
            UserDecision::ApproveAlways {
                rule: exec_bash_c_rule,
            },
            &projector(),
        );
        assert!(
            matches!(resolved, ResolvedDecision::ApproveOnce),
            "exec bash -c ApproveAlways must be downgraded to ApproveOnce"
        );
        let destructive_exec = bash("exec bash -c 'rm -rf /'");
        assert!(
            !p.evaluate(&destructive_exec).is_allow(),
            "exec bash -c destructive variant must not be allowed"
        );

        // Control: exec with a non-interpreter command can be persisted and matched safely.
        let safe_exec_git = bash("exec git status");
        let exec_git_rule = ApprovalRule {
            id: "exec-git".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["exec".to_owned(), "git".to_owned(), "status".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        let resolved_git = policy().resolve(
            &safe_exec_git,
            UserDecision::ApproveAlways {
                rule: exec_git_rule,
            },
            &projector(),
        );
        assert!(
            matches!(resolved_git, ResolvedDecision::ApproveAlways(_)),
            "exec git status should be persistable"
        );

        // Counterexample 2: path-bearing option assignments must be extracted and checked.
        let p2 = policy()
            .try_with_rule(ApprovalRule {
                id: "tar-safe".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["tar".to_owned(), "--file=/workspace/notes.tar".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "git-safe".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec![
                    "git".to_owned(),
                    "--git-dir=/workspace/project".to_owned(),
                    "status".to_owned(),
                ],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();

        assert!(
            p2.evaluate(&bash("tar --file=/workspace/notes.tar -cvf ."))
                .is_allow()
        );
        assert!(
            p2.evaluate(&bash("git --git-dir=/workspace/project status"))
                .is_allow()
        );

        let outside_tar = bash("tar --file=/etc/passwd -cvf .");
        assert!(
            matches!(p2.evaluate(&outside_tar), PolicyDecision::Forbidden { .. }),
            "tar --file=/etc/passwd must escape workspace: {:?}",
            p2.evaluate(&outside_tar)
        );

        let outside_git = bash("git --git-dir=/etc status");
        assert!(
            matches!(p2.evaluate(&outside_git), PolicyDecision::Forbidden { .. }),
            "git --git-dir=/etc must escape workspace: {:?}",
            p2.evaluate(&outside_git)
        );

        // A persisted safe rule must not allow an outside file appended later.
        let appended_outside = bash("tar --file=/workspace/notes.tar -cvf /etc/passwd");
        assert!(
            matches!(
                p2.evaluate(&appended_outside),
                PolicyDecision::Forbidden { .. }
            ),
            "appending an outside file to a safe tar rule must not allow: {:?}",
            p2.evaluate(&appended_outside)
        );

        // Short-option glued forms should also be rejected or fail-closed.
        let short_glued = bash("tar -f/etc/passwd -cvf .");
        assert!(
            !p2.evaluate(&short_glued).is_allow(),
            "tar -f/etc/passwd must not allow: {:?}",
            p2.evaluate(&short_glued)
        );
    }

    #[test]
    fn adversarial_glob_and_brace_expansion() {
        let p = policy()
            .try_with_rule(ApprovalRule {
                id: "cat-brace".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cat".to_owned(), "{a,b}.txt".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "cat-glob".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cat".to_owned(), "*.txt".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap()
            .try_with_rule(ApprovalRule {
                id: "cat-bracket".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cat".to_owned(), "[ab].txt".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();

        // Quoted/escaped forms are literal and can be allowed by the persisted rules.
        assert!(p.evaluate(&bash("cat '{a,b}.txt'")).is_allow());
        assert!(p.evaluate(&bash("cat '*.txt'")).is_allow());
        assert!(p.evaluate(&bash("cat '[ab].txt'")).is_allow());

        // Unquoted pathname/brace expansion has broader runtime meaning, so it must not
        // match the same literal-prefix rule and must not Allow.
        assert!(
            !p.evaluate(&bash("cat {a,b}.txt")).is_allow(),
            "unquoted brace expansion must not allow"
        );
        assert!(
            !p.evaluate(&bash("cat *.txt")).is_allow(),
            "unquoted glob must not allow"
        );
        assert!(
            !p.evaluate(&bash("cat [ab].txt")).is_allow(),
            "unquoted bracket expansion must not allow"
        );

        // A brace that expands to an outside path must not be allowed.
        assert!(
            !p.evaluate(&bash("cat {../secret,.sumi}.txt")).is_allow(),
            "brace expansion to outside paths must not allow"
        );
    }

    #[test]
    fn adversarial_unmatched_paren_and_grouping() {
        let p = policy()
            .try_with_rule(ApprovalRule {
                id: "echo-paren".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["echo".to_owned(), "safe)".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();

        // Quoted/escaped closing paren is literal and safe.
        assert!(p.evaluate(&bash("echo 'safe)'")).is_allow());
        assert!(p.evaluate(&bash("echo safe\\)")).is_allow());

        // Unmatched unquoted `)` must make the segment unverifiable, preventing a later
        // destructive command from being coerced through the same literal-prefix rule.
        let with_unmatched = bash("echo safe) && rm -rf /");
        assert!(
            !p.evaluate(&with_unmatched).is_allow(),
            "unmatched ) must prevent allow: {:?}",
            p.evaluate(&with_unmatched)
        );

        // Command grouping via `{ ...; }` is unmodelled and must not be allowed.
        assert!(
            !policy().evaluate(&bash("{ echo safe; }")).is_allow(),
            "brace command grouping must not allow by default"
        );
    }

    #[test]
    fn adversarial_wrappers_privilege_and_forbidden_resolution() {
        let p = policy();
        for command in [
            "command git status",
            "source ./script.sh",
            "builtin echo safe",
            "eval echo safe",
            "trap 'rm -rf /' EXIT",
            "FOO=bar eval echo safe",
            "awk 'BEGIN {system(\"rm -rf /\")}'",
        ] {
            assert!(
                !p.evaluate(&bash(command)).is_allow(),
                "unmodeled wrapper must not allow: {command}"
            );
        }

        for command in ["/usr/bin/su -c id", "/usr/bin/doas id"] {
            assert!(
                p.evaluate(&bash(command)).is_forbidden(),
                "path-form privilege escalation must be forbidden: {command}"
            );
        }

        let forbidden = bash("rm -rf /");
        let candidate = ApprovalRule {
            id: "rm-root".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["rm".to_owned(), "-rf".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        assert!(matches!(
            p.resolve(
                &forbidden,
                UserDecision::ApproveAlways {
                    rule: candidate.clone()
                },
                &projector()
            ),
            ResolvedDecision::Rejected { .. }
        ));
        assert!(matches!(
            p.resolve(&forbidden, UserDecision::ApproveOnce, &projector()),
            ResolvedDecision::Rejected { .. }
        ));
    }

    #[test]
    fn adversarial_option_payloads_are_not_persisted_or_allowed() {
        let p = policy();
        let checkpoint = ApprovalRule {
            id: "tar-checkpoint".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec![
                "tar".to_owned(),
                "--checkpoint-action=exec=/bin/sh".to_owned(),
            ],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        assert!(matches!(
            p.clone().try_with_rule(checkpoint),
            Err(RuleValidationError::BroadPrefix)
        ));
        assert!(
            !p.evaluate(&bash("tar --checkpoint-action=exec=/bin/sh -cf out.tar ."))
                .is_allow()
        );

        let mount = ApprovalRule {
            id: "mount-bind".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec![
                "mount".to_owned(),
                "-o".to_owned(),
                "bind,src=/etc,target=/workspace/etc".to_owned(),
            ],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        assert!(matches!(
            p.clone().try_with_rule(mount),
            Err(RuleValidationError::BroadPrefix)
        ));
        assert!(
            p.evaluate(&bash(
                "mount -o bind,src=/etc,target=/workspace/etc /workspace/etc"
            ))
            .is_forbidden()
        );

        let git_hooks = ApprovalRule {
            id: "git-hooks".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec![
                "git".to_owned(),
                "-c".to_owned(),
                "core.hooksPath=/etc".to_owned(),
                "status".to_owned(),
            ],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        assert!(matches!(
            p.clone().try_with_rule(git_hooks),
            Err(RuleValidationError::BroadPrefix)
        ));
        assert!(
            p.evaluate(&bash("git -c core.hooksPath=/etc status"))
                .is_forbidden()
        );
    }

    #[test]
    fn action_context_must_be_normalized_and_narrow() {
        let mut cwd_escape = bash("git status");
        cwd_escape.cwd = PathBuf::from("/etc");
        assert!(policy().evaluate(&cwd_escape).is_forbidden());

        let mut parent_escape = bash("git status");
        parent_escape.cwd = PathBuf::from("/workspace/../etc");
        assert!(policy().evaluate(&parent_escape).is_forbidden());

        let mut broad_sandbox = bash("git status");
        broad_sandbox.sandbox = super::super::action::SandboxSummary {
            workspace_only: false,
            network_allowed: false,
        };
        assert!(policy().evaluate(&broad_sandbox).is_forbidden());

        let mut network_sandbox = bash("git status");
        network_sandbox.sandbox = super::super::action::SandboxSummary {
            workspace_only: true,
            network_allowed: true,
        };
        assert!(policy().evaluate(&network_sandbox).is_forbidden());
    }

    #[test]
    fn approve_always_scans_rule_and_action_material_for_secrets() {
        let action = bash("printf safe");
        let mut candidate = ApprovalRule {
            id: "AWS_SECRET_ACCESS_KEY=abcdef1234567890".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["printf".to_owned(), "safe".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec!["token.example".to_owned()],
        };
        assert!(matches!(
            policy().resolve(
                &action,
                UserDecision::ApproveAlways {
                    rule: candidate.clone()
                },
                &projector()
            ),
            ResolvedDecision::ApproveOnce
        ));

        candidate.id = "printf".to_owned();
        let mut secret_action = action;
        secret_action.justification =
            Some("Proxy-Authorization: Basic abcdef1234567890".to_owned());
        assert!(matches!(
            policy().resolve(
                &secret_action,
                UserDecision::ApproveAlways { rule: candidate },
                &projector()
            ),
            ResolvedDecision::ApproveOnce
        ));
    }

    #[test]
    fn persistent_rules_fail_closed_for_shell_state_and_embedded_execution() {
        for prefix in [
            vec![
                "exec".to_owned(),
                "--".to_owned(),
                "bash".to_owned(),
                "-c".to_owned(),
            ],
            vec![
                "exec".to_owned(),
                "-c".to_owned(),
                "sudo".to_owned(),
                "id".to_owned(),
            ],
            vec![
                "find".to_owned(),
                ".".to_owned(),
                "-exec".to_owned(),
                "sh".to_owned(),
            ],
            vec![
                "git".to_owned(),
                "-c".to_owned(),
                "alias.x=!sh".to_owned(),
                "status".to_owned(),
            ],
            vec![
                "tar".to_owned(),
                "--to-command=sh".to_owned(),
                "-xf".to_owned(),
            ],
            vec![
                "rsync".to_owned(),
                "--rsync-path=sh".to_owned(),
                "src".to_owned(),
            ],
            vec!["export".to_owned(), "PATH=/tmp".to_owned()],
            vec!["set".to_owned(), "--".to_owned(), "foo".to_owned()],
            vec!["A+=value".to_owned(), "echo".to_owned()],
            vec!["A[0]=value".to_owned(), "echo".to_owned()],
        ] {
            let result = policy().try_with_rule(ApprovalRule {
                id: "candidate".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: prefix,
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            });
            assert!(result.is_err(), "unsafe prefix was persisted");
        }
    }

    #[test]
    fn git_network_verbs_require_network_and_never_persist() {
        for command in [
            "git push origin main",
            "git clone https://example.com/repo",
            "git fetch origin",
            "git pull",
        ] {
            let action = bash(command);
            assert!(action.requested_permissions.contains(&Permission::Network));
            assert!(matches!(
                policy().evaluate(&action),
                PolicyDecision::NeedsApproval { .. }
            ));
            assert!(
                policy()
                    .try_with_rule(ApprovalRule {
                        id: "git-network".to_owned(),
                        tool: "bash".to_owned(),
                        literal_prefix: action
                            .argv
                            .first()
                            .map(|command| shell::tokenize_command(command))
                            .unwrap_or_default(),
                        effect: RuleEffect::Allow,
                        workspace_only: true,
                        allowed_permissions: vec![Permission::Exec, Permission::Network],
                        allowed_network_domains: vec![],
                    })
                    .is_err()
            );
        }
    }

    #[test]
    fn forged_canonical_actions_are_not_allowed_by_policy() {
        let mut forged = bash("echo safe");
        forged.operation = "write".to_owned();
        assert!(matches!(
            policy().evaluate(&forged),
            PolicyDecision::Forbidden { reason, .. }
                if reason == "canonical action failed invariant validation"
        ));

        let mut forged_permissions = bash("echo safe");
        forged_permissions
            .requested_permissions
            .push(Permission::WriteWorkspace);
        assert!(policy().evaluate(&forged_permissions).is_forbidden());

        let mut forged_paths = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "read_file",
            &args(json!({"path": "notes.txt"})),
        )
        .expect("read action");
        forged_paths.argv[1] = "../outside.txt".to_owned();
        assert!(policy().evaluate(&forged_paths).is_forbidden());
    }

    #[test]
    fn generic_wrappers_and_assignments_are_unmodeled() {
        let p = policy();
        for command in [
            "nice git status",
            "nohup cat file.txt",
            "env FOO=bar cat file.txt",
            "FOO=bar cat file.txt",
            "stdbuf -oL cat file.txt",
            "timeout 5 cat file.txt",
        ] {
            assert!(
                !p.evaluate(&bash(command)).is_allow(),
                "wrapper/assignment must not allow: {command}"
            );
        }
    }

    #[test]
    fn exec_wrapper_resolves_and_still_rejects_broad_nested_command() {
        let p = policy();
        assert!(
            !p.evaluate(&bash("exec bash -c 'echo safe'")).is_allow(),
            "exec bash must be unmodeled"
        );
        assert!(
            !p.evaluate(&bash("exec -a name git status")).is_allow(),
            "exec with option should not allow without rule"
        );

        let mut rule = ApprovalRule {
            id: "git-status".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["exec".to_owned(), "git".to_owned(), "status".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        let p = p.try_with_rule(rule.clone()).unwrap();
        assert!(p.evaluate(&bash("exec git status")).is_allow());

        rule.literal_prefix = vec![
            "exec".to_owned(),
            "-a".to_owned(),
            "name".to_owned(),
            "git".to_owned(),
            "status".to_owned(),
        ];
        let p = p.try_with_rule(rule).unwrap();
        assert!(p.evaluate(&bash("exec -a name git status")).is_allow());
    }

    #[test]
    fn git_network_classification_uses_subcommand_position() {
        let p = policy();
        assert!(
            !p.evaluate(&bash("git status push")).is_allow(),
            "git status with extra token must not be network by accident"
        );
        assert!(
            !p.evaluate(&bash("git status")).is_allow(),
            "git status must not require network"
        );
    }

    #[test]
    fn embedded_execution_payloads_reject_persistence_and_runtime() {
        let p = policy();
        for command in [
            "rsync -e 'sh -c id' src/ dst/",
            "tar -i sh -cf out.tar .",
            "ssh -o ProxyCommand='sh -c id' example.com",
        ] {
            assert!(
                !p.evaluate(&bash(command)).is_allow(),
                "embedded execution must not allow: {command}"
            );
        }

        let candidate = ApprovalRule {
            id: "rsync-e".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec![
                "rsync".to_owned(),
                "-e".to_owned(),
                "'sh -c id'".to_owned(),
                "src/".to_owned(),
                "dst/".to_owned(),
            ],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        assert!(matches!(
            p.try_with_rule(candidate),
            Err(RuleValidationError::BroadPrefix)
        ));
    }

    #[test]
    fn compound_reserved_words_are_unmodeled() {
        let p = policy();
        for command in [
            "if true; then :; fi",
            "for i in 1; do :; done",
            "while true; do :; done",
            "case x in x) :;; esac",
            "select x in a; do break; done",
            "function f() { :; }",
            "coproc echo hi",
        ] {
            assert!(
                !p.evaluate(&bash(command)).is_allow(),
                "compound reserved word must not allow: {command}"
            );
        }
    }

    #[test]
    fn prefix_ending_in_operand_requiring_flag_is_broad() {
        let _p = policy();
        for prefix in [
            vec!["rm".to_owned(), "-rf".to_owned()],
            vec!["cp".to_owned(), "-r".to_owned()],
            vec!["tar".to_owned(), "-f".to_owned()],
            vec!["chmod".to_owned(), "-R".to_owned()],
        ] {
            assert!(
                is_broad_prefix(&prefix),
                "prefix ending in operand-requiring flag must be broad: {prefix:?}"
            );
        }

        for prefix in [
            vec!["rm".to_owned(), "--help".to_owned()],
            vec!["git".to_owned(), "--version".to_owned()],
        ] {
            assert!(
                !is_broad_prefix(&prefix),
                "self-contained safe flags should not be broad: {prefix:?}"
            );
        }
    }

    #[test]
    fn build_runners_are_unmodeled_for_persistence_and_runtime() {
        let p = policy();
        for command in [
            "make build",
            "ninja",
            "cargo test",
            "go test",
            "npm install",
            "yarn test",
            "pnpm run build",
            "cmake --build .",
            "meson compile",
            "just build",
        ] {
            assert!(
                !p.evaluate(&bash(command)).is_allow(),
                "build runner must not allow: {command}"
            );
            let tokens = shell::tokenize_command(command);
            assert!(
                is_broad_prefix(&tokens),
                "build runner prefix must not persist: {command}"
            );
        }
    }

    #[test]
    fn canon_limited_npm_test_prefix_can_be_persisted() {
        let rule = ApprovalRule {
            id: "npm-test".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["npm".to_owned(), "test".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: Vec::new(),
        };
        let p = policy().try_with_rule(rule).expect("limited npm test rule");

        assert!(p.evaluate(&bash("npm test")).is_allow());
        assert!(!p.evaluate(&bash("npm install")).is_allow());
    }

    #[test]
    fn git_command_executing_config_keys_fail_closed() {
        let p = policy();
        for command in [
            "git -c core.pager=cat status",
            "git -c core.editor=vi status",
            "git -c core.sshCommand=ssh status",
            "git -c core.hooksPath=/etc status",
            "git -c diff.external=diff status",
            "git -c merge.tool=vim status",
            "git -c mergetool.vim.cmd=vimdiff status",
            "git -c filter.lfs.clean=git-lfs-clean status",
            "git -c sendemail.tool=sendmail status",
            "git -c credential.helper=store status",
            "git --config-env=core.pager=MY_PAGER status",
            "git -ccore.pager=cat status",
        ] {
            assert!(
                !p.evaluate(&bash(command)).is_allow(),
                "git command-executing config must not allow: {command}"
            );
            let tokens = shell::tokenize_command(command);
            assert!(
                is_broad_prefix(&tokens),
                "git command-executing prefix must not persist: {command}"
            );
        }

        // Harmless config keys remain eligible for explicit allow rules.
        let p = policy()
            .try_with_rule(ApprovalRule {
                id: "git-name".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec![
                    "git".to_owned(),
                    "-c".to_owned(),
                    "user.name=foo".to_owned(),
                    "status".to_owned(),
                ],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();
        assert!(p.evaluate(&bash("git -c user.name=foo status")).is_allow());
    }

    #[test]
    fn git_network_subcommands_require_network_and_do_not_persist() {
        for command in [
            "git submodule update --init",
            "git remote update",
            "git ls-remote origin",
            "git archive --remote=origin main",
            "git --config-env=core.pager=PAGER ls-remote origin",
        ] {
            let action = bash(command);
            assert!(
                action.requested_permissions.contains(&Permission::Network),
                "expected network permission: {command}"
            );
            let tokens = shell::tokenize_command(command);
            assert!(
                is_broad_prefix(&tokens),
                "network git prefix must not persist: {command}"
            );
        }
    }

    #[test]
    fn find_delete_and_ok_suffixes_fail_closed() {
        let p = policy();
        for command in [
            "find . -name foo -delete",
            "find . -ok rm \"{}\"",
            "find . -okdir rm \"{}\"",
        ] {
            assert!(
                !p.evaluate(&bash(command)).is_allow(),
                "find destructive/execution suffix must not allow: {command}"
            );
            let tokens = shell::tokenize_command(command);
            assert!(
                is_broad_prefix(&tokens),
                "find destructive prefix must not persist: {command}"
            );
        }
    }

    #[test]
    fn find_read_prefix_cannot_authorize_delete_suffix() {
        let p = policy()
            .try_with_rule(ApprovalRule {
                id: "find-read".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec![
                    "find".to_owned(),
                    ".".to_owned(),
                    "-name".to_owned(),
                    "foo".to_owned(),
                ],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();
        assert!(
            p.evaluate(&bash("find . -name foo")).is_allow(),
            "read-like find prefix should allow"
        );
        assert!(
            !p.evaluate(&bash("find . -name foo -delete")).is_allow(),
            "earlier read-like prefix must not authorize -delete suffix"
        );
        assert!(
            !p.evaluate(&bash("find . -name foo -ok rm \"{}\""))
                .is_allow(),
            "earlier read-like prefix must not authorize -ok suffix"
        );
    }

    #[test]
    fn approve_always_downgrades_shell_credential_rules() {
        let p = policy();
        let action = bash("curl -u user:pass https://example.com");
        let candidate = ApprovalRule {
            id: "curl-auth".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: shell::tokenize_command("curl -u user:pass https://example.com"),
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec, Permission::Network],
            allowed_network_domains: vec!["example.com".to_owned()],
        };
        assert!(
            p.clone().try_with_rule(candidate.clone()).is_err(),
            "credential-bearing rule must not be persisted"
        );
        assert!(matches!(
            p.resolve(
                &action,
                UserDecision::ApproveAlways { rule: candidate },
                &projector()
            ),
            ResolvedDecision::ApproveOnce
        ));
    }

    #[test]
    fn t22_security_gaps_fail_closed() {
        let p = policy();

        // High-risk execution environments must not be persistently allowed.
        for command in [
            "sed -n 's/x/y/' file.txt",
            "ed file.txt",
            "ex file.txt",
            "vi -c 'q!' file.txt",
            "vim file.txt",
            "script -c 'echo hi'",
            "expect -c 'spawn sh'",
            "tclsh script.tcl",
            "wish script.tcl",
            "gdb -batch -x script.gdb",
            "lldb -b -o 'script'",
            "sqlite3 db '.dump'",
            "parallel echo ::: a b c",
            "socat TCP:host:80 -",
        ] {
            let action = bash(command);
            assert!(
                !p.evaluate(&action).is_allow(),
                "{command} must not be allowed by default"
            );
            let rule = ApprovalRule {
                id: "high-risk".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: shell::tokenize_command(command)
                    .into_iter()
                    .take(3)
                    .collect(),
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            };
            assert!(
                matches!(
                    p.clone().try_with_rule(rule),
                    Err(RuleValidationError::BroadPrefix)
                ),
                "{command} must not be persistable"
            );
        }

        // Privilege/identity/namespace commands are forbidden.
        for command in [
            "runuser -u nobody id",
            "newgrp users",
            "sg users id",
            "unshare -r sh",
            "nsenter -t 1 -m sh",
            "chroot /workspace sh",
            "setpriv --reuid=0 id",
            "runcon user_r:user_t sh",
        ] {
            assert!(
                p.evaluate(&bash(command)).is_forbidden(),
                "{command} must be forbidden"
            );
        }

        // Network clients require approval and do not persist.
        for command in [
            "telnet host",
            "ncat host 80",
            "aws s3 ls",
            "gcloud compute instances list",
            "az group list",
            "gh repo clone org/repo",
            "rclone copy remote:bucket .",
            "redis-cli -a secret ping",
            "mysql -h db -u root -psecret",
            "psql -h db -U postgres",
            "mongosh -u user -psecret",
            "sqlcmd -S db -P secret",
            "cqlsh db -p secret",
            "sshpass -p secret ssh user@host",
        ] {
            let action = bash(command);
            assert!(
                !p.evaluate(&action).is_allow(),
                "{command} must not be allowed by default"
            );
            assert!(
                action.requested_permissions.contains(&Permission::Network),
                "{command} must request Network permission"
            );
        }

        // Git execution-configuration keys and .git/hooks paths fail closed.
        for command in [
            "git -c gpg.program=/tmp/gpg status",
            "git -c core.fsmonitor=/tmp/hook status",
            "git -c submodule.foo.command=sh status",
            "git -C .git/hooks status",
            "git --git-dir=.git/hooks status",
            "git hooks run pre-commit",
            "git commit -m message",
            "git merge topic",
            "git rebase main",
            "git am patch",
            "git checkout topic",
            "git switch topic",
            "git worktree add ../other topic",
        ] {
            assert!(
                !p.evaluate(&bash(command)).is_allow(),
                "{command} must not be allowed"
            );
        }

        // Recursive or pattern non-bash reads that may touch internal state fail closed,
        // while literal safe subtrees remain allowed.
        let glob = |pattern: &str| {
            CanonicalAction::from_tool_call(
                PathBuf::from("/workspace"),
                "glob",
                &args(json!({"pattern": pattern})),
            )
            .unwrap()
        };
        assert!(
            !p.evaluate(&glob("**")).is_allow(),
            "glob ** must not be allowed by default"
        );
        assert!(
            !p.evaluate(&glob(".sumi*")).is_allow(),
            "glob .sumi* must not be allowed by default"
        );
        assert!(
            p.evaluate(&glob("src/*.rs")).is_allow(),
            "glob src/*.rs should be allowed"
        );

        let grep = |path: &str| {
            CanonicalAction::from_tool_call(
                PathBuf::from("/workspace"),
                "grep",
                &args(json!({"path": path, "pattern": "foo"})),
            )
            .unwrap()
        };
        assert!(
            !p.evaluate(&grep(".")).is_allow(),
            "grep . must not be allowed by default"
        );
        assert!(
            !p.evaluate(&grep(".git")).is_allow(),
            "grep .git must not be allowed by default"
        );
        assert!(
            p.evaluate(&grep("src")).is_allow(),
            "grep src should be allowed"
        );
    }

    #[test]
    fn git_c_preserves_network_permission() {
        for command in [
            "git -C /workspace/repo push origin main",
            "git -C repo push origin main",
            "git -C /workspace/repo clone https://example.com/repo.git",
            "git -C repo clone https://example.com/repo.git",
            "git -C /workspace/repo fetch origin",
            "git -C /workspace/repo pull",
            "git -C /workspace/repo ls-remote origin",
            "git -Crepo push origin main",
        ] {
            let action = bash(command);
            assert!(
                action.requested_permissions.contains(&Permission::Network),
                "expected network permission for {command}"
            );
        }
    }

    #[test]
    fn dotgit_internal_state_is_fail_closed() {
        let p = policy();

        // glob .git* should be internal state
        let glob_dotgit = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "glob",
            &args(json!({"pattern": ".git*"})),
        )
        .unwrap();
        assert!(
            !p.evaluate(&glob_dotgit).is_allow(),
            "glob .git* must not be allowed by default"
        );

        // grep .git/config should be internal state
        let grep_gitconfig = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "grep",
            &args(json!({"path": ".git/config", "pattern": "foo"})),
        )
        .unwrap();
        assert!(
            !p.evaluate(&grep_gitconfig).is_allow(),
            "grep .git/config must not be allowed by default"
        );

        // bash cat .git/config should be internal state even with a rule
        let cat_gitconfig = bash("cat .git/config");
        let p2 = p
            .try_with_rule(ApprovalRule {
                id: "cat-gitconfig".to_owned(),
                tool: "bash".to_owned(),
                literal_prefix: vec!["cat".to_owned(), ".git/config".to_owned()],
                effect: RuleEffect::Allow,
                workspace_only: true,
                allowed_permissions: vec![Permission::Exec],
                allowed_network_domains: vec![],
            })
            .unwrap();
        assert!(
            !p2.evaluate(&cat_gitconfig).is_allow(),
            "cat .git/config must not be allowed even with a literal rule"
        );
    }

    #[test]
    fn glued_rsync_tar_execution_payloads_fail_closed() {
        let p = policy();

        // rsync glued -e with single-quoted payload
        let rsync_cmd = "rsync -e'sh -c id' src/ dst/";
        let rsync_action = bash(rsync_cmd);
        let rsync_rule = ApprovalRule {
            id: "rsync-e".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: shell::tokenize_command(rsync_cmd),
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        assert!(
            p.clone().try_with_rule(rsync_rule.clone()).is_err(),
            "glued rsync -e prefix must not persist"
        );
        assert!(
            !p.evaluate(&rsync_action).is_allow(),
            "glued rsync -e must not allow"
        );

        // tar glued -I (use-compress-program) with command
        let tar_cmd = "tar -Ish -cf out.tar .";
        let tar_action = bash(tar_cmd);
        let tar_rule = ApprovalRule {
            id: "tar-I".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: shell::tokenize_command(tar_cmd),
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        assert!(
            p.clone().try_with_rule(tar_rule.clone()).is_err(),
            "glued tar -I prefix must not persist"
        );
        assert!(
            !p.evaluate(&tar_action).is_allow(),
            "glued tar -I must not allow"
        );
    }

    #[test]
    fn glued_ssh_proxy_command_is_detected() {
        let p = policy();

        // glued ssh -o ProxyCommand must not be treated as ordinary network use
        let ssh_cmd = "ssh -oProxyCommand='sh -c id' example.com";
        let ssh_action = bash(ssh_cmd);
        assert!(
            ssh_action
                .requested_permissions
                .contains(&Permission::Network),
            "ssh must request network permission"
        );

        let ssh_rule = ApprovalRule {
            id: "ssh-proxy".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: shell::tokenize_command(ssh_cmd),
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec, Permission::Network],
            allowed_network_domains: vec![],
        };
        // Network rules cannot persist anyway (T22 no domain extraction), but the
        // runtime should not allow either.
        assert!(
            p.clone().try_with_rule(ssh_rule.clone()).is_err(),
            "glued ssh -o prefix must not persist"
        );
        let decision = p.evaluate(&ssh_action);
        assert!(
            matches!(
                &decision,
                PolicyDecision::Forbidden { reason, .. }
                    if reason == "unmodeled shell wrapper or option payload"
            ),
            "glued ssh -o ProxyCommand must be forbidden, got {decision:?}"
        );

        assert!(
            matches!(
                p.resolve(&ssh_action, UserDecision::ApproveOnce, &projector()),
                ResolvedDecision::Rejected { .. }
            ),
            "ApproveOnce must not silently allow ssh ProxyCommand execution"
        );
    }

    #[test]
    fn privileged_filesystem_and_block_commands_are_forbidden() {
        let p = policy();
        for command in [
            "mount -o bind,src=/etc,target=/workspace/etc /workspace/etc",
            "umount /workspace/mnt",
            "losetup /dev/loop0 /workspace/disk.img",
            "mkfs.ext4 /workspace/disk.img",
            "mkswap /workspace/swapfile",
            "swapon /workspace/swapfile",
            "swapoff /workspace/swapfile",
            "fdisk /workspace/disk.img",
            "sfdisk /workspace/disk.img",
            "parted /workspace/disk.img mklabel msdos",
            "partprobe /workspace/disk.img",
            "blockdev --getsize /dev/loop0",
            "hdparm -I /dev/sda",
            "dd if=/dev/sda of=/workspace/image",
            "fsck /workspace/disk.img",
            "e2fsck /workspace/disk.img",
            "tune2fs /workspace/disk.img",
            "resize2fs /workspace/disk.img",
            "debugfs /workspace/disk.img",
            "wipefs /workspace/disk.img",
            "fusermount3 -u /workspace/mnt",
        ] {
            let action = bash(command);
            assert!(
                p.evaluate(&action).is_forbidden(),
                "{command} must be forbidden"
            );
            let prefix = shell::tokenize_command(command)
                .into_iter()
                .take(3)
                .collect::<Vec<_>>();
            assert!(
                is_broad_prefix(&prefix),
                "{command} must not be persistable"
            );
        }
    }

    #[test]
    fn versioned_interpreter_and_privileged_names_fail_closed() {
        let p = policy();

        // These versioned/variant names must be classified like their base family.
        for command in [
            "python3.11 -c 'print(1)'",
            "python3 -c 'print(1)'",
            "ruby3.2 script.rb",
            "php8.2 script.php",
            "perl5.34 script.pl",
            "node18 app.js",
            "ksh93 script.ksh",
            "bash-5.2 -c 'echo hi'",
            "mount.nfs server:/export /workspace/mnt",
            "fusermount3 -u /workspace/mnt",
        ] {
            let action = bash(command);
            assert!(
                !p.evaluate(&action).is_allow(),
                "{command} must not be allowed by default"
            );
            let prefix = shell::tokenize_command(command)
                .into_iter()
                .take(3)
                .collect::<Vec<_>>();
            assert!(
                is_broad_prefix(&prefix),
                "{command} must not be persistable"
            );
        }

        // Benign digit/dotted names that are not real family variants must not
        // be accidentally truncated or misclassified.
        assert!(!is_unmodeled_command("python3-foo"));
        assert!(!is_unmodeled_command("node-sass"));
        assert!(!is_unmodeled_command("ruby-build"));
        assert!(!is_unmodeled_command("perl-doc"));
        assert!(!is_unmodeled_command("bash-static"));
        assert!(!is_unmodeled_command("phpunit"));
        assert!(!is_unmodeled_command("luafoo"));
        assert!(!is_privilege_escalation_command("mountaintop"));
        assert!(!is_privilege_escalation_command("ddrescue"));

        let benign_prefix = ["python3-foo".to_owned(), "script.py".to_owned()];
        assert!(
            !is_broad_prefix(&benign_prefix),
            "benign python3-foo must not be treated as python"
        );
    }

    #[test]
    fn list_dir_on_workspace_root_needs_approval() {
        let p = policy();
        let list_dir = |path: &str| {
            CanonicalAction::from_tool_call(
                PathBuf::from("/workspace"),
                "list_dir",
                &args(json!({"path": path})),
            )
            .unwrap()
        };

        for path in ["/workspace", "/workspace/", "//workspace"] {
            let action = list_dir(path);
            assert!(
                matches!(p.evaluate(&action), PolicyDecision::NeedsApproval { .. }),
                "list_dir {path} must require approval"
            );
        }

        let action = list_dir("src");
        assert!(
            p.evaluate(&action).is_allow(),
            "list_dir src should be allowed"
        );

        // ApproveAlways for the workspace root must not persist; it downgrades
        // to ApproveOnce because the root guard keeps the action in
        // NeedsApproval.
        let root_action = list_dir("/workspace");
        let rule = ApprovalRule {
            id: "list-root".to_owned(),
            tool: "list_dir".to_owned(),
            literal_prefix: vec!["list_dir".to_owned(), "/workspace".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::ReadWorkspace],
            allowed_network_domains: vec![],
        };
        assert!(
            matches!(
                p.resolve(
                    &root_action,
                    UserDecision::ApproveAlways { rule },
                    &projector()
                ),
                ResolvedDecision::ApproveOnce
            ),
            "list_dir root ApproveAlways must downgrade to ApproveOnce"
        );
    }

    #[test]
    fn openssl_s_client_is_network_and_fail_closed() {
        let p = policy();
        let action = bash("openssl s_client -connect example.com:443");
        assert!(
            action.requested_permissions.contains(&Permission::Network),
            "openssl s_client must request Network permission"
        );
        assert!(
            !p.evaluate(&action).is_allow(),
            "openssl s_client must not be allowed by default"
        );
        let prefix = shell::tokenize_command("openssl s_client -connect example.com:443")
            .into_iter()
            .take(3)
            .collect::<Vec<_>>();
        assert!(
            is_broad_prefix(&prefix),
            "openssl s_client must not persist"
        );

        // Non-network openssl subcommands should not request Network.
        let local_action = bash("openssl x509 -in cert.pem");
        assert!(
            !local_action
                .requested_permissions
                .contains(&Permission::Network),
            "openssl x509 must not request Network permission"
        );
    }

    #[test]
    fn pytest_pytest_and_rustup_are_unmodeled() {
        let p = policy();
        for command in ["pytest", "py.test", "rustup run stable cargo test"] {
            let action = bash(command);
            assert!(
                !p.evaluate(&action).is_allow(),
                "{command} must not be allowed by default"
            );
            let prefix = shell::tokenize_command(command)
                .into_iter()
                .take(3)
                .collect::<Vec<_>>();
            assert!(
                is_broad_prefix(&prefix),
                "{command} must not be persistable"
            );
        }
    }

    #[test]
    fn short_option_path_values_are_utf8_safe() {
        // ASCII short option with a path value.
        assert_eq!(
            option_path_values(&["tar".to_owned(), "-I/workspace/src".to_owned()]),
            vec!["/workspace/src"]
        );

        // Multi-byte option characters; no value for -あ and a value for -á.
        assert!(option_path_values(&["cmd".to_owned(), "-あ".to_owned()]).is_empty());
        assert_eq!(
            option_path_values(&["cmd".to_owned(), "-á/etc".to_owned()]),
            vec!["/etc"]
        );

        // Plain paths are still detected.
        assert_eq!(
            option_path_values(&["cmd".to_owned(), "/workspace".to_owned()]),
            vec!["/workspace"]
        );
    }

    #[test]
    fn multiple_parent_traversal_is_forbidden() {
        for path in [
            "../../etc/passwd",
            "../etc/passwd",
            "/workspace/../etc/passwd",
            "foo/../../etc/passwd",
            "foo/bar/../../../etc/passwd",
        ] {
            let action = CanonicalAction::from_tool_call(
                PathBuf::from("/workspace"),
                "write_file",
                &args(json!({"path": path, "content": "x"})),
            )
            .unwrap_or_else(|e| panic!("{path} should parse: {e:?}"));
            assert!(
                policy().evaluate(&action).is_forbidden(),
                "{path} should escape workspace and be forbidden"
            );
        }

        let normal_relative = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "write_file",
            &args(json!({"path": "foo/bar/../baz", "content": "x"})),
        )
        .unwrap();
        assert!(
            policy().evaluate(&normal_relative).is_allow(),
            "foo/bar/../baz should stay inside workspace"
        );
    }
}
