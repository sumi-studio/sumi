//! Canonical action representation and secret-aware projection for approval.

use std::{
    ffi::{OsStr, OsString},
    fmt,
    path::{Component, Path, PathBuf},
};

use hmac::{Hmac, Mac};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::Sha256;
use zeroize::Zeroize;

use crate::provider::types::ValidatedToolArguments;
use crate::store::Redactor;

pub const BASH_TOOL_NAME: &str = "bash";

/// Runtime-internal representation of a tool call. Not serializable, not stored,
/// and its `Debug` implementation redacts `argv`, `cwd`, `affected_paths`, and
/// `justification` so secret material never leaks through diagnostics.
pub struct CanonicalAction {
    pub tool: String,
    pub operation: String,
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    pub affected_paths: Vec<PathBuf>,
    pub sandbox: SandboxSummary,
    pub requested_permissions: Vec<Permission>,
    pub justification: Option<String>,
}

impl CanonicalAction {
    /// Best-effort conversion from a validated tool call into a runtime-internal
    /// canonical action. The mapping is intentionally minimal: it only knows
    /// the standard workspace tool vocabulary used by the current executor.
    pub fn from_tool_call(
        workspace_root: PathBuf,
        tool_name: &str,
        args: &ValidatedToolArguments,
    ) -> Result<Self, ActionError> {
        let map = args.as_object();
        let mut action = match tool_name {
            "read_file" => Self {
                tool: tool_name.to_owned(),
                operation: "read".to_owned(),
                argv: vec![tool_name.to_owned(), get_path_arg(map, "path")?],
                cwd: workspace_root.clone(),
                affected_paths: vec![PathBuf::from(get_path_arg(map, "path")?)],
                sandbox: SandboxSummary::workspace(),
                requested_permissions: vec![Permission::ReadWorkspace],
                justification: None,
            },
            "write_file" => Self {
                tool: tool_name.to_owned(),
                operation: "write".to_owned(),
                argv: vec![tool_name.to_owned(), get_path_arg(map, "path")?],
                cwd: workspace_root.clone(),
                affected_paths: vec![PathBuf::from(get_path_arg(map, "path")?)],
                sandbox: SandboxSummary::workspace(),
                requested_permissions: vec![Permission::WriteWorkspace],
                justification: None,
            },
            "edit_file" => Self {
                tool: tool_name.to_owned(),
                operation: "edit".to_owned(),
                argv: vec![tool_name.to_owned(), get_path_arg(map, "path")?],
                cwd: workspace_root.clone(),
                affected_paths: vec![PathBuf::from(get_path_arg(map, "path")?)],
                sandbox: SandboxSummary::workspace(),
                requested_permissions: vec![Permission::EditWorkspace],
                justification: None,
            },
            "delete" => Self {
                tool: tool_name.to_owned(),
                operation: "delete".to_owned(),
                argv: vec![tool_name.to_owned(), get_path_arg(map, "path")?],
                cwd: workspace_root.clone(),
                affected_paths: vec![PathBuf::from(get_path_arg(map, "path")?)],
                sandbox: SandboxSummary::workspace(),
                requested_permissions: vec![Permission::DeleteWorkspace],
                justification: None,
            },
            "list_dir" => Self {
                tool: tool_name.to_owned(),
                operation: "read".to_owned(),
                argv: vec![tool_name.to_owned(), get_path_arg(map, "path")?],
                cwd: workspace_root.clone(),
                affected_paths: vec![PathBuf::from(get_path_arg(map, "path")?)],
                sandbox: SandboxSummary::workspace(),
                requested_permissions: vec![Permission::ReadWorkspace],
                justification: None,
            },
            "glob" => Self {
                tool: tool_name.to_owned(),
                operation: "read".to_owned(),
                argv: vec![tool_name.to_owned(), get_path_arg(map, "pattern")?],
                cwd: workspace_root.clone(),
                affected_paths: vec![PathBuf::from(get_path_arg(map, "pattern")?)],
                sandbox: SandboxSummary::workspace(),
                requested_permissions: vec![Permission::ReadWorkspace],
                justification: None,
            },
            "grep" => Self {
                tool: tool_name.to_owned(),
                operation: "read".to_owned(),
                argv: vec![
                    tool_name.to_owned(),
                    get_path_arg(map, "path")?,
                    get_string(map, "pattern")?,
                ],
                cwd: workspace_root.clone(),
                affected_paths: vec![PathBuf::from(get_path_arg(map, "path")?)],
                sandbox: SandboxSummary::workspace(),
                requested_permissions: vec![Permission::ReadWorkspace],
                justification: None,
            },
            "bash" => {
                let command = get_string(map, "command")?;
                let mut permissions = vec![Permission::Exec];
                if network_indicators_in_command(&command) {
                    permissions.push(Permission::Network);
                }
                permissions.sort();
                permissions.dedup();
                Self {
                    tool: tool_name.to_owned(),
                    operation: "exec".to_owned(),
                    argv: vec![command],
                    cwd: workspace_root.clone(),
                    affected_paths: vec![],
                    sandbox: SandboxSummary::workspace(),
                    requested_permissions: permissions,
                    justification: None,
                }
            }
            _ => return Err(ActionError::UnknownTool(tool_name.to_owned())),
        };
        action.cwd = workspace_root;
        action.requested_permissions.sort();
        action.requested_permissions.dedup();
        action.validate()?;
        Ok(action)
    }

    /// Validate the invariants that make a canonical action meaningful to the
    /// approval policy. The fields remain runtime-internal in spirit (the
    /// struct is not serializable), but callers can still construct one in
    /// tests or integration glue, so the policy boundary must not trust a
    /// forged tool/operation/permission tuple.
    pub fn validate(&self) -> Result<(), ActionError> {
        let expected_operation = match self.tool.as_str() {
            "read_file" | "list_dir" | "glob" | "grep" => "read",
            "write_file" => "write",
            "edit_file" => "edit",
            "delete" => "delete",
            BASH_TOOL_NAME => "exec",
            _ => return Err(ActionError::InvalidAction("unknown canonical tool")),
        };
        if self.operation != expected_operation {
            return Err(ActionError::InvalidAction("tool operation mismatch"));
        }

        let expected_permission = match self.tool.as_str() {
            "read_file" | "list_dir" | "glob" | "grep" => Permission::ReadWorkspace,
            "write_file" => Permission::WriteWorkspace,
            "edit_file" => Permission::EditWorkspace,
            "delete" => Permission::DeleteWorkspace,
            BASH_TOOL_NAME => Permission::Exec,
            _ => unreachable!("tool matched above"),
        };
        if !self.requested_permissions.contains(&expected_permission)
            || self
                .requested_permissions
                .contains(&Permission::PrivilegeEscalation)
        {
            return Err(ActionError::InvalidAction("permission scope mismatch"));
        }

        if self.requested_permissions.windows(2).any(|w| w[1] <= w[0]) {
            return Err(ActionError::InvalidAction(
                "duplicate or unsorted permissions",
            ));
        }

        if !is_lexically_normalized(&self.cwd) {
            return Err(ActionError::InvalidAction("noncanonical cwd"));
        }
        for path in &self.affected_paths {
            if !is_lexically_normalized(path) {
                return Err(ActionError::InvalidAction("noncanonical affected path"));
            }
        }

        if self.tool == BASH_TOOL_NAME {
            let command = self.argv.first().map(String::as_str).unwrap_or("");
            if self.argv.len() != 1
                || !self.affected_paths.is_empty()
                || self
                    .requested_permissions
                    .iter()
                    .any(|permission| !matches!(permission, Permission::Exec | Permission::Network))
                || network_indicators_in_command(command)
                    != self.requested_permissions.contains(&Permission::Network)
            {
                return Err(ActionError::InvalidAction("shell action shape mismatch"));
            }
        } else if self.affected_paths.len() != 1 {
            return Err(ActionError::InvalidAction(
                "path action has no affected path",
            ));
        } else {
            let expected_argv_len = if self.tool == "grep" { 3 } else { 2 };
            if self.argv.len() != expected_argv_len
                || self.argv.first().map(String::as_str) != Some(self.tool.as_str())
                || self
                    .argv
                    .get(1)
                    .is_none_or(|path| Path::new(path) != self.affected_paths[0])
                || self
                    .requested_permissions
                    .iter()
                    .any(|permission| *permission != expected_permission)
            {
                return Err(ActionError::InvalidAction("path action shape mismatch"));
            }
        }

        if !self.cwd.is_absolute() {
            return Err(ActionError::InvalidAction("canonical cwd must be absolute"));
        }
        if self.sandbox.network_allowed
            && !self.requested_permissions.contains(&Permission::Network)
        {
            return Err(ActionError::InvalidAction(
                "sandbox and permission scope mismatch",
            ));
        }
        if !self.sandbox.workspace_only {
            return Err(ActionError::InvalidAction(
                "sandbox scope is broader than canonical workspace tools",
            ));
        }

        for (index, value) in self.argv.iter().enumerate() {
            if let Some(handle) = value.strip_prefix("artifact://") {
                if !matches!(self.tool.as_str(), "read_file" | "grep")
                    || index != 1
                    || !is_valid_artifact_handle(handle)
                {
                    return Err(ActionError::InvalidAction(
                        "artifact handle is not valid for this tool",
                    ));
                }
            } else if value.contains("artifact://") {
                return Err(ActionError::InvalidAction(
                    "artifact handle is not valid for this tool",
                ));
            }
        }
        if self.cwd.to_string_lossy().contains("artifact://") {
            return Err(ActionError::InvalidAction(
                "artifact handle is not valid for this tool",
            ));
        }
        if self
            .justification
            .as_deref()
            .is_some_and(|text| text.contains("artifact://"))
        {
            return Err(ActionError::InvalidAction(
                "artifact handle is not valid for this tool",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for CanonicalAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CanonicalAction")
            .field("tool", &self.tool)
            .field("operation", &self.operation)
            .field(
                "argv",
                &format_args!("[{} tokens redacted]", self.argv.len()),
            )
            .field("cwd", &"[REDACTED]")
            .field(
                "affected_paths",
                &format_args!("[{} paths redacted]", self.affected_paths.len()),
            )
            .field("sandbox", &self.sandbox)
            .field("requested_permissions", &self.requested_permissions)
            .field(
                "justification",
                &self.justification.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    ReadWorkspace,
    WriteWorkspace,
    EditWorkspace,
    DeleteWorkspace,
    Exec,
    Network,
    DomainMutation,
    PrivilegeEscalation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxSummary {
    pub network_allowed: bool,
    pub workspace_only: bool,
}

impl SandboxSummary {
    pub fn workspace() -> Self {
        Self {
            network_allowed: false,
            workspace_only: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReviewToken {
    Literal { text: String },
    SecretRef { kind: String, digest: String },
    Omitted,
}

impl ReviewToken {
    fn is_empty_literal(&self) -> bool {
        matches!(self, Self::Literal { text } if text.is_empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReviewPathComponent {
    Literal { text: String },
    SecretRef { kind: String, digest: String },
    Omitted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewPath(pub Vec<ReviewPathComponent>);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RedactedText(pub String);

impl fmt::Display for RedactedText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewableAction {
    pub tool: String,
    pub operation: String,
    pub argv: Vec<ReviewToken>,
    pub cwd: ReviewPath,
    pub affected_paths: Vec<ReviewPath>,
    pub sandbox: SandboxSummary,
    pub requested_permissions: Vec<Permission>,
    pub justification: Option<RedactedText>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewProjection {
    Reviewable(ReviewableAction),
    InsufficientEvidence { reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRef {
    pub kind: String,
    pub digest: String,
}

/// Keyed digest for secret identity comparison without value reconstruction.
pub struct SecretDigestKey([u8; 32]);

impl SecretDigestKey {
    pub fn new(key: [u8; 32]) -> Self {
        Self(key)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[cfg(test)]
    pub const fn fixture() -> Self {
        Self([0u8; 32])
    }
}

impl Zeroize for SecretDigestKey {
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}

impl Drop for SecretDigestKey {
    fn drop(&mut self) {
        self.zeroize();
    }
}

/// Redactor plus credential inventory that turns a `CanonicalAction` into a
/// review projection where secret material is replaced by `SecretRef` tokens.
pub struct SecretAwareActionProjector {
    redactor: Redactor,
    inventory: SecretInventory,
    key: SecretDigestKey,
}

fn credential_options_for_command(cmd: &str) -> Option<&'static [(&'static str, &'static str)]> {
    const CREDENTIAL_FAMILIES: &[&str] = &[
        "curl",
        "wget",
        "lftp",
        "sshpass",
        "mysql",
        "mariadb",
        "mongosh",
        "cqlsh",
        "sqlcmd",
        "redis-cli",
    ];
    let family = shell::canonicalize_command_name(cmd, CREDENTIAL_FAMILIES)?;
    match family {
        "curl" => Some(&[
            ("-u", "curl_user"),
            ("--user", "curl_user"),
            ("--oauth2-bearer", "bearer_token"),
            ("--proxy-user", "curl_user"),
            ("--proxy-password", "curl_password"),
        ]),
        "wget" => Some(&[
            ("--password", "wget_password"),
            ("--user", "wget_user"),
            ("--http-password", "wget_password"),
            ("--http-user", "wget_user"),
            ("--ftp-password", "wget_password"),
            ("--ftp-user", "wget_user"),
            ("--proxy-user", "wget_user"),
            ("--proxy-password", "wget_password"),
        ]),
        "lftp" => Some(&[("-u", "lftp_user")]),
        "sshpass" => Some(&[("-p", "sshpass_password")]),
        "mysql" | "mariadb" => Some(&[("-p", "mysql_password"), ("--password", "mysql_password")]),
        "mongosh" => Some(&[
            ("-p", "mongosh_password"),
            ("--password", "mongosh_password"),
        ]),
        "cqlsh" => Some(&[("-p", "cqlsh_password"), ("--password", "cqlsh_password")]),
        "sqlcmd" => Some(&[("-P", "sqlcmd_password"), ("--password", "sqlcmd_password")]),
        "redis-cli" => Some(&[
            ("-a", "redis_auth"),
            ("--pass", "redis_auth"),
            ("--password", "redis_auth"),
        ]),
        _ => None,
    }
}

fn shell_credential_spans(command: &str) -> Vec<Vec<(usize, usize, &'static str)>> {
    let token_spans = shell::tokenize_command_spans(command);
    let mut spans: Vec<Vec<(usize, usize, &'static str)>> = vec![Vec::new(); token_spans.len()];

    fn scan_region(
        command: &str,
        region_start: usize,
        region_end: usize,
        token_spans: &[(usize, usize, String)],
        spans: &mut [Vec<(usize, usize, &'static str)>],
        depth: usize,
        invalid: &mut bool,
    ) {
        // A malformed or adversarially deeply nested command is handled by the
        // caller's fail-closed dynamic-shell check. Do not recurse indefinitely
        // while computing redaction metadata.
        if depth > 32 {
            *invalid = true;
            return;
        }
        if region_start >= region_end {
            return;
        }
        let region = &command[region_start..region_end];
        for segment in shell::segment_command(region) {
            let segment_tokens = shell::tokenize_command(&segment.raw);
            let segment_spans = shell::tokenize_command_spans(&segment.raw);

            // Map each token in this segment to the global token index that
            // contains it, and retain the byte offset inside that token. This
            // also handles separators or grouping characters glued to words.
            let local_to_global: Vec<Option<(usize, usize)>> = segment_spans
                .iter()
                .map(|(local_start, local_end, local_text)| {
                    let token_start = region_start + segment.raw_start + local_start;
                    let token_end = region_start + segment.raw_start + local_end;
                    token_spans.iter().enumerate().find_map(
                        |(global_idx, (global_start, global_end, global_text))| {
                            if *global_start <= token_start
                                && *global_end >= token_end
                                && token_end >= token_start
                            {
                                // Both tokenizers return normalized token text
                                // (quotes/backslashes removed), so derive the
                                // offset from the text rather than raw bytes.
                                // Raw offsets differ when a nested token is
                                // inside quoted or command-substitution syntax.
                                let token_offset = if local_text.is_empty() {
                                    0
                                } else {
                                    global_text.find(local_text)?
                                };
                                (token_offset + local_text.len() <= global_text.len())
                                    .then_some((global_idx, token_offset))
                            } else {
                                None
                            }
                        },
                    )
                })
                .collect();

            let Some(eff) = shell::effective_command(&segment_tokens, 1) else {
                continue;
            };
            let cmd = shell::command_basename(eff.tokens.first().map(String::as_str).unwrap_or(""))
                .to_ascii_lowercase();
            let Some(options) = credential_options_for_command(&cmd) else {
                continue;
            };

            let mut i = eff.index + 1;
            while i < segment_tokens.len() {
                let token = &segment_tokens[i];
                let mut matched = None::<(usize, usize, usize, &'static str, usize)>;
                for (prefix, kind) in options {
                    if prefix.starts_with("--") {
                        let lower = token.to_ascii_lowercase();
                        if lower == *prefix {
                            if i + 1 < segment_tokens.len()
                                && !segment_tokens[i + 1].starts_with('-')
                            {
                                matched = Some((i + 1, 0, segment_tokens[i + 1].len(), *kind, 2));
                            }
                            break;
                        }
                        let eq = format!("{}=", prefix);
                        if lower.starts_with(&eq) {
                            let start = prefix.len() + 1;
                            if start < token.len() {
                                matched = Some((i, start, token.len(), *kind, 1));
                            }
                            break;
                        }
                    } else if token.starts_with(*prefix) {
                        let prefix_len = prefix.len();
                        if token.len() > prefix_len {
                            matched = Some((i, prefix_len, token.len(), *kind, 1));
                        } else if i + 1 < segment_tokens.len()
                            && !segment_tokens[i + 1].starts_with('-')
                        {
                            matched = Some((i + 1, 0, segment_tokens[i + 1].len(), *kind, 2));
                        }
                        break;
                    }
                }
                if let Some((idx, start, end, kind, consumed)) = matched {
                    let mut emitted = false;
                    if start <= end
                        && end <= segment_tokens[idx].len()
                        && let Some((global_idx, token_offset)) =
                            local_to_global.get(idx).copied().flatten()
                        && token_offset <= token_spans[global_idx].2.len()
                        && end.saturating_add(token_offset) <= token_spans[global_idx].2.len()
                    {
                        spans[global_idx].push((start + token_offset, end + token_offset, kind));
                        emitted = true;
                    }
                    if !emitted {
                        *invalid = true;
                    }
                    i += consumed;
                } else {
                    i += 1;
                }
            }
        }

        for (nested_start, nested_end) in shell::nested_shell_regions(region) {
            scan_region(
                command,
                region_start + nested_start,
                region_start + nested_end,
                token_spans,
                spans,
                depth + 1,
                invalid,
            );
        }
    }

    let mut invalid = false;
    scan_region(
        command,
        0,
        command.len(),
        &token_spans,
        &mut spans,
        0,
        &mut invalid,
    );
    if invalid && !spans.is_empty() {
        // Preserve the existing flat return type while making an impossible
        // mapping observable to every consumer. Each consumer validates
        // intervals before slicing and therefore fails closed on this marker.
        spans[0].push((usize::MAX, usize::MAX, "shell_unverifiable"));
    }
    spans
}

fn contains_shell_credential(text: &str) -> bool {
    shell_credential_spans(text)
        .iter()
        .any(|token_spans| !token_spans.is_empty())
}

impl SecretAwareActionProjector {
    pub fn new(redactor: Redactor, key: SecretDigestKey) -> Self {
        Self {
            redactor,
            inventory: SecretInventory::new(),
            key,
        }
    }

    /// Redact a raw tool-argument summary for the wire-visible `args_summary`
    /// field. First applies the injected versioned `Redactor` (which preserves
    /// durable-projection semantics and structured-key redaction), then the
    /// projector's own `SecretInventory` to catch URL userinfo and generic query
    /// secrets that the Store redactor does not yet cover.
    pub fn redact_arguments(&self, args: &ValidatedToolArguments) -> anyhow::Result<JsonValue> {
        let value = JsonValue::Object(args.as_object().clone());
        let redacted = self.redactor.redact_value(&value)?;
        self.redact_json_with_inventory(&redacted)
    }

    fn redact_json_with_inventory(&self, value: &JsonValue) -> anyhow::Result<JsonValue> {
        match value {
            JsonValue::String(text) => Ok(JsonValue::String(self.redact_text_with_inventory(text))),
            JsonValue::Array(values) => Ok(JsonValue::Array(
                values
                    .iter()
                    .map(|v| self.redact_json_with_inventory(v))
                    .collect::<anyhow::Result<Vec<_>>>()?,
            )),
            JsonValue::Object(object) => {
                let mut redacted = serde_json::Map::with_capacity(object.len());
                for (key, value) in object {
                    let value = if key == "command" {
                        match value {
                            JsonValue::String(text) => {
                                JsonValue::String(self.redact_bash_command_text(text))
                            }
                            _ => self.redact_json_with_inventory(value)?,
                        }
                    } else {
                        self.redact_json_with_inventory(value)?
                    };
                    if redacted.insert(key.clone(), value).is_some() {
                        return Err(anyhow::anyhow!(
                            "JSON object keys collide after inventory redaction"
                        ));
                    }
                }
                Ok(JsonValue::Object(redacted))
            }
            scalar => Ok(scalar.clone()),
        }
    }

    fn redact_text_with_inventory(&self, text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut cursor = 0usize;
        for m in self.inventory.find(text) {
            out.push_str(&text[cursor..m.start]);
            let secret = &text[m.start..m.end];
            if !looks_like_redacted_placeholder(secret) {
                out.push_str("[REDACTED:");
                out.push_str(m.kind);
                out.push(']');
            } else {
                out.push_str(secret);
            }
            cursor = m.end;
        }
        out.push_str(&text[cursor..]);
        out
    }

    /// Redact `text` by running the SecretInventory over a normalized view
    /// (e.g. shell tokens with quotes and escapes removed) and mapping each
    /// match back to original byte positions. This preserves surrounding
    /// punctuation such as closing quotes that a raw-text regex would otherwise
    /// consume as part of a secret.
    fn redact_text_with_mapping(&self, text: &str, normalized: &str, mapping: &[usize]) -> String {
        let mut out = String::with_capacity(text.len());
        let mut cursor = 0usize;
        for m in self.inventory.find(normalized) {
            if m.start >= mapping.len() || m.end >= mapping.len() {
                return "[REDACTED:shell_unverifiable]".to_owned();
            }
            let secret_start = mapping[m.start];
            let secret_end = mapping[m.end];
            if secret_start < cursor || secret_end < secret_start {
                return "[REDACTED:shell_unverifiable]".to_owned();
            }
            out.push_str(&text[cursor..secret_start]);
            let secret = &text[secret_start..secret_end];
            if !looks_like_redacted_placeholder(secret) {
                out.push_str("[REDACTED:");
                out.push_str(m.kind);
                out.push(']');
            } else {
                out.push_str(secret);
            }
            cursor = secret_end;
        }
        out.push_str(&text[cursor..]);
        out
    }

    fn shell_token_normalized(span: &str) -> (String, Vec<usize>) {
        let mut out = String::with_capacity(span.len());
        let mut mapping = Vec::with_capacity(span.len() + 1);
        let mut in_single = false;
        let mut in_double = false;
        let mut escaped = false;
        let mut orig_pos = 0usize;
        let mut end_after_last = 0usize;

        while orig_pos < span.len() {
            if escaped {
                let c = span[orig_pos..].chars().next().unwrap();
                let start = orig_pos - 1;
                out.push(c);
                for _ in 0..c.len_utf8() {
                    mapping.push(start);
                }
                end_after_last = orig_pos + c.len_utf8();
                orig_pos += c.len_utf8();
                escaped = false;
                continue;
            }
            let c = span[orig_pos..].chars().next().unwrap();
            if c == '\\' && !in_single {
                escaped = true;
                orig_pos += c.len_utf8();
                continue;
            }
            if !in_double && c == '\'' {
                orig_pos += c.len_utf8();
                in_single = !in_single;
                continue;
            }
            if !in_single && c == '"' {
                orig_pos += c.len_utf8();
                in_double = !in_double;
                continue;
            }
            out.push(c);
            for _ in 0..c.len_utf8() {
                mapping.push(orig_pos);
            }
            end_after_last = orig_pos + c.len_utf8();
            orig_pos += c.len_utf8();
        }
        mapping.push(if out.is_empty() {
            span.len()
        } else {
            end_after_last
        });
        (out, mapping)
    }

    fn redact_bash_command_text(&self, command: &str) -> String {
        // Nested shell regions are not represented faithfully by the flat
        // review-token projection. Redact the entire command rather than
        // risking plaintext credentials from an inner command substitution or
        // grouped command reaching the wire-visible summary.
        if shell::has_nested_shell_construct(command) {
            return "[REDACTED:shell_unverifiable]".to_owned();
        }
        // Apply whole-command SecretInventory coverage over a normalized view
        // (quotes and escapes removed) so that secrets that span multiple shell
        // tokens (e.g. unquoted `Authorization: Basic <token>`) are redacted
        // before per-token processing. Mapping matches back to original byte
        // positions preserves surrounding punctuation such as closing quotes.
        fn append_piece(
            normalized: &mut String,
            mapping: &mut Vec<usize>,
            piece: &str,
            piece_map: &[usize],
        ) {
            let base = normalized.len();
            assert_eq!(mapping.len(), base + 1);
            normalized.push_str(piece);
            mapping.pop();
            mapping.extend_from_slice(piece_map);
        }

        let mut normalized = String::with_capacity(command.len());
        let mut mapping: Vec<usize> = vec![0];
        let mut prev_end = 0usize;
        let spans = shell::tokenize_command_spans(command);
        for (start, end, _) in &spans {
            let (start, end) = (*start, *end);
            let span = &command[start..end];
            let (span_norm, span_map) = Self::shell_token_normalized(span);
            if span_norm.is_empty() {
                prev_end = end;
                continue;
            }
            if !normalized.is_empty() {
                append_piece(&mut normalized, &mut mapping, " ", &[prev_end, start]);
            }
            let mut piece_map = Vec::with_capacity(span_map.len());
            for &pos in &span_map {
                piece_map.push(start + pos);
            }
            append_piece(&mut normalized, &mut mapping, &span_norm, &piece_map);
            prev_end = end;
        }
        let command = self.redact_text_with_mapping(command, &normalized, &mapping);
        let credential_spans = shell_credential_spans(&command);
        let mut out = String::new();
        let mut cursor = 0usize;
        for (idx, (start, end, _)) in shell::tokenize_command_spans(&command)
            .into_iter()
            .enumerate()
        {
            out.push_str(&command[cursor..start]);
            let span = &command[start..end];
            let (normalized, mapping) = Self::shell_token_normalized(span);
            if mapping.len() != normalized.len() + 1 {
                return "[REDACTED:shell_unverifiable]".to_owned();
            }
            let cred = &credential_spans[idx];
            let mut intervals: Vec<(usize, usize, &'static str)> = self
                .inventory
                .find(&normalized)
                .into_iter()
                .filter(|m| !cred.iter().any(|(s, e, _)| m.start < *e && m.end > *s))
                .map(|m| (m.start, m.end, m.kind))
                .collect();
            for (cs, ce, kind) in cred.iter().copied() {
                intervals.push((cs, ce, kind));
            }
            if intervals
                .iter()
                .any(|(s, e, _)| *s > *e || *e > normalized.len() || *e >= mapping.len())
            {
                return "[REDACTED:shell_unverifiable]".to_owned();
            }
            intervals.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
            if intervals.is_empty() {
                out.push_str(span);
                cursor = end;
                continue;
            }
            let mut orig_cursor = start;
            let mut last_end = 0usize;
            for (s, e, kind) in intervals {
                if s >= last_end {
                    let secret_start = start + mapping[s];
                    let secret_end = start + mapping[e];
                    if secret_start > secret_end
                        || secret_start < start
                        || secret_end > end
                        || !command.is_char_boundary(secret_start)
                        || !command.is_char_boundary(secret_end)
                    {
                        return "[REDACTED:shell_unverifiable]".to_owned();
                    }
                    out.push_str(&command[orig_cursor..secret_start]);
                    out.push_str("[REDACTED:");
                    out.push_str(kind);
                    out.push(']');
                    orig_cursor = secret_end;
                    last_end = e;
                }
            }
            out.push_str(&command[orig_cursor..end]);
            cursor = end;
        }
        out.push_str(&command[cursor..]);
        out
    }

    /// Project a `CanonicalAction` into a reviewable form. If redaction removes
    /// information needed to judge host, operation, path, or permission scope,
    /// returns `InsufficientEvidence` instead of guessing.
    pub fn project(&self, action: &CanonicalAction) -> ReviewProjection {
        if action.validate().is_err() {
            return ReviewProjection::InsufficientEvidence {
                reason: "canonical action failed invariant validation".to_owned(),
            };
        }

        let mut token_projections: Vec<TokenProjection> = Vec::new();
        if action.tool == BASH_TOOL_NAME && action.operation == "exec" {
            let command = action.argv.first().map(String::as_str).unwrap_or("");
            if shell::has_unverifiable_construct(command) {
                return ReviewProjection::InsufficientEvidence {
                    reason: "shell construct is not representable as a literal review token"
                        .to_owned(),
                };
            }
            let credential_spans = shell_credential_spans(command);
            for (idx, (start, end, text)) in shell::tokenize_command_spans(command)
                .into_iter()
                .enumerate()
            {
                let span = &command[start..end];
                if shell::has_unverifiable_construct(span) {
                    token_projections.push(TokenProjection {
                        tokens: vec![ReviewToken::Omitted],
                        has_visible_host: false,
                        has_url: false,
                    });
                } else {
                    token_projections.push(self.project_bash_token(&text, &credential_spans[idx]));
                }
            }
        } else {
            for raw in &action.argv {
                token_projections.push(self.project_token(raw));
            }
        }

        let argv: Vec<ReviewToken> = token_projections
            .iter()
            .flat_map(|tp| tp.tokens.clone())
            .filter(|t| !t.is_empty_literal())
            .collect();

        if argv.is_empty() {
            return ReviewProjection::InsufficientEvidence {
                reason: "argv is empty or omitted".to_owned(),
            };
        }

        if token_projections
            .iter()
            .any(|tp| tp.tokens.iter().any(|t| matches!(t, ReviewToken::Omitted)))
        {
            return ReviewProjection::InsufficientEvidence {
                reason: "argv contains dynamic or omitted material".to_owned(),
            };
        }

        if !self.all_argv_secrets_projected(action, &token_projections) {
            return ReviewProjection::InsufficientEvidence {
                reason: "argv secret could not be projected without loss".to_owned(),
            };
        }

        if (action.requested_permissions.contains(&Permission::Network)
            || action
                .requested_permissions
                .contains(&Permission::DomainMutation))
            && (!token_projections.iter().any(|tp| tp.has_visible_host)
                || token_projections
                    .iter()
                    .any(|tp| tp.has_url && !tp.has_visible_host))
        {
            return ReviewProjection::InsufficientEvidence {
                reason: "network destination is redacted, dynamic, or missing".to_owned(),
            };
        }

        let cwd = self.project_path(&action.cwd);
        let affected_paths: Vec<ReviewPath> = action
            .affected_paths
            .iter()
            .map(|p| self.project_path(p))
            .collect();

        if has_lost_material_path_component(&cwd) {
            return ReviewProjection::InsufficientEvidence {
                reason: "cwd component is redacted or omitted".to_owned(),
            };
        }

        for path in &affected_paths {
            if has_lost_material_path_component(path) {
                return ReviewProjection::InsufficientEvidence {
                    reason: "affected path component is redacted or omitted".to_owned(),
                };
            }
        }

        let justification = action
            .justification
            .as_ref()
            .map(|text| RedactedText(self.render_redacted_text(text)));

        ReviewProjection::Reviewable(ReviewableAction {
            tool: self.render_redacted_text(&action.tool),
            operation: self.render_redacted_text(&action.operation),
            argv,
            cwd,
            affected_paths,
            sandbox: action.sandbox.clone(),
            requested_permissions: action.requested_permissions.clone(),
            justification,
        })
    }

    /// True if `text` contains a known secret pattern, regardless of any dynamic
    /// constructs around it. Used to fail-closed on `ApproveAlways` candidate
    /// rules that would otherwise persist credentials.
    pub(crate) fn text_contains_secret(&self, text: &str) -> bool {
        !self.inventory.find(text).is_empty() || contains_shell_credential(text)
    }

    fn all_argv_secrets_projected(
        &self,
        action: &CanonicalAction,
        token_projections: &[TokenProjection],
    ) -> bool {
        let projected_digests: Vec<&str> = token_projections
            .iter()
            .flat_map(|projection| projection.tokens.iter())
            .filter_map(|token| match token {
                ReviewToken::SecretRef { digest, .. } => Some(digest.as_str()),
                _ => None,
            })
            .collect();

        let check_text = |text: &str| {
            self.inventory.find(text).into_iter().all(|secret| {
                let normalized = secret.secret.trim_matches(['\'', '"']);
                let digest = keyed_digest(&self.key, normalized);
                projected_digests.contains(&digest.as_str())
            })
        };
        if action.tool == BASH_TOOL_NAME && action.operation == "exec" {
            if let Some(command) = action.argv.first() {
                if !check_text(command) {
                    return false;
                }
                let tokens: Vec<String> = shell::tokenize_command_spans(command)
                    .into_iter()
                    .map(|(_, _, token)| token)
                    .collect();
                for (idx, spans) in shell_credential_spans(command).iter().enumerate() {
                    let Some(token) = tokens.get(idx) else {
                        continue;
                    };
                    for (start, end, _kind) in spans {
                        if *start > *end
                            || *end > token.len()
                            || !token.is_char_boundary(*start)
                            || !token.is_char_boundary(*end)
                        {
                            return false;
                        }
                        let secret = token[*start..*end].trim_matches(['\'', '"']);
                        let digest = keyed_digest(&self.key, secret);
                        if !projected_digests.contains(&digest.as_str()) {
                            return false;
                        }
                    }
                }
            }
            true
        } else {
            action.argv.iter().all(|token| check_text(token))
        }
    }

    fn project_token(&self, text: &str) -> TokenProjection {
        let dynamic = find_dynamic_spans(text);
        let secrets = self.inventory.find(text);
        self.emit_tokens_with_host_check(text, &dynamic, &secrets)
    }

    fn project_bash_token(
        &self,
        text: &str,
        credential_spans: &[(usize, usize, &'static str)],
    ) -> TokenProjection {
        if credential_spans.iter().any(|(start, end, _)| {
            *start > *end
                || *end > text.len()
                || !text.is_char_boundary(*start)
                || !text.is_char_boundary(*end)
        }) {
            return TokenProjection {
                tokens: vec![ReviewToken::Omitted],
                has_visible_host: false,
                has_url: false,
            };
        }
        let mut secrets: Vec<SecretMatch> = self
            .inventory
            .find(text)
            .into_iter()
            .filter(|m| {
                !credential_spans
                    .iter()
                    .any(|(s, e, _)| m.start < *e && m.end > *s)
            })
            .collect();
        for (start, end, kind) in credential_spans.iter().copied() {
            secrets.push(SecretMatch {
                start,
                end,
                kind,
                secret: &text[start..end],
                order: 0,
            });
        }
        secrets.sort_by(|a, b| a.start.cmp(&b.start).then(b.end.cmp(&a.end)));
        self.emit_tokens_with_host_check(text, &[], &secrets)
    }

    fn project_literal_token(&self, text: &str) -> TokenProjection {
        let secrets = self.inventory.find(text);
        self.emit_tokens_with_host_check(text, &[], &secrets)
    }

    fn emit_tokens_with_host_check(
        &self,
        text: &str,
        dynamic: &[(usize, usize)],
        secrets: &[SecretMatch<'_>],
    ) -> TokenProjection {
        let intervals = merge_secret_and_dynamic(text, dynamic, secrets);
        if intervals.iter().any(|iv| {
            iv.start > iv.end
                || iv.end > text.len()
                || !text.is_char_boundary(iv.start)
                || !text.is_char_boundary(iv.end)
        }) {
            return TokenProjection {
                tokens: vec![ReviewToken::Omitted],
                has_visible_host: false,
                has_url: false,
            };
        }
        let mut tokens = Vec::new();
        let mut cursor = 0usize;
        for iv in &intervals {
            if iv.start > cursor {
                tokens.push(ReviewToken::Literal {
                    text: text[cursor..iv.start].to_owned(),
                });
            }
            match iv.kind {
                IntervalKind::Dynamic => tokens.push(ReviewToken::Omitted),
                IntervalKind::Secret { kind } => {
                    let digest = keyed_digest(&self.key, &text[iv.start..iv.end]);
                    tokens.push(ReviewToken::SecretRef {
                        kind: kind.to_owned(),
                        digest,
                    });
                }
            }
            cursor = iv.end;
        }
        if cursor < text.len() {
            tokens.push(ReviewToken::Literal {
                text: text[cursor..].to_owned(),
            });
        }

        let (has_url, all_hosts_visible) = url_host_visibility(text, &intervals);
        TokenProjection {
            tokens,
            has_visible_host: has_url && all_hosts_visible,
            has_url,
        }
    }

    fn project_path(&self, path: &Path) -> ReviewPath {
        let components: Vec<_> = path
            .components()
            .filter(|c| {
                !matches!(
                    c,
                    std::path::Component::RootDir | std::path::Component::CurDir
                )
            })
            .map(|c| match c {
                std::path::Component::Normal(s) => s.to_string_lossy().into_owned(),
                std::path::Component::ParentDir => "..".to_owned(),
                _ => String::new(),
            })
            .filter(|s| !s.is_empty())
            .collect();

        let mut projected = Vec::with_capacity(components.len());
        for comp in components {
            let tokens = self.project_token(&comp).tokens;
            projected.push(component_from_tokens(&comp, &tokens));
        }
        ReviewPath(projected)
    }

    fn render_redacted_text(&self, text: &str) -> String {
        let tp = self.project_token(text);
        let mut out = String::new();
        for token in tp.tokens {
            match token {
                ReviewToken::Literal { text } => out.push_str(&text),
                ReviewToken::SecretRef { kind, digest } => {
                    out.push_str("[REDACTED:");
                    out.push_str(&kind);
                    out.push(':');
                    out.push_str(&digest);
                    out.push(']');
                }
                ReviewToken::Omitted => out.push_str("[OMITTED]"),
            }
        }
        out
    }
}

fn component_from_tokens(original: &str, tokens: &[ReviewToken]) -> ReviewPathComponent {
    if tokens.is_empty() {
        return ReviewPathComponent::Literal {
            text: original.to_owned(),
        };
    }
    if tokens.len() == 1 {
        match &tokens[0] {
            ReviewToken::Literal { text } => {
                return ReviewPathComponent::Literal { text: text.clone() };
            }
            ReviewToken::SecretRef { kind, digest } => {
                return ReviewPathComponent::SecretRef {
                    kind: kind.clone(),
                    digest: digest.clone(),
                };
            }
            ReviewToken::Omitted => return ReviewPathComponent::Omitted,
        }
    }
    if tokens.iter().any(|t| matches!(t, ReviewToken::Omitted)) {
        ReviewPathComponent::Omitted
    } else if let Some(secret) = tokens.iter().find_map(|t| match t {
        ReviewToken::SecretRef { kind, digest } => Some((kind.clone(), digest.clone())),
        _ => None,
    }) {
        ReviewPathComponent::SecretRef {
            kind: secret.0,
            digest: secret.1,
        }
    } else {
        ReviewPathComponent::Literal {
            text: original.to_owned(),
        }
    }
}

fn has_lost_material_path_component(path: &ReviewPath) -> bool {
    path.0
        .iter()
        .any(|c| !matches!(c, ReviewPathComponent::Literal { .. }))
}

fn looks_like_redacted_placeholder(s: &str) -> bool {
    s.starts_with("[REDACTED:") && s.ends_with(']')
}

struct TokenProjection {
    tokens: Vec<ReviewToken>,
    has_visible_host: bool,
    has_url: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ActionError {
    #[error("missing required argument '{0}' for tool '{1}'")]
    MissingArgument(&'static str, String),
    #[error("tool '{0}' is not supported by the approval boundary")]
    UnknownTool(String),
    #[error("canonical action failed invariant validation")]
    InvalidAction(&'static str),
}

fn get_string(
    map: &serde_json::Map<String, JsonValue>,
    key: &'static str,
) -> Result<String, ActionError> {
    map.get(key)
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
        .ok_or(ActionError::MissingArgument(key, "(unknown)".to_owned()))
}

fn get_path_arg(
    map: &serde_json::Map<String, JsonValue>,
    key: &'static str,
) -> Result<String, ActionError> {
    let raw = get_string(map, key)?;
    if raw.starts_with("artifact://") {
        Ok(raw)
    } else {
        Ok(lexical_normalize_to_string(&raw))
    }
}

fn lexical_normalize_to_string(raw: &str) -> String {
    lexical_normalize(Path::new(raw))
        .to_string_lossy()
        .into_owned()
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut comps: Vec<OsString> = Vec::new();
    let mut absolute = false;
    for c in path.components() {
        match c {
            Component::Prefix(_) => {}
            Component::RootDir => {
                absolute = true;
                comps.clear();
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if comps
                    .last()
                    .is_some_and(|last| last.as_os_str() != OsStr::new(".."))
                {
                    comps.pop();
                } else if !absolute {
                    comps.push(OsString::from(".."));
                }
            }
            Component::Normal(s) => comps.push(s.to_os_string()),
        }
    }
    let mut out = PathBuf::new();
    if absolute {
        out.push(std::path::MAIN_SEPARATOR_STR);
    }
    for c in comps {
        out.push(c);
    }
    out
}

fn is_lexically_normalized(path: &Path) -> bool {
    lexical_normalize(path).components().eq(path.components())
}

fn network_indicators_in_command(command: &str) -> bool {
    shell::segment_command(command)
        .iter()
        .map(|segment| shell::tokenize_command(&segment.raw))
        .any(|tokens| shell::is_network_command(&tokens))
}

fn is_valid_artifact_handle(handle: &str) -> bool {
    let parts: Vec<&str> = handle.split('/').collect();
    parts.len() == 3
        && matches!(parts[1], "attachments" | "tool-output")
        && parts[0].len() <= 128
        && parts[2].len() <= 200
        && parts.iter().enumerate().all(|(index, part)| {
            let max = if index == 0 { 128 } else { 200 };
            !part.is_empty()
                && part.len() <= max
                && *part != "."
                && *part != ".."
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
}

fn keyed_digest(key: &SecretDigestKey, secret: &str) -> String {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac =
        HmacSha256::new_from_slice(key.as_bytes()).expect("32-byte key is valid for HMAC-SHA256");
    mac.update(secret.as_bytes());
    hex_lower(&mac.finalize().into_bytes())
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        write!(s, "{:02x}", b).expect("string write cannot fail");
    }
    s
}

#[derive(Clone, Copy)]
struct SecretMatch<'a> {
    start: usize,
    end: usize,
    kind: &'static str,
    secret: &'a str,
    order: usize,
}

struct SecretPattern {
    regex: Regex,
    secret_group: usize,
    kind: &'static str,
}

struct SecretInventory {
    patterns: Vec<SecretPattern>,
}

impl SecretInventory {
    fn new() -> Self {
        let patterns = vec![
            SecretPattern {
                regex: Regex::new(r"sk-[A-Za-z0-9_-]{12,}").expect("static API key regex"),
                secret_group: 0,
                kind: "api_key",
            },
            SecretPattern {
                regex: Regex::new(
                    r#"(?i)\b(?:authorization|proxy-authorization)\s*:\s*bearer[ \t]+([A-Za-z0-9._~+/=-]+)"#,
                )
                .expect("static bearer authorization regex"),
                secret_group: 1,
                kind: "bearer_token",
            },
            SecretPattern {
                regex: Regex::new(
                    r#"(?i)\b(?:authorization|proxy-authorization)\s*:\s*basic[ \t]+([A-Za-z0-9._~+/=-]+)"#,
                )
                .expect("static basic authorization regex"),
                secret_group: 1,
                kind: "basic_token",
            },
            SecretPattern {
                regex: Regex::new(
                    r#"(?i)\b(?:authorization|proxy-authorization)\s*:\s*([A-Za-z][A-Za-z0-9+.\-/]*)(?:[ \t]*=[ \t]*|[ \t]+)([A-Za-z0-9._~+/=-]+)"#,
                )
                .expect("static generic authorization regex"),
                secret_group: 2,
                kind: "authorization_token",
            },
            SecretPattern {
                regex: Regex::new(r#"(?i)\bBearer[ \t]+([A-Za-z0-9._~+/=-]+)"#)
                    .expect("static bearer regex"),
                secret_group: 1,
                kind: "bearer_token",
            },
            SecretPattern {
                regex: Regex::new(r#"(?i)\bBasic[ \t]+([A-Za-z0-9._~+/=-]+)"#)
                    .expect("static basic regex"),
                secret_group: 1,
                kind: "basic_token",
            },
            SecretPattern {
                regex: Regex::new(
                    r#"(?i)\b(?:[A-Z0-9_]*(?:API[_-]?KEY|ACCESS[_-]?TOKEN|AUTH[_-]?TOKEN|SECRET|PASSWORD|PASSWD|PRIVATE[_-]?KEY|TOKEN)[A-Z0-9_]*)\s*=\s*[\"']?([A-Za-z0-9._~+/=-]+)"#,
                )
                .expect("static secret environment regex"),
                secret_group: 1,
                kind: "secret_env",
            },
            SecretPattern {
                regex: Regex::new(
                    r#"(?i)(X-Amz-Signature|X-Goog-Signature|signature)=([A-Za-z0-9%._~-]{8,})"#,
                )
                .expect("static signed-url regex"),
                secret_group: 2,
                kind: "signature",
            },
            SecretPattern {
                regex: Regex::new(
                    r#"(?i)\b(api[_-]?key|access[_-]?token|secret|password|passwd|pwd|pass)[ \t]*[:=][ \t]*['\"]?([A-Za-z0-9._~+/=-]+)"#,
                )
                .expect("static named-secret regex"),
                secret_group: 2,
                kind: "secret",
            },
            SecretPattern {
                regex: Regex::new(r#"://([^/:]+):([^@\s]+)@"#)
                    .expect("static url-userinfo regex"),
                secret_group: 2,
                kind: "url_credential",
            },
            SecretPattern {
                regex: Regex::new(r#"://([^@:/\s]+)@"#)
                    .expect("static url-userinfo-token regex"),
                secret_group: 1,
                kind: "url_credential",
            },
            SecretPattern {
                regex: Regex::new(
                    r#"(?i)([?&])(?:X-Amz-Signature|X-Goog-Signature|signature)=([^&\s]+)"#,
                )
                .expect("static signed-url-query regex"),
                secret_group: 2,
                kind: "signature",
            },
            SecretPattern {
                regex: Regex::new(
                    r#"(?i)([?&])(token|api[_-]?key|access[_-]?token|secret|password|passwd|pwd|pass)=([^&\s]+)"#,
                )
                .expect("static secret-query regex"),
                secret_group: 3,
                kind: "url_query_secret",
            },
        ];
        Self { patterns }
    }
}

fn pattern_priority(index: usize) -> usize {
    match index {
        0 => 0,   // api_key
        1 => 1,   // auth_bearer
        2 => 2,   // auth_basic
        3 => 3,   // auth_generic
        4 => 4,   // standalone_bearer
        5 => 5,   // standalone_basic
        11 => 6,  // signed_url_query
        7 => 7,   // signed_url
        8 => 8,   // named_secret
        12 => 9,  // secret_query
        6 => 10,  // secret_env
        9 => 11,  // url_userinfo_password
        10 => 12, // url_userinfo_token
        _ => index,
    }
}

impl SecretInventory {
    fn find<'a>(&self, text: &'a str) -> Vec<SecretMatch<'a>> {
        let mut matches = Vec::new();
        for (index, pattern) in self.patterns.iter().enumerate() {
            for caps in pattern.regex.captures_iter(text) {
                if let Some(m) = caps.get(pattern.secret_group) {
                    matches.push(SecretMatch {
                        start: m.start(),
                        end: m.end(),
                        kind: pattern.kind,
                        secret: m.as_str(),
                        order: pattern_priority(index),
                    });
                }
            }
        }
        matches.sort_by(|a, b| {
            a.start
                .cmp(&b.start)
                .then_with(|| a.order.cmp(&b.order))
                .then_with(|| b.end.cmp(&a.end))
        });
        let mut selected: Vec<SecretMatch<'a>> = Vec::new();
        let mut last_end = 0usize;
        for m in matches {
            if m.start >= last_end {
                selected.push(m);
                last_end = m.end;
            }
        }
        selected
    }
}

fn secret_inventory() -> &'static SecretInventory {
    static INVENTORY: std::sync::OnceLock<SecretInventory> = std::sync::OnceLock::new();
    INVENTORY.get_or_init(SecretInventory::new)
}

/// Check persisted metadata without retaining or returning the matching
/// material. Policy rule diagnostics deliberately expose only the typed error,
/// never the matched value.
pub(crate) fn text_contains_secret_material(text: &str) -> bool {
    !secret_inventory().find(text).is_empty() || contains_shell_credential(text)
}

#[derive(Clone, Copy)]
enum IntervalKind {
    Dynamic,
    Secret { kind: &'static str },
}

struct Interval {
    start: usize,
    end: usize,
    kind: IntervalKind,
}

fn merge_secret_and_dynamic(
    _text: &str,
    dynamic: &[(usize, usize)],
    secrets: &[SecretMatch<'_>],
) -> Vec<Interval> {
    let mut intervals: Vec<Interval> = dynamic
        .iter()
        .map(|&(s, e)| Interval {
            start: s,
            end: e,
            kind: IntervalKind::Dynamic,
        })
        .chain(secrets.iter().map(|m| Interval {
            start: m.start,
            end: m.end,
            kind: IntervalKind::Secret { kind: m.kind },
        }))
        .collect();

    intervals.sort_by(|a, b| {
        a.start
            .cmp(&b.start)
            .then_with(|| b.end.cmp(&a.end))
            .then_with(|| {
                matches!(a.kind, IntervalKind::Dynamic)
                    .cmp(&matches!(b.kind, IntervalKind::Dynamic))
            })
    });

    let mut merged: Vec<Interval> = Vec::new();
    for iv in intervals {
        if let Some(last) = merged.last_mut()
            && iv.start <= last.end
        {
            last.end = last.end.max(iv.end);
            if matches!(iv.kind, IntervalKind::Dynamic)
                || matches!(last.kind, IntervalKind::Dynamic)
            {
                last.kind = IntervalKind::Dynamic;
            }
            // When two secrets overlap, keep the first (longer/earliest) kind.
            continue;
        }
        merged.push(iv);
    }
    merged
}

pub(crate) fn find_dynamic_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;

    while i < bytes.len() {
        let b = bytes[i];
        if escaped {
            escaped = false;
            i += 1;
            continue;
        }
        if b == b'\\' && !in_single {
            escaped = true;
            i += 1;
            continue;
        }
        if !in_double && b == b'\'' {
            in_single = !in_single;
            i += 1;
            continue;
        }
        if !in_single && b == b'"' {
            in_double = !in_double;
            i += 1;
            continue;
        }
        if in_single || in_double {
            i += 1;
            continue;
        }

        if b == b'$' && i + 1 < bytes.len() {
            match bytes[i + 1] {
                b'(' => {
                    if let Some(end) = consume_delim(bytes, i + 1, b'(', b')') {
                        spans.push((i, end + 1));
                        i = end + 1;
                        continue;
                    }
                }
                b'{' => {
                    if let Some(end) = consume_delim(bytes, i + 1, b'{', b'}') {
                        spans.push((i, end + 1));
                        i = end + 1;
                        continue;
                    }
                }
                _ => {
                    if let Some(end) = consume_var_name(bytes, i) {
                        spans.push((i, end));
                        i = end;
                        continue;
                    }
                }
            }
        }

        if b == b'`'
            && let Some(end) = consume_backtick(bytes, i)
        {
            spans.push((i, end + 1));
            i = end + 1;
            continue;
        }

        if b == b'<' && i + 1 < bytes.len() && bytes[i + 1] == b'<' {
            // Heredoc: treat the remainder of this token as dynamic.
            spans.push((i, text.len()));
            break;
        }

        i += 1;
    }
    spans
}

fn consume_delim(bytes: &[u8], open_idx: usize, open: u8, close: u8) -> Option<usize> {
    if bytes[open_idx] != open {
        return None;
    }
    let mut depth = 1usize;
    let mut i = open_idx + 1;
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;
    while i < bytes.len() {
        let b = bytes[i];
        if escaped {
            escaped = false;
            i += 1;
            continue;
        }
        if b == b'\\' && !in_single {
            escaped = true;
            i += 1;
            continue;
        }
        if !in_double && b == b'\'' {
            in_single = !in_single;
            i += 1;
            continue;
        }
        if !in_single && b == b'"' {
            in_double = !in_double;
            i += 1;
            continue;
        }
        if !in_single && !in_double {
            if b == open {
                depth += 1;
            } else if b == close {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
        }
        i += 1;
    }
    None
}

fn consume_var_name(bytes: &[u8], dollar_idx: usize) -> Option<usize> {
    if bytes.get(dollar_idx) != Some(&b'$') {
        return None;
    }
    let mut i = dollar_idx + 1;
    if i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphabetic()) {
        i += 1;
        while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
            i += 1;
        }
        return Some(i);
    }
    None
}

fn consume_backtick(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes[start] != b'`' {
        return None;
    }
    let mut i = start + 1;
    let mut escaped = false;
    while i < bytes.len() {
        if escaped {
            escaped = false;
            i += 1;
            continue;
        }
        if bytes[i] == b'\\' {
            escaped = true;
            i += 1;
            continue;
        }
        if bytes[i] == b'`' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn url_host_visibility(text: &str, intervals: &[Interval]) -> (bool, bool) {
    static HOST_RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = HOST_RE.get_or_init(|| {
        Regex::new(r"(?i)([a-z][a-z0-9+.-]*)://(?:[^/?#@]+@)?([^/?#:]+)")
            .expect("static host regex")
    });
    let mut has_url = false;
    let mut all_visible = true;
    for caps in re.captures_iter(text) {
        has_url = true;
        let host = caps.get(2).expect("host capture");
        let mut host_range = host.start()..host.end();
        if !host_range.all(|pos| !intervals.iter().any(|iv| iv.start <= pos && pos < iv.end)) {
            all_visible = false;
        }
    }
    (has_url, all_visible)
}

pub(crate) mod shell {
    use super::*;

    #[derive(Debug, Clone)]
    pub(crate) struct Segment {
        pub raw: String,
        /// Byte offset of `raw` in the original command string.
        pub raw_start: usize,
        pub is_subshell: bool,
    }

    impl Segment {
        pub fn is_dynamic(&self) -> bool {
            self.is_subshell || has_unverifiable_construct(&self.raw)
        }
    }

    /// Fail-closed classifier for shell constructs that a literal-prefix policy
    /// cannot prove. Returns `true` for unquoted/unescaped redirection operators
    /// (`>`, `<`), pathname/brace expansion, unmodelled grouping, tilde expansion
    /// at the start of a word, special shell parameters (`$$`, `$?`, `$!`,
    /// `$0`..`$9`, `$@`, `$*`, `$#`, `$-`), command substitution, parameter
    /// expansion, and backticks.
    pub(crate) fn has_unverifiable_construct(raw: &str) -> bool {
        let bytes = raw.as_bytes();
        let mut i = 0usize;
        let mut in_single = false;
        let mut in_double = false;
        let mut escaped = false;
        let mut word_start = true; // segment raw is trimmed

        while i < bytes.len() {
            let b = bytes[i];
            if escaped {
                escaped = false;
                i += 1;
                continue;
            }
            if b == b'\\' && !in_single {
                escaped = true;
                i += 1;
                continue;
            }
            if !in_double && b == b'\'' {
                in_single = !in_single;
                i += 1;
                continue;
            }
            if !in_single && b == b'"' {
                in_double = !in_double;
                i += 1;
                continue;
            }
            if in_single {
                i += 1;
                continue;
            }

            if b == b'$' && i + 1 < bytes.len() && is_dollar_special(bytes[i + 1], in_double) {
                return true;
            }
            if b == b'`' {
                return true;
            }
            if !in_double && matches!(b, b'>' | b'<') {
                return true;
            }
            if !in_double && matches!(b, b'*' | b'?' | b'[' | b']') {
                return true;
            }
            if !in_double && matches!(b, b'{' | b'}' | b'(' | b')') {
                return true;
            }
            if !in_double && b == b'~' && word_start {
                return true;
            }

            word_start = b.is_ascii_whitespace()
                || matches!(b, b'=' | b':' | b';' | b'(' | b')' | b'|' | b'&');

            i += 1;
        }
        escaped || in_single || in_double
    }

    fn is_dollar_special(b: u8, in_double: bool) -> bool {
        if matches!(b, b'(' | b'{') {
            return true;
        }
        if b == b'_' || b.is_ascii_alphabetic() || b.is_ascii_digit() {
            return true;
        }
        if matches!(b, b'*' | b'@' | b'#' | b'?' | b'-' | b'$' | b'!') {
            return true;
        }
        // $'...' and $"...' quoting expansions occur only outside quotes.
        !in_double && (b == b'\'' || b == b'"')
    }

    pub(crate) fn segment_command(command: &str) -> Vec<Segment> {
        let mut segments = Vec::new();
        let bytes = command.as_bytes();
        let mut start = 0usize;
        let mut i = 0usize;
        let mut in_single = false;
        let mut in_double = false;
        let mut escaped = false;
        let mut is_subshell = false;

        while i < bytes.len() {
            let b = bytes[i];
            if escaped {
                escaped = false;
                i += 1;
                continue;
            }
            if b == b'\\' && !in_single {
                escaped = true;
                i += 1;
                continue;
            }
            if !in_double && b == b'\'' {
                in_single = !in_single;
                i += 1;
                continue;
            }
            if !in_single && b == b'"' {
                in_double = !in_double;
                i += 1;
                continue;
            }

            if !in_single && !in_double {
                if b == b'$'
                    && i + 1 < bytes.len()
                    && bytes[i + 1] == b'('
                    && let Some(end) = consume_delim(bytes, i + 1, b'(', b')')
                {
                    i = end + 1;
                    continue;
                }
                if b == b'$'
                    && i + 1 < bytes.len()
                    && bytes[i + 1] == b'{'
                    && let Some(end) = consume_delim(bytes, i + 1, b'{', b'}')
                {
                    i = end + 1;
                    continue;
                }
                if b == b'`'
                    && let Some(end) = consume_backtick(bytes, i)
                {
                    i = end + 1;
                    continue;
                }
                if b == b'(' {
                    is_subshell = true;
                    if let Some(end) = consume_delim(bytes, i, b'(', b')') {
                        i = end + 1;
                        continue;
                    }
                }

                if b == b'\n' || b == b'\r' {
                    push_segment(&mut segments, command, start, i, is_subshell);
                    is_subshell = false;
                    start = i + 1;
                    i += 1;
                    continue;
                }
                if b == b';' {
                    push_segment(&mut segments, command, start, i, is_subshell);
                    is_subshell = false;
                    start = i + 1;
                    i += 1;
                    continue;
                }
                if b == b'|' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'|' {
                        push_segment(&mut segments, command, start, i, is_subshell);
                        is_subshell = false;
                        start = i + 2;
                        i += 2;
                        continue;
                    }
                    push_segment(&mut segments, command, start, i, is_subshell);
                    is_subshell = false;
                    start = i + 1;
                    i += 1;
                    continue;
                }
                if b == b'&' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'&' {
                        push_segment(&mut segments, command, start, i, is_subshell);
                        is_subshell = false;
                        start = i + 2;
                        i += 2;
                        continue;
                    }
                    push_segment(&mut segments, command, start, i, is_subshell);
                    is_subshell = false;
                    start = i + 1;
                    i += 1;
                    continue;
                }
            }
            i += 1;
        }

        if start < command.len() {
            push_segment(&mut segments, command, start, command.len(), is_subshell);
        }
        segments
    }

    /// Return the contents of nested shell regions that may contain a command
    /// of their own. The returned ranges exclude the delimiters and are byte
    /// offsets into `command`; callers can recursively tokenize each range and
    /// map any findings back to the original command.
    pub(crate) fn nested_shell_regions(command: &str) -> Vec<(usize, usize)> {
        let mut regions = Vec::new();
        let bytes = command.as_bytes();
        let mut i = 0usize;
        let mut in_single = false;
        let mut in_double = false;
        let mut escaped = false;

        while i < bytes.len() {
            let b = bytes[i];
            if escaped {
                escaped = false;
                i += 1;
                continue;
            }
            if b == b'\\' && !in_single {
                escaped = true;
                i += 1;
                continue;
            }
            if !in_double && b == b'\'' {
                in_single = !in_single;
                i += 1;
                continue;
            }
            if !in_single && b == b'"' {
                in_double = !in_double;
                i += 1;
                continue;
            }

            if !in_single {
                if b == b'$'
                    && i + 1 < bytes.len()
                    && bytes[i + 1] == b'('
                    && let Some(end) = consume_delim(bytes, i + 1, b'(', b')')
                {
                    regions.push((i + 2, end));
                    i = end + 1;
                    continue;
                }
                if b == b'$'
                    && i + 1 < bytes.len()
                    && bytes[i + 1] == b'{'
                    && let Some(end) = consume_delim(bytes, i + 1, b'{', b'}')
                {
                    regions.push((i + 2, end));
                    i = end + 1;
                    continue;
                }
                if b == b'`'
                    && let Some(end) = consume_backtick(bytes, i)
                {
                    regions.push((i + 1, end));
                    i = end + 1;
                    continue;
                }
                if !in_double
                    && b == b'('
                    && let Some(end) = consume_delim(bytes, i, b'(', b')')
                {
                    regions.push((i + 1, end));
                    i = end + 1;
                    continue;
                }
            }
            i += 1;
        }
        regions
    }

    pub(crate) fn has_nested_shell_construct(command: &str) -> bool {
        if !nested_shell_regions(command).is_empty() {
            return true;
        }
        let bytes = command.as_bytes();
        let mut i = 0usize;
        let mut in_single = false;
        let mut in_double = false;
        let mut escaped = false;
        while i < bytes.len() {
            let b = bytes[i];
            if escaped {
                escaped = false;
                i += 1;
                continue;
            }
            if b == b'\\' && !in_single {
                escaped = true;
                i += 1;
                continue;
            }
            if !in_double && b == b'\'' {
                in_single = !in_single;
                i += 1;
                continue;
            }
            if !in_single && b == b'"' {
                in_double = !in_double;
                i += 1;
                continue;
            }
            if !in_single && (!in_double && matches!(b, b'(' | b')' | b'`')) {
                return true;
            }
            if !in_single && b == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'(' {
                return true;
            }
            i += 1;
        }
        false
    }

    fn push_segment(
        segments: &mut Vec<Segment>,
        command: &str,
        start: usize,
        end: usize,
        is_subshell: bool,
    ) {
        let slice = &command[start..end];
        let trimmed = slice.trim();
        if !trimmed.is_empty() {
            let raw_start = start + (trimmed.as_ptr() as usize - slice.as_ptr() as usize);
            segments.push(Segment {
                raw: trimmed.to_owned(),
                raw_start,
                is_subshell,
            });
        }
    }

    pub(crate) fn tokenize_command(command: &str) -> Vec<String> {
        tokenize_command_spans(command)
            .into_iter()
            .map(|(_, _, token)| token)
            .collect()
    }

    pub(crate) fn tokenize_command_spans(command: &str) -> Vec<(usize, usize, String)> {
        let mut tokens = Vec::new();
        let mut current = String::new();
        let mut current_start = 0usize;
        let mut started = false;
        let mut in_single = false;
        let mut in_double = false;
        let mut escaped = false;

        for (i, c) in command.char_indices() {
            if escaped {
                current.push(c);
                escaped = false;
                continue;
            }
            if c == '\\' && !in_single {
                escaped = true;
                if !started {
                    current_start = i;
                    started = true;
                }
                continue;
            }
            if !in_double && c == '\'' {
                if !started {
                    current_start = i;
                    started = true;
                }
                in_single = !in_single;
                continue;
            }
            if !in_single && c == '"' {
                if !started {
                    current_start = i;
                    started = true;
                }
                in_double = !in_double;
                continue;
            }
            if !in_single && !in_double && c.is_whitespace() {
                if !current.is_empty() {
                    tokens.push((current_start, i, current));
                    current = String::new();
                    started = false;
                }
                continue;
            }
            if !started {
                current_start = i;
                started = true;
            }
            current.push(c);
        }
        if !current.is_empty() || started {
            tokens.push((current_start, command.len(), current));
        }
        tokens
    }

    pub(crate) fn unescape_backslashes(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        let mut chars = s.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\\' && chars.peek().is_some() {
                out.push(chars.next().unwrap());
            } else {
                out.push(c);
            }
        }
        out
    }

    pub(crate) fn command_basename(token: &str) -> String {
        Path::new(token)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(token)
            .to_ascii_lowercase()
    }

    /// Return the candidate from `candidates` that `name` belongs to as a
    /// versioned or variant executable family member (e.g. `python3.11`,
    /// `mount.nfs`, `bash-5.2`), or `None` if `name` is not a known family
    /// variant. The match is longest-prefix so `python3` stays `python3` while
    /// `python3.11` collapses to `python`.
    pub(crate) fn canonicalize_command_name<'a>(
        name: &'a str,
        candidates: &[&'a str],
    ) -> Option<&'a str> {
        let mut best: Option<&'a str> = None;
        for candidate in candidates {
            let candidate_len = candidate.len();
            if (name == *candidate
                || (name.starts_with(candidate)
                    && is_command_version_suffix(&name[candidate_len..])))
                && best.is_none_or(|b| candidate_len > b.len())
            {
                best = Some(*candidate);
            }
        }
        best
    }

    const KNOWN_VARIANT_SUFFIXES: &[&str] = &["static", "shared", "minimal", "orig", "dbg"];

    fn is_command_version_suffix(suffix: &str) -> bool {
        if suffix.is_empty() {
            return false;
        }
        match suffix.as_bytes()[0] {
            b'.' => is_dotted_extension_suffix(suffix),
            b'-' => {
                let rest = &suffix[1..];
                if KNOWN_VARIANT_SUFFIXES.contains(&rest) {
                    return true;
                }
                suffix.len() > 1
                    && suffix.as_bytes()[1].is_ascii_digit()
                    && is_numeric_version_suffix(rest)
            }
            b'0'..=b'9' => is_numeric_version_suffix(suffix),
            _ => false,
        }
    }

    fn is_known_variant_trailer(suffix: &str, i: usize) -> bool {
        suffix.as_bytes().get(i) == Some(&b'-')
            && suffix
                .get(i + 1..)
                .is_some_and(|rest| KNOWN_VARIANT_SUFFIXES.contains(&rest))
    }

    fn is_dotted_extension_suffix(suffix: &str) -> bool {
        let bytes = suffix.as_bytes();
        if bytes.is_empty() || bytes[0] != b'.' {
            return false;
        }
        let mut i = 1;
        if i >= bytes.len() || !bytes[i].is_ascii_lowercase() {
            return false;
        }
        while i < bytes.len() && bytes[i].is_ascii_lowercase() {
            i += 1;
        }
        if i == bytes.len() {
            return true;
        }
        if is_known_variant_trailer(suffix, i) {
            return true;
        }
        if bytes[i].is_ascii_digit() {
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i == bytes.len() {
                return true;
            }
            if i + 1 == bytes.len() && bytes[i].is_ascii_lowercase() {
                return true;
            }
            if is_known_variant_trailer(suffix, i) {
                return true;
            }
            return false;
        }
        if bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i == bytes.len() {
                return true;
            }
            if i + 1 == bytes.len() && bytes[i].is_ascii_lowercase() {
                return true;
            }
            if is_known_variant_trailer(suffix, i) {
                return true;
            }
            return false;
        }
        false
    }

    fn is_numeric_version_suffix(suffix: &str) -> bool {
        let bytes = suffix.as_bytes();
        if bytes.is_empty() || !bytes[0].is_ascii_digit() {
            return false;
        }
        let mut i = 0;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        loop {
            if i == bytes.len() {
                return true;
            }
            if i + 1 == bytes.len() && bytes[i].is_ascii_lowercase() {
                return true;
            }
            if is_known_variant_trailer(suffix, i) {
                return true;
            }
            if bytes[i] != b'.' {
                return false;
            }
            i += 1;
            if i >= bytes.len() || !bytes[i].is_ascii_digit() {
                return false;
            }
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
        }
    }

    pub(crate) fn is_assignment_token(token: &str) -> bool {
        let Some((name, _value)) = token.split_once('=') else {
            return false;
        };
        let name = name.strip_suffix('+').unwrap_or(name);
        if name.is_empty() {
            return false;
        }
        let bracketed = name.find('[').map(|start| {
            name.ends_with(']')
                && start > 0
                && !name[start + 1..name.len() - 1].contains(|ch: char| {
                    !(ch == '_' || ch.is_ascii_alphanumeric() || ch == '@' || ch == '*')
                })
        });
        if bracketed == Some(true) {
            return name[..name.find('[').unwrap()]
                .bytes()
                .enumerate()
                .all(|(i, b)| {
                    (i == 0 && (b == b'_' || b.is_ascii_alphabetic()))
                        || (i > 0 && (b == b'_' || b.is_ascii_alphanumeric()))
                });
        }
        name.bytes().enumerate().all(|(i, b)| {
            (i == 0 && (b == b'_' || b.is_ascii_alphabetic()))
                || (i > 0 && (b == b'_' || b.is_ascii_alphanumeric()))
        })
    }

    #[derive(Debug, Clone)]
    pub(crate) struct CommandView<'a> {
        pub tokens: &'a [String],
        /// Index of the effective command token in the slice passed to
        /// `effective_command`.
        pub index: usize,
        pub leading_assignments: usize,
        pub had_exec: bool,
        pub had_generic_wrapper: bool,
    }

    pub(crate) fn effective_command(tokens: &[String], depth: usize) -> Option<CommandView<'_>> {
        const MAX_DEPTH: usize = 5;
        if depth > MAX_DEPTH {
            return None;
        }
        let mut index = 0usize;
        let mut leading = 0usize;
        while index < tokens.len() && is_assignment_token(&tokens[index]) {
            leading += 1;
            index += 1;
        }
        if index >= tokens.len() {
            return None;
        }
        let first = command_basename(&tokens[index]);
        if first == "exec" {
            return parse_exec_wrapper(tokens, index, leading, depth);
        }
        if is_generic_wrapper(&first) {
            return parse_generic_wrapper(tokens, index, leading, depth);
        }
        Some(CommandView {
            tokens: &tokens[index..],
            index,
            leading_assignments: leading,
            had_exec: false,
            had_generic_wrapper: false,
        })
    }

    pub(crate) fn is_remote_spec(operand: &str) -> bool {
        if operand.starts_with("rsync://") {
            return true;
        }
        if let Some(colon) = operand.find(':') {
            let before = &operand[..colon];
            if before.contains('@') && !before.contains('/') {
                return true;
            }
            if colon + 1 < operand.len()
                && operand.as_bytes()[colon + 1] == b':'
                && !before.contains('/')
            {
                return true;
            }
            // Standard single-colon remote form: host:/path or host:dest.
            // Fail closed by requiring a non-empty, slash-free prefix that does
            // not look like a local relative path (.foo, .., ~) or an absolute
            // path (would contain '/').
            if colon + 1 < operand.len()
                && !before.is_empty()
                && !before.contains('/')
                && before != "."
                && before != ".."
                && !before.starts_with('.')
                && !before.starts_with('~')
            {
                return true;
            }
        }
        false
    }

    fn rsync_is_remote(tokens: &[String]) -> bool {
        let mut i = 0usize;
        while i < tokens.len() {
            let lower = tokens[i].to_ascii_lowercase();
            if lower.starts_with('-') {
                if matches!(lower.as_str(), "-e" | "--rsh") {
                    i += 2;
                    continue;
                }
                if lower.starts_with("-e=") || lower.starts_with("--rsh=") {
                    i += 1;
                    continue;
                }
                i += 1;
                continue;
            }
            if is_remote_spec(&tokens[i]) {
                return true;
            }
            i += 1;
        }
        false
    }

    pub(crate) fn is_network_command(tokens: &[String]) -> bool {
        const NETWORK_FAMILIES: &[&str] = &[
            "curl",
            "wget",
            "nc",
            "ncat",
            "ssh",
            "scp",
            "sftp",
            "lftp",
            "ftp",
            "telnet",
            "aws",
            "gcloud",
            "az",
            "gh",
            "rclone",
            "redis-cli",
            "mysql",
            "mariadb",
            "psql",
            "mongosh",
            "sqlcmd",
            "cqlsh",
            "sshpass",
            "git",
            "rsync",
            "openssl",
        ];
        let Some(eff) = effective_command(tokens, 1) else {
            return false;
        };
        let Some(first) = eff.tokens.first() else {
            return false;
        };
        let base = command_basename(first);
        let family = canonicalize_command_name(&base, NETWORK_FAMILIES).unwrap_or(&base);
        match family {
            "curl" | "wget" | "nc" | "ssh" | "scp" | "sftp" | "lftp" | "ftp" | "telnet"
            | "ncat" | "aws" | "gcloud" | "az" | "gh" | "rclone" | "redis-cli" | "mysql"
            | "mariadb" | "psql" | "mongosh" | "sqlcmd" | "cqlsh" | "sshpass" => true,
            "git" => git_is_network_operation(&eff.tokens[1..]),
            "rsync" => rsync_is_remote(&eff.tokens[1..]),
            "openssl" => {
                // Global openssl options that consume a following argument. These
                // can appear before the subcommand and would otherwise hide
                // s_client / s_server from the scanner.
                const OPENSSL_VALUE_OPTIONS: &[&str] = &[
                    "provider",
                    "provider-path",
                    "propquery",
                    "rand",
                    "writerand",
                    "config",
                    "section",
                    "cafile",
                    "capath",
                    "crlfile",
                    "cert",
                    "key",
                    "passin",
                    "passout",
                    "cipher",
                    "ciphersuites",
                    "curves",
                    "sigalgs",
                    "client_sigalgs",
                    "groups",
                    "alpn",
                    "keylogfile",
                    "unix",
                    "target",
                ];
                let mut i = 1;
                while i < eff.tokens.len() {
                    let token = &eff.tokens[i];
                    if let Some(stripped) = token.strip_prefix('-') {
                        let (name, has_glued_value) = if let Some((n, _)) = stripped.split_once('=')
                        {
                            (n, true)
                        } else {
                            (stripped, false)
                        };
                        let lower = name.to_ascii_lowercase();
                        if OPENSSL_VALUE_OPTIONS.contains(&lower.as_str()) && !has_glued_value {
                            i += 2;
                        } else {
                            i += 1;
                        }
                        continue;
                    }
                    return matches!(token.as_str(), "s_client" | "s_server");
                }
                false
            }
            _ => false,
        }
    }

    fn is_generic_wrapper(name: &str) -> bool {
        matches!(
            name,
            "nice"
                | "nohup"
                | "setsid"
                | "stdbuf"
                | "chrt"
                | "busybox"
                | "watch"
                | "flock"
                | "ionice"
                | "taskset"
                | "npx"
        )
    }

    fn parse_exec_wrapper(
        tokens: &[String],
        exec_index: usize,
        leading_before: usize,
        depth: usize,
    ) -> Option<CommandView<'_>> {
        let mut i = exec_index + 1;
        while i < tokens.len() {
            let t = &tokens[i];
            if t == "--" {
                i += 1;
                break;
            }
            if t.starts_with('-') {
                if t == "-a" && i + 1 < tokens.len() {
                    i += 2;
                    continue;
                }
                if t == "-c" || t == "-l" {
                    i += 1;
                    continue;
                }
                return None;
            }
            break;
        }
        if i >= tokens.len() {
            return None;
        }
        let inner = effective_command(&tokens[i..], depth + 1)?;
        Some(CommandView {
            tokens: inner.tokens,
            index: i + inner.index,
            leading_assignments: leading_before + inner.leading_assignments,
            had_exec: true,
            had_generic_wrapper: inner.had_generic_wrapper,
        })
    }

    fn parse_generic_wrapper(
        tokens: &[String],
        wrapper_index: usize,
        leading_before: usize,
        depth: usize,
    ) -> Option<CommandView<'_>> {
        let name = command_basename(&tokens[wrapper_index]);
        let cmd_index = match name.as_str() {
            "nice" => parse_nice_options(tokens, wrapper_index),
            "nohup" => parse_nohup_options(tokens, wrapper_index),
            "setsid" => parse_setsid_options(tokens, wrapper_index),
            "stdbuf" => parse_stdbuf_options(tokens, wrapper_index),
            "chrt" => parse_chrt_options(tokens, wrapper_index),
            "busybox" => Some(wrapper_index + 1),
            "watch" => parse_watch_options(tokens, wrapper_index),
            "flock" => parse_flock_options(tokens, wrapper_index),
            "ionice" => parse_ionice_options(tokens, wrapper_index),
            "taskset" => parse_taskset_options(tokens, wrapper_index),
            "npx" => parse_npx_options(tokens, wrapper_index),
            _ => return None,
        }?;
        if cmd_index >= tokens.len() {
            return None;
        }
        let inner = effective_command(&tokens[cmd_index..], depth + 1)?;
        Some(CommandView {
            tokens: inner.tokens,
            index: cmd_index + inner.index,
            leading_assignments: leading_before + inner.leading_assignments,
            had_exec: inner.had_exec,
            had_generic_wrapper: true,
        })
    }

    fn parse_nice_options(tokens: &[String], wrapper_index: usize) -> Option<usize> {
        let mut i = wrapper_index + 1;
        while i < tokens.len() {
            let t = &tokens[i];
            if t == "--" {
                return Some(i + 1);
            }
            if t == "-n" || t == "--adjustment" {
                if i + 1 >= tokens.len() {
                    return None;
                }
                i += 2;
                continue;
            }
            if t.starts_with("--adjustment=") {
                i += 1;
                continue;
            }
            if t.starts_with('-') && t.len() > 1 && t[1..].parse::<i32>().is_ok() {
                i += 1;
                continue;
            }
            if t.starts_with('-') {
                return None;
            }
            return Some(i);
        }
        None
    }

    fn parse_nohup_options(tokens: &[String], wrapper_index: usize) -> Option<usize> {
        let mut i = wrapper_index + 1;
        if i < tokens.len() && tokens[i] == "--" {
            i += 1;
        }
        Some(i)
    }

    fn parse_setsid_options(tokens: &[String], wrapper_index: usize) -> Option<usize> {
        const KNOWN: &[&str] = &[
            "-c",
            "-f",
            "-w",
            "-V",
            "--wait",
            "--fork",
            "--ctty",
            "--version",
            "--help",
        ];
        let mut i = wrapper_index + 1;
        while i < tokens.len() {
            let t = &tokens[i];
            if t == "--" {
                return Some(i + 1);
            }
            if t.starts_with('-') {
                if KNOWN.contains(&t.as_str()) {
                    i += 1;
                    continue;
                }
                return None;
            }
            return Some(i);
        }
        None
    }

    fn parse_stdbuf_options(tokens: &[String], wrapper_index: usize) -> Option<usize> {
        let mut i = wrapper_index + 1;
        while i < tokens.len() {
            let t = &tokens[i];
            if t == "--" {
                return Some(i + 1);
            }
            if matches!(t.as_str(), "-i" | "-o" | "-e") {
                if i + 1 >= tokens.len() {
                    return None;
                }
                i += 2;
                continue;
            }
            if t.starts_with("--input=")
                || t.starts_with("--output=")
                || t.starts_with("--error=")
                || (t.starts_with("-i=") || t.starts_with("-o=") || t.starts_with("-e="))
            {
                i += 1;
                continue;
            }
            if t.starts_with('-') {
                return None;
            }
            return Some(i);
        }
        None
    }

    fn parse_chrt_options(tokens: &[String], wrapper_index: usize) -> Option<usize> {
        const KNOWN_NO_ARG: &[&str] = &[
            "-a",
            "-b",
            "-d",
            "-f",
            "-i",
            "-m",
            "-R",
            "-T",
            "-v",
            "-z",
            "-V",
            "-h",
            "--all-tasks",
            "--batch",
            "--deadline",
            "--fifo",
            "--idle",
            "--max",
            "--reset-on-fork",
            "--strict",
            "--verbose",
            "--help",
            "--version",
        ];
        let mut i = wrapper_index + 1;
        while i < tokens.len() {
            let t = &tokens[i];
            if t == "-p" {
                if i + 1 >= tokens.len() {
                    return None;
                }
                i += 2;
                continue;
            }
            if t.starts_with('-') {
                if KNOWN_NO_ARG.contains(&t.as_str()) {
                    i += 1;
                    continue;
                }
                return None;
            }
            if t.parse::<u64>().is_ok() && i + 1 < tokens.len() {
                i += 1;
            }
            return Some(i);
        }
        None
    }

    fn parse_watch_options(tokens: &[String], wrapper_index: usize) -> Option<usize> {
        let mut i = wrapper_index + 1;
        while i < tokens.len() {
            let t = &tokens[i];
            if t == "--" {
                return Some(i + 1);
            }
            if t.starts_with('-') {
                let lower = t.to_ascii_lowercase();
                if matches!(lower.as_str(), "-n" | "--interval") {
                    if i + 1 >= tokens.len() {
                        return None;
                    }
                    i += 2;
                    continue;
                }
                if lower.starts_with("-n=") || lower.starts_with("--interval=") {
                    i += 1;
                    continue;
                }
                // -d/--differences is a boolean unless glued with '=permanent'.
                // All remaining - options are no-argument flags.
                i += 1;
                continue;
            }
            return Some(i);
        }
        None
    }

    fn parse_flock_options(tokens: &[String], wrapper_index: usize) -> Option<usize> {
        let mut i = wrapper_index + 1;
        while i < tokens.len() {
            let t = &tokens[i];
            if t == "--" {
                if i + 2 < tokens.len() {
                    return Some(i + 2);
                }
                return None;
            }
            if t.starts_with('-') {
                let lower = t.to_ascii_lowercase();
                if matches!(lower.as_str(), "-c" | "--command")
                    || lower.starts_with("-c=")
                    || lower.starts_with("--command=")
                {
                    // The command is a shell string, not a tokenized argv.
                    return None;
                }
                if matches!(
                    lower.as_str(),
                    "-w" | "--wait" | "--timeout" | "-E" | "--conflict-exit-code"
                ) {
                    if i + 1 >= tokens.len() {
                        return None;
                    }
                    i += 2;
                    continue;
                }
                if lower.starts_with("-w=")
                    || lower.starts_with("--wait=")
                    || lower.starts_with("--timeout=")
                    || lower.starts_with("-E=")
                    || lower.starts_with("--conflict-exit-code=")
                {
                    i += 1;
                    continue;
                }
                i += 1;
                continue;
            }
            // First positional is the lock file; the command follows it.
            if i + 1 < tokens.len() {
                return Some(i + 1);
            }
            return None;
        }
        None
    }

    fn parse_ionice_options(tokens: &[String], wrapper_index: usize) -> Option<usize> {
        let mut i = wrapper_index + 1;
        while i < tokens.len() {
            let t = &tokens[i];
            if t == "--" {
                return Some(i + 1);
            }
            if t.starts_with('-') {
                let lower = t.to_ascii_lowercase();
                if matches!(lower.as_str(), "-p" | "--pid") {
                    // Modifies an existing process; no inner command to model.
                    return None;
                }
                if matches!(lower.as_str(), "-c" | "--class" | "-n" | "--classdata") {
                    if i + 1 >= tokens.len() {
                        return None;
                    }
                    i += 2;
                    continue;
                }
                if lower.starts_with("-c=")
                    || lower.starts_with("--class=")
                    || lower.starts_with("-n=")
                    || lower.starts_with("--classdata=")
                {
                    i += 1;
                    continue;
                }
                i += 1;
                continue;
            }
            return Some(i);
        }
        None
    }

    fn parse_taskset_options(tokens: &[String], wrapper_index: usize) -> Option<usize> {
        let mut i = wrapper_index + 1;
        let mut saw_cpu_list = false;
        while i < tokens.len() {
            let t = &tokens[i];
            if t == "--" {
                if i + 2 < tokens.len() {
                    return Some(i + 2);
                }
                return None;
            }
            if t.starts_with('-') {
                let lower = t.to_ascii_lowercase();
                if matches!(lower.as_str(), "-p" | "--pid") {
                    // Modifies an existing process; no inner command to model.
                    return None;
                }
                if matches!(lower.as_str(), "-c" | "--cpu-list") {
                    if i + 1 >= tokens.len() {
                        return None;
                    }
                    i += 2;
                    saw_cpu_list = true;
                    continue;
                }
                if lower.starts_with("-c=") || lower.starts_with("--cpu-list=") {
                    i += 1;
                    saw_cpu_list = true;
                    continue;
                }
                if matches!(
                    lower.as_str(),
                    "-a" | "--all-tasks" | "-h" | "--help" | "-v" | "--version"
                ) {
                    i += 1;
                    continue;
                }
                return None;
            }
            if saw_cpu_list {
                return Some(i);
            }
            // First positional is the CPU mask; the command follows it.
            if i + 1 < tokens.len() {
                return Some(i + 1);
            }
            return None;
        }
        None
    }

    fn parse_npx_options(tokens: &[String], wrapper_index: usize) -> Option<usize> {
        let mut i = wrapper_index + 1;
        while i < tokens.len() {
            let t = &tokens[i];
            if t == "--" {
                return Some(i + 1);
            }
            if t.starts_with('-') {
                let lower = t.to_ascii_lowercase();
                if matches!(lower.as_str(), "-p" | "--package" | "--node-arg") {
                    if i + 1 >= tokens.len() {
                        return None;
                    }
                    i += 2;
                    continue;
                }
                if lower.starts_with("-p=")
                    || lower.starts_with("--package=")
                    || lower.starts_with("--node-arg=")
                {
                    i += 1;
                    continue;
                }
                if matches!(
                    lower.as_str(),
                    "-y" | "--yes"
                        | "--no-install"
                        | "--ignore-existing"
                        | "-h"
                        | "--help"
                        | "-v"
                        | "--version"
                ) {
                    i += 1;
                    continue;
                }
                return None;
            }
            return Some(i);
        }
        None
    }

    fn git_is_network_operation(tokens: &[String]) -> bool {
        let mut i = 0usize;
        while i < tokens.len() {
            let t_lower = tokens[i].to_ascii_lowercase();
            if t_lower.starts_with('-') {
                if matches!(
                    t_lower.as_str(),
                    "-c" | "--config"
                        | "--config-env"
                        | "--git-dir"
                        | "--work-tree"
                        | "--exec-path"
                        | "--namespace"
                ) {
                    i += 2;
                    continue;
                }
                if matches!(
                    t_lower.as_str(),
                    "--no-pager"
                        | "-p"
                        | "--paginate"
                        | "--no-replace-objects"
                        | "--bare"
                        | "--version"
                        | "--help"
                ) {
                    i += 1;
                    continue;
                }
                if t_lower.starts_with("--git-dir=")
                    || t_lower.starts_with("--work-tree=")
                    || t_lower.starts_with("--exec-path=")
                    || t_lower.starts_with("--namespace=")
                    || t_lower.starts_with("--config=")
                    || t_lower.starts_with("--config-env=")
                    || (t_lower.starts_with("-c") && t_lower.contains('='))
                    || t_lower.starts_with("-C")
                {
                    i += 1;
                    continue;
                }
                i += 1;
                continue;
            }
            if matches!(
                t_lower.as_str(),
                "push"
                    | "clone"
                    | "fetch"
                    | "pull"
                    | "ls-remote"
                    | "send-email"
                    | "imap-send"
                    | "smtp-send"
                    | "send-pack"
                    | "fetch-pack"
            ) {
                return true;
            }
            if t_lower == "submodule"
                && tokens.get(i + 1).is_some_and(|t| {
                    t.eq_ignore_ascii_case("add") || t.eq_ignore_ascii_case("update")
                })
            {
                return true;
            }
            if t_lower == "remote"
                && tokens.get(i + 1).is_some_and(|t| {
                    t.eq_ignore_ascii_case("update")
                        || t.eq_ignore_ascii_case("show")
                        || t.eq_ignore_ascii_case("prune")
                })
            {
                return true;
            }
            if t_lower == "p4"
                && tokens.get(i + 1).is_some_and(|t| {
                    matches!(
                        t.to_ascii_lowercase().as_str(),
                        "clone" | "sync" | "submit" | "rebase" | "rollback" | "cherry-pick"
                    )
                })
            {
                return true;
            }
            if t_lower == "svn"
                && tokens.get(i + 1).is_some_and(|t| {
                    matches!(
                        t.to_ascii_lowercase().as_str(),
                        "clone"
                            | "fetch"
                            | "dcommit"
                            | "rebase"
                            | "log"
                            | "blame"
                            | "info"
                            | "commit-diff"
                            | "set-tree"
                            | "mkdirs"
                    )
                })
            {
                return true;
            }
            if t_lower == "archive"
                && tokens[i + 1..].iter().any(|t| {
                    let l = t.to_ascii_lowercase();
                    l == "--remote" || l.starts_with("--remote=")
                })
            {
                return true;
            }
            return false;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::provider::types::ValidatedToolArguments;

    fn projector() -> SecretAwareActionProjector {
        SecretAwareActionProjector::new(Redactor::v1(), SecretDigestKey::fixture())
    }

    fn args(value: serde_json::Value) -> ValidatedToolArguments {
        serde_json::from_value(value).expect("valid args")
    }

    fn bash_action(command: &str) -> CanonicalAction {
        let mut perms = vec![Permission::Exec];
        if network_indicators_in_command(command) {
            perms.push(Permission::Network);
        }
        CanonicalAction {
            tool: BASH_TOOL_NAME.to_owned(),
            operation: "exec".to_owned(),
            argv: vec![command.to_owned()],
            cwd: PathBuf::from("/workspace"),
            affected_paths: vec![],
            sandbox: SandboxSummary::workspace(),
            requested_permissions: perms,
            justification: None,
        }
    }

    #[test]
    fn canonical_action_debug_does_not_leak_argv_or_justification() {
        let action = CanonicalAction {
            tool: "bash".to_owned(),
            operation: "exec".to_owned(),
            argv: vec!["curl -H Authorization: Bearer sk-abc".to_owned()],
            cwd: PathBuf::from("/workspace"),
            affected_paths: vec![],
            sandbox: SandboxSummary::workspace(),
            requested_permissions: vec![Permission::Exec],
            justification: Some("secret password foo".to_owned()),
        };
        let text = format!("{:?}", action);
        assert!(!text.contains("sk-abc"));
        assert!(!text.contains("password"));
        assert!(text.contains("1 tokens redacted"));
        assert!(text.contains("[REDACTED]"));
    }

    #[test]
    fn projector_redacts_bearer_token_and_preserves_host() {
        let action =
            bash_action(r#"curl -H "Authorization: Bearer abcdef1234567890" https://example.com"#);
        let ReviewProjection::Reviewable(projected) = projector().project(&action) else {
            panic!("expected reviewable");
        };
        let argv_text = serde_json::to_string(&projected.argv).unwrap();
        assert!(!argv_text.contains("abcdef1234567890"));
        assert!(argv_text.contains("bearer_token"));
        assert!(argv_text.contains("https://example.com"));
    }

    #[test]
    fn projector_redacts_signed_url_signature() {
        let action = bash_action(r#"curl "https://example.com?X-Amz-Signature=abcdef1234567890""#);
        let ReviewProjection::Reviewable(projected) = projector().project(&action) else {
            panic!("expected reviewable");
        };
        let argv_text = serde_json::to_string(&projected.argv).unwrap();
        assert!(!argv_text.contains("abcdef1234567890"));
        assert!(argv_text.contains("signature"));
        assert!(argv_text.contains("https://example.com"));
    }

    #[test]
    fn projector_returns_insufficient_evidence_when_host_is_fully_redacted() {
        let action = bash_action(r#"curl https://sk-abcdefghijklmnop.example.com"#);
        assert!(matches!(
            projector().project(&action),
            ReviewProjection::InsufficientEvidence { .. }
        ));
    }

    #[test]
    fn shell_tokenizer_respects_quotes_and_escapes() {
        let tokens = shell::tokenize_command(r#"echo "a && b" 'c || d' e\ f"#);
        assert_eq!(tokens, vec!["echo", "a && b", "c || d", "e f"]);
    }

    #[test]
    fn shell_segmenter_does_not_split_inside_quotes() {
        let segments = shell::segment_command(r#"echo "a && b" ; echo c"#);
        assert_eq!(segments.len(), 2);
        assert!(segments[0].raw.contains("a && b"));
    }

    #[test]
    fn from_tool_call_maps_workspace_tools() {
        let args = serde_json::from_value::<ValidatedToolArguments>(json!({
            "path": "notes.txt",
            "offset": 0,
            "limit": 100
        }))
        .unwrap();
        let action =
            CanonicalAction::from_tool_call(PathBuf::from("/workspace"), "read_file", &args)
                .expect("read_file");
        assert_eq!(action.tool, "read_file");
        assert_eq!(action.operation, "read");
        assert_eq!(action.argv, vec!["read_file", "notes.txt"]);
    }

    #[test]
    fn network_classification_uses_effective_command_positions() {
        assert!(network_indicators_in_command("curl https://example.com"));
        assert!(network_indicators_in_command(
            "echo safe; curl https://example.com"
        ));
        assert!(network_indicators_in_command("exec git push origin main"));
        assert!(network_indicators_in_command("git push origin main"));
        assert!(network_indicators_in_command(
            "git clone https://example.com/repo"
        ));
        assert!(!network_indicators_in_command("git status"));
        assert!(!network_indicators_in_command(
            "echo 'curl https://example.com'"
        ));
        assert!(!network_indicators_in_command(
            "printf 'https://example.com'"
        ));
    }

    #[test]
    fn remote_copy_and_file_transfer_commands_are_network() {
        for command in [
            "rsync user@host:/path /workspace/backup",
            "rsync -e 'ssh -p 2222' user@host:/path /workspace/backup",
            "rsync 'rsync://host/module/path' /workspace/backup",
            "rsync /workspace/src host::module",
            "rsync /workspace/src host:/dest",
            "rsync /workspace/src host:dest",
            "sftp user@host",
            "lftp -u user,pass sftp://host",
        ] {
            assert!(
                network_indicators_in_command(command),
                "expected network indicator: {command}"
            );
        }
    }

    #[test]
    fn ordinary_local_rsync_is_not_network() {
        for command in [
            "rsync src/ dst/",
            "rsync -avz /workspace/src/ /workspace/dst/",
            "rsync --exclude='*.so' src/ dst/",
            "rsync /abs:path /workspace/dst",
            "rsync ./rel:path /workspace/dst",
            "rsync ../rel:path /workspace/dst",
            "rsync ..:path /workspace/dst",
            "rsync .hidden:path /workspace/dst",
            "rsync ~:path /workspace/dst",
        ] {
            assert!(
                !network_indicators_in_command(command),
                "expected no network indicator: {command}"
            );
        }
    }

    #[test]
    fn build_variant_suffixes_canonicalize_to_base_family() {
        let candidates = &["bash", "python", "python3", "curl", "node", "sudo", "find"];
        for (name, expected) in [
            ("bash-static", Some("bash")),
            ("bash-5.2-static", Some("bash")),
            ("bash-static-5.2", None), // variant suffix is not a version
            ("curl-static", Some("curl")),
            ("curl-7.85.0-static", Some("curl")),
            ("python3.11-dbg", Some("python")),
            ("python3-static", Some("python3")),
            ("node18.5.0-static", Some("node")),
            ("sudo-static", Some("sudo")),
            ("find-static", Some("find")),
            ("python3-foo", None),
            ("node-sass", None),
            ("ruby-build", None),
            ("bashful", None),
        ] {
            let got = shell::canonicalize_command_name(name, candidates);
            assert_eq!(
                got, expected,
                "{name} should canonicalize to {:?}, got {:?}",
                expected, got
            );
        }
    }

    #[test]
    fn generic_wrappers_preserve_inner_command_classification() {
        // watch unwraps to the inner curl, so the network/credential metadata
        // is still visible and the wrapper itself is broad.
        assert!(network_indicators_in_command(
            "watch -n 1 curl -u user:pass https://example.com"
        ));
        // Wrapped local commands remain non-network.
        assert!(!network_indicators_in_command(
            "watch -n 1 cat /workspace/notes"
        ));
        // npx unwraps to the inner command binary.
        assert!(!network_indicators_in_command("npx -y cowsay hello"));
    }

    #[test]
    fn git_network_subcommands_require_network() {
        for command in [
            "git push origin main",
            "git clone https://example.com/repo",
            "git fetch origin",
            "git pull origin main",
            "git ls-remote origin",
            "git submodule update --init",
            "git submodule add https://example.com/repo .",
            "git remote update",
            "git remote show origin",
            "git remote prune origin",
            "git archive --remote=origin main",
            "git send-email --to foo@example.com HEAD",
            "git imap-send",
            "git smtp-send",
            "git p4 sync",
            "git svn clone https://example.com/repo",
            "git send-pack origin main",
            "git fetch-pack origin main",
            "git --config-env=core.pager=MY_PAGER ls-remote origin",
        ] {
            assert!(
                network_indicators_in_command(command),
                "expected network indicator: {command}"
            );
        }
        for command in [
            "git status",
            "git archive --format=tar HEAD",
            "git submodule status",
            "git remote -v",
            "git remote add origin https://example.com/repo",
        ] {
            assert!(
                !network_indicators_in_command(command),
                "expected no network indicator: {command}"
            );
        }
    }

    #[test]
    fn artifact_handles_are_opaque_read_only_references() {
        let read = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "read_file",
            &args(json!({"path": "artifact://conversation/tool-output/execution-1"})),
        );
        assert!(read.is_ok());

        let grep = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "grep",
            &args(json!({
                "path": "artifact://conversation/tool-output/execution-1",
                "pattern": "needle"
            })),
        );
        assert!(grep.is_ok());

        for tool in ["write_file", "edit_file", "delete", "list_dir", "glob"] {
            let key = if tool == "glob" { "pattern" } else { "path" };
            let args = args(json!({key: "artifact://conversation/tool-output/execution-1"}));
            assert!(
                CanonicalAction::from_tool_call(PathBuf::from("/workspace"), tool, &args).is_err(),
                "{tool} must not consume artifact handles"
            );
        }
        assert!(
            CanonicalAction::from_tool_call(
                PathBuf::from("/workspace"),
                "read_file",
                &args(json!({"path": "artifact://conversation/tool-output"})),
            )
            .is_err()
        );
    }

    #[test]
    fn projector_redacts_generic_authorization_and_password_forms() {
        let action = bash_action(
            r#"curl -H "Authorization: OAuth abc" "https://example.com/?password=x&pass=short""#,
        );
        let ReviewProjection::Reviewable(projected) = projector().project(&action) else {
            panic!("literal host should remain reviewable after redaction");
        };
        let text = format!("{projected:?}");
        assert!(!text.contains("abc"));
        assert!(!text.contains("short"));
        assert!(text.contains("authorization_token"));
        assert!(text.contains("secret"));
    }

    #[test]
    fn projector_rejects_unverifiable_shell_glob_and_unmatched_quote() {
        for action in [bash_action("echo *.rs"), bash_action("echo 'unterminated")] {
            assert!(matches!(
                projector().project(&action),
                ReviewProjection::InsufficientEvidence { .. }
            ));
        }
    }

    #[test]
    fn canonical_action_debug_redacts_cwd_and_affected_paths() {
        let action = CanonicalAction {
            tool: "test_tool".to_owned(),
            operation: "test_operation".to_owned(),
            argv: vec!["arg_with_SECRET_ARGV".to_owned()],
            cwd: PathBuf::from("/workspace/SECRET_CWD"),
            affected_paths: vec![
                PathBuf::from("/workspace/SECRET_PATH_1"),
                PathBuf::from("/workspace/SECRET_PATH_2"),
            ],
            sandbox: SandboxSummary::workspace(),
            requested_permissions: vec![Permission::Exec, Permission::Network],
            justification: Some("user said SECRET_JUSTIFICATION".to_owned()),
        };
        let out = format!("{:?}", action);
        assert!(!out.contains("SECRET_CWD"), "cwd leaked: {}", out);
        assert!(
            !out.contains("SECRET_PATH_1"),
            "affected_paths leaked: {}",
            out
        );
        assert!(!out.contains("SECRET_PATH_2"));
        assert!(!out.contains("SECRET_ARGV"));
        assert!(!out.contains("SECRET_JUSTIFICATION"));
        assert!(out.contains("test_tool"), "tool missing: {}", out);
        assert!(out.contains("test_operation"));
        assert!(out.contains("Exec"));
        assert!(out.contains("Network"));
        assert!(out.contains("[1 tokens redacted]"));
        assert!(out.contains("[2 paths redacted]"));
        assert!(out.contains("[REDACTED]"));
    }

    #[test]
    fn has_unverifiable_construct_respects_double_quotes_and_escapes() {
        // Double-quoted expansions must remain unverifiable.
        assert!(
            shell::has_unverifiable_construct(r#"echo "$TOKEN""#),
            "double-quoted $VAR"
        );
        assert!(
            shell::has_unverifiable_construct(r#"echo "${TOKEN}""#),
            "double-quoted braced parameter expansion"
        );
        assert!(
            shell::has_unverifiable_construct(r#"echo "$(cmd)""#),
            "double-quoted $(...)"
        );
        assert!(
            shell::has_unverifiable_construct("echo \"`cmd`\""),
            "double-quoted backticks"
        );

        // Double-quoted > and < are literal characters, not redirections.
        assert!(
            !shell::has_unverifiable_construct(r#"echo ">""#),
            "double-quoted >"
        );
        assert!(
            !shell::has_unverifiable_construct(r#"echo "<""#),
            "double-quoted <"
        );
        assert!(
            !shell::has_unverifiable_construct(r#"echo "a > b""#),
            "double-quoted > inside a word"
        );

        // Escaped $ and backticks are literal, both inside and outside double quotes.
        assert!(
            !shell::has_unverifiable_construct(r#"echo \$"#),
            "escaped $ outside quotes"
        );
        assert!(
            !shell::has_unverifiable_construct(r##"echo \`"##),
            "escaped backtick outside quotes"
        );
        assert!(
            !shell::has_unverifiable_construct("echo \"\\$TOKEN\""),
            "escaped $ inside double quotes"
        );
        assert!(
            !shell::has_unverifiable_construct("echo \"\\`cmd\\`\""),
            "escaped backticks inside double quotes"
        );

        // Single-quoted contents are always literal.
        assert!(
            !shell::has_unverifiable_construct(r#"echo '$TOKEN'"#),
            "single-quoted $VAR"
        );
        assert!(
            !shell::has_unverifiable_construct(r#"echo '`cmd`'"#),
            "single-quoted backticks"
        );

        // Unparseable/trailing quote or escape states must be unverifiable.
        assert!(
            shell::has_unverifiable_construct(r#"echo 'unmatched"#),
            "unmatched single quote"
        );
        assert!(
            shell::has_unverifiable_construct(r#"echo "unmatched"#),
            "unmatched double quote"
        );
        assert!(
            shell::has_unverifiable_construct(r#"echo trailing\"#),
            "trailing backslash"
        );

        // Balanced quoted literals remain non-dynamic.
        assert!(
            !shell::has_unverifiable_construct(r#"echo 'literal' and "literal""#),
            "balanced quoted literals"
        );
    }

    #[test]
    fn redact_arguments_redacts_url_userinfo_credential() {
        let args = args(json!({
            "command": "git clone https://deploy:hunter2@example.com/repo.git"
        }));
        let redacted = projector().redact_arguments(&args).unwrap();
        let text = serde_json::to_string(&redacted).unwrap();
        assert!(
            !text.contains("hunter2"),
            "userinfo password leaked: {text}"
        );
        assert!(
            text.contains("[REDACTED:url_credential]"),
            "url_credential placeholder missing: {text}"
        );
        assert!(text.contains("https://deploy:"), "user part lost: {text}");
        assert!(
            text.contains("@example.com/repo.git"),
            "host part lost: {text}"
        );
    }

    #[test]
    fn redact_arguments_redacts_generic_query_secret() {
        let args = args(json!({
            "url": "https://example.com/api?token=foo%bar"
        }));
        let redacted = projector().redact_arguments(&args).unwrap();
        let text = serde_json::to_string(&redacted).unwrap();
        assert!(!text.contains("foo%bar"), "query secret leaked: {text}");
        assert!(
            text.contains("[REDACTED:url_query_secret]"),
            "url_query_secret placeholder missing: {text}"
        );
        assert!(
            text.contains("https://example.com/api?token="),
            "url structure lost: {text}"
        );
    }

    #[test]
    fn redact_arguments_preserves_structured_authorization_and_signed_url() {
        let auth = args(json!({"Authorization": "Bearer abcdef1234567890"}));
        let redacted = projector().redact_arguments(&auth).unwrap();
        let text = serde_json::to_string(&redacted).unwrap();
        assert!(
            !text.contains("abcdef1234567890"),
            "bearer token leaked: {text}"
        );
        assert!(
            text.contains("bearer_token"),
            "bearer_token placeholder missing: {text}"
        );

        let signed = args(json!({"X-Amz-Signature": "abcdef1234567890"}));
        let redacted = projector().redact_arguments(&signed).unwrap();
        let text = serde_json::to_string(&redacted).unwrap();
        assert!(
            !text.contains("abcdef1234567890"),
            "signed URL signature leaked: {text}"
        );
        assert!(
            text.contains("signature"),
            "signature placeholder missing: {text}"
        );
    }

    #[test]
    fn projector_redacts_basic_proxy_authorization_and_secret_environment() {
        for text in [
            "Proxy-Authorization: Basic abcdef1234567890",
            "Authorization: Basic abcdef1234567890",
            "AWS_SECRET_ACCESS_KEY=abcdef1234567890",
        ] {
            assert!(
                projector().text_contains_secret(text),
                "secret not detected: {text}"
            );
            let action = bash_action(text);
            let projection = projector().project(&action);
            let encoded = serde_json::to_string(&projection).unwrap();
            assert!(
                !encoded.contains("abcdef1234567890"),
                "secret leaked: {encoded}"
            );
        }
    }

    #[test]
    fn projector_rejects_dynamic_argv_as_insufficient_evidence() {
        let action = bash_action("echo $UNTRUSTED");
        assert!(matches!(
            projector().project(&action),
            ReviewProjection::InsufficientEvidence { .. }
        ));
    }

    #[test]
    fn projector_rejects_secret_in_final_affected_path_component() {
        let action = CanonicalAction {
            tool: "read_file".to_owned(),
            operation: "read".to_owned(),
            argv: vec![
                "read_file".to_owned(),
                "/workspace/sk-abcdefghijklmnop".to_owned(),
            ],
            cwd: PathBuf::from("/workspace"),
            affected_paths: vec![PathBuf::from("/workspace/sk-abcdefghijklmnop")],
            sandbox: SandboxSummary::workspace(),
            requested_permissions: vec![Permission::ReadWorkspace],
            justification: None,
        };
        assert!(matches!(
            projector().project(&action),
            ReviewProjection::InsufficientEvidence { .. }
        ));
    }

    #[test]
    fn projector_rejects_secret_or_omitted_cwd_component() {
        let with_secret = CanonicalAction {
            tool: "read_file".to_owned(),
            operation: "read".to_owned(),
            argv: vec!["read_file".to_owned(), "/workspace/notes.txt".to_owned()],
            cwd: PathBuf::from("/workspace/sk-abcdefghijklmnop"),
            affected_paths: vec![PathBuf::from("/workspace/notes.txt")],
            sandbox: SandboxSummary::workspace(),
            requested_permissions: vec![Permission::ReadWorkspace],
            justification: None,
        };
        assert!(matches!(
            projector().project(&with_secret),
            ReviewProjection::InsufficientEvidence { .. }
        ));

        let with_dynamic = CanonicalAction {
            tool: "read_file".to_owned(),
            operation: "read".to_owned(),
            argv: vec!["read_file".to_owned(), "/workspace/notes.txt".to_owned()],
            cwd: PathBuf::from("/workspace/$(echo secret)"),
            affected_paths: vec![PathBuf::from("/workspace/notes.txt")],
            sandbox: SandboxSummary::workspace(),
            requested_permissions: vec![Permission::ReadWorkspace],
            justification: None,
        };
        assert!(matches!(
            projector().project(&with_dynamic),
            ReviewProjection::InsufficientEvidence { .. }
        ));
    }

    #[test]
    fn projector_accepts_fully_literal_cwd_and_affected_paths() {
        let action = CanonicalAction {
            tool: "read_file".to_owned(),
            operation: "read".to_owned(),
            argv: vec!["read_file".to_owned(), "/workspace/notes.txt".to_owned()],
            cwd: PathBuf::from("/workspace"),
            affected_paths: vec![PathBuf::from("/workspace/notes.txt")],
            sandbox: SandboxSummary::workspace(),
            requested_permissions: vec![Permission::ReadWorkspace],
            justification: None,
        };
        let projection = projector().project(&action);
        assert!(
            matches!(projection, ReviewProjection::Reviewable(_)),
            "expected reviewable, got {projection:?}"
        );
    }

    #[test]
    fn redact_arguments_redacts_backslash_escaped_secret() {
        let args = args(json!({
            "command": "curl https://example.com/api?token=foo\\\\&bar"
        }));
        let redacted = projector().redact_arguments(&args).unwrap();
        let text = serde_json::to_string(&redacted).unwrap();
        assert!(
            !text.contains("foo"),
            "backslash-escaped secret leaked: {text}"
        );
        assert!(
            text.contains("[REDACTED:url_query_secret]"),
            "url_query_secret placeholder missing: {text}"
        );
        assert!(text.contains("?token="), "url structure lost: {text}");
    }

    #[test]
    fn redact_arguments_redacts_token_only_url_userinfo() {
        let args = args(json!({
            "command": "git clone https://ghp_abc123@github.com/repo.git"
        }));
        let redacted = projector().redact_arguments(&args).unwrap();
        let text = serde_json::to_string(&redacted).unwrap();
        assert!(
            !text.contains("ghp_abc123"),
            "token-only userinfo leaked: {text}"
        );
        assert!(
            text.contains("[REDACTED:url_credential]"),
            "url_credential placeholder missing: {text}"
        );
        assert!(text.contains("https://"), "scheme lost: {text}");
        assert!(
            text.contains("@github.com/repo.git"),
            "host part lost: {text}"
        );
    }

    #[test]
    fn redact_arguments_redacts_generic_authorization_forms() {
        for (header, value) in [
            ("Authorization", "token=abc123"),
            ("Proxy-Authorization", "OAuth abc123"),
            ("Authorization", "Basic abc123"),
        ] {
            let text = format!("{header}: {value}");
            let redacted = projector().redact_text_with_inventory(&text);
            assert!(!redacted.contains("abc123"), "secret leaked: {redacted}");
            assert!(
                redacted.contains("[REDACTED:"),
                "placeholder missing: {redacted}"
            );
        }
    }

    #[test]
    fn canonical_action_validate_rejects_noncanonical_metadata() {
        let mut action = CanonicalAction {
            tool: "read_file".to_owned(),
            operation: "read".to_owned(),
            argv: vec!["read_file".to_owned(), "/workspace/notes.txt".to_owned()],
            cwd: PathBuf::from("/workspace"),
            affected_paths: vec![PathBuf::from("/workspace/notes.txt")],
            sandbox: SandboxSummary::workspace(),
            requested_permissions: vec![Permission::ReadWorkspace, Permission::ReadWorkspace],
            justification: None,
        };
        assert!(
            action.validate().is_err(),
            "duplicate permissions must be rejected"
        );

        action.requested_permissions = vec![Permission::ReadWorkspace];
        action.cwd = PathBuf::from("/workspace/../etc");
        assert!(
            action.validate().is_err(),
            "noncanonical cwd must be rejected"
        );

        action.cwd = PathBuf::from("/workspace");
        action.affected_paths = vec![PathBuf::from("/workspace/../etc/notes.txt")];
        assert!(
            action.validate().is_err(),
            "noncanonical affected path must be rejected"
        );
    }

    #[test]
    fn artifact_handle_rejected_in_non_path_argument() {
        let grep_pattern = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "grep",
            &args(json!({
                "path": "/workspace/notes.txt",
                "pattern": "artifact://conversation/tool-output/execution-1"
            })),
        );
        assert!(
            grep_pattern.is_err(),
            "artifact handle in grep pattern must be rejected"
        );
    }

    #[test]
    fn from_tool_call_rejects_remove_file_alias() {
        let result = CanonicalAction::from_tool_call(
            PathBuf::from("/workspace"),
            "remove_file",
            &args(json!({"path": "notes.txt"})),
        );
        assert!(
            matches!(result, Err(ActionError::UnknownTool(_))),
            "remove_file alias must be rejected: {result:?}"
        );
    }

    #[test]
    fn project_rejects_forged_canonical_action() {
        let mut forged = bash_action("echo safe");
        forged.operation = "write".to_owned();
        assert!(matches!(
            projector().project(&forged),
            ReviewProjection::InsufficientEvidence { .. }
        ));

        let mut forged_perms = bash_action("echo safe");
        forged_perms
            .requested_permissions
            .push(Permission::WriteWorkspace);
        assert!(matches!(
            projector().project(&forged_perms),
            ReviewProjection::InsufficientEvidence { .. }
        ));
    }

    #[test]
    fn projector_preserves_single_quoted_and_escaped_dollar() {
        for command in ["echo '$TOKEN'", r#"echo \$TOKEN"#] {
            let action = bash_action(command);
            let ReviewProjection::Reviewable(projected) = projector().project(&action) else {
                panic!("expected reviewable for literal dollar: {command}");
            };
            let argv_text = serde_json::to_string(&projected.argv).unwrap();
            assert!(
                argv_text.contains("$TOKEN"),
                "literal $ should remain: {argv_text}"
            );
        }
    }

    #[test]
    fn projector_rejects_unquoted_and_double_quoted_dollar() {
        for command in ["echo $TOKEN", r#"echo "$TOKEN""#] {
            let action = bash_action(command);
            assert!(
                matches!(
                    projector().project(&action),
                    ReviewProjection::InsufficientEvidence { .. }
                ),
                "dynamic $ should require approval: {command}"
            );
        }
    }

    #[test]
    fn projector_rejects_network_when_one_of_two_hosts_redacted() {
        let action =
            bash_action("curl https://example.com https://sk-abcdefghijklmnop.example.com");
        assert!(matches!(
            projector().project(&action),
            ReviewProjection::InsufficientEvidence { .. }
        ));
    }

    #[test]
    fn redact_bash_command_text_preserves_quotes_and_nonsecret_separators() {
        let redacted = projector().redact_bash_command_text(
            r#"curl "https://example.com?token=foo" "https://example.com?token=bar""#,
        );
        assert!(!redacted.contains("foo"), "foo leaked: {redacted}");
        assert!(!redacted.contains("bar"), "bar leaked: {redacted}");
        assert!(
            redacted.contains(r#""https://example.com?token=[REDACTED:url_query_secret]""#),
            "closing quote missing: {redacted}"
        );
        assert!(
            redacted.matches("https://example.com").count() == 2,
            "both hosts must remain visible: {redacted}"
        );
    }

    #[test]
    fn redacts_inline_credential_option_forms() {
        let cases = [
            ("curl -u user:pass https://example.com", "curl_user"),
            ("curl --user user:pass https://example.com", "curl_user"),
            ("curl -uuser:pass https://example.com", "curl_user"),
            (
                "wget --user=foo --password=bar https://example.com",
                "wget_password",
            ),
            ("wget --password bar https://example.com", "wget_password"),
            ("lftp -u user,pass sftp://host", "lftp_user"),
            ("sshpass -p secret ssh user@host", "sshpass_password"),
            ("sshpass -psecret ssh user@host", "sshpass_password"),
            ("mysql -psecret -h db", "mysql_password"),
            ("mysql -p secret -h db", "mysql_password"),
            ("mongosh -u user -psecret", "mongosh_password"),
            ("sqlcmd -S db -P secret", "sqlcmd_password"),
            ("cqlsh db -psecret", "cqlsh_password"),
            ("redis-cli -a secret ping", "redis_auth"),
        ];
        for (command, expected_kind) in cases {
            let redacted = projector().redact_bash_command_text(command);
            let placeholder = format!("[REDACTED:{expected_kind}]");
            assert!(
                redacted.contains(&placeholder),
                "{command} did not produce {placeholder}: {redacted}"
            );

            let action = bash_action(command);
            if let ReviewProjection::Reviewable(projected) = projector().project(&action) {
                let kinds: Vec<&str> = projected
                    .argv
                    .iter()
                    .filter_map(|token| match token {
                        ReviewToken::SecretRef { kind, .. } => Some(kind.as_str()),
                        _ => None,
                    })
                    .collect();
                assert!(
                    kinds.contains(&expected_kind),
                    "{command} did not project {expected_kind}: {:?}",
                    projected.argv
                );
            }
        }
    }

    #[test]
    fn credential_detection_is_command_aware() {
        // ssh -p is a port, not a password.
        let ssh = "ssh -p 2222 user@host";
        assert!(
            !projector().text_contains_secret(ssh),
            "ssh port misclassified as secret"
        );
        // wget -p is page requisites, not a password.
        let wget = "wget -p https://example.com";
        assert!(
            !projector().text_contains_secret(wget),
            "wget -p misclassified as secret"
        );
    }

    #[test]
    fn projector_redacts_secret_after_non_ascii_prefix() {
        let action = bash_action("printf 'あsk-abcdefghijklmnop'");
        let ReviewProjection::Reviewable(projected) = projector().project(&action) else {
            panic!("expected reviewable projection");
        };
        let argv_text = serde_json::to_string(&projected.argv).unwrap();
        assert!(
            !argv_text.contains("sk-abcdefghijklmnop"),
            "secret leaked: {argv_text}"
        );
        assert!(
            argv_text.contains("api_key"),
            "api_key placeholder missing: {argv_text}"
        );
    }

    #[test]
    fn empty_quoted_tokens_do_not_cause_projection_panic() {
        // tokenize_command_spans emits spans for empty '' tokens; shell_credential_spans
        // must stay aligned so project/redact never indexes out of bounds.
        let action = bash_action("echo '' ''");
        let _ = projector().project(&action);
        let _ = projector().redact_bash_command_text("echo '' ''");
    }

    #[test]
    fn compound_command_credentials_are_redacted_in_summary_and_projection() {
        for command in [
            "echo ok ; curl -u alice:supersecret https://example.com",
            "echo ok && curl -u alice:supersecret https://example.com",
            "echo ok || curl -u alice:supersecret https://example.com",
            "echo ok | curl -u alice:supersecret https://example.com",
            "echo ok ; exec nice curl -u alice:supersecret https://example.com",
        ] {
            let redacted = projector().redact_bash_command_text(command);
            assert!(
                !redacted.contains("supersecret"),
                "summary leaked credential for {command}: {redacted}"
            );
            assert!(
                redacted.contains("[REDACTED:curl_user]"),
                "summary missing curl_user placeholder for {command}: {redacted}"
            );

            let action = bash_action(command);
            let ReviewProjection::Reviewable(projected) = projector().project(&action) else {
                panic!("expected reviewable projection for {command}");
            };
            let argv_text = serde_json::to_string(&projected.argv).unwrap();
            assert!(
                !argv_text.contains("supersecret"),
                "projection leaked credential for {command}: {argv_text}"
            );
            assert!(
                argv_text.contains("curl_user"),
                "projection missing curl_user for {command}: {argv_text}"
            );
            assert!(
                argv_text.contains("https://example.com"),
                "projection lost host for {command}: {argv_text}"
            );
        }
    }

    #[test]
    fn nested_shell_credentials_never_reach_summary_or_projection() {
        for command in [
            "(curl -u alice:supersecret https://example.com)",
            "echo $(curl -u alice:supersecret https://example.com)",
            "echo $(echo $(curl -u alice:supersecret https://example.com))",
        ] {
            let summary = projector()
                .redact_arguments(&args(json!({"command": command})))
                .unwrap();
            let summary_text = serde_json::to_string(&summary).unwrap();
            assert!(
                !summary_text.contains("supersecret"),
                "summary leaked nested credential for {command}: {summary_text}"
            );

            let projection = projector().project(&bash_action(command));
            assert!(
                matches!(projection, ReviewProjection::InsufficientEvidence { .. }),
                "nested shell must fail closed: {command}: {projection:?}"
            );
            let projection_text = serde_json::to_string(&projection).unwrap();
            assert!(
                !projection_text.contains("supersecret"),
                "projection leaked nested credential for {command}: {projection_text}"
            );
        }
    }

    #[test]
    fn credential_spans_preserve_empty_utf8_and_separator_glued_tokens() {
        for command in [
            "curl '' -u alice:supersecret https://example.com",
            "curl あ -u alice:supersecret https://example.com",
            "echo ok;curl -ualice:supersecret https://example.com",
        ] {
            let summary = projector().redact_bash_command_text(command);
            assert!(
                !summary.contains("supersecret"),
                "summary leaked credential for {command}: {summary}"
            );
            assert!(
                summary.contains("[REDACTED:curl_user]"),
                "summary missing curl credential marker for {command}: {summary}"
            );

            let projection = projector().project(&bash_action(command));
            let projection_text = serde_json::to_string(&projection).unwrap();
            assert!(
                !projection_text.contains("supersecret"),
                "projection leaked credential for {command}: {projection_text}"
            );
        }
    }

    #[test]
    fn lexical_normalize_preserves_leading_parent_traversal() {
        assert_eq!(
            lexical_normalize_to_string("../../etc/passwd"),
            "../../etc/passwd"
        );
        assert_eq!(
            lexical_normalize_to_string("../etc/passwd"),
            "../etc/passwd"
        );
        assert_eq!(
            lexical_normalize_to_string("foo/../../etc/passwd"),
            "../etc/passwd"
        );
        assert_eq!(
            lexical_normalize_to_string("foo/bar/../../../etc/passwd"),
            "../etc/passwd"
        );
        assert_eq!(lexical_normalize_to_string("foo/bar/../baz"), "foo/baz");
        assert_eq!(
            lexical_normalize_to_string("/workspace/foo/../../etc/passwd"),
            "/etc/passwd"
        );
        assert_eq!(
            lexical_normalize_to_string("/../../etc/passwd"),
            "/etc/passwd"
        );
    }

    #[test]
    fn curl_oauth2_and_proxy_credentials_are_redacted() {
        let cases = [
            (
                "curl --oauth2-bearer abcdef1234567890 https://example.com",
                "abcdef1234567890",
                "bearer_token",
            ),
            (
                "curl --oauth2-bearer=abcdef1234567890 https://example.com",
                "abcdef1234567890",
                "bearer_token",
            ),
            (
                "curl --proxy-user alice:supersecret https://example.com",
                "supersecret",
                "curl_user",
            ),
            (
                "curl --proxy-user=alice:supersecret https://example.com",
                "supersecret",
                "curl_user",
            ),
            (
                "curl --proxy-password supersecret https://example.com",
                "supersecret",
                "curl_password",
            ),
            (
                "curl --proxy-password=supersecret https://example.com",
                "supersecret",
                "curl_password",
            ),
            (
                "wget --proxy-user=alice --proxy-password=supersecret https://example.com",
                "supersecret",
                "wget_password",
            ),
        ];
        for (command, secret, kind) in cases {
            let summary = projector().redact_bash_command_text(command);
            assert!(
                !summary.contains(secret),
                "summary leaked credential for {command}: {summary}"
            );
            assert!(
                summary.contains(&format!("[REDACTED:{kind}]")),
                "summary missing {kind} marker for {command}: {summary}"
            );

            let projection = projector().project(&bash_action(command));
            let projection_text = serde_json::to_string(&projection).unwrap();
            assert!(
                !projection_text.contains(secret),
                "projection leaked credential for {command}: {projection_text}"
            );
            assert!(
                projection_text.contains(kind),
                "projection missing {kind} marker for {command}: {projection_text}"
            );

            assert!(
                projector().text_contains_secret(command),
                "text_contains_secret failed for {command}"
            );
        }
    }

    #[test]
    fn unquoted_authorization_headers_are_redacted_in_args_summary() {
        for command in [
            "echo Authorization: Basic abcdef1234567890",
            "echo Proxy-Authorization: Basic abcdef1234567890",
            "curl -H Authorization: Basic abcdef1234567890 https://example.com",
            "echo Authorization: token=abcdef1234567890",
            "echo Proxy-Authorization: OAuth abcdef1234567890",
        ] {
            let summary = projector().redact_bash_command_text(command);
            assert!(
                !summary.contains("abcdef1234567890"),
                "args_summary leaked header credential for {command}: {summary}"
            );
            assert!(
                summary.contains("[REDACTED:"),
                "args_summary missing placeholder for {command}: {summary}"
            );

            let args = args(json!({"command": command}));
            let redacted_args = projector().redact_arguments(&args).unwrap();
            let args_text = serde_json::to_string(&redacted_args).unwrap();
            assert!(
                !args_text.contains("abcdef1234567890"),
                "redact_arguments leaked header credential for {command}: {args_text}"
            );

            let projection = projector().project(&bash_action(command));
            let projection_text = serde_json::to_string(&projection).unwrap();
            assert!(
                !projection_text.contains("abcdef1234567890"),
                "projection leaked header credential for {command}: {projection_text}"
            );
        }
    }
}
