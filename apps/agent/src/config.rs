use std::{
    env,
    ffi::OsString,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::provider::{
    ModelSpec, ProtocolCompat,
    assembler::ResponseBudget,
    model::{MaxTokensField, ThinkingFormat},
    types::ApiProtocol,
};

const DEFAULT_CONVERSATION_ID: &str = "default";
const DEFAULT_STATE_DIR: &str = "/var/lib/sumi";
const DEFAULT_MODEL_PRESET: &str = "opencode-go";
const DEFAULT_SYSTEM_PROMPT: &str = crate::prompts::SYSTEM_PROMPT;

#[derive(Clone, Debug)]
pub struct Config {
    pub conversation_id: String,
    pub workspace: PathBuf,
    pub database_path: PathBuf,
    pub system_prompt: String,
    pub model: ModelConfig,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
/// Runtime TOML example:
///
/// ```toml
/// [model]
/// preset = "kimi-k3"
/// id = "kimi-k3"
/// base_url = "https://api.moonshot.ai/v1"
/// account_scope = "production"
/// max_output_tokens = 1048576
/// default_output_tokens = 16384
/// ```
pub struct ModelConfig {
    pub preset: Option<String>,
    pub id: Option<String>,
    pub base_url: Option<String>,
    pub account_scope: Option<String>,
    pub api_key_env: Option<String>,
    pub context_window: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub default_output_tokens: Option<u64>,
    pub reasoning: Option<bool>,
    pub supports_images: Option<bool>,
    pub compat: CompatConfig,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct CompatConfig {
    pub max_tokens_field: Option<String>,
    pub supports_usage_in_streaming: Option<bool>,
    pub thinking_format: Option<String>,
    pub requires_reasoning_content_on_assistant: Option<bool>,
    pub zai_tool_stream: Option<bool>,
    pub supports_strict_mode: Option<bool>,
    pub supports_required_tool_choice: Option<bool>,
    pub supports_store: Option<bool>,
    pub supports_developer_role: Option<bool>,
    pub allows_sampling_parameters: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    conversation_id: Option<String>,
    workspace: Option<PathBuf>,
    state_dir: Option<PathBuf>,
    system_prompt: Option<String>,
    system_prompt_file: Option<PathBuf>,
    model: ModelConfig,
}

#[derive(Debug, Default)]
struct EnvOverrides {
    conversation_id: Option<String>,
    workspace: Option<PathBuf>,
    state_dir: Option<PathBuf>,
    system_prompt: Option<String>,
    system_prompt_file: Option<PathBuf>,
    model_preset: Option<String>,
    model_id: Option<String>,
    model_base_url: Option<String>,
    model_api_key_env: Option<String>,
}

impl Config {
    pub async fn load() -> Result<Self> {
        if let Some(path) = env::var_os("SUMI_ENV_FILE") {
            dotenvy::from_path(&path).with_context(|| {
                format!(
                    "failed to load environment file {}",
                    Path::new(&path).display()
                )
            })?;
        }

        let config_path = env::var_os("SUMI_CONFIG").map(PathBuf::from);
        let file = match &config_path {
            Some(path) => Some(load_file(path).await?),
            None => None,
        };
        let file = file.unwrap_or_default();
        let overrides = EnvOverrides::from_env();
        let config_dir = config_path.as_deref().and_then(Path::parent);
        let system_prompt = resolve_system_prompt(&file, &overrides, config_dir).await?;

        let mut config = Self::resolve(file, overrides)?;
        config.system_prompt = system_prompt;
        Ok(config)
    }

    pub fn model_spec(&self) -> Result<ModelSpec> {
        let preset = self.model.preset.as_deref().unwrap_or(DEFAULT_MODEL_PRESET);
        let mut spec =
            ModelSpec::preset(preset).with_context(|| format!("unknown model preset {preset}"))?;
        if let Some(id) = &self.model.id {
            spec.set_model_id(id);
        }
        if let Some(base_url) = &self.model.base_url {
            spec.base_url.clone_from(base_url);
        }
        if let Some(account_scope) = &self.model.account_scope {
            spec.account_scope.clone_from(account_scope);
        }
        if let Some(api_key_env) = &self.model.api_key_env {
            spec.api_key_env.clone_from(api_key_env);
        }
        if let Some(context_window) = self.model.context_window {
            spec.context_window = context_window;
        }
        if let Some(max_output_tokens) = self.model.max_output_tokens {
            spec.max_output_tokens = max_output_tokens;
            if self.model.default_output_tokens.is_none() {
                spec.default_output_tokens = spec.default_output_tokens.min(max_output_tokens);
            }
        }
        if let Some(default_output_tokens) = self.model.default_output_tokens {
            spec.default_output_tokens = default_output_tokens;
        }
        if let Some(reasoning) = self.model.reasoning {
            spec.reasoning = reasoning;
        }
        if let Some(supports_images) = self.model.supports_images {
            spec.supports_images = supports_images;
        }

        let compat_config = &self.model.compat;
        if let ProtocolCompat::Chat(compat) = &mut spec.compat {
            if let Some(field) = compat_config.max_tokens_field.as_deref() {
                compat.max_tokens_field = match field {
                    "max_tokens" => MaxTokensField::MaxTokens,
                    "max_completion_tokens" => MaxTokensField::MaxCompletionTokens,
                    other => anyhow::bail!("unknown max_tokens_field {other}"),
                };
            }
            if let Some(format) = compat_config.thinking_format.as_deref() {
                compat.thinking_format = match format {
                    "off" => ThinkingFormat::Off,
                    "deepseek" => ThinkingFormat::Deepseek,
                    "zai" => ThinkingFormat::Zai,
                    "openai_effort" => ThinkingFormat::OpenAiEffort,
                    "provider_default" => ThinkingFormat::ProviderDefault,
                    other => anyhow::bail!("unknown thinking_format {other}"),
                };
            }
            if let Some(value) = compat_config.supports_usage_in_streaming {
                compat.supports_usage_in_streaming = value;
            }
            if let Some(value) = compat_config.requires_reasoning_content_on_assistant {
                compat.requires_reasoning_content_on_assistant = value;
            }
            if let Some(value) = compat_config.zai_tool_stream {
                compat.zai_tool_stream = value;
            }
            if let Some(value) = compat_config.supports_strict_mode {
                compat.supports_strict_mode = value;
            }
            if let Some(value) = compat_config.supports_required_tool_choice {
                compat.supports_required_tool_choice = value;
            }
            if let Some(value) = compat_config.supports_store {
                compat.supports_store = value;
            }
            if let Some(value) = compat_config.supports_developer_role {
                compat.supports_developer_role = value;
            }
            if let Some(value) = compat_config.allows_sampling_parameters {
                compat.allows_sampling_parameters = value;
            }
        } else if *compat_config != CompatConfig::default() {
            anyhow::bail!("Chat compatibility overrides require a Chat protocol preset");
        }
        match (&spec.protocol, &spec.compat) {
            (ApiProtocol::OpenAiChatCompletions, ProtocolCompat::Chat(_))
            | (ApiProtocol::OpenAiResponses, ProtocolCompat::Responses(_))
            | (ApiProtocol::AnthropicMessages, ProtocolCompat::Anthropic(_)) => {}
            _ => anyhow::bail!("model protocol/compat variant mismatch"),
        }
        validate_model_base_url(&spec.base_url)?;
        if spec.context_window == 0 {
            anyhow::bail!("context_window must be greater than zero");
        }
        if spec.default_output_tokens == 0 || spec.default_output_tokens > spec.max_output_tokens {
            anyhow::bail!(
                "default_output_tokens must be within 1..={}",
                spec.max_output_tokens
            );
        }
        if ResponseBudget::for_output_tokens(spec.max_output_tokens).is_none() {
            anyhow::bail!(
                "max_output_tokens cannot be represented by the provider response budget"
            );
        }
        Ok(spec)
    }

    fn resolve(file: FileConfig, overrides: EnvOverrides) -> Result<Self> {
        let current_dir = env::current_dir().context("failed to resolve current directory")?;
        Self::resolve_from(file, overrides, &current_dir)
    }

    fn resolve_from(
        mut file: FileConfig,
        overrides: EnvOverrides,
        current_dir: &Path,
    ) -> Result<Self> {
        let workspace = overrides
            .workspace
            .or(file.workspace)
            .unwrap_or_else(|| current_dir.to_owned());
        let state_dir = overrides
            .state_dir
            .or(file.state_dir)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_STATE_DIR));
        let workspace = resolve_for_boundary(&workspace, current_dir)
            .context("failed to resolve workspace path")?;
        let state_dir = resolve_for_boundary(&state_dir, current_dir)
            .context("failed to resolve agent state directory")?;
        ensure_isolated_state_dir(&workspace, &state_dir)?;
        let database_path = state_dir.join("agent.db");

        if let Some(value) = overrides.model_preset {
            file.model.preset = Some(value);
        }
        if let Some(value) = overrides.model_id {
            file.model.id = Some(value);
        }
        if let Some(value) = overrides.model_base_url {
            file.model.base_url = Some(value);
        }
        if let Some(value) = overrides.model_api_key_env {
            file.model.api_key_env = Some(value);
        }

        Ok(Self {
            conversation_id: overrides
                .conversation_id
                .or(file.conversation_id)
                .unwrap_or_else(|| DEFAULT_CONVERSATION_ID.to_owned()),
            workspace,
            database_path,
            system_prompt: overrides
                .system_prompt
                .or(file.system_prompt)
                .unwrap_or_else(|| DEFAULT_SYSTEM_PROMPT.to_owned()),
            model: file.model,
        })
    }
}

fn ensure_isolated_state_dir(workspace: &Path, state_dir: &Path) -> Result<()> {
    if workspace == state_dir
        || workspace.starts_with(state_dir)
        || state_dir.starts_with(workspace)
    {
        bail!(
            "agent state directory {} must not overlap workspace {}",
            state_dir.display(),
            workspace.display()
        );
    }
    Ok(())
}

fn validate_model_base_url(base_url: &str) -> Result<()> {
    let url = reqwest::Url::parse(base_url).context("model base_url must be an absolute URL")?;
    if !matches!(url.scheme(), "http" | "https") {
        anyhow::bail!("model base_url must use http or https");
    }
    if !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!("model base_url must not contain credentials");
    }
    if url.query().is_some() || url.fragment().is_some() {
        anyhow::bail!("model base_url must not contain a query or fragment");
    }
    Ok(())
}

/// Resolve every existing path component through the filesystem, while still
/// supporting a state leaf that has not been created yet. This must remain
/// read-only: Store owns creation and permission changes after this boundary.
fn resolve_for_boundary(path: &Path, current_dir: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        current_dir.join(path)
    };
    let mut existing_prefix = absolute.clone();
    let mut missing_suffix = Vec::<OsString>::new();
    let canonical_prefix = loop {
        match std::fs::canonicalize(&existing_prefix) {
            Ok(canonical) => break canonical,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let component = existing_prefix
                    .components()
                    .next_back()
                    .ok_or_else(|| anyhow::anyhow!("path has no resolvable filesystem prefix"))?
                    .as_os_str()
                    .to_os_string();
                if component == ".." {
                    bail!(
                        "path contains parent traversal in unresolved filesystem suffix: {}",
                        absolute.display()
                    );
                }
                if !existing_prefix.pop() {
                    return Err(error).context("path has no existing filesystem prefix");
                }
                missing_suffix.push(component);
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to resolve filesystem path {}",
                        existing_prefix.display()
                    )
                });
            }
        }
    };

    let mut resolved = canonical_prefix;
    for component in missing_suffix.iter().rev() {
        resolved.push(component);
    }
    Ok(normalize_absolute(&resolved))
}

fn normalize_absolute(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

async fn resolve_system_prompt(
    file: &FileConfig,
    overrides: &EnvOverrides,
    config_dir: Option<&Path>,
) -> Result<String> {
    if let Some(prompt) = &overrides.system_prompt {
        return Ok(prompt.clone());
    }
    if let Some(path) = &overrides.system_prompt_file {
        // 環境変数指定は他の *_FILE 環境変数と同じくプロセスCWD基準。
        return tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("failed to read system prompt file {}", path.display()));
    }
    match (&file.system_prompt, &file.system_prompt_file) {
        (Some(_), Some(_)) => {
            anyhow::bail!("config sets both system_prompt and system_prompt_file; keep only one")
        }
        (Some(prompt), None) => Ok(prompt.clone()),
        (None, Some(path)) => {
            // TOML内の相対パスは設定ファイル自身の場所を基準に解決する。
            // 起動時のCWDに依存させない。
            let path = match config_dir {
                Some(dir) if path.is_relative() => dir.join(path),
                _ => path.clone(),
            };
            tokio::fs::read_to_string(&path)
                .await
                .with_context(|| format!("failed to read system prompt file {}", path.display()))
        }
        (None, None) => Ok(DEFAULT_SYSTEM_PROMPT.to_owned()),
    }
}

impl EnvOverrides {
    fn from_env() -> Self {
        Self {
            conversation_id: env::var("SUMI_CONVERSATION_ID").ok(),
            workspace: env::var_os("SUMI_WORKSPACE").map(PathBuf::from),
            state_dir: env::var_os("SUMI_STATE_DIR").map(PathBuf::from),
            system_prompt: env::var("SUMI_SYSTEM_PROMPT").ok(),
            system_prompt_file: env::var_os("SUMI_SYSTEM_PROMPT_FILE").map(PathBuf::from),
            model_preset: env::var("SUMI_MODEL_PRESET").ok(),
            model_id: env::var("SUMI_MODEL_ID").ok(),
            model_base_url: env::var("SUMI_MODEL_BASE_URL").ok(),
            model_api_key_env: env::var("SUMI_MODEL_API_KEY_ENV").ok(),
        }
    }
}

async fn load_file(path: &Path) -> Result<FileConfig> {
    let source = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read config file {}", path.display()))?;
    toml::from_str(&source)
        .with_context(|| format!("failed to parse config file {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixtureRoot(PathBuf);

    impl FixtureRoot {
        fn new(label: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("sumi-config-{label}-{}", uuid::Uuid::now_v7()));
            std::fs::create_dir_all(&path).expect("create config fixture root");
            Self(path)
        }
    }

    impl Drop for FixtureRoot {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).expect("remove config fixture root");
        }
    }

    fn paths_config(workspace: impl Into<PathBuf>, state_dir: impl Into<PathBuf>) -> FileConfig {
        FileConfig {
            workspace: Some(workspace.into()),
            state_dir: Some(state_dir.into()),
            ..FileConfig::default()
        }
    }

    #[test]
    fn parses_toml_and_derives_database_path_from_state_dir() {
        let file: FileConfig = toml::from_str(
            r#"
conversation_id = "conversation-1"
workspace = "/workspace"
state_dir = "/state"
system_prompt = "Be useful."

[model]
preset = "kimi-k3"
id = "kimi-k3"
base_url = "https://example.test/v1"
api_key_env = "EXAMPLE_API_KEY"
"#,
        )
        .expect("valid config");

        let config = Config::resolve(file, EnvOverrides::default()).expect("resolved config");

        assert_eq!(config.conversation_id, "conversation-1");
        assert_eq!(config.workspace, PathBuf::from("/workspace"));
        assert_eq!(config.database_path, PathBuf::from("/state/agent.db"));
        assert_eq!(config.system_prompt, "Be useful.");
        assert_eq!(
            config.model,
            ModelConfig {
                preset: Some("kimi-k3".to_owned()),
                id: Some("kimi-k3".to_owned()),
                base_url: Some("https://example.test/v1".to_owned()),
                api_key_env: Some("EXAMPLE_API_KEY".to_owned()),
                ..ModelConfig::default()
            }
        );
    }

    #[test]
    fn embedded_system_prompt_is_the_default() {
        let config =
            Config::resolve(FileConfig::default(), EnvOverrides::default()).expect("resolved");

        assert_eq!(config.system_prompt, crate::prompts::SYSTEM_PROMPT);
        assert!(!config.system_prompt.trim().is_empty());
    }

    #[tokio::test]
    async fn environment_prompt_file_overrides_inline_file_config() {
        let path =
            std::env::temp_dir().join(format!("sumi-system-prompt-{}.md", uuid::Uuid::now_v7()));
        tokio::fs::write(&path, "prompt from file")
            .await
            .expect("write prompt");
        let file = FileConfig {
            system_prompt: Some("inline config prompt".to_owned()),
            ..FileConfig::default()
        };
        let overrides = EnvOverrides {
            system_prompt_file: Some(path.clone()),
            ..EnvOverrides::default()
        };

        let prompt = resolve_system_prompt(&file, &overrides, None)
            .await
            .expect("resolve prompt");

        tokio::fs::remove_file(path).await.expect("remove prompt");
        assert_eq!(prompt, "prompt from file");
    }

    #[tokio::test]
    async fn inline_and_file_prompt_together_are_rejected() {
        let file = FileConfig {
            system_prompt: Some("inline".to_owned()),
            system_prompt_file: Some(PathBuf::from("prompts/system.md")),
            ..FileConfig::default()
        };

        let error = resolve_system_prompt(&file, &EnvOverrides::default(), None)
            .await
            .expect_err("both prompt sources must fail");

        assert!(error.to_string().contains("both system_prompt"));
    }

    #[tokio::test]
    async fn relative_prompt_file_resolves_from_the_config_directory() {
        let directory =
            std::env::temp_dir().join(format!("sumi-config-dir-{}", uuid::Uuid::now_v7()));
        tokio::fs::create_dir(&directory).await.expect("create dir");
        tokio::fs::write(directory.join("system.md"), "prompt near config")
            .await
            .expect("write prompt");
        let file = FileConfig {
            system_prompt_file: Some(PathBuf::from("system.md")),
            ..FileConfig::default()
        };

        let prompt = resolve_system_prompt(&file, &EnvOverrides::default(), Some(&directory))
            .await
            .expect("resolve prompt");

        tokio::fs::remove_dir_all(directory).await.expect("cleanup");
        assert_eq!(prompt, "prompt near config");
    }

    #[test]
    fn environment_values_override_file_values() {
        let file: FileConfig = toml::from_str(
            r#"
conversation_id = "from-file"
workspace = "/file-workspace"
state_dir = "/file-state"

[model]
id = "file-model"
"#,
        )
        .expect("valid config");
        let overrides = EnvOverrides {
            conversation_id: Some("from-env".to_owned()),
            workspace: Some(PathBuf::from("/env-workspace")),
            state_dir: Some(PathBuf::from("/env-state")),
            model_id: Some("env-model".to_owned()),
            ..EnvOverrides::default()
        };

        let config = Config::resolve(file, overrides).expect("resolved config");

        assert_eq!(config.conversation_id, "from-env");
        assert_eq!(config.workspace, PathBuf::from("/env-workspace"));
        assert_eq!(config.database_path, PathBuf::from("/env-state/agent.db"));
        assert_eq!(config.model.id.as_deref(), Some("env-model"));
    }

    #[test]
    fn default_database_path_is_isolated_from_workspace() {
        let file: FileConfig = toml::from_str(
            r#"
workspace = "/workspace/customer-data"
"#,
        )
        .expect("valid config");

        let config = Config::resolve(file, EnvOverrides::default()).expect("resolved config");

        assert_eq!(
            config.database_path,
            PathBuf::from("/var/lib/sumi/agent.db")
        );
        assert!(!config.database_path.starts_with(&config.workspace));
    }

    #[test]
    fn resolves_relative_workspace_and_nonexistent_state_leaf_before_comparison() {
        let root = FixtureRoot::new("relative");
        std::fs::create_dir(root.0.join("workspace")).expect("create workspace");

        let config = Config::resolve_from(
            paths_config("workspace", "private/state"),
            EnvOverrides::default(),
            &root.0,
        )
        .expect("non-overlapping relative paths");

        assert_eq!(config.workspace, root.0.join("workspace"));
        assert_eq!(config.database_path, root.0.join("private/state/agent.db"));
        assert!(
            !root.0.join("private").exists(),
            "boundary validation must not create the intended state leaf"
        );
    }

    #[test]
    fn rejects_parent_traversal_in_unresolved_suffix() {
        let root = FixtureRoot::new("unresolved-parent-traversal");
        let workspace = root.0.join("workspace");
        std::fs::create_dir(&workspace).expect("create workspace");

        let error = Config::resolve_from(
            paths_config(&workspace, workspace.join("nope/../../alias")),
            EnvOverrides::default(),
            &root.0,
        )
        .expect_err("unresolved parent traversal must fail closed");

        assert!(format!("{error:#}").contains("parent traversal"));
        assert!(!root.0.join("alias").exists());
    }

    #[test]
    fn existing_parent_traversal_is_resolved_by_filesystem_canonicalization() {
        let root = FixtureRoot::new("existing-parent-traversal");
        let workspace = root.0.join("workspace");
        let state = root.0.join("state");
        std::fs::create_dir(&workspace).expect("create workspace");
        std::fs::create_dir(&state).expect("create state");

        let config = Config::resolve_from(
            paths_config(&workspace, state.join("..").join("state")),
            EnvOverrides::default(),
            &root.0,
        )
        .expect("existing parent traversal should canonicalize");

        assert_eq!(config.database_path, state.join("agent.db"));
    }

    #[test]
    fn rejects_equal_and_bidirectional_workspace_state_overlap() {
        let root = FixtureRoot::new("overlap");
        let workspace = root.0.join("workspace");
        std::fs::create_dir(&workspace).expect("create workspace");

        for (workspace_path, state_path) in [
            (workspace.clone(), workspace.clone()),
            (workspace.join("customer"), workspace.clone()),
            (workspace.clone(), workspace.join("private/state")),
        ] {
            let error = Config::resolve_from(
                paths_config(workspace_path, state_path),
                EnvOverrides::default(),
                &root.0,
            )
            .expect_err("workspace/state overlap must fail closed");
            assert!(error.to_string().contains("must not overlap workspace"));
        }
    }

    #[test]
    fn rejects_relative_workspace_descendant_state() {
        let root = FixtureRoot::new("relative-overlap");
        std::fs::create_dir(root.0.join("workspace")).expect("create workspace");

        let error = Config::resolve_from(
            paths_config("./workspace", "workspace/private/state"),
            EnvOverrides::default(),
            &root.0,
        )
        .expect_err("relative overlap must fail closed");

        assert!(error.to_string().contains("must not overlap workspace"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_existing_symlink_alias_without_mutating_workspace_parent() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let root = FixtureRoot::new("symlink");
        let workspace = root.0.join("workspace");
        std::fs::create_dir(&workspace).expect("create workspace");
        std::fs::set_permissions(&workspace, std::fs::Permissions::from_mode(0o755))
            .expect("set workspace fixture mode");
        let alias = root.0.join("workspace-alias");
        symlink(&workspace, &alias).expect("create workspace alias");

        let error = Config::resolve_from(
            paths_config(&workspace, alias.join("agent-state")),
            EnvOverrides::default(),
            &root.0,
        )
        .expect_err("symlinked overlap must fail closed");

        assert!(error.to_string().contains("must not overlap workspace"));
        assert_eq!(
            std::fs::metadata(&workspace)
                .expect("workspace metadata")
                .permissions()
                .mode()
                & 0o777,
            0o755,
            "config validation must not chmod any workspace path"
        );
        assert!(!workspace.join("agent-state").exists());
    }

    #[test]
    fn accepts_existing_non_overlapping_sibling_trees() {
        let root = FixtureRoot::new("siblings");
        let workspace = root.0.join("workspace");
        let state = root.0.join("agent-state");
        std::fs::create_dir(&workspace).expect("create workspace");
        std::fs::create_dir(&state).expect("create state");

        let config = Config::resolve_from(
            paths_config(&workspace, &state),
            EnvOverrides::default(),
            &root.0,
        )
        .expect("sibling workspace and state trees are isolated");

        assert_eq!(config.workspace, workspace);
        assert_eq!(config.database_path, state.join("agent.db"));
    }

    #[test]
    fn rejects_unknown_model_keys() {
        let error = toml::from_str::<FileConfig>(
            r#"
[model]
modle = "typo"
"#,
        )
        .expect_err("unknown model key must fail");

        assert!(error.to_string().contains("unknown field `modle`"));
    }

    #[test]
    fn rejects_unknown_compat_keys() {
        let error = toml::from_str::<FileConfig>(
            r#"
[model]
preset = "kimi-k3"

[model.compat]
supports_required_tools_choice = false
"#,
        )
        .expect_err("unknown compat key must fail");

        assert!(
            error
                .to_string()
                .contains("unknown field `supports_required_tools_choice`")
        );
    }

    #[test]
    fn required_tool_choice_can_be_disabled_for_a_supporting_preset() {
        let file: FileConfig = toml::from_str(
            r#"
[model]
preset = "kimi-k3"

[model.compat]
supports_required_tool_choice = false
"#,
        )
        .expect("valid config");
        let config = Config::resolve(file, EnvOverrides::default()).expect("resolved");

        let spec = config.model_spec().expect("model spec");

        assert!(
            !spec
                .chat_compat()
                .expect("Chat compat")
                .supports_required_tool_choice
        );
    }

    #[test]
    fn required_tool_choice_can_be_enabled_for_opencode() {
        let file: FileConfig = toml::from_str(
            r#"
[model]
preset = "opencode-go"

[model.compat]
supports_required_tool_choice = true
"#,
        )
        .expect("valid config");
        let config = Config::resolve(file, EnvOverrides::default()).expect("resolved");

        let spec = config.model_spec().expect("model spec");

        assert!(
            spec.chat_compat()
                .expect("Chat compat")
                .supports_required_tool_choice
        );
    }

    #[test]
    fn rejects_chat_compat_overrides_for_non_chat_presets() {
        for preset in ["anthropic", "openai-responses"] {
            let file = FileConfig {
                model: ModelConfig {
                    preset: Some(preset.to_owned()),
                    compat: CompatConfig {
                        supports_store: Some(true),
                        ..CompatConfig::default()
                    },
                    ..ModelConfig::default()
                },
                ..FileConfig::default()
            };
            let config = Config::resolve(file, EnvOverrides::default()).expect("resolved config");

            let error = config
                .model_spec()
                .expect_err("Chat compat overrides must fail for non-Chat presets");

            assert_eq!(
                error.to_string(),
                "Chat compatibility overrides require a Chat protocol preset",
                "preset={preset}"
            );
        }
    }

    #[test]
    fn model_preset_values_can_be_overridden_without_recompiling() {
        let file: FileConfig = toml::from_str(
            r#"
[model]
preset = "glm-5.2"
id = "custom-glm"
base_url = "https://proxy.example/v1"
api_key_env = "CUSTOM_KEY"
context_window = 200000
max_output_tokens = 64000
default_output_tokens = 12000

[model.compat]
max_tokens_field = "max_tokens"
zai_tool_stream = false
"#,
        )
        .expect("valid config");
        let config = Config::resolve(file, EnvOverrides::default()).expect("resolved");

        let spec = config.model_spec().expect("model spec");

        assert_eq!(spec.id, "custom-glm");
        assert_eq!(spec.base_url, "https://proxy.example/v1");
        assert_eq!(spec.api_key_env, "CUSTOM_KEY");
        assert_eq!(spec.provider, "zai");
        assert_eq!(spec.context_window, 200_000);
        assert_eq!(spec.max_output_tokens, 64_000);
        assert_eq!(spec.default_output_tokens, 12_000);
        let compat = spec.chat_compat().expect("Chat compat");
        assert_eq!(compat.max_tokens_field, MaxTokensField::MaxTokens);
        assert!(!compat.zai_tool_stream);
    }

    #[test]
    fn opencode_model_override_keeps_gateway_specific_compat() {
        let file: FileConfig = toml::from_str(
            r#"
[model]
preset = "opencode-go"
id = "glm-5.2"
"#,
        )
        .expect("valid config");
        let config = Config::resolve(file, EnvOverrides::default()).expect("resolved");

        let spec = config.model_spec().expect("model spec");

        assert_eq!(spec.id, "glm-5.2");
        assert_eq!(spec.provider, "opencode-go");
        assert_eq!(spec.base_url, "https://opencode.ai/zen/go/v1");
        assert_eq!(spec.api_key_env, "OPENCODE_GO_API_KEY");
        let compat = spec.chat_compat().expect("Chat compat");
        assert_eq!(compat.thinking_format, ThinkingFormat::ProviderDefault);
        assert!(!compat.requires_reasoning_content_on_assistant);
        assert!(!compat.zai_tool_stream);
        assert!(!spec.supports_images);
        assert_eq!(spec.context_window, 262_144);
        assert_eq!(spec.max_output_tokens, 32_768);
    }

    #[test]
    fn explicit_overrides_win_over_id_re_resolution() {
        let file: FileConfig = toml::from_str(
            r#"
[model]
preset = "opencode-go"
id = "glm-5.2"
supports_images = true
max_output_tokens = 64000

[model.compat]
zai_tool_stream = false
"#,
        )
        .expect("valid config");
        let config = Config::resolve(file, EnvOverrides::default()).expect("resolved");

        let spec = config.model_spec().expect("model spec");

        assert!(spec.supports_images);
        assert_eq!(spec.max_output_tokens, 64_000);
        let compat = spec.chat_compat().expect("Chat compat");
        assert!(!compat.zai_tool_stream);
        assert_eq!(compat.thinking_format, ThinkingFormat::ProviderDefault);
    }

    #[test]
    fn inherited_default_output_tokens_clamps_to_overridden_maximum() {
        let file: FileConfig = toml::from_str(
            r#"
[model]
preset = "opencode-go"
max_output_tokens = 8000
"#,
        )
        .expect("valid config");
        let config = Config::resolve(file, EnvOverrides::default()).expect("resolved");

        let spec = config.model_spec().expect("model spec");

        assert_eq!(spec.max_output_tokens, 8_000);
        assert_eq!(spec.default_output_tokens, 8_000);
    }

    #[test]
    fn explicit_default_output_tokens_above_overridden_maximum_is_rejected() {
        let file: FileConfig = toml::from_str(
            r#"
[model]
preset = "opencode-go"
max_output_tokens = 8000
default_output_tokens = 16000
"#,
        )
        .expect("valid config");
        let config = Config::resolve(file, EnvOverrides::default()).expect("resolved");

        let error = config
            .model_spec()
            .expect_err("explicit invalid default must fail");

        assert!(
            error
                .to_string()
                .contains("default_output_tokens must be within 1..=8000")
        );
    }

    #[test]
    fn default_model_uses_opencode_go_kimi_k2_7_code() {
        let config =
            Config::resolve(FileConfig::default(), EnvOverrides::default()).expect("resolved");

        let spec = config.model_spec().expect("model spec");

        assert_eq!(spec.provider, "opencode-go");
        assert_eq!(spec.id, "kimi-k2.7-code");
        assert_eq!(spec.base_url, "https://opencode.ai/zen/go/v1");
        assert_eq!(spec.api_key_env, "OPENCODE_GO_API_KEY");
        assert_eq!(spec.context_window, 262_144);
        assert_eq!(spec.max_output_tokens, 32_768);
        assert_eq!(spec.default_output_tokens, 16_384);
        assert!(!spec.supports_images);
    }

    #[test]
    fn model_base_url_rejects_credentials_queries_and_non_http_schemes() {
        for base_url in [
            "https://user:secret@example.test/v1",
            "https://example.test/v1?token=secret",
            "https://example.test/v1#fragment",
            "file:///tmp/provider",
            "not-a-url",
        ] {
            let config = Config {
                conversation_id: "test".to_owned(),
                workspace: PathBuf::from("/workspace"),
                database_path: PathBuf::from("/workspace/agent.db"),
                system_prompt: String::new(),
                model: ModelConfig {
                    preset: Some("kimi-k3".to_owned()),
                    base_url: Some(base_url.to_owned()),
                    ..ModelConfig::default()
                },
            };
            assert!(config.model_spec().is_err(), "{base_url}");
        }
    }

    #[test]
    fn zero_context_window_is_rejected() {
        let mut config =
            Config::resolve(FileConfig::default(), EnvOverrides::default()).expect("resolved");
        config.model.context_window = Some(0);

        let error = config
            .model_spec()
            .expect_err("zero context window must fail");

        assert!(error.to_string().contains("context_window"));
    }
}
