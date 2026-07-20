use std::{
    env,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::provider::{
    adapters::chat_completions::{MaxTokensField, ModelSpec, ThinkingFormat},
    assembler::ResponseBudget,
    types::ApiProtocol,
};

const DEFAULT_CONVERSATION_ID: &str = "default";
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
    pub supports_store: Option<bool>,
    pub supports_developer_role: Option<bool>,
    pub allows_sampling_parameters: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    conversation_id: Option<String>,
    workspace: Option<PathBuf>,
    database_path: Option<PathBuf>,
    system_prompt: Option<String>,
    system_prompt_file: Option<PathBuf>,
    model: ModelConfig,
}

#[derive(Debug, Default)]
struct EnvOverrides {
    conversation_id: Option<String>,
    workspace: Option<PathBuf>,
    database_path: Option<PathBuf>,
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

        let compat = &self.model.compat;
        if let Some(field) = compat.max_tokens_field.as_deref() {
            spec.compat.max_tokens_field = match field {
                "max_tokens" => MaxTokensField::MaxTokens,
                "max_completion_tokens" => MaxTokensField::MaxCompletionTokens,
                other => anyhow::bail!("unknown max_tokens_field {other}"),
            };
        }
        if let Some(format) = compat.thinking_format.as_deref() {
            spec.compat.thinking_format = match format {
                "off" => ThinkingFormat::Off,
                "deepseek" => ThinkingFormat::Deepseek,
                "zai" => ThinkingFormat::Zai,
                "openai_effort" => ThinkingFormat::OpenAiEffort,
                "provider_default" => ThinkingFormat::ProviderDefault,
                other => anyhow::bail!("unknown thinking_format {other}"),
            };
        }
        if let Some(value) = compat.supports_usage_in_streaming {
            spec.compat.supports_usage_in_streaming = value;
        }
        if let Some(value) = compat.requires_reasoning_content_on_assistant {
            spec.compat.requires_reasoning_content_on_assistant = value;
        }
        if let Some(value) = compat.zai_tool_stream {
            spec.compat.zai_tool_stream = value;
        }
        if let Some(value) = compat.supports_strict_mode {
            spec.compat.supports_strict_mode = value;
        }
        if let Some(value) = compat.supports_store {
            spec.compat.supports_store = value;
        }
        if let Some(value) = compat.supports_developer_role {
            spec.compat.supports_developer_role = value;
        }
        if let Some(value) = compat.allows_sampling_parameters {
            spec.compat.allows_sampling_parameters = value;
        }
        if spec.protocol != ApiProtocol::OpenAiChatCompletions {
            anyhow::bail!("CP2 supports only open_ai_chat_completions presets");
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

    fn resolve(mut file: FileConfig, overrides: EnvOverrides) -> Result<Self> {
        let workspace = overrides
            .workspace
            .or(file.workspace)
            .map(Ok)
            .unwrap_or_else(env::current_dir)
            .context("failed to resolve workspace path")?;
        let database_path = overrides
            .database_path
            .or(file.database_path)
            .unwrap_or_else(|| workspace.join(".sumi/agent.db"));

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
            database_path: env::var_os("SUMI_DATABASE_PATH").map(PathBuf::from),
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

    #[test]
    fn parses_toml_and_derives_database_path() {
        let file: FileConfig = toml::from_str(
            r#"
conversation_id = "conversation-1"
workspace = "/workspace"
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
        assert_eq!(
            config.database_path,
            PathBuf::from("/workspace/.sumi/agent.db")
        );
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
database_path = "/file.db"

[model]
id = "file-model"
"#,
        )
        .expect("valid config");
        let overrides = EnvOverrides {
            conversation_id: Some("from-env".to_owned()),
            workspace: Some(PathBuf::from("/env-workspace")),
            database_path: Some(PathBuf::from("/env.db")),
            model_id: Some("env-model".to_owned()),
            ..EnvOverrides::default()
        };

        let config = Config::resolve(file, overrides).expect("resolved config");

        assert_eq!(config.conversation_id, "from-env");
        assert_eq!(config.workspace, PathBuf::from("/env-workspace"));
        assert_eq!(config.database_path, PathBuf::from("/env.db"));
        assert_eq!(config.model.id.as_deref(), Some("env-model"));
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
        assert_eq!(spec.compat.max_tokens_field, MaxTokensField::MaxTokens);
        assert!(!spec.compat.zai_tool_stream);
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
        assert_eq!(spec.compat.thinking_format, ThinkingFormat::ProviderDefault);
        assert!(!spec.compat.requires_reasoning_content_on_assistant);
        assert!(!spec.compat.zai_tool_stream);
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
        assert!(!spec.compat.zai_tool_stream);
        assert_eq!(spec.compat.thinking_format, ThinkingFormat::ProviderDefault);
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
