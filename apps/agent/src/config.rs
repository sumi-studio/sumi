use std::{
    env,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::Deserialize;

const DEFAULT_CONVERSATION_ID: &str = "default";
const DEFAULT_STATE_DIR: &str = "/var/lib/sumi";

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
pub struct ModelConfig {
    pub preset: Option<String>,
    pub id: Option<String>,
    pub base_url: Option<String>,
    pub api_key_env: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    conversation_id: Option<String>,
    workspace: Option<PathBuf>,
    state_dir: Option<PathBuf>,
    system_prompt: Option<String>,
    model: ModelConfig,
}

#[derive(Debug, Default)]
struct EnvOverrides {
    conversation_id: Option<String>,
    workspace: Option<PathBuf>,
    state_dir: Option<PathBuf>,
    system_prompt: Option<String>,
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

        let file = match env::var_os("SUMI_CONFIG") {
            Some(path) => Some(load_file(Path::new(&path)).await?),
            None => None,
        };

        Self::resolve(file.unwrap_or_default(), EnvOverrides::from_env())
    }

    fn resolve(mut file: FileConfig, overrides: EnvOverrides) -> Result<Self> {
        let workspace = overrides
            .workspace
            .or(file.workspace)
            .map(Ok)
            .unwrap_or_else(env::current_dir)
            .context("failed to resolve workspace path")?;
        let database_path = overrides
            .state_dir
            .or(file.state_dir)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_STATE_DIR))
            .join("agent.db");

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
                .unwrap_or_default(),
            model: file.model,
        })
    }
}

impl EnvOverrides {
    fn from_env() -> Self {
        Self {
            conversation_id: env::var("SUMI_CONVERSATION_ID").ok(),
            workspace: env::var_os("SUMI_WORKSPACE").map(PathBuf::from),
            state_dir: env::var_os("SUMI_STATE_DIR").map(PathBuf::from),
            system_prompt: env::var("SUMI_SYSTEM_PROMPT").ok(),
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
    fn parses_toml_and_derives_database_path_from_state_dir() {
        let file: FileConfig = toml::from_str(
            r#"
conversation_id = "conversation-1"
workspace = "/workspace"
state_dir = "/state"
system_prompt = "Be useful."

[model]
preset = "kimi"
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
                preset: Some("kimi".to_owned()),
                id: Some("kimi-k3".to_owned()),
                base_url: Some("https://example.test/v1".to_owned()),
                api_key_env: Some("EXAMPLE_API_KEY".to_owned()),
            }
        );
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
}
