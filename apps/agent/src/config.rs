use std::{
    env,
    ffi::OsString,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
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
                .unwrap_or_default(),
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
}
