//! Explicit offline maintenance. This mode never hydrates a session or starts
//! providers, tools, executors, or gateway delivery.
use std::{env, path::Path, sync::Arc};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

use crate::{
    config::Config,
    runtime::contracts::PersonalityAgentId,
    store::{AgentScope, EnvironmentKeyProvider, KeyProvider, Store},
};

const MAX_REQUEST_BYTES: usize = 2048;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PreExternalAuthorization {
    personality_agent_id: PersonalityAgentId,
    through_seq: u64,
}

fn parse_request(bytes: &[u8]) -> Result<PreExternalAuthorization> {
    if bytes.len() > MAX_REQUEST_BYTES {
        bail!("maintenance request exceeds 2048 bytes");
    }
    serde_json::from_slice(bytes).context("invalid pre-external authorization request")
}

async fn authorize_request(
    request: PreExternalAuthorization,
    configured_agent: &PersonalityAgentId,
    database_path: &Path,
    key_provider: Arc<dyn KeyProvider>,
) -> Result<PreExternalAuthorization> {
    // This check precedes Store::open, which can create files and run migrations.
    if &request.personality_agent_id != configured_agent {
        bail!("maintenance request targets a different configured personality agent");
    }
    let store = Store::open(
        database_path,
        AgentScope::new(configured_agent.clone()),
        key_provider,
    )
    .await?;
    let result = store
        .authorize_pre_external_event_boundary(request.through_seq)
        .await;
    store.pool().close().await;
    result?;
    Ok(request)
}

/// The operator must stop writers and verify the pre-external deployment and
/// inventory before invoking this mode. A v1 row is not operator authorization.
pub(crate) async fn run_authorize_pre_external_events() -> Result<()> {
    let mut bytes = Vec::new();
    tokio::io::stdin()
        .take((MAX_REQUEST_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .context("failed to read maintenance request")?;
    let request = parse_request(&bytes)?;
    let config = Config::load().await?;
    // Reject the wrong target even before reading wrapping-key material.
    if request.personality_agent_id != config.personality_agent_id {
        bail!("maintenance request targets a different configured personality agent");
    }
    let key_provider = Arc::new(EnvironmentKeyProvider::from_env(
        "SUMI_AGENT_WRAPPING_KEY",
        env::var("SUMI_AGENT_WRAPPING_KEY_ID")
            .unwrap_or_else(|_| "local-env-wrapping-key/v1".to_owned()),
    )?);
    let result = authorize_request(
        request,
        &config.personality_agent_id,
        &config.database_path,
        key_provider,
    )
    .await?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{DATA_KEY_BYTES, WrappingKey};

    struct TestKeys;
    #[async_trait::async_trait]
    impl KeyProvider for TestKeys {
        async fn current_key(&self) -> Result<WrappingKey> {
            Ok(WrappingKey::new("maintenance-test", [7; DATA_KEY_BYTES]))
        }
        async fn key_by_id(&self, id: &str) -> Result<WrappingKey> {
            if id != "maintenance-test" {
                bail!("unknown maintenance fixture key");
            }
            self.current_key().await
        }
    }
    fn paid() -> PersonalityAgentId {
        "0198f0f4-9b72-7000-8000-000000000001".parse().unwrap()
    }

    #[test]
    fn maintenance_input_is_bounded_and_explicit() {
        assert!(parse_request(&vec![b' '; MAX_REQUEST_BYTES + 1]).is_err());
        assert!(parse_request(br#"{"through_seq":0}"#).is_err());
        let valid = format!(r#"{{"personality_agent_id":"{}","through_seq":0}}"#, paid());
        assert_eq!(parse_request(valid.as_bytes()).unwrap().through_seq, 0);
        assert!(parse_request(valid.replace("0}", "0,\"implicit\":true}").as_bytes()).is_err());
    }

    #[tokio::test]
    async fn wrong_target_is_rejected_before_creating_database() {
        let root = std::env::temp_dir().join(format!("sumi-maintenance-{}", uuid::Uuid::now_v7()));
        let request = PreExternalAuthorization {
            personality_agent_id: "0198f0f4-9b72-7000-8000-000000000002".parse().unwrap(),
            through_seq: 0,
        };
        let error = authorize_request(request, &paid(), &root.join("agent.db"), Arc::new(TestKeys))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("different configured personality agent")
        );
        assert!(!root.exists());
    }

    #[tokio::test]
    async fn stale_head_never_installs_an_implicit_boundary() {
        let root = std::env::temp_dir().join(format!("sumi-maintenance-{}", uuid::Uuid::now_v7()));
        let path = root.join("agent.db");
        let request = PreExternalAuthorization {
            personality_agent_id: paid(),
            through_seq: 1,
        };
        let error = authorize_request(request, &paid(), &path, Arc::new(TestKeys))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("cutover head changed"));
        let store = Store::open(&path, AgentScope::new(paid()), Arc::new(TestKeys))
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM legacy_event_audience")
                .fetch_one(store.pool())
                .await
                .unwrap(),
            0
        );
        store.pool().close().await;
        let result = authorize_request(
            PreExternalAuthorization {
                personality_agent_id: paid(),
                through_seq: 0,
            },
            &paid(),
            &path,
            Arc::new(TestKeys),
        )
        .await
        .unwrap();
        assert_eq!(result.through_seq, 0);
        let store = Store::open(&path, AgentScope::new(paid()), Arc::new(TestKeys))
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT through_seq FROM legacy_event_audience")
                .fetch_one(store.pool())
                .await
                .unwrap(),
            0
        );
        store.pool().close().await;
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
}
