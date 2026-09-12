//! Resolve user-owned API credentials at each request, never from a boot-time key.
use super::model::ModelSpec;
use async_trait::async_trait;
use serde::Serialize;
use std::{env, fmt, sync::Arc};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ApiCredentialBinding {
    pub connection_id: String,
    pub version: String,
    pub human_id: String,
}
impl ApiCredentialBinding {
    pub fn from_env() -> Result<Option<Self>, ApiCredentialError> {
        let names = [
            "SUMI_API_CONNECTION_ID",
            "SUMI_API_CONNECTION_VERSION",
            "SUMI_API_CONNECTION_HUMAN_ID",
        ];
        let values: Result<Vec<_>, _> = names
            .iter()
            .map(|name| match env::var(name) {
                Ok(value) => Ok(Some(value)),
                Err(env::VarError::NotPresent) => Ok(None),
                Err(_) => Err(ApiCredentialError),
            })
            .collect();
        let values = values?;
        if values.iter().all(Option::is_none) {
            return Ok(None);
        }
        if values
            .iter()
            .any(|value| value.as_ref().is_none_or(|v| v.is_empty() || v.len() > 256))
        {
            return Err(ApiCredentialError);
        }
        Ok(Some(Self {
            connection_id: values[0].clone().unwrap(),
            version: values[1].clone().unwrap(),
            human_id: values[2].clone().unwrap(),
        }))
    }
    pub fn account_scope(&self) -> String {
        format!(
            "human-api:{}:{}:{}",
            self.human_id, self.connection_id, self.version
        )
    }
}

#[derive(Debug, thiserror::Error)]
#[error("user API connection is unavailable or has changed; reconnect before continuing")]
pub struct ApiCredentialError;

#[async_trait]
pub trait ApiCredentialResolver: Send + Sync {
    async fn resolve(
        &self,
        binding: &ApiCredentialBinding,
    ) -> Result<Zeroizing<String>, ApiCredentialError>;
}
#[derive(Clone)]
pub struct ApiCredentialSource {
    binding: ApiCredentialBinding,
    resolver: Arc<dyn ApiCredentialResolver>,
}
impl ApiCredentialSource {
    pub fn new(binding: ApiCredentialBinding, resolver: Arc<dyn ApiCredentialResolver>) -> Self {
        Self { binding, resolver }
    }
}
impl fmt::Debug for ApiCredentialSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiCredentialSource")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}
impl PartialEq for ApiCredentialSource {
    fn eq(&self, other: &Self) -> bool {
        self.binding == other.binding
    }
}
impl Eq for ApiCredentialSource {}

pub(super) async fn resolve(
    spec: &ModelSpec,
    fallback: Option<String>,
    cancel: &CancellationToken,
) -> Result<Option<String>, ApiCredentialError> {
    let Some(source) = &spec.api_credentials else {
        return if spec.public_endpoint {
            Err(ApiCredentialError)
        } else {
            Ok(fallback)
        };
    };
    if source.binding.account_scope() != spec.account_scope || !spec.public_endpoint {
        return Err(ApiCredentialError);
    }
    let mut key = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(ApiCredentialError),
        result = tokio::time::timeout(super::RESPONSE_HEADER_TIMEOUT, source.resolver.resolve(&source.binding)) => result.map_err(|_| ApiCredentialError)??,
    };
    if key.is_empty() || key.len() > 65536 || reqwest::header::HeaderValue::from_str(&key).is_err()
    {
        return Err(ApiCredentialError);
    }
    super::error_redaction::record(&key);
    Ok(Some(std::mem::take(&mut *key)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{
        model::RequestOptions,
        types::{PromptContext, ProviderEvent},
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct Resolver {
        allowed: AtomicBool,
        calls: AtomicUsize,
    }
    #[async_trait]
    impl ApiCredentialResolver for Resolver {
        async fn resolve(
            &self,
            _: &ApiCredentialBinding,
        ) -> Result<Zeroizing<String>, ApiCredentialError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.allowed.load(Ordering::SeqCst) {
                Ok(Zeroizing::new("user-key".into()))
            } else {
                Err(ApiCredentialError)
            }
        }
    }
    fn spec(preset: &str, resolver: Arc<Resolver>) -> ModelSpec {
        let binding = ApiCredentialBinding {
            connection_id: "connection".into(),
            version: "v1".into(),
            human_id: "human".into(),
        };
        let mut spec = ModelSpec::preset(preset).unwrap();
        spec.public_endpoint = true;
        spec.account_scope = binding.account_scope();
        spec.api_credentials = Some(ApiCredentialSource::new(binding, resolver));
        spec
    }
    fn context() -> PromptContext {
        PromptContext {
            system_prompt: "test".into(),
            memory_blocks: vec![],
            messages: vec![],
            provider_context: vec![],
            tools: vec![],
            replay_provenance: None,
        }
    }
    #[tokio::test]
    async fn every_request_reauthorizes_and_revocation_never_falls_back() {
        let resolver = Arc::new(Resolver {
            allowed: AtomicBool::new(true),
            calls: AtomicUsize::new(0),
        });
        let spec = spec("openai-chat", resolver.clone());
        let cancel = CancellationToken::new();
        for _ in 0..2 {
            assert_eq!(
                resolve(&spec, Some("operator-key".into()), &cancel)
                    .await
                    .unwrap()
                    .as_deref(),
                Some("user-key")
            );
        }
        resolver.allowed.store(false, Ordering::SeqCst);
        assert!(
            resolve(&spec, Some("operator-key".into()), &cancel)
                .await
                .is_err()
        );
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 3);
        let mut missing = spec.clone();
        missing.api_credentials = None;
        assert!(
            resolve(&missing, Some("operator-key".into()), &cancel)
                .await
                .is_err()
        );
        let mut mismatched = spec;
        mismatched.account_scope = "new-owner".into();
        assert!(
            resolve(&mismatched, Some("operator-key".into()), &cancel)
                .await
                .is_err()
        );
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 3);
    }
    #[tokio::test]
    async fn all_adapters_and_compaction_reject_revoked_connection_before_network() {
        let resolver = Arc::new(Resolver {
            allowed: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        });
        for preset in ["openai-chat", "openai-responses", "anthropic"] {
            let mut stream = crate::provider::stream_with_api_key(
                spec(preset, resolver.clone()),
                context(),
                RequestOptions::default(),
                CancellationToken::new(),
                Some("operator-key".into()),
            );
            let terminal = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while let Some(event) = stream.recv().await {
                    if let ProviderEvent::Error { output, .. } = event {
                        return output;
                    }
                }
                panic!("missing auth error for {preset}");
            })
            .await
            .unwrap();
            assert_eq!(
                terminal.message.provider_code.as_deref(),
                Some("provider_authentication_failed"),
                "{preset}"
            );
        }
        let result = crate::provider::compact_native_with_api_key(
            spec("openai-responses", resolver.clone()),
            context(),
            CancellationToken::new(),
            "operator-key".into(),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 4);
    }
}
