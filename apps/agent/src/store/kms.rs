//! Cloud KMS hierarchy: tenant KEK -> agent key -> conversation data keys.
//!
//! `KmsKeyProvider` is the non-environment `KeyProvider` used for Cloud
//! acceptance.  The plaintext agent key is kept in process memory only and is
//! never written to SQLite, logs, or environment variables.  A faithful
//! fail-closed `MockKmsClient` is supplied for tests and local Cloud
//! configuration drills; a live external KMS integration is left as a
//! pluggable `KmsClient` implementation.
#![allow(dead_code)]

use std::{
    collections::{HashMap, HashSet},
    env, fmt,
    sync::Mutex,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

use super::crypto::{
    CONTENT_NONCE_BYTES, DATA_KEY_BYTES, WrappingKey, aead_decrypt, aead_encrypt, canonical_fields,
    decode_hex_key, random_nonce,
};

pub const AGENT_KEY_WRAP_VERSION: u8 = 1;

const AGENT_KEY_WRAP_DOMAIN: &[u8] = b"sumi-agent-key-wrap/v1";

pub(crate) fn agent_key_wrap_aad(
    tenant_id: &str,
    agent_id: &str,
    key_id: &str,
    wrap_key_id: &str,
) -> Vec<u8> {
    canonical_fields(
        AGENT_KEY_WRAP_DOMAIN,
        [
            tenant_id.as_bytes(),
            agent_id.as_bytes(),
            key_id.as_bytes(),
            wrap_key_id.as_bytes(),
        ],
    )
}

/// KMS backend contract.  The runtime calls this only at runtime to unwrap
/// agent keys; the tenant KEK and wrapped agent key blobs are owned by the
/// KMS / control plane and never enter agent storage.
#[async_trait]
pub trait KmsClient: Send + Sync {
    /// Return the agent key id that the control plane currently considers
    /// current for this agent.
    async fn current_key_id(&self) -> Result<String>;

    /// Unwrap one agent key.  Fail closed for revoked, disabled, unknown, or
    /// cryptographically invalid material.
    async fn unwrap_agent_key(&self, key_id: &str) -> Result<WrappingKey>;
}

/// In-memory, fail-closed KMS stand-in.  It stores agent keys wrapped by a
/// single tenant KEK and can disable individual key ids to prove revocation.
pub struct MockKmsClient {
    tenant_id: String,
    agent_id: String,
    kek: WrappingKey,
    current: Mutex<String>,
    keys: Mutex<HashMap<String, WrappedAgentKey>>,
    disabled: Mutex<HashSet<String>>,
}

struct WrappedAgentKey {
    wrap_key_id: String,
    nonce: [u8; CONTENT_NONCE_BYTES],
    wrapped: Vec<u8>,
}

impl fmt::Debug for MockKmsClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MockKmsClient")
            .field("tenant_id", &self.tenant_id)
            .field("agent_id", &self.agent_id)
            .field("kek", &self.kek)
            .field(
                "current",
                &self.current.lock().unwrap_or_else(|p| p.into_inner()),
            )
            .finish()
    }
}

impl MockKmsClient {
    pub fn new(
        tenant_id: impl Into<String>,
        agent_id: impl Into<String>,
        kek: WrappingKey,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            agent_id: agent_id.into(),
            kek,
            current: Mutex::new(String::new()),
            keys: Mutex::new(HashMap::new()),
            disabled: Mutex::new(HashSet::new()),
        }
    }

    pub fn register_agent_key(
        &self,
        key_id: impl Into<String>,
        agent_key: &WrappingKey,
    ) -> Result<()> {
        let key_id = key_id.into();
        let wrap_key_id = self.kek.key_id().to_owned();
        let nonce = random_nonce()?;
        let aad = agent_key_wrap_aad(&self.tenant_id, &self.agent_id, &key_id, &wrap_key_id);
        let wrapped = aead_encrypt(self.kek.bytes(), &nonce, agent_key.bytes(), &aad)
            .context("failed to wrap agent key with tenant KEK")?;
        let mut keys = self
            .keys
            .lock()
            .map_err(|_| anyhow!("mock KMS keys lock poisoned"))?;
        keys.insert(
            key_id,
            WrappedAgentKey {
                wrap_key_id,
                nonce,
                wrapped,
            },
        );
        Ok(())
    }

    pub fn set_current_key_id(&self, key_id: impl Into<String>) {
        *self.current.lock().unwrap_or_else(|p| p.into_inner()) = key_id.into();
    }

    pub fn disable_key(&self, key_id: &str) {
        self.disabled
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key_id.to_owned());
    }

    fn inner_unwrap(&self, key_id: &str) -> Result<WrappingKey> {
        if self
            .disabled
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(key_id)
        {
            bail!("KMS key {key_id} is disabled or revoked");
        }
        let keys = self
            .keys
            .lock()
            .map_err(|_| anyhow!("mock KMS keys lock poisoned"))?;
        let wrapped = keys
            .get(key_id)
            .ok_or_else(|| anyhow!("KMS has no wrapped agent key for {key_id}"))?;
        let aad = agent_key_wrap_aad(
            &self.tenant_id,
            &self.agent_id,
            key_id,
            &wrapped.wrap_key_id,
        );
        let mut plaintext = aead_decrypt(self.kek.bytes(), &wrapped.nonce, &wrapped.wrapped, &aad)
            .context("KMS agent-key unwrap failed")?;
        let bytes: [u8; DATA_KEY_BYTES] = plaintext
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("KMS unwrapped agent key has wrong length"))?;
        plaintext.zeroize();
        Ok(WrappingKey::new(key_id, bytes))
    }
}

#[async_trait]
impl KmsClient for MockKmsClient {
    async fn current_key_id(&self) -> Result<String> {
        let id = self
            .current
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if id.is_empty() {
            bail!("mock KMS has no current agent key id");
        }
        Ok(id)
    }

    async fn unwrap_agent_key(&self, key_id: &str) -> Result<WrappingKey> {
        self.inner_unwrap(key_id)
    }
}

/// Bounded HTTP KMS client. The tenant KEK lives in the remote KMS; only the
/// plaintext agent key ever enters this process, and only over TLS (or explicit
/// test HTTP).
#[derive(Clone)]
pub struct HttpKmsClient {
    client: reqwest::Client,
    base_url: Url,
    api_token: String,
    tenant_id: String,
    agent_id: String,
    current_key_id: Option<String>,
    allow_http: bool,
}

impl fmt::Debug for HttpKmsClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpKmsClient")
            .field("base_url", &self.base_url)
            .field("tenant_id", &self.tenant_id)
            .field("agent_id", &self.agent_id)
            .field("current_key_id", &self.current_key_id)
            .field("allow_http", &self.allow_http)
            .field("api_token", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CurrentKeyResponse {
    key_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct UnwrapKeyResponse {
    plaintext_hex: String,
}

impl HttpKmsClient {
    pub fn from_env() -> Result<Self> {
        let base_url = env::var("SUMI_KMS_URL")
            .context("SUMI_KMS_URL is required when SUMI_KEY_PROVIDER=kms")?;
        let api_token = env::var("SUMI_KMS_API_TOKEN")
            .context("SUMI_KMS_API_TOKEN is required when SUMI_KEY_PROVIDER=kms")?;
        let tenant_id = env::var("SUMI_TENANT_ID").unwrap_or_else(|_| "local-tenant".to_owned());
        let agent_id = env::var("SUMI_AGENT_ID").unwrap_or_else(|_| "local-agent".to_owned());
        let current_key_id = env::var("SUMI_KMS_AGENT_KEY_ID").ok();
        let allow_http = env::var("SUMI_KMS_ALLOW_HTTP")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        Self::new(
            &base_url,
            api_token,
            tenant_id,
            agent_id,
            current_key_id,
            allow_http,
        )
    }

    pub fn new(
        base_url: &str,
        api_token: String,
        tenant_id: String,
        agent_id: String,
        current_key_id: Option<String>,
        allow_http: bool,
    ) -> Result<Self> {
        let base_url = Url::parse(base_url)
            .with_context(|| format!("SUMI_KMS_URL is not a valid URL: {base_url}"))?;
        if !allow_http && base_url.scheme() != "https" {
            bail!("SUMI_KMS_URL must use https unless SUMI_KMS_ALLOW_HTTP=1");
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("failed to build KMS HTTP client")?;
        Ok(Self {
            client,
            base_url,
            api_token,
            tenant_id,
            agent_id,
            current_key_id,
            allow_http,
        })
    }

    fn auth_header(&self) -> String {
        format!("Bearer {}", self.api_token)
    }

    fn map_kms_error(&self, key_id: &str, status: reqwest::StatusCode) -> anyhow::Error {
        match status {
            reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::GONE => {
                anyhow!("KMS key {key_id} is disabled or revoked")
            }
            reqwest::StatusCode::NOT_FOUND => anyhow!("KMS has no wrapped key {key_id}"),
            _ => anyhow!("KMS refused unwrap for {key_id}: HTTP {status}"),
        }
    }
}

#[async_trait]
impl KmsClient for HttpKmsClient {
    async fn current_key_id(&self) -> Result<String> {
        if let Some(id) = self.current_key_id.as_ref() {
            return Ok(id.clone());
        }
        let url = self
            .base_url
            .join(&format!("v1/agents/{}/current-key", self.agent_id))
            .context("failed to build KMS current-key URL")?;
        let response = self
            .client
            .get(url)
            .header("Authorization", self.auth_header())
            .send()
            .await
            .context("KMS current-key request failed")?;
        let status = response.status();
        if !status.is_success() {
            bail!("KMS current-key request failed: HTTP {status}");
        }
        let body: CurrentKeyResponse = response
            .json()
            .await
            .context("KMS current-key response is not valid JSON")?;
        if body.key_id.is_empty() {
            bail!("KMS returned an empty current key id");
        }
        Ok(body.key_id)
    }

    async fn unwrap_agent_key(&self, key_id: &str) -> Result<WrappingKey> {
        let url = self
            .base_url
            .join(&format!("v1/keys/{key_id}/unwrap"))
            .context("failed to build KMS unwrap URL")?;
        let response = self
            .client
            .post(url)
            .header("Authorization", self.auth_header())
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "tenant_id": &self.tenant_id,
                "agent_id": &self.agent_id,
            }))
            .send()
            .await
            .context("KMS unwrap request failed")?;
        let status = response.status();
        if !status.is_success() {
            return Err(self.map_kms_error(key_id, status));
        }
        let body: UnwrapKeyResponse = response
            .json()
            .await
            .context("KMS unwrap response is not valid JSON")?;
        let bytes = decode_hex_key(&body.plaintext_hex)
            .with_context(|| format!("KMS returned an invalid plaintext for {key_id}"))?;
        Ok(WrappingKey::new(key_id, bytes))
    }
}

/// `KeyProvider` implementation backed by a KMS / control-plane tenant KEK.
///
/// A current-key request always performs an unwrap.  Caching a plaintext
/// current key is unsafe because a control plane can revoke that key without
/// changing its id; the unwrap is the authorization check as well as the key
/// retrieval operation.
pub struct KmsKeyProvider {
    client: std::sync::Arc<dyn KmsClient>,
}

impl fmt::Debug for KmsKeyProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KmsKeyProvider")
            .finish_non_exhaustive()
    }
}

impl KmsKeyProvider {
    pub fn new(client: std::sync::Arc<dyn KmsClient>) -> Result<Self> {
        Ok(Self { client })
    }
}

#[async_trait]
impl super::KeyProvider for KmsKeyProvider {
    async fn current_key(&self) -> Result<WrappingKey> {
        let key_id = self
            .client
            .current_key_id()
            .await
            .context("failed to retrieve current KMS key id")?;
        self.client
            .unwrap_agent_key(&key_id)
            .await
            .with_context(|| format!("KMS refused current agent key {key_id}"))
    }

    async fn key_by_id(&self, key_id: &str) -> Result<WrappingKey> {
        self.client
            .unwrap_agent_key(key_id)
            .await
            .with_context(|| format!("KMS refused agent key {key_id}"))
    }
}

#[cfg(test)]
mod tests {
    use super::super::crypto::{DATA_KEY_BYTES, KeyProvider, WrappingKey};
    use super::*;

    fn test_kek() -> WrappingKey {
        WrappingKey::new("tenant-kek/v1", [0x11; DATA_KEY_BYTES])
    }

    fn test_agent_key(id: &str, byte: u8) -> WrappingKey {
        WrappingKey::new(id, [byte; DATA_KEY_BYTES])
    }

    #[tokio::test]
    async fn kms_key_provider_rechecks_current_key_authorization() {
        let client = std::sync::Arc::new(MockKmsClient::new("tenant-1", "agent-1", test_kek()));
        client
            .register_agent_key("agent-key/v1", &test_agent_key("agent-key/v1", 0x22))
            .unwrap();
        client.set_current_key_id("agent-key/v1");

        let provider = KmsKeyProvider::new(client.clone()).unwrap();

        let key = provider.current_key().await.unwrap();
        assert_eq!(key.key_id(), "agent-key/v1");

        // The same key id must still be unwrapped, because revocation does
        // not necessarily change the control-plane current key id.
        let again = provider.current_key().await.unwrap();
        assert_eq!(again.bytes(), key.bytes());

        client.disable_key("agent-key/v1");
        assert!(provider.current_key().await.is_err());
    }

    #[tokio::test]
    async fn disabled_kms_key_fails_closed() {
        let client = MockKmsClient::new("tenant-1", "agent-1", test_kek());
        client
            .register_agent_key("agent-key/v1", &test_agent_key("agent-key/v1", 0x33))
            .unwrap();
        client.disable_key("agent-key/v1");
        client.set_current_key_id("agent-key/v1");

        let provider = KmsKeyProvider::new(std::sync::Arc::new(client)).unwrap();

        assert!(provider.current_key().await.is_err());
        assert!(provider.key_by_id("agent-key/v1").await.is_err());
    }

    #[tokio::test]
    async fn unknown_kms_key_fails_closed() {
        let client = MockKmsClient::new("tenant-1", "agent-1", test_kek());
        client.set_current_key_id("missing");
        let provider = KmsKeyProvider::new(std::sync::Arc::new(client)).unwrap();
        assert!(provider.current_key().await.is_err());
    }

    mod http_kms {
        use std::collections::{HashMap, HashSet};
        use std::sync::{Arc, Mutex};

        use super::*;
        use axum::{
            Json, Router,
            extract::{Path, State},
            http::StatusCode,
            routing::{get, post},
        };
        use tokio::net::TcpListener;

        fn to_hex(bytes: &[u8]) -> String {
            bytes.iter().map(|b| format!("{b:02x}")).collect()
        }

        struct KmsState {
            current: Mutex<String>,
            keys: Mutex<HashMap<String, [u8; DATA_KEY_BYTES]>>,
            disabled: Mutex<HashSet<String>>,
        }

        async fn current_key(State(state): State<Arc<KmsState>>) -> Json<CurrentKeyResponse> {
            Json(CurrentKeyResponse {
                key_id: state.current.lock().unwrap().clone(),
            })
        }

        async fn unwrap_key(
            Path(key_id): Path<String>,
            State(state): State<Arc<KmsState>>,
        ) -> Result<Json<UnwrapKeyResponse>, StatusCode> {
            if state.disabled.lock().unwrap().contains(&key_id) {
                return Err(StatusCode::FORBIDDEN);
            }
            let keys = state.keys.lock().unwrap();
            let bytes = keys.get(&key_id).ok_or(StatusCode::NOT_FOUND)?;
            Ok(Json(UnwrapKeyResponse {
                plaintext_hex: to_hex(bytes),
            }))
        }

        async fn start_server(state: Arc<KmsState>) -> String {
            let app = Router::new()
                .route("/v1/agents/{agent_id}/current-key", get(current_key))
                .route("/v1/keys/{key_id}/unwrap", post(unwrap_key))
                .with_state(state);
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            // Give the server a moment to start accepting connections.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            format!("http://127.0.0.1:{port}")
        }

        #[tokio::test]
        async fn http_kms_unwraps_current_key_and_revoked_key_fails_closed() {
            let state = Arc::new(KmsState {
                current: Mutex::new("agent-key-v1".to_owned()),
                keys: Mutex::new(HashMap::from([(
                    "agent-key-v1".to_owned(),
                    [0x44; DATA_KEY_BYTES],
                )])),
                disabled: Mutex::new(HashSet::new()),
            });
            let base_url = start_server(state.clone()).await;
            let client = HttpKmsClient::new(
                &base_url,
                "test-token".to_owned(),
                "tenant-1".to_owned(),
                "agent-1".to_owned(),
                None,
                true,
            )
            .unwrap();

            let provider = KmsKeyProvider::new(Arc::new(client)).unwrap();
            let key = provider.current_key().await.unwrap();
            assert_eq!(key.key_id(), "agent-key-v1");
            assert_eq!(key.bytes(), &[0x44; DATA_KEY_BYTES]);

            // Revoke the current key without changing its id. This must fail
            // even though the key was successfully unwrapped above.
            state
                .disabled
                .lock()
                .unwrap()
                .insert("agent-key-v1".to_owned());
            assert!(provider.current_key().await.is_err());
            assert!(provider.key_by_id("agent-key-v1").await.is_err());
        }

        #[tokio::test]
        async fn http_kms_static_current_key_id_skips_fetch() {
            let state = Arc::new(KmsState {
                current: Mutex::new("unused".to_owned()),
                keys: Mutex::new(HashMap::from([(
                    "agent-key-v2".to_owned(),
                    [0x55; DATA_KEY_BYTES],
                )])),
                disabled: Mutex::new(HashSet::new()),
            });
            let base_url = start_server(state).await;
            let client = HttpKmsClient::new(
                &base_url,
                "test-token".to_owned(),
                "tenant-1".to_owned(),
                "agent-1".to_owned(),
                Some("agent-key-v2".to_owned()),
                true,
            )
            .unwrap();

            let key = client.current_key_id().await.unwrap();
            assert_eq!(key, "agent-key-v2");
        }

        #[tokio::test]
        async fn http_kms_unknown_key_fails_closed() {
            let state = Arc::new(KmsState {
                current: Mutex::new("agent-key-v1".to_owned()),
                keys: Mutex::new(HashMap::new()),
                disabled: Mutex::new(HashSet::new()),
            });
            let base_url = start_server(state).await;
            let client = HttpKmsClient::new(
                &base_url,
                "test-token".to_owned(),
                "tenant-1".to_owned(),
                "agent-1".to_owned(),
                None,
                true,
            )
            .unwrap();

            assert!(client.unwrap_agent_key("missing").await.is_err());
        }

        #[tokio::test]
        async fn http_kms_requires_https_by_default() {
            assert!(
                HttpKmsClient::new(
                    "http://insecure.example.com",
                    "token".to_owned(),
                    "t".to_owned(),
                    "a".to_owned(),
                    None,
                    false,
                )
                .is_err()
            );
        }
    }
}
