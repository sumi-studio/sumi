//! Exact credential removal at the request's public error boundary.
use std::{cell::RefCell, future::Future};
use zeroize::Zeroizing;

tokio::task_local! {
    static KEY: RefCell<Option<Zeroizing<String>>>;
}

pub(super) async fn scope<T>(future: impl Future<Output = T>) -> T {
    KEY.scope(RefCell::new(None), future).await
}

pub(super) fn record(key: &str) {
    let _ = KEY.try_with(|slot| *slot.borrow_mut() = Some(Zeroizing::new(key.to_owned())));
}

pub(super) fn redact_known(text: &str, key: &str) -> String {
    if key.is_empty() {
        text.to_owned()
    } else {
        text.replace(key, "[REDACTED]")
    }
}

pub(super) fn redact(text: &str) -> String {
    KEY.try_with(|slot| match slot.borrow().as_deref() {
        Some(key) => redact_known(text, key),
        None => text.to_owned(),
    })
    .unwrap_or_else(|_| text.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{
        assembler::MessageAssembler,
        finish_failure,
        model::ModelSpec,
        types::{ProviderEvent, Usage},
    };

    #[tokio::test]
    async fn opaque_credential_echo_is_removed_from_public_error_and_code() {
        scope(async {
            let key = "opaque-credential-without-a-known-provider-prefix";
            use crate::provider::api_credentials::{
                self, ApiCredentialBinding, ApiCredentialError, ApiCredentialResolver,
                ApiCredentialSource,
            };
            struct Resolver;
            #[async_trait::async_trait]
            impl ApiCredentialResolver for Resolver {
                async fn resolve(
                    &self,
                    _: &ApiCredentialBinding,
                ) -> Result<Zeroizing<String>, ApiCredentialError> {
                    Ok(Zeroizing::new(
                        "opaque-credential-without-a-known-provider-prefix".into(),
                    ))
                }
            }
            let binding = ApiCredentialBinding {
                connection_id: "connection".into(),
                version: "v1".into(),
                human_id: "human".into(),
            };
            let mut spec = ModelSpec::preset("openai-chat").unwrap();
            spec.public_endpoint = true;
            spec.account_scope = binding.account_scope();
            spec.api_credentials = Some(ApiCredentialSource::new(
                binding,
                std::sync::Arc::new(Resolver),
            ));
            let resolved =
                api_credentials::resolve(&spec, None, &tokio_util::sync::CancellationToken::new())
                    .await
                    .unwrap();
            assert_eq!(resolved.as_deref(), Some(key));
            let (tx, mut rx) = tokio::sync::mpsc::channel(1);
            let mut assembler = MessageAssembler::new();
            assembler.apply(&ProviderEvent::Start).unwrap();
            finish_failure(
                &tx,
                &mut assembler,
                &spec,
                Usage::default(),
                format!("rejected credential {key}"),
                &format!("invalid:{key}"),
                false,
            )
            .await;
            let ProviderEvent::Error { output, .. } = rx.recv().await.unwrap() else {
                panic!("missing error")
            };
            assert_eq!(
                output.message.error_message.as_deref(),
                Some("rejected credential [REDACTED]")
            );
            assert_eq!(
                output.message.provider_code.as_deref(),
                Some("invalid:[REDACTED]")
            );
        })
        .await;
    }

    #[tokio::test]
    async fn concurrent_requests_keep_distinct_credentials() {
        let (a, b) = tokio::join!(
            scope(async {
                record("opaque-A");
                tokio::task::yield_now().await;
                redact("opaque-A opaque-B")
            }),
            scope(async {
                record("opaque-B");
                tokio::task::yield_now().await;
                redact("opaque-A opaque-B")
            })
        );
        assert_eq!(a, "[REDACTED] opaque-B");
        assert_eq!(b, "opaque-A [REDACTED]");
        assert_eq!(redact("opaque-A"), "opaque-A");
    }
}
