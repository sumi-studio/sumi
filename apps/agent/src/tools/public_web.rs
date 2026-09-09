//! Read an external page as tool evidence, never as a new Human instruction.

use super::{
    AdapterIdentity, AppActionDescriptor, BoundExecutionArguments, BoundToolAdapter, BoundToolCtx,
    BoundToolExecutionOutcome, CapabilityClass, DescribeError, ResourceScope, ReviewProjection,
    Tool, ToolBindCtx, ToolBinding, ToolCtx, ToolError, ToolOutput, ToolRisk,
};
use crate::{
    apiclient::public_web::{PublicWebApi, PublicWebRequest},
    provider::types::{ToolDefinition, UserContent},
    store::Redactor,
};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(crate) struct PublicWebTool {
    api: Arc<dyn PublicWebApi>,
}

impl PublicWebTool {
    pub fn new(api: Arc<dyn PublicWebApi>) -> Self {
        Self { api }
    }

    async fn perform(
        &self,
        request: &PublicWebRequest,
        cancel: &CancellationToken,
    ) -> Result<ToolOutput, ToolError> {
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(ToolError::Cancelled),
            result = self.api.read(request) => result,
        };
        let (value, is_error) = match result {
            Ok(page) => (serde_json::to_value(page), false),
            Err(error) => (serde_json::to_value(error), true),
        };
        let value = value
            .map_err(|_| ToolError::Protocol("public page result could not be rendered".into()))?;
        let text = serde_json::to_string(&value)
            .map_err(|_| ToolError::Protocol("public page result could not be rendered".into()))?;
        Ok(ToolOutput {
            content: vec![UserContent::Text { text }],
            details: value,
            is_error,
        })
    }
}

#[async_trait]
impl Tool for PublicWebTool {
    fn def(&self) -> ToolDefinition {
        ToolDefinition {
            name: "public_url_read".into(),
            description: "Read text from one public HTTPS URL. Sends a GET without cookies or account credentials; the complete URL, including its query, is reviewed before sending. Fragments are retained as references but not sent. Redirects require a separate call. Supports UTF-8 HTML and plain text, without JavaScript, PDF, or sign-in. Fetches at most 1 MiB within 15 seconds and returns up to 128 KiB of extracted text with its source and truncation status.".into(),
            parameters: json!({"type":"object","properties":{"url":{"type":"string","minLength":1,"maxLength":8192}},"required":["url"],"additionalProperties":false}),
        }
    }
    fn risk(&self) -> ToolRisk {
        ToolRisk::Exec
    }
    fn bound_adapter(self: Arc<Self>) -> Option<Arc<dyn BoundToolAdapter>> {
        Some(self)
    }
    async fn execute(&self, _: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
        Err(ToolError::Protocol(
            "public URL access requires bound execution".into(),
        ))
    }
}

#[async_trait]
impl BoundToolAdapter for PublicWebTool {
    fn identity(&self) -> AdapterIdentity {
        AdapterIdentity::new("sumi.public_web", 1).expect("static adapter identity")
    }
    fn reviewer_read_capable(&self) -> bool {
        false
    }
    async fn bind(&self, ctx: ToolBindCtx<'_>) -> Result<ToolBinding, DescribeError> {
        let request: PublicWebRequest =
            serde_json::from_value(Value::Object(ctx.args.as_object().clone()))
                .map_err(|_| DescribeError::InvalidArguments)?;
        if !request.validate() || Redactor::v1().redact_text(&request.url) != request.url {
            return Err(DescribeError::InvalidArguments);
        }
        let exact = json!({"url":request.url});
        Ok(ToolBinding::new(
            AppActionDescriptor::new(
                "fetch_public_url",
                CapabilityClass::Execute,
                vec![ResourceScope::resource("public_web", "url", &request.url)],
            )?,
            ReviewProjection::from_value(
                json!({"url":request.url,"method":"GET","credentials":"none","redirects":"not followed"}),
            )?,
            BoundExecutionArguments::from_value(exact)?,
        ))
    }
    async fn execute(&self, ctx: BoundToolCtx<'_>) -> Result<BoundToolExecutionOutcome, ToolError> {
        let request: PublicWebRequest =
            serde_json::from_value(Value::Object(ctx.args.as_object().clone()))
                .map_err(|_| ToolError::InvalidArguments)?;
        let receipt = ctx
            .committed_effect_permit
            .begin_local_effect()
            .complete(|| self.perform(&request, &ctx.cancel))
            .await?;
        Ok(BoundToolExecutionOutcome::without_live_post_commit(receipt))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        apiclient::public_web::{PublicWebError, PublicWebErrorCode, PublicWebPage},
        provider::types::{ToolCall, ToolInvocationRoute, ValidatedToolArguments},
        tools::{ToolRegistryBuilder, WorkspacePaths},
    };
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeApi {
        requests: Mutex<Vec<String>>,
    }
    #[async_trait]
    impl PublicWebApi for FakeApi {
        async fn read(&self, request: &PublicWebRequest) -> Result<PublicWebPage, PublicWebError> {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request.url.clone());
            if requests.len() == 1 {
                return Err(PublicWebError::new(PublicWebErrorCode::Timeout));
            }
            Ok(PublicWebPage {
                requested_url: request.url.clone(),
                fetched_url: request.network_url().into(),
                fetched_at: chrono::Utc::now(),
                status_code: 200,
                media_type: "text/plain".into(),
                title: None,
                text: "Observed page-specific content.".into(),
                body_bytes: 31,
                body_sha256: "a".repeat(64),
                text_truncated: false,
            })
        }
    }

    fn arguments(value: Value) -> ValidatedToolArguments {
        serde_json::from_value(value).unwrap()
    }

    #[tokio::test]
    async fn public_web_binding_keeps_full_url_and_cannot_be_reviewer_read() {
        let api = Arc::new(FakeApi::default());
        let tool = Arc::new(PublicWebTool::new(api.clone()));
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        let mut bindings = Vec::new();
        for url in [
            "https://example.com/page?q=one#part",
            "https://example.com/page?q=two#part",
        ] {
            let args = arguments(json!({"url":url}));
            let bound = tool
                .bind(ToolBindCtx {
                    args: &args,
                    workspace: &workspace,
                    executor_identity: None,
                })
                .await
                .unwrap();
            assert_eq!(bound.descriptor.capability, CapabilityClass::Execute);
            assert_eq!(
                bound.descriptor.resource_scopes,
                vec![ResourceScope::resource("public_web", "url", url)]
            );
            assert_eq!(bound.review_projection.as_object()["url"], url);
            assert_eq!(bound.execution_arguments.as_object()["url"], url);
            bindings.push(bound.descriptor);
        }
        assert_ne!(bindings[0], bindings[1]);
        for value in [
            json!({"url":"https://example.com/?access_token=abcdefghijk"}),
            json!({"url":"https://user:pass@example.com/"}),
            json!({"url":"https://example.com/", "headers":{"Authorization":"forged"}}),
            json!({"url":"https://example.com/", "personality_agent_id":"forged"}),
        ] {
            let args = arguments(value);
            assert!(
                tool.bind(ToolBindCtx {
                    args: &args,
                    workspace: &workspace,
                    executor_identity: None
                })
                .await
                .is_err()
            );
        }
        let mut builder = ToolRegistryBuilder::default();
        builder.register(tool).unwrap();
        assert!(builder.build().reviewer_read_definitions().is_empty());
        assert!(api.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn public_web_failure_returns_tool_evidence_and_later_read_succeeds() {
        let api = Arc::new(FakeApi::default());
        let mut builder = ToolRegistryBuilder::default();
        builder
            .register(Arc::new(PublicWebTool::new(api.clone())))
            .unwrap();
        let registry = builder.build();
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        for (id, is_error) in [("first", true), ("second", false)] {
            let sealed = registry
                .bind(
                    &ToolCall {
                        provider_call_id: None,
                        id: id.into(),
                        name: "public_url_read".into(),
                        route: ToolInvocationRoute::Normal,
                        arguments: arguments(json!({"url":"https://example.com/page?q=1#section"})),
                    },
                    id,
                    &workspace,
                )
                .await
                .unwrap();
            let outcome = registry
                .execute_bound(
                    crate::approval::authority::AuthorizedBoundInvocation::for_test(sealed),
                    CancellationToken::new(),
                    Arc::new(|_| {}),
                )
                .await
                .unwrap();
            assert_eq!(outcome.output.is_error, is_error);
            let UserContent::Text { text } = &outcome.output.content[0] else {
                panic!("text evidence");
            };
            let wire: Value = serde_json::from_str(text).unwrap();
            if is_error {
                assert_eq!(wire["error"], "timeout");
            } else {
                assert_eq!(wire["text"], "Observed page-specific content.");
                assert_eq!(wire["fetched_url"], "https://example.com/page?q=1");
                assert_eq!(
                    wire["requested_url"],
                    "https://example.com/page?q=1#section"
                );
            }
        }
        assert_eq!(api.requests.lock().unwrap().len(), 2);
    }
}
