//! Human and PA Feedback use the same Inbox. Only explicitly authored content
//! is sent; the adapter never harvests conversations or selects another actor.
use super::{
    AdapterIdentity, AppActionDescriptor, BoundExecutionArguments, BoundToolAdapter, BoundToolCtx,
    BoundToolExecutionOutcome, CapabilityClass, DescribeError, ResourceScope, ReviewProjection,
    Tool, ToolBindCtx, ToolBinding, ToolCtx, ToolError, ToolOutput, ToolRisk,
};
use crate::{
    apiclient::feedback::{FeedbackApi, FeedbackRequest},
    provider::types::{ToolDefinition, UserContent},
};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(crate) struct FeedbackTool {
    api: Arc<dyn FeedbackApi>,
}
impl FeedbackTool {
    pub fn new(api: Arc<dyn FeedbackApi>) -> Self {
        Self { api }
    }
    async fn perform(
        &self,
        request: &FeedbackRequest,
        cancel: &CancellationToken,
    ) -> Result<ToolOutput, ToolError> {
        let result = tokio::select! { biased; _ = cancel.cancelled() => return Err(ToolError::Cancelled), result = self.api.request(request) => result };
        let (mut value, is_error) = match result {
            Ok(value) => (value, false),
            Err(error) => (json!({"error":error.error}), true),
        };
        // A transport failure may happen after commit. Retrying this exact nonce
        // returns the original message rather than sending a second copy.
        if let Some(id) = request.request_id() {
            value
                .as_object_mut()
                .ok_or_else(|| ToolError::Protocol("Feedback result must be an object".into()))?
                .insert("request_id".into(), json!(id));
        }
        let text = serde_json::to_string(&value)
            .map_err(|_| ToolError::Protocol("Feedback result could not be rendered".into()))?;
        Ok(ToolOutput {
            content: vec![UserContent::Text { text }],
            details: value,
            is_error,
        })
    }
}
#[async_trait]
impl Tool for FeedbackTool {
    fn def(&self) -> ToolDefinition {
        ToolDefinition {
            name:"feedback".into(),
            description:"Use your own Feedback Inbox to exchange reports, ideas, questions and replies with Sumi開発. Feedback is built into Sumi and requires no Workspace or personal installation. bootstrap reports service availability and scope=builtin. This scope describes app provision, not access to other people’s reports; your authenticated identity and each thread’s access rules still apply. create sends only the title/body you explicitly write; reply sends body to the thread's author and development recipients. No conversation or attachment is included automatically. open returns a page of messages and status activities; next_cursor loads older events. list returns explicit is_summary excerpts (up to 320 characters); use open for full content. list next_cursor continues the list. read explicitly marks through an observed revision; opening does not mark read. status requires the latest thread revision. For create/reply, request_id is generated if omitted: reuse the returned request_id with identical content after an uncertain send. Do not assume an error means nothing was sent.".into(),
            parameters:json!({"type":"object","properties":{
                "action":{"type":"string","enum":["bootstrap","list","open","create","reply","status","read"]},
                "thread_id":{"type":"string"},"title":{"type":"string","minLength":1,"maxLength":160},"body":{"type":"string","minLength":1,"maxLength":20000},
                "status":{"type":"string","enum":["open","resolved","all"]},"revision":{"type":"integer","minimum":1},"cursor":{"type":"string"},"request_id":{"type":"string","description":"UUIDv4; reuse a previous send's key only for the exact same request."}
            },"required":["action"],"additionalProperties":false})
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
            "Feedback requires bound execution".into(),
        ))
    }
}
#[async_trait]
impl BoundToolAdapter for FeedbackTool {
    fn identity(&self) -> AdapterIdentity {
        AdapterIdentity::new("sumi.feedback", 1).expect("static adapter identity")
    }
    fn reviewer_read_capable(&self) -> bool {
        false
    }
    async fn bind(&self, ctx: ToolBindCtx<'_>) -> Result<ToolBinding, DescribeError> {
        let mut request: FeedbackRequest =
            serde_json::from_value(Value::Object(ctx.args.as_object().clone()))
                .map_err(|_| DescribeError::InvalidArguments)?;
        if !request.validate() {
            return Err(DescribeError::InvalidArguments);
        }
        request.assign_request_id();
        let arguments =
            serde_json::to_value(&request).map_err(|_| DescribeError::InvalidArguments)?;
        let scope = request.thread_id().map_or_else(
            || ResourceScope::collection("feedback", "thread"),
            |id| ResourceScope::resource("feedback", "thread", id),
        );
        Ok(ToolBinding::new(
            AppActionDescriptor::new(
                request.action(),
                if request.is_read_only() {
                    CapabilityClass::Read
                } else {
                    CapabilityClass::Mutate
                },
                vec![scope],
            )?,
            ReviewProjection::from_value(
                json!({"actor":"self","destination":"Sumi開発 / Feedback Inbox","visibility":"thread author and configured development recipients","request":arguments}),
            )?,
            BoundExecutionArguments::from_value(arguments)?,
        ))
    }
    async fn execute(&self, ctx: BoundToolCtx<'_>) -> Result<BoundToolExecutionOutcome, ToolError> {
        let request: FeedbackRequest =
            serde_json::from_value(Value::Object(ctx.args.as_object().clone()))
                .map_err(|_| ToolError::InvalidArguments)?;
        if !request.validate() {
            return Err(ToolError::InvalidArguments);
        }
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
        apiclient::feedback::FeedbackError,
        provider::types::{ToolCall, ToolInvocationRoute, ValidatedToolArguments},
        tools::{ToolRegistryBuilder, WorkspacePaths},
    };
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeApi {
        requests: Mutex<Vec<Value>>,
    }
    #[async_trait]
    impl FeedbackApi for FakeApi {
        async fn request(&self, request: &FeedbackRequest) -> Result<Value, FeedbackError> {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request.wire());
            if requests.len() == 1 {
                Err(FeedbackError::new("transport_error"))
            } else {
                Ok(
                    json!({"id":"0198f0f4-9b72-7000-8000-000000000201","body":"画像の通知が届かない"}),
                )
            }
        }
    }
    fn args(value: Value) -> ValidatedToolArguments {
        serde_json::from_value(value).unwrap()
    }

    #[tokio::test]
    async fn feedback_binding_reviews_exact_content_and_rejects_author_override() {
        let api = Arc::new(FakeApi::default());
        let tool = Arc::new(FeedbackTool::new(api.clone()));
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        let arguments = args(
            json!({"action":"create","title":"通知について","body":"画像の通知が届かない\nhttps://example.com/details"}),
        );
        let binding = tool
            .bind(ToolBindCtx {
                args: &arguments,
                workspace: &workspace,
                executor_identity: None,
            })
            .await
            .unwrap();
        assert_eq!(binding.descriptor.capability, CapabilityClass::Mutate);
        assert_eq!(
            binding.review_projection.as_object()["request"]["body"],
            "画像の通知が届かない\nhttps://example.com/details"
        );
        assert_eq!(
            binding.review_projection.as_object()["destination"],
            "Sumi開発 / Feedback Inbox"
        );
        assert_eq!(
            binding.review_projection.as_object()["request"]["request_id"],
            binding.execution_arguments.as_object()["request_id"]
        );
        for invalid in [
            json!({"action":"create","title":"report","body":"text","author":"human"}),
            json!({"action":"bootstrap","author":"human"}),
            json!({"action":"create","title":"report","body":"text","request_id":"not-a-uuid"}),
            json!({"action":"reply","thread_id":"0198f0f4-9b72-7000-8000-000000000201","body":"  "}),
            json!({"action":"open","thread_id":"0198f0f4-9b72-7000-8000-000000000201","body":"unexpected send"}),
        ] {
            let arguments = args(invalid);
            assert!(
                tool.bind(ToolBindCtx {
                    args: &arguments,
                    workspace: &workspace,
                    executor_identity: None
                })
                .await
                .is_err()
            );
        }
        assert!(api.requests.lock().unwrap().is_empty());
        let arguments =
            args(json!({"action":"open","thread_id":"0198f0f4-9b72-7000-8000-000000000201"}));
        let binding = tool
            .bind(ToolBindCtx {
                args: &arguments,
                workspace: &workspace,
                executor_identity: None,
            })
            .await
            .unwrap();
        assert_eq!(binding.descriptor.capability, CapabilityClass::Read);
    }

    #[tokio::test]
    async fn feedback_uncertain_send_exposes_nonce_for_identical_retry() {
        let api = Arc::new(FakeApi::default());
        let mut builder = ToolRegistryBuilder::default();
        builder
            .register(Arc::new(FeedbackTool::new(api.clone())))
            .unwrap();
        let registry = builder.build();
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        let mut arguments = json!({"action":"create","title":"通知","body":"画像の通知が届かない"});
        for (id, is_error) in [("first", true), ("retry", false)] {
            let sealed = registry
                .bind(
                    &ToolCall {
                        provider_call_id: None,
                        id: id.into(),
                        name: "feedback".into(),
                        route: ToolInvocationRoute::Normal,
                        arguments: args(arguments.clone()),
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
            let value = &outcome.output.details;
            arguments["request_id"] = value["request_id"].clone();
            assert!(value["request_id"].as_str().is_some());
        }
        let requests = api.requests.lock().unwrap();
        assert_eq!(requests[0], requests[1]);
    }
}
