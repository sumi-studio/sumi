//! The memory fork must preserve the parent input through the real HTTP path.
//! Fixture responses establish transport completion, not model or cache quality.

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Request, Response},
    routing::post,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{ModelSpec, RequestOptions, model::StructuredOutputSchema, types::*};
use crate::memory::context_assembler::{ContextAssembler, bind_native_replay_for_test};

const IMAGE: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j2XcAAAAASUVORK5CYII=";
const MIDDLE_SUMMARY: &str = "The intervening source was checked before the latest correction.";
const MIDDLE_RAW: &str = "Intervening original observation replaced by its summary.";
const DIRECTIVE: &str = "Reorganize the designated record using the complete preceding context.";

async fn parent_prompt(
    spec: &ModelSpec,
    native: bool,
) -> (PromptContext, Vec<VisibleMemoryFragment>) {
    let mut visible_memory = Vec::new();
    let timestamp = DateTime::parse_from_rfc3339("2026-09-07T23:40:12.123456789Z")
        .unwrap()
        .with_timezone(&Utc);
    let image = UserContent::Image {
        data: IMAGE.into(),
        mime_type: "image/png".into(),
    };
    let mut content = vec![AssistantContent::Text {
        text: "I will check the original source.".into(),
        wire_item_index: 1,
    }];
    if spec.protocol != ApiProtocol::OpenAiResponses {
        content.insert(
            0,
            AssistantContent::Thinking {
                thinking: "Preserve the source and its later correction.".into(),
                signature_field: "reasoning_content".into(),
                wire_item_index: 0,
            },
        );
    }
    content.push(AssistantContent::ToolCall {
        tool_call: ToolCall {
            provider_call_id: None,
            id: "call_parent".into(),
            name: "read_file".into(),
            route: ToolInvocationRoute::Elevated,
            arguments: serde_json::from_value(json!({"path":"notes.txt"})).unwrap(),
        },
        wire_item_index: 2,
    });
    let messages = [
        Message::User(UserMessage {
            incoming_source: Some(super::incoming_timing_tests::external_source_fixture(false)),
            incoming_timing: Some(IncomingEventTiming {
                previous_receipt: None,
            }),
            content: vec![
                UserContent::Text {
                    text: "Original source".into(),
                },
                image.clone(),
            ],
            timestamp,
        }),
        Message::Assistant(AssistantMessage {
            content,
            model: spec.id.clone(),
            provider: spec.provider.clone(),
            origin: spec.origin(),
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp,
        }),
        Message::ToolResult(ToolResultMessage {
            provider_call_id: None,
            tool_call_id: "call_parent".into(),
            tool_name: "read_file".into(),
            content: vec![
                UserContent::Text {
                    text: "Original tool observation".into(),
                },
                image,
            ],
            details: json!({"complete":false}),
            is_error: false,
            timestamp,
        }),
        Message::User(UserMessage {
            incoming_source: None,
            incoming_timing: Some(IncomingEventTiming {
                previous_receipt: Some(IncomingEventReceipt {
                    received_at: timestamp,
                    command_seq: 10,
                }),
            }),
            content: vec![UserContent::Text {
                text: "Latest correction outside the edit target".into(),
            }],
            timestamp: timestamp + chrono::Duration::seconds(125),
        }),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, message)| ContextMessage::Persisted {
        id: format!("parent-{}", (index + 1) * 10),
        seq: ((index + 1) * 10) as u64,
        message,
    })
    .collect();
    let mut prompt = PromptContext::new(
        "Parent instructions with current permissions.".into(),
        vec![],
        messages,
        vec![],
        vec![
            ToolDefinition {
                name: "read_file".into(),
                description: "Read the source.".into(),
                parameters: json!({"type":"object","properties":{"path":{"$ref":"#/$defs/path"}},"required":["path"],"additionalProperties":false,"$defs":{"path":{"type":"string"}}}),
            },
            // The unchanged existing Responses SSE fixture calls this tool.
            ToolDefinition {
                name: "weather".into(),
                description: "Read weather.".into(),
                parameters: json!({"type":"object","properties":{"city":{"type":"string"}},"required":["city"],"additionalProperties":false}),
            },
        ],
    );
    if spec.protocol != ApiProtocol::OpenAiChatCompletions {
        let anchor = ProviderContextAnchor {
            message_id: "parent-20".into(),
            message_seq: 20,
        };
        prompt.provider_context.push(ProviderContextItem {
            retention_owner: anchor.clone(),
            origin_message: Some(anchor),
            wire_item_index: Some(0),
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: spec.protocol,
                item: if spec.protocol == ApiProtocol::OpenAiResponses {
                    json!({"type":"reasoning","id":"rs_parent","summary":[],"encrypted_content":"opaque-parent-reasoning"})
                } else {
                    json!({"type":"thinking_signature","signature":"parent-thinking-signature"})
                },
            },
        });
    }
    if native {
        let coverage = NativeCompactionCoverage {
            through_message_seq: 3,
            context_fingerprint: super::context_fingerprint::compute_context_fingerprint(
                spec,
                &prompt.system_prompt,
                &prompt.tools,
            )
            .unwrap(),
        };
        prompt.provider_context.push(ProviderContextItem {
            retention_owner: ProviderContextAnchor {
                message_id: "native-3".into(),
                message_seq: 3,
            },
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: if spec.protocol == ApiProtocol::OpenAiResponses {
                ProviderContextPayload::OpenAiCompactedWindow {
                    items: vec![
                        json!({"type":"compaction","encrypted_content":"opaque-parent-window"}),
                    ],
                    coverage,
                }
            } else {
                ProviderContextPayload::AnthropicCompaction {
                    block: json!({"type":"compaction","content":"opaque-parent-window"}),
                    coverage,
                }
            },
        });
        bind_native_replay_for_test(&mut prompt, spec.origin(), 3, Some(40)).unwrap();
    } else {
        // Build the parent through the production assembler, with B promoted
        // between still-raw A and C. The summary has no fabricated transcript ID
        // or sequence; only its original batch establishes its chronological slot.
        use crate::memory::{
            BatchState, CompactResult, DecryptedMemorySummary, L0Batch, ThreeLayerMemory,
            estimate::{
                ProviderContextItemWithFootprint, TokenCalibration, eviction_footprint_for_payload,
            },
        };
        let middle = ContextMessage::Persisted {
            id: "parent-35".into(),
            seq: 35,
            message: Message::Assistant(AssistantMessage {
                content: vec![AssistantContent::Text {
                    text: MIDDLE_RAW.into(),
                    wire_item_index: 0,
                }],
                model: spec.id.clone(),
                provider: spec.provider.clone(),
                origin: spec.origin(),
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                provider_code: None,
                interrupted: false,
                timestamp: timestamp + chrono::Duration::seconds(60),
            }),
        };
        let mut life_log = prompt.messages.clone();
        life_log.insert(3, middle.clone());
        let mut memory = ThreeLayerMemory::new(
            std::collections::VecDeque::new(),
            TokenCalibration::default(),
        );
        memory.push_l0(L0Batch::new(life_log[..3].to_vec(), 1, 0, 100));
        let mut middle_batch = L0Batch::new(vec![middle], 2, 0, 30);
        middle_batch.state = BatchState::Sealed;
        let source_batch = middle_batch.id;
        memory.push_l0(middle_batch);
        memory.push_l0(L0Batch::new(life_log[4..].to_vec(), 3, 0, 30));
        memory.store_compact_result(
            source_batch,
            CompactResult {
                original_seq_span: Some(crate::memory::OriginalSequenceSpan {
                    from_seq: 35,
                    to_seq: 35,
                }),
                summary: DecryptedMemorySummary::new(MIDDLE_SUMMARY.into()),
                est_tokens: 15,
                time_range: (
                    timestamp + chrono::Duration::seconds(60),
                    timestamp + chrono::Duration::seconds(60),
                ),
            },
        );
        memory.promote_l0_to_l1(source_batch).unwrap();
        let provider_context = prompt
            .provider_context
            .iter()
            .map(|item| {
                ProviderContextItemWithFootprint::new(
                    item.clone(),
                    eviction_footprint_for_payload(spec, &item.payload).unwrap(),
                )
            })
            .collect();
        let assembler = ContextAssembler::from_prompt_with_spec(prompt, spec.clone()).unwrap();
        assembler
            .install_hydrated_memory(memory, &life_log, provider_context)
            .unwrap();
        let assembled = assembler
            .assemble_with_estimate(&life_log, 0)
            .await
            .unwrap();
        visible_memory = assembled.visible_memory;
        prompt = assembled.prompt;
        assert!(prompt.messages.iter().any(|message| matches!(message,
            ContextMessage::Synthetic { message: Message::User(user) }
                if user.incoming_timing.is_none() && user.content.iter().any(|content|
                    matches!(content, UserContent::Text { text } if text.contains(MIDDLE_SUMMARY)))
        )), "L1 must remain synthetic, not acquire a persisted transcript identity");
    }
    (prompt, visible_memory)
}

fn anthropic_content(messages: &[Value]) -> Vec<(Value, Value)> {
    messages
        .iter()
        .flat_map(|message| {
            message["content"].as_array().unwrap().iter().map(|block| {
                let mut block = block.clone();
                block.as_object_mut().unwrap().remove("cache_control");
                (message["role"].clone(), block)
            })
        })
        .collect()
}

#[tokio::test]
async fn memory_fork_keeps_parent_context_on_the_actual_provider_wire() {
    let (record, mut captured) =
        tokio::sync::mpsc::unbounded_channel::<(String, HeaderMap, Vec<u8>)>();
    let app = Router::new().fallback(post(move |request: Request<Body>| {
        let record = record.clone();
        async move {
            let (parts, body) = request.into_parts();
            let path = parts.uri.path().to_owned();
            let fixture = match path.as_str() {
                "/chat/completions" => include_str!("../../tests/fixtures/kimi_text.sse"),
                "/responses" => include_str!("../../tests/fixtures/openai_responses_official.sse"),
                "/messages" => include_str!("../../tests/fixtures/anthropic_messages_official.sse"),
                _ => panic!("unexpected provider endpoint: {path}"),
            };
            record
                .send((
                    path,
                    parts.headers,
                    to_bytes(body, 1024 * 1024).await.unwrap().to_vec(),
                ))
                .unwrap();
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from(fixture))
                .unwrap()
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    // Abort on assertion failure too: the fixture server never outlives this test.
    struct Server(tokio::task::JoinHandle<()>);
    impl Drop for Server {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _server = Server(server);

    for (preset, native) in [
        ("kimi-k3", false),
        ("openai-responses", false),
        ("anthropic", false),
        ("openai-responses", true),
        ("anthropic", true),
    ] {
        let mut spec = ModelSpec::preset(preset).unwrap();
        spec.base_url = base_url.clone();
        spec.account_scope = "parent-wire-test".into();
        let options = RequestOptions {
            session_id: Some("019927a0-0000-7000-8000-000000000001".to_owned()),
            max_tokens: Some(8192),
            temperature: Some(0.2),
            tool_choice: Some(json!("auto")),
            reasoning_effort: Some("max".into()),
            structured_output: Some(StructuredOutputSchema {
                name: "parent_response".into(),
                description: "Parent response format.".into(),
                schema: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}),
            }),
            native_compaction: native,
        };
        let (prompt, visible_memory) = parent_prompt(&spec, native).await;
        let snapshot =
            ParentContextSnapshot::capture_with_memory(&prompt, &spec, &options, &visible_memory)
                .unwrap();
        assert_eq!(snapshot.visible_memory().len(), usize::from(!native));
        let fork = snapshot
            .fork_with_directive(UserMessage {
                incoming_source: None,
                incoming_timing: None,
                content: vec![UserContent::Text {
                    text: DIRECTIVE.into(),
                }],
                timestamp: Utc::now(),
            })
            .unwrap();
        let mut requests = Vec::new();
        // Send the original parent independently, so a lossy capture cannot
        // accidentally make both sides of this comparison identically wrong.
        for (request_spec, context, request_options) in [
            (spec.clone(), prompt, options),
            (snapshot.spec().clone(), fork, snapshot.options().clone()),
        ] {
            let mut events = super::stream_with_api_key(
                request_spec,
                context,
                request_options,
                CancellationToken::new(),
                Some("local-fixture-key".into()),
            );
            let terminal = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    match events.recv().await.expect("terminal event") {
                        done @ ProviderEvent::Done { .. } => break done,
                        ProviderEvent::Error { output, .. } => {
                            panic!("{preset} native={native}: {:?}", output.message)
                        }
                        _ => {}
                    }
                }
            })
            .await
            .expect("local provider response deadline");
            assert!(matches!(terminal, ProviderEvent::Done { .. }));
            let (path, headers, bytes) = captured.try_recv().expect("actual HTTP request captured");
            assert_eq!(format!("{base_url}{path}"), spec.endpoint());
            assert_eq!(headers["content-type"], "application/json");
            assert!(!headers.contains_key("content-encoding"));
            if spec.protocol == ApiProtocol::AnthropicMessages {
                assert_eq!(headers["x-api-key"], "local-fixture-key");
                assert_eq!(
                    headers["anthropic-version"],
                    super::context_fingerprint::ANTHROPIC_VERSION
                );
                assert_eq!(
                    headers["anthropic-beta"].to_str().unwrap(),
                    spec.anthropic_compat().unwrap().beta_headers.join(",")
                );
            } else {
                assert_eq!(headers["authorization"], "Bearer local-fixture-key");
            }
            requests.push(serde_json::from_slice::<Value>(&bytes).unwrap());
        }
        let [mut parent, mut fork]: [Value; 2] = requests.try_into().unwrap();
        let field = if spec.protocol == ApiProtocol::OpenAiResponses {
            "input"
        } else {
            "messages"
        };
        let parent_items = parent.as_object_mut().unwrap().remove(field).unwrap();
        let fork_items = fork.as_object_mut().unwrap().remove(field).unwrap();
        assert_eq!(
            parent, fork,
            "{preset} native={native}: system/tools/model/options changed"
        );
        assert_eq!(parent["model"], spec.id);
        assert_eq!(parent["tools"].as_array().unwrap().len(), 2);
        let parent_items = parent_items.as_array().unwrap();
        let fork_items = fork_items.as_array().unwrap();
        if spec.protocol == ApiProtocol::AnthropicMessages {
            // Adjacent users merge and the last-block cache hint moves forward.
            // Compare ordered role/payload blocks; this is not a cache-hit claim.
            let prefix = anthropic_content(parent_items);
            let extended = anthropic_content(fork_items);
            assert_eq!(&extended[..prefix.len()], prefix);
            assert_eq!(extended.len(), prefix.len() + 1);
            assert_eq!(
                extended.last().unwrap(),
                &(json!("user"), json!({"type":"text","text":DIRECTIVE}))
            );
        } else {
            assert_eq!(&fork_items[..parent_items.len()], parent_items);
            assert_eq!(fork_items.len(), parent_items.len() + 1);
            assert_eq!(fork_items.last().unwrap()["content"][0]["text"], DIRECTIVE);
        }
        let encoded = serde_json::to_string(parent_items).unwrap();
        assert!(encoded.contains(IMAGE), "parent image must reach the wire");
        assert!(encoded.contains("Latest correction outside the edit target"));
        assert_eq!(encoded.matches("[Received ").count(), 2);
        assert_eq!(encoded.matches("[Source ").count(), 1);
        assert!(encoded.contains("messaging_mention"));
        assert!(encoded.contains("Received 2026-09-07 23:40:12 UTC"));
        assert!(encoded.contains("2 minutes 5 seconds since the previous incoming message"));
        assert_eq!(encoded.contains("opaque-parent-window"), native);
        if !native {
            let before = encoded.find("Original tool observation").unwrap();
            let summary = encoded
                .find(MIDDLE_SUMMARY)
                .expect("assembled L1 reaches actual HTTP");
            let after = encoded
                .find("Latest correction outside the edit target")
                .unwrap();
            assert!(
                before < summary && summary < after,
                "{preset}: raw A → summary B → raw C order"
            );
            assert_eq!(
                encoded.matches(MIDDLE_SUMMARY).count(),
                1,
                "summary must not also be prepended"
            );
            assert!(
                !encoded.contains(MIDDLE_RAW),
                "replaced B must not leak back into parent wire"
            );
        } else {
            // Exact native replay owns its opaque prefix and canonical suffix;
            // this branch is not evidence about ordering inside that prefix.
            assert!(!encoded.contains(MIDDLE_SUMMARY));
        }
        let (call_id, arguments) = match spec.protocol {
            ApiProtocol::OpenAiChatCompletions => {
                let assistant = parent_items
                    .iter()
                    .find(|item| item["role"] == "assistant")
                    .unwrap();
                assert_eq!(
                    assistant["reasoning_content"],
                    "Preserve the source and its later correction."
                );
                let call = &assistant["tool_calls"][0];
                (
                    call["id"].clone(),
                    serde_json::from_str::<Value>(call["function"]["arguments"].as_str().unwrap())
                        .unwrap(),
                )
            }
            ApiProtocol::OpenAiResponses => {
                let reasoning = parent_items
                    .iter()
                    .find(|item| item["type"] == "reasoning")
                    .unwrap();
                assert_eq!(reasoning["encrypted_content"], "opaque-parent-reasoning");
                let call = parent_items
                    .iter()
                    .find(|item| item["type"] == "function_call")
                    .unwrap();
                (
                    call["call_id"].clone(),
                    serde_json::from_str::<Value>(call["arguments"].as_str().unwrap()).unwrap(),
                )
            }
            ApiProtocol::AnthropicMessages => {
                let blocks = anthropic_content(parent_items);
                let thinking = &blocks
                    .iter()
                    .find(|(_, block)| block["type"] == "thinking")
                    .unwrap()
                    .1;
                assert_eq!(thinking["signature"], "parent-thinking-signature");
                assert_eq!(
                    thinking["thinking"],
                    "Preserve the source and its later correction."
                );
                let call = &blocks
                    .iter()
                    .find(|(_, block)| block["type"] == "tool_use")
                    .unwrap()
                    .1;
                (call["id"].clone(), call["input"].clone())
            }
        };
        assert_eq!(call_id, "call_parent");
        assert_eq!(
            arguments,
            json!({"route":"elevated","input":{"path":"notes.txt"}})
        );
        assert!(
            captured.try_recv().is_err(),
            "exactly parent and fork were sent"
        );
    }
}

fn scope_parent_tool_identity(prompt: &mut PromptContext) {
    let internal = scoped_tool_call_id("parent-20", "call_parent");
    for entry in &mut prompt.messages {
        let message = match entry {
            ContextMessage::Persisted { message, .. } | ContextMessage::Synthetic { message } => {
                message
            }
        };
        match message {
            Message::Assistant(assistant) => {
                for content in &mut assistant.content {
                    if let AssistantContent::ToolCall { tool_call, .. } = content {
                        tool_call.provider_call_id = Some(tool_call.id.clone());
                        tool_call.id = internal.clone();
                    }
                }
            }
            Message::ToolResult(result) => {
                result.provider_call_id = Some(result.tool_call_id.clone());
                result.tool_call_id = internal.clone();
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn scoped_tool_identity_preserves_exact_provider_request_and_opaque_prefix() {
    for (preset, native) in [
        ("kimi-k3", false),
        ("openai-responses", false),
        ("anthropic", false),
        ("openai-responses", true),
        ("anthropic", true),
        ("chatgpt-responses", false),
    ] {
        let spec = ModelSpec::preset(preset).unwrap();
        // Clone the same assembled snapshot: independently generated memory batches
        // have different IDs, which would test fixture randomness instead of identity.
        let (raw, _) = parent_prompt(&spec, native).await;
        let provenance = raw.verified_replay_provenance().unwrap();
        let mut scoped = raw.clone();
        scoped.replay_provenance = None;
        scope_parent_tool_identity(&mut scoped);
        // Production assigns identity before assembly seals the send view.
        // Seal this fixture with the same origin and coverage as its raw twin.
        match provenance {
            Some(VerifiedReplayProvenance::SumiNormalized {
                provider_origin,
                canonical_through_seq,
            }) => crate::memory::context_assembler::bind_sumi_replay_for_origin_test(
                &mut scoped,
                provider_origin,
                canonical_through_seq,
            )
            .unwrap(),
            Some(VerifiedReplayProvenance::ProviderNativeExact {
                provider_origin,
                native_coverage_through_seq,
                canonical_suffix_through_seq,
            }) => bind_native_replay_for_test(
                &mut scoped,
                provider_origin,
                native_coverage_through_seq,
                canonical_suffix_through_seq,
            )
            .unwrap(),
            None => {}
        }
        assert_eq!(
            raw.provider_context, scoped.provider_context,
            "opaque bytes/metadata unchanged"
        );
        let options = RequestOptions {
            native_compaction: native,
            ..RequestOptions::default()
        };
        let build = |context: &PromptContext| match spec.protocol {
            ApiProtocol::OpenAiChatCompletions => {
                super::adapters::chat_completions::build_request(&spec, context, &options).unwrap()
            }
            ApiProtocol::OpenAiResponses => {
                super::adapters::responses::build_request(&spec, context, &options).unwrap()
            }
            ApiProtocol::AnthropicMessages => {
                super::adapters::anthropic::build_request(&spec, context, &options).unwrap()
            }
        };
        assert_eq!(
            build(&raw),
            build(&scoped),
            "identity split changed {preset} native={native} provider payload"
        );
        assert!(
            !build(&scoped)
                .to_string()
                .contains(&scoped_tool_call_id("parent-20", "call_parent"))
        );
    }
}

#[test]
fn scoped_tool_identity_is_stable_unambiguous_and_keeps_retained_wire_ids() {
    let scoped = scoped_tool_call_id("assistant-a", "echo_0");
    assert_eq!(scoped, scoped_tool_call_id("assistant-a", "echo_0"));
    assert_ne!(scoped, scoped_tool_call_id("assistant-b", "echo_0"));
    assert_ne!(
        scoped_tool_call_id("a", "bc"),
        scoped_tool_call_id("ab", "c")
    );
    let mut rejected = RejectedToolCall {
        id: "echo_0".into(),
        provider_call_id: None,
        name: "read_file".into(),
        error: ToolArgumentError::InvalidJson,
    };
    let legacy = serde_json::to_value(&rejected).unwrap();
    assert!(legacy.get("provider_call_id").is_none());
    assert_eq!(
        serde_json::from_value::<RejectedToolCall>(legacy)
            .unwrap()
            .wire_id(),
        "echo_0"
    );
    rejected.provider_call_id = Some(rejected.id.clone());
    rejected.id = scoped;
    assert_eq!(rejected.wire_id(), "echo_0");
}
