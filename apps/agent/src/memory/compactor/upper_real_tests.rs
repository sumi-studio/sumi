// Opt-in, synthetic staged-history experiment. Never enabled by ordinary CI.
// Included inside compactor::tests so it uses the real EventWriter and compactor.
#[derive(Clone)]
struct UpperTrace {
    path: std::path::PathBuf,
    records: Arc<Mutex<Vec<serde_json::Value>>>,
}
impl UpperTrace {
    fn record(&self, value: serde_json::Value) {
        let mut records = self.records.lock().unwrap();
        records.push(value);
        std::fs::write(&self.path, serde_json::to_vec_pretty(&*records).unwrap()).unwrap();
    }
}
// Only fixed categories and numeric status cross this diagnostic boundary.
// Never serialize arbitrary provider code, URL, message, request, or response body.
fn upper_safe_provider_error(message: &AssistantMessage) -> serde_json::Value {
    let code = message.provider_code.as_deref().unwrap_or("");
    let http_status = code
        .strip_prefix("http_")
        .filter(|s| s.len() == 3)
        .and_then(|s| s.parse::<u16>().ok())
        .filter(|s| (100..=599).contains(s));
    let text = message.error_message.as_deref().unwrap_or("");
    let category = match code {
        "model_context_window_exceeded" | "context_length_exceeded" => "context_limit",
        "cancelled" => "cancelled",
        "transport_error" | "network_error" => "transport",
        "invalid_request_error" => "invalid_request",
        "authentication_error" => "authentication",
        "rate_limit_exceeded" | "rate_limit_error" => "rate_limit",
        _ if http_status.is_some() => "http",
        _ => "unclassified",
    };
    let diagnostic = if text.contains("Error from provider (Console Go): Upstream request failed") {
        "opencode_console_upstream_failed"
    } else if text.contains("missing API key environment variable") {
        "missing_api_key"
    } else if text.contains("maximum context length") || text.contains("context length exceeded") {
        "context_limit"
    } else if text.contains("reasoning_content") {
        "provider_mentions_reasoning_content"
    } else if text.contains("tool") && (text.contains("schema") || text.contains("invalid")) {
        "provider_mentions_tool_validation"
    } else {
        "unclassified"
    };
    json!({"category":category,"http_status":http_status,"diagnostic":diagnostic,
        "retryable_by_current_adapter_policy":crate::provider::retry::is_retryable(message),
        "message_present":!text.is_empty(),"provider_code_present":!code.is_empty()})
}

#[test]
fn upper_error_diagnostics_keep_status_without_reflecting_secrets() {
    let mut message = assistant("", StopReason::Error);
    message.provider_code = Some("http_400".into());
    message.error_message=Some("400: Error from provider (Console Go): Upstream request failed; secret-canary https://secret.invalid/token".into());
    let value = upper_safe_provider_error(&message);
    assert_eq!(value["http_status"], 400);
    assert_eq!(value["diagnostic"], "opencode_console_upstream_failed");
    assert!(!value.to_string().contains("secret"));
    message.provider_code = Some("secret-canary".into());
    message.error_message = Some("secret-canary".into());
    assert_eq!(
        upper_safe_provider_error(&message)["category"],
        "unclassified"
    );
    assert!(
        !upper_safe_provider_error(&message)
            .to_string()
            .contains("secret")
    );
}

fn upper_redacted_error_detail(text: &str, secrets: &[String]) -> serde_json::Value {
    let redact = |value: &str, limit: usize| {
        let mut value = value.to_owned();
        let mut secrets = secrets.iter().filter(|s| !s.is_empty()).collect::<Vec<_>>();
        secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
        for secret in secrets {
            value = value.replace(secret.as_str(), "[REDACTED]");
            let escaped = serde_json::to_string(secret).unwrap();
            value = value.replace(&escaped[1..escaped.len() - 1], "[REDACTED]");
        }
        value.chars().take(limit).collect::<String>()
    };
    let body = text
        .split_once(": ")
        .filter(|(prefix, _)| prefix.len() == 3 && prefix.bytes().all(|b| b.is_ascii_digit()))
        .map_or(text, |(_, body)| body);
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(body) {
        if let Some(error) = value.get("error") {
            let mut out = serde_json::Map::new();
            for (field, limit) in [("message", 1536), ("type", 128), ("code", 128)] {
                if let Some(value) = error.get(field).and_then(|v| v.as_str()) {
                    out.insert(field.into(), json!(redact(value, limit)));
                }
            }
            if !out.is_empty() {
                return serde_json::Value::Object(out);
            }
        }
    }
    json!({"message":redact(text,2048)})
}

#[test]
fn upper_error_detail_preserves_validation_reason_and_redacts_configured_secrets() {
    let secret = "private-key-731".to_owned();
    let text = format!(
        "400: {{\"error\":{{\"message\":\"Invalid max_tokens; token {secret}\",\"type\":\"invalid_request_error\",\"code\":\"invalid_value\"}}}}"
    );
    let result = upper_redacted_error_detail(&text, &[secret.clone()]);
    assert_eq!(result["message"], "Invalid max_tokens; token [REDACTED]");
    assert_eq!(result["type"], "invalid_request_error");
    assert!(!result.to_string().contains(&secret));
    let fallback = upper_redacted_error_detail(&(secret.clone() + &"x".repeat(5000)), &[secret]);
    assert_eq!(fallback["message"].as_str().unwrap().chars().count(), 2048);
    assert!(
        fallback["message"]
            .as_str()
            .unwrap()
            .starts_with("[REDACTED]")
    );
}

struct UpperObservedProvider {
    trace: UpperTrace,
    calls: Arc<AtomicUsize>,
    expected: ParentContextSnapshot,
    fork: bool,
    real: bool,
    started: Arc<Notify>,
    release: Option<Arc<Notify>>,
}
impl CompactProvider for UpperObservedProvider {
    fn start(
        &self,
        spec: ModelSpec,
        context: PromptContext,
        options: RequestOptions,
        cancel: CancellationToken,
    ) -> ProviderEventStream {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        assert!(
            call <= 4,
            "experiment never starts more than four provider streams"
        );
        assert_eq!(
            options.session_id.as_deref(),
            Some(PERSONALITY_AGENT_ID),
            "one owned PA session across parent, forks and reopen"
        );
        let parent = self.expected.prompt();
        assert_eq!(context.system_prompt, parent.system_prompt);
        assert_eq!(context.tools, parent.tools);
        assert_eq!(context.memory_blocks, parent.memory_blocks);
        assert_eq!(context.provider_context, parent.provider_context);
        assert_eq!(
            &context.messages[..parent.messages.len()],
            parent.messages.as_slice()
        );
        assert_eq!(
            context.messages.len(),
            parent.messages.len() + usize::from(self.fork)
        );
        assert_eq!(spec.id, self.expected.spec().id);
        assert_eq!(&options, self.expected.options());
        self.trace.record(json!({"stage":"request", "call":call, "model":spec.id,
            "fork":self.fork, "full_parent":parent, "actual_request_context":context,
            "visible_targets_available":self.expected.visible_memory(),
            "usage_presence":"raw usage presence is not exposed by this seam; normalized zero is not evidence of zero"}));
        let (tx, rx) = mpsc::channel(16);
        let trace = self.trace.clone();
        let started = self.started.clone();
        let release = self.release.clone();
        let child = cancel.child_token();
        let secret_values = std::env::vars()
            .filter(|(name, _)| {
                let upper = name.to_ascii_uppercase();
                name == &spec.api_key_env
                    || ["API_KEY", "TOKEN", "SECRET", "PASSWORD"]
                        .iter()
                        .any(|part| upper.contains(part))
            })
            .map(|(_, value)| value)
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();
        let result_spec = spec.clone();
        let real = self.real;
        let task = tokio::spawn(async move {
            let mut source =
                if real {
                    crate::provider::stream(spec, context, options, child.clone())
                } else {
                    FakeProvider { text: match call {
                    2 => "Birch: provisional October 12, venue uncertain, notes /notes/birch.",
                    3 => "Cedar: blue cover and tentative date. Details /notes/cedar-original.",
                    _ => "Birch is now October 21; venue remains unconfirmed.",
                }.into(), ..Default::default() }.start(spec, context, options, child.clone())
                };
            started.notify_one();
            if let Some(release) = release {
                tokio::select! { _ = release.notified() => {}, _ = child.cancelled() => return }
            }
            while let Some(event) = source.recv().await {
                if let ProviderEvent::Done { reason, output } = &event {
                    let text: String = output
                        .message
                        .content
                        .iter()
                        .filter_map(|item| match item {
                            AssistantContent::Text { text, .. } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect();
                    trace.record(json!({"stage":"response", "call":call,"reason":reason,
                        "text":text,"normalized_usage":output.message.usage,
                        "raw_usage_presence":"unknown", "cot_text":"not captured"}));
                }
                if let ProviderEvent::Error { reason, output } = &event {
                    trace.record(json!({"stage":"provider_error","call":call,"reason":reason,
                        "safe_diagnostic":upper_safe_provider_error(&output.message),
                        "provider_detail":upper_redacted_error_detail(output.message.error_message.as_deref().unwrap_or(""), &secret_values)}));
                }
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });
        ProviderEventStream::new(
            rx,
            cancel,
            result_spec.provider.clone(),
            result_spec.origin(),
        )
        .own_producer(task)
    }
}
async fn upper_real_parent(store: &Arc<Store>, spec: &ModelSpec) -> ParentContextSnapshot {
    let restored = hydrate(store).await;
    let assembler = crate::memory::context_assembler::ContextAssembler::from_prompt_with_spec(
        PromptContext::new("This is a synthetic continuous workspace conversation. Reply briefly to the latest user. No tools are available in this experiment.".into(),vec![],vec![],vec![],vec![]), spec.clone()).unwrap()
        .with_mode(crate::memory::overflow::AssemblyMode::SumiThreeLayer);
    assembler
        .install_hydrated_memory_at(
            ThreeLayerMemory::from_hydrated(restored.memory).unwrap(),
            restored.transcript_through_seq,
            restored.provider_context,
        )
        .unwrap();
    let mut assembled = assembler
        .assemble_with_estimate(&restored.messages, 0)
        .await
        .unwrap();
    // The experiment's current input is deliberately not a durable admission.
    // Stored past experience remains real; no fake cancellation is manufactured.
    let PublicMessage::User(question) = user(
        "What is the current Birch meeting date, what remains uncertain, and where are the original notes?",
    ) else {
        unreachable!()
    };
    assembled.prompt = assembled
        .prompt
        .with_appended_user_directive(question)
        .expect("append pending question preserving replay provenance");

    ParentContextSnapshot::capture_with_memory(
        &assembled.prompt,
        spec,
        &RequestOptions {
            session_id: Some(store.scope().personality_agent_id.to_string()),
            max_tokens: Some(8192),
            ..Default::default()
        },
        &assembled.visible_memory,
    )
    .unwrap()
}
async fn upper_fixture_stage(store: &Arc<Store>, text: String, kind: &str) {
    let provider = FakeProvider {
        text,
        ..Default::default()
    };
    let parent = assembled_upper_parent(store).await;
    if kind != "compact_l0" {
        prepare_upper_job(store, &parent).await.unwrap();
    }
    let (mut job, input) = claim_next_pending_job(store, &parent)
        .await
        .unwrap()
        .expect("fixture job");
    assert_eq!(job.kind.as_str(), kind);
    start_attempt(store, &mut job).await.unwrap();
    let result = compact(&input, CancellationToken::new(), &provider)
        .await
        .unwrap()
        .expect("shrinking fixture");
    assert!(complete_job(store, &job, &result).await.unwrap());
    let row = sqlx::query("SELECT * FROM memory_jobs WHERE kind=? AND status='completed'")
        .bind(kind)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert!(
        apply_completed_job(store.clone(), &parse_job(&row).unwrap())
            .await
            .unwrap(),
        "explicit fixture staging only"
    );
}
async fn upper_observed_answer(provider: &UpperObservedProvider) {
    let mut events = provider.start(
        provider.expected.spec().clone(),
        provider.expected.prompt().clone(),
        provider.expected.options().clone(),
        CancellationToken::new(),
    );
    while let Some(event) = events.recv().await {
        match event {
            ProviderEvent::Done {
                reason: StopReason::Stop,
                ..
            } => return,
            ProviderEvent::Error { .. } | ProviderEvent::Done { .. } => {
                panic!("incomplete real answer; inspect trace")
            }
            _ => {}
        }
    }
    panic!("answer stream closed without successful terminal");
}

#[tokio::test]
#[ignore = "four real provider calls; requires explicit operator approval and synthetic-only trace directory"]
async fn real_upper_memory_staged_history_acceptance() {
    assert_eq!(std::env::var("SUMI_UPPER_REAL_RUN").as_deref(), Ok("1"));
    let preset =
        std::env::var("SUMI_UPPER_REAL_PRESET").expect("explicit approved provider preset");
    let directory = std::path::PathBuf::from(
        std::env::var("SUMI_UPPER_REAL_OUTPUT").expect("fresh owned output directory"),
    );
    run_upper_memory_scenario(preset, directory, true).await;
}

#[tokio::test]
async fn upper_memory_acceptance_offline_rehearsal() {
    let path = std::env::temp_dir().join(format!("sumi-upper-offline-{}", Uuid::now_v7()));
    run_upper_memory_scenario("kimi-k3".into(), path.clone(), false).await;
    std::fs::remove_dir_all(path).unwrap();
}

async fn run_upper_memory_scenario(preset: String, directory: std::path::PathBuf, real: bool) {
    let spec = ModelSpec::preset(&preset).expect("known preset");
    std::fs::create_dir(&directory).expect("refuse existing output directory");
    let trace = UpperTrace {
        path: directory.join("trace.json"),
        records: Arc::new(Mutex::new(vec![])),
    };
    trace.record(json!({"stage":"experiment","real_transport":real,"kind":"synthetic staged initial history, not a longitudinal natural operating run","preset":preset,"planned_provider_streams":4,"raw_http_attempt_count":"unknown; adapters may retry","initial_staging":"fake provider outputs through authenticated real transitions; no claim these were real model compactions"}));
    let db = directory.join("state.sqlite");
    let store = Arc::new(
        Store::session_test_file_store(&db, PERSONALITY_AGENT_ID)
            .await
            .unwrap(),
    );
    // Explicit initial L2 stage. Text lengths, not mutated token counters, create pressure.
    seed_completed_authenticated_turn(&store,&chat_model(),"Observe Cedar.","upper-cedar-original",public_assistant(&"Cedar prototype: blue cover, date tentative and unconfirmed, original details /notes/cedar-original. ".repeat(3000)),vec![]).await;
    seed_completed_authenticated_turn(
        &store,
        &chat_model(),
        "Continue after Cedar.",
        "upper-cedar-tail",
        public_assistant("Continuing."),
        vec![],
    )
    .await;
    upper_fixture_stage(&store,"Archive observations: the cedar prototype used a blue cover. The date was tentative, not confirmed. Details remain in /notes/cedar-original. ".repeat(1800),"compact_l0").await;
    upper_fixture_stage(&store,"Cedar observations: blue cover, tentative date; original details in /notes/cedar-original. ".repeat(1800),"compact_l1").await;
    assert!(upper_layer_tokens(&store, 2).await.unwrap() > super::super::L2_LIMIT);
    seed_completed_authenticated_turn(&store,&chat_model(),"Observe the next project.","upper-real-original",public_assistant(&"Birch plan: the provisional meeting is October 12. The venue is unconfirmed. Notes live at /notes/birch. ".repeat(2200)),vec![]).await;
    seed_completed_authenticated_turn(
        &store,
        &chat_model(),
        "A separate later note: Birch is October 19; marker violet-kestrel-731.",
        "upper-later-l1-original",
        public_assistant(
            &"Later note: Birch is October 19; violet-kestrel-731; venue still unconfirmed. "
                .repeat(1000),
        ),
        vec![],
    )
    .await;
    seed_completed_authenticated_turn(&store,&chat_model(),"Later correction: Birch meeting is now October 19. Outside marker: violet-kestrel-731. What is the current date and what remains uncertain?","upper-real-tail",public_assistant("The correction is recorded in the current conversation."),vec![]).await;
    upper_fixture_stage(
        &store,
        "Birch plan: provisional meeting October 12; venue unconfirmed; notes /notes/birch. "
            .repeat(1800),
        "compact_l0",
    )
    .await;
    upper_fixture_stage(
        &store,
        "Later note: Birch October 19; violet-kestrel-731; venue unconfirmed. ".repeat(40),
        "compact_l0",
    )
    .await;
    assert!(upper_layer_tokens(&store, 1).await.unwrap() > super::super::L1_LIMIT);
    let calls = Arc::new(AtomicUsize::new(0));
    let parent = upper_real_parent(&store, &spec).await;
    assert!(
        matches!(
            parent.prompt().messages.last(),
            Some(ContextMessage::Synthetic {
                message: Message::User(_)
            })
        ),
        "baseline must end with explicitly synthetic pending question"
    );
    trace.record(json!({"stage":"initial_staged_memory","fragments":parent.visible_memory()}));
    let make_provider =
        |parent: ParentContextSnapshot, fork: bool, release| UpperObservedProvider {
            trace: trace.clone(),
            calls: calls.clone(),
            expected: parent,
            fork,
            real,
            started: Arc::new(Notify::new()),
            release,
        };
    tokio::time::timeout(
        Duration::from_secs(300),
        upper_observed_answer(&make_provider(parent.clone(), false, None)),
    )
    .await
    .expect("parent timeout");
    for (index, kind) in ["compact_l1", "consolidate_l2"].into_iter().enumerate() {
        let parent = if index == 0 {
            parent.clone()
        } else {
            upper_real_parent(&store, &spec).await
        };
        let before = parent.visible_memory().to_vec();
        let original_l0 = parent
            .prompt()
            .messages
            .iter()
            .filter(|message| matches!(message, ContextMessage::Persisted { .. }))
            .cloned()
            .collect::<Vec<_>>();
        let release = Arc::new(Notify::new());
        let provider = Arc::new(make_provider(parent.clone(), true, Some(release.clone())));
        let task_store = store.clone();
        let task_provider = provider.clone();
        let mut task = tokio::spawn(async move {
            compact_next_memory_with_provider(
                task_store,
                parent,
                CancellationToken::new(),
                task_provider.as_ref(),
            )
            .await
        });
        let started = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::select! {
                _ = provider.started.notified() => {},
                result = &mut task => panic!("upper job ended before provider start: {result:?}"),
            }
        })
        .await;
        if started.is_err() {
            task.abort();
            let _ = task.await;
            panic!("eligible upper job did not start");
        }
        if index == 0 {
            seed_completed_authenticated_turn(
                &store,
                &chat_model(),
                "After the fork began: Birch is now October 21. Outside marker: amber-otter-942.",
                "upper-real-during-fork",
                public_assistant("I heard the new correction."),
                vec![],
            )
            .await;
        }
        release.notify_one();
        let ready = match tokio::time::timeout(Duration::from_secs(300), &mut task).await {
            Ok(result) => result.unwrap().unwrap(),
            Err(_) => {
                task.abort();
                let _ = task.await;
                panic!("fork timeout");
            }
        };
        trace.record(json!({"stage":"candidate_completed","kind":kind,"ready":ready,"visible":upper_real_parent(&store,&spec).await.visible_memory()}));
        assert!(
            ready,
            "KEEP_UNCHANGED/nonshrinking output is an incomplete experiment, never forcibly promoted"
        );
        let row = sqlx::query("SELECT * FROM memory_jobs WHERE kind=? AND status='completed'")
            .bind(kind)
            .fetch_one(store.pool())
            .await
            .unwrap();
        let job = parse_job(&row).unwrap();
        let completed = upper_real_parent(&store, &spec).await;
        for source in &job.source_ids {
            assert!(
                completed
                    .visible_memory()
                    .iter()
                    .any(|f| f.batch_id.to_string() == *source),
                "candidate must not remove original"
            );
        }
        assert_eq!(apply_ready_memory(store.clone()).await.unwrap(), 1);
        let after = upper_real_parent(&store, &spec).await;
        for message in &original_l0 {
            assert!(
                after.prompt().messages.contains(message),
                "persisted past L0 must survive upper replacement"
            );
        }
        trace.record(json!({"stage":"applied","kind":kind,"source_ids":job.source_ids,"before":before,"after":after.visible_memory()}));
        for original in before
            .iter()
            .filter(|f| !job.source_ids.contains(&f.batch_id.to_string()))
        {
            let actual = after
                .visible_memory()
                .iter()
                .find(|f| f.batch_id == original.batch_id)
                .expect("unselected memory retained");
            let mut expected = original.clone();
            expected.message_index = actual.message_index;
            expected.adjacency_group = actual.adjacency_group;
            assert_eq!(
                actual, &expected,
                "unselected content/version/lineage must not change; layout indices may shift"
            );
        }
    }
    let before_close = upper_real_parent(&store, &spec).await;
    store.pool().close().await;
    drop(store);
    let reopened = Arc::new(
        Store::session_test_file_store(&db, PERSONALITY_AGENT_ID)
            .await
            .unwrap(),
    );
    let after_reopen = upper_real_parent(&reopened, &spec).await;
    assert!(
        matches!(
            after_reopen.prompt().messages.last(),
            Some(ContextMessage::Synthetic {
                message: Message::User(_)
            })
        ),
        "restart answer must end with explicitly synthetic pending question"
    );
    assert_eq!(before_close.visible_memory(), after_reopen.visible_memory());
    for fragment in after_reopen.visible_memory() {
        let mut after_seq = None;
        let mut originals = Vec::new();
        let mut complete = false;
        // This fixture has only a few original turns. Bound a broken cursor
        // without mistaking the byte-limited first page for complete lineage.
        for page_index in 0..16 {
            let page = reopened
                .recall_messages(&crate::store::RecallRequest {
                    batch_id: Some(fragment.batch_id.to_string()),
                    after_seq,
                    limit: 20,
                    ..Default::default()
                })
                .await
                .unwrap();
            let mut previous = after_seq;
            for message in &page.messages {
                assert!(
                    previous.is_none_or(|seq| message.source.seq > seq),
                    "original recall must advance without duplicate or reordered messages"
                );
                previous = Some(message.source.seq);
            }
            trace.record(json!({"stage":"original_history_after_reopen", "batch_id":fragment.batch_id,
                "page_index":page_index, "after_seq":after_seq, "next_after_seq":page.next_after_seq,
                "sources":page.messages.iter().map(|m| &m.source).collect::<Vec<_>>(),
                "messages":page.messages.iter().map(|m| &m.search_text).collect::<Vec<_>>()}));
            let next = page.next_after_seq;
            originals.extend(page.messages.into_iter().map(|m| m.search_text));
            match next {
                None => {
                    complete = true;
                    break;
                }
                Some(next) => {
                    assert_eq!(
                        Some(next),
                        previous,
                        "cursor must name last returned original"
                    );
                    assert!(
                        after_seq.is_none_or(|seq| next > seq),
                        "recall cursor must progress"
                    );
                    after_seq = Some(next);
                }
            }
        }
        assert!(
            complete,
            "original recall exceeded bounded fixture pagination"
        );
        assert!(!originals.is_empty());
        let original_text = originals.join("\n");
        if fragment.layer == crate::provider::types::MemoryLayer::L2 {
            for fact in [
                "Cedar prototype: blue cover",
                "date tentative and unconfirmed",
                "/notes/cedar-original",
                "provisional meeting is October 12",
                "venue is unconfirmed",
                "/notes/birch",
            ] {
                assert!(
                    original_text.contains(fact),
                    "merged L2 original fact missing: {fact}"
                );
            }
            for later in [
                "October 19",
                "October 21",
                "violet-kestrel-731",
                "amber-otter-942",
            ] {
                assert!(
                    !original_text.contains(later),
                    "unselected later original leaked into merged L2: {later}"
                );
            }
        } else {
            assert_eq!(fragment.layer, crate::provider::types::MemoryLayer::L1);
            assert!(
                original_text.contains("October 19")
                    && original_text.contains("violet-kestrel-731")
            );
            assert!(
                !original_text.contains("Cedar prototype")
                    && !original_text.contains("amber-otter-942")
            );
        }
        trace.record(
            json!({"stage":"original_history_complete", "batch_id":fragment.batch_id,
            "message_count":originals.len(), "selected_fixture_facts_verified":true}),
        );
    }
    tokio::time::timeout(
        Duration::from_secs(300),
        upper_observed_answer(&make_provider(after_reopen, false, None)),
    )
    .await
    .expect("restart answer timeout");
    reopened.pool().close().await;
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    trace.record(json!({"stage":"mechanical_checks_complete","semantic_acceptance":"pending independent original/target/replacement review; marker absence alone is not semantic proof","usage":"normalized provider counters captured; raw presence and HTTP retry count unknown; local estimates are not billed token reduction"}));
}
