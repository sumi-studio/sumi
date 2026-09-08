// Included in compactor::tests, sharing its authenticated turn/provider fixtures.
async fn assembled_upper_parent(store: &Arc<Store>) -> ParentContextSnapshot {
    let restored = hydrate(store).await;
    let spec = chat_model();
    let assembler = crate::memory::context_assembler::ContextAssembler::from_prompt_with_spec(
        PromptContext::new(
            "A person's continuous experience.".into(),
            vec![],
            vec![],
            vec![],
            vec![],
        ),
        spec.clone(),
    )
    .unwrap()
    .with_mode(crate::memory::overflow::AssemblyMode::SumiThreeLayer);
    assembler
        .install_hydrated_memory_at(
            ThreeLayerMemory::from_hydrated(restored.memory).unwrap(),
            restored.transcript_through_seq,
            restored.provider_context,
        )
        .unwrap();
    let assembled = assembler
        .assemble_with_estimate(&restored.messages, 0)
        .await
        .unwrap();
    ParentContextSnapshot::capture_with_memory(
        &assembled.prompt,
        &spec,
        &RequestOptions::default(),
        &assembled.visible_memory,
    )
    .unwrap()
}

async fn seed_upper_pressure_from_real_experience(store: &Arc<Store>) -> ParentContextSnapshot {
    let parent = real_parent_with_queued_target(store).await;
    let provider = FakeProvider {
        text: "Remembered observation. ".repeat(4000),
        ..Default::default()
    };
    assert!(
        compact_next_memory_with_provider(
            store.clone(),
            parent,
            CancellationToken::new(),
            &provider
        )
        .await
        .unwrap()
    );
    let row =
        sqlx::query("SELECT * FROM memory_jobs WHERE kind='compact_l0' AND status='completed'")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert!(
        apply_completed_job(store.clone(), &parse_job(&row).unwrap())
            .await
            .unwrap()
    );
    assert!(upper_layer_tokens(store, 1).await.unwrap() > super::super::L1_LIMIT);
    let parent = assembled_upper_parent(store).await;
    assert_eq!(parent.visible_memory().len(), 1);
    assert_eq!(
        parent.visible_memory()[0].layer,
        crate::provider::types::MemoryLayer::L1
    );
    parent
}

#[tokio::test]
async fn upper_chain_preserves_later_append_and_rehydrates_selected_lineage() {
    let store = test_store().await;
    let parent = seed_upper_pressure_from_real_experience(&store).await;
    let original = parent.visible_memory()[0].clone();
    let finish = Arc::new(Notify::new());
    let provider = Arc::new(FakeProvider {
        text: "Earlier observation. ".repeat(2500),
        finish: Some(finish.clone()),
        ..Default::default()
    });
    let task_store = store.clone();
    let task_provider = provider.clone();
    let expected_prefix = parent.prompt().clone();
    let task = tokio::spawn(async move {
        compact_next_memory_with_provider(
            task_store,
            parent,
            CancellationToken::new(),
            task_provider.as_ref(),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(10), provider.started.notified())
        .await
        .expect("upper provider started");
    let pending = assembled_upper_parent(&store).await;
    assert_eq!(
        pending.visible_memory(),
        std::slice::from_ref(&original),
        "preparation preserves source identity and actual visible text"
    );
    let correction = "Later correction outside the selected old experience.";
    seed_completed_authenticated_turn(
        &store,
        &chat_model(),
        correction,
        "upper-later-correction",
        public_assistant("I heard the later correction."),
        vec![],
    )
    .await;
    finish.notify_one();
    assert!(task.await.unwrap().unwrap());
    let completed = assembled_upper_parent(&store).await;
    assert_eq!(
        completed.visible_memory()[0].batch_id,
        original.batch_id,
        "candidate completion does not apply it"
    );
    let observed = provider.observed.lock().unwrap();
    let fork = &observed.as_ref().unwrap().1;
    assert_eq!(
        &fork.messages[..expected_prefix.messages.len()],
        expected_prefix.messages.as_slice()
    );
    assert_eq!(fork.system_prompt, expected_prefix.system_prompt);
    assert_eq!(fork.memory_blocks, expected_prefix.memory_blocks);
    assert_eq!(fork.tools, expected_prefix.tools);
    assert_eq!(fork.provider_context, expected_prefix.provider_context);
    drop(observed);
    assert_eq!(apply_ready_memory(store.clone()).await.unwrap(), 1);
    let first_l2 = assembled_upper_parent(&store).await;
    assert_eq!(first_l2.visible_memory().len(), 1);
    let fragment = &first_l2.visible_memory()[0];
    assert_eq!(fragment.layer, crate::provider::types::MemoryLayer::L2);
    assert_eq!(fragment.source_ids, vec![original.batch_id]);
    assert_eq!(fragment.original_seq_span, original.original_seq_span);
    assert!(
        serde_json::to_string(first_l2.prompt())
            .unwrap()
            .contains(correction)
    );
    assert!(!fragment.text.contains(correction));
    assert!(upper_layer_tokens(&store, 2).await.unwrap() > super::super::L2_LIMIT);
    let first_l2_id = fragment.batch_id;
    let provider = FakeProvider {
        text: "The earlier observations were integrated within their original range.".into(),
        ..Default::default()
    };
    assert!(
        compact_next_memory_with_provider(
            store.clone(),
            first_l2,
            CancellationToken::new(),
            &provider
        )
        .await
        .unwrap()
    );
    assert_eq!(apply_ready_memory(store.clone()).await.unwrap(), 1);
    let rebuilt = assembled_upper_parent(&store).await;
    let fragment = &rebuilt.visible_memory()[0];
    assert_eq!(fragment.source_ids, vec![first_l2_id]);
    assert_eq!(fragment.original_seq_span, original.original_seq_span);
    assert!(
        serde_json::to_string(rebuilt.prompt())
            .unwrap()
            .contains(correction)
    );
    assert_eq!(
        apply_ready_memory(store.clone()).await.unwrap(),
        0,
        "duplicate completion/application leaves current memory alone"
    );
    let originals = store
        .recall_messages(&crate::store::RecallRequest {
            batch_id: Some(fragment.batch_id.to_string()),
            limit: 20,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        !originals.messages.is_empty(),
        "reintegrated L2 reaches actual original transcript"
    );
    assert!(
        originals
            .messages
            .iter()
            .any(|message| message.search_text.contains("/workspace/source"))
    );
    assert!(
        !originals
            .messages
            .iter()
            .any(|message| message.search_text.contains(correction))
    );
}

#[tokio::test]
async fn upper_keep_unchanged_preserves_identity_ciphertext_and_does_not_loop() {
    let store = test_store().await;
    let parent = seed_upper_pressure_from_real_experience(&store).await;
    let before = parent.visible_memory().to_vec();
    let id = before[0].batch_id.to_string();
    let ciphertext: Vec<u8> =
        sqlx::query_scalar("SELECT summary_ciphertext FROM memory_batches WHERE id=?")
            .bind(&id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    let provider = FakeProvider {
        text: KEEP_UNCHANGED.into(),
        ..Default::default()
    };
    assert!(
        !compact_next_memory_with_provider(
            store.clone(),
            parent,
            CancellationToken::new(),
            &provider
        )
        .await
        .unwrap()
    );
    assert_eq!(apply_ready_memory(store.clone()).await.unwrap(), 0);
    let rebuilt = assembled_upper_parent(&store).await;
    assert_eq!(rebuilt.visible_memory(), before.as_slice());
    let after: Vec<u8> =
        sqlx::query_scalar("SELECT summary_ciphertext FROM memory_batches WHERE id=?")
            .bind(&id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(after, ciphertext);
    assert!(
        !compact_next_memory_with_provider(
            store.clone(),
            rebuilt,
            CancellationToken::new(),
            &provider
        )
        .await
        .unwrap()
    );
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        1,
        "same unchanged target is not regenerated each turn"
    );

    // A later real experience becomes a distinct L1 source. The retained
    // oldest tuple must not monopolize scheduling for the whole layer.
    seed_completed_authenticated_turn(
        &store,
        &chat_model(),
        "Observe another independent area.",
        "upper-independent-original",
        public_assistant(&"Later observed detail. ".repeat(5000)),
        vec![],
    )
    .await;
    seed_completed_authenticated_turn(
        &store,
        &chat_model(),
        "Continue after that observation.",
        "upper-independent-tail",
        public_assistant("I am here after the new observation."),
        vec![],
    )
    .await;
    let provider = FakeProvider {
        text: "Later independent observation. ".repeat(800),
        ..Default::default()
    };
    assert!(
        compact_next_memory_with_provider(
            store.clone(),
            assembled_upper_parent(&store).await,
            CancellationToken::new(),
            &provider,
        )
        .await
        .unwrap()
    );
    let row =
        sqlx::query("SELECT * FROM memory_jobs WHERE kind='compact_l0' AND status='completed'")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert!(
        apply_completed_job(store.clone(), &parse_job(&row).unwrap())
            .await
            .unwrap()
    );
    let parent = assembled_upper_parent(&store).await;
    assert_eq!(parent.visible_memory().len(), 2);
    assert_eq!(parent.visible_memory()[0], before[0]);
    let later_id = parent.visible_memory()[1].batch_id;
    let provider = FakeProvider {
        text: "The later independent area was observed.".into(),
        ..Default::default()
    };
    assert!(
        compact_next_memory_with_provider(
            store.clone(),
            parent,
            CancellationToken::new(),
            &provider,
        )
        .await
        .unwrap(),
        "an unchanged oldest group does not block a later group"
    );
    let row =
        sqlx::query("SELECT * FROM memory_jobs WHERE kind='compact_l1' AND status='completed'")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(
        parse_job(&row).unwrap().source_ids,
        vec![later_id.to_string()]
    );
    assert_eq!(apply_ready_memory(store.clone()).await.unwrap(), 1);
    let rebuilt = assembled_upper_parent(&store).await;
    assert_eq!(rebuilt.visible_memory()[0], before[0]);
    assert_eq!(
        rebuilt.visible_memory()[1].layer,
        crate::provider::types::MemoryLayer::L2
    );
    assert_eq!(rebuilt.visible_memory()[1].source_ids, vec![later_id]);
    let after: Vec<u8> =
        sqlx::query_scalar("SELECT summary_ciphertext FROM memory_batches WHERE id=?")
            .bind(&id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(
        after, ciphertext,
        "old retained source stays byte-for-byte untouched"
    );
}

#[test]
fn upper_target_group_stops_at_intervening_parent_message() {
    let mut first = crate::provider::types::VisibleMemoryFragment {
        message_index: 0,
        adjacency_group: Uuid::now_v7(),
        layer: crate::provider::types::MemoryLayer::L1,
        batch_id: Uuid::now_v7(),
        version: 1,
        source_ids: vec![Uuid::now_v7()],
        original_seq_span: Some(super::super::OriginalSequenceSpan::new(1, 10).unwrap()),
        time_range: (timestamp(), timestamp()),
        text: "Earlier".into(),
    };
    let mut later = first.clone();
    later.batch_id = Uuid::now_v7();
    later.message_index = 2;
    later.original_seq_span = Some(super::super::OriginalSequenceSpan::new(20, 30).unwrap());
    assert!(!contiguous_fragments(&[first.clone(), later.clone()]));
    later.message_index = 1;
    assert!(contiguous_fragments(&[first.clone(), later.clone()]));
    later.adjacency_group = Uuid::now_v7();
    assert!(
        !contiguous_fragments(&[first.clone(), later.clone()]),
        "normalization cannot close a gap from the original parent view"
    );
    later.adjacency_group = first.adjacency_group;
    first.original_seq_span = None;
    assert!(
        !contiguous_fragments(&[first, later]),
        "unknown historical bounds are not invented for eligibility"
    );
}
