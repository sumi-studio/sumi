use super::*;
use crate::agent::reflex::{ReflexDecision, render_event_content};

fn with_external_source(
    mut writes: Vec<EventWrite>,
    source: &IncomingProvenance,
) -> Vec<EventWrite> {
    for write in &mut writes {
        if let Some(event) = write.event.as_ref() {
            let replacement = match &event.value {
                AgentEvent::MessageStart {
                    message_id,
                    message,
                } => Some((
                    "message_start",
                    message_id.clone(),
                    message.as_ref().clone(),
                )),
                AgentEvent::MessageEnd {
                    message_id,
                    message,
                } => Some(("message_end", message_id.clone(), message.as_ref().clone())),
                _ => None,
            };
            if let Some((kind, id, mut message)) = replacement {
                if let PublicMessage::User(user) = &mut message {
                    user.incoming_source = Some(source.clone());
                }
                write.event = Some(DurableEvent::message(kind, &id, &message).unwrap());
            }
        }
        for projection in &mut write.projections {
            if let Projection::MessageEnd {
                message: PublicMessage::User(user),
                ..
            } = projection
            {
                user.incoming_source = Some(source.clone());
            }
        }
    }
    writes
}

#[tokio::test]
async fn interpreted_notification_requires_its_exact_durable_assessment() {
    let store = test_store().await;
    let writer = EventWriter::new(store.clone());
    let id = "00000000-0000-4000-8000-000000000831";
    let mut source = serde_json::to_value(crate::gateway::test_messaging_provenance()).unwrap();
    source["personality_agent_id"] = json!(scope().personality_agent_id);
    let source: IncomingProvenance = serde_json::from_value(source).unwrap();
    let command = CommandEnvelope {
        seq: 1,
        command_id: CommandId::parse(id).unwrap(),
        personality_agent_id: scope().personality_agent_id,
        provenance: source.clone(),
        command: Command::ExternalEvent {
            content: "Original event stays unchanged".into(),
        },
    };
    writer
        .persist_inbound(&InboundCommand::Valid(command.clone()))
        .await
        .unwrap();
    writer
        .pin_incoming_timing_for_test(id, durable_test_timestamp())
        .await
        .unwrap();
    store
        .record_reflex_decision(
            &command,
            ReflexDecision::Soft {
                interpretation: Some("May matter after the current response".into()),
            },
        )
        .await
        .unwrap();
    writer
        .apply(EventBatch {
            writes: vec![EventWrite {
                event: None,
                projections: vec![Projection::CommandClassified {
                    command_id: id.into(),
                    application_kind: ApplicationKind::IdleRun,
                    run_id: format!("run-{id}"),
                    turn_id: format!("turn-{id}"),
                }],
            }],
            injected_commands: vec![],
        })
        .await
        .unwrap();
    let binding = InjectedCommand::new(1, command.command_id.clone(), source.clone());
    let forged = render_event_content(
        "Original event stays unchanged",
        Some("Unrecorded extra instruction"),
    );
    let error = writer
        .apply(EventBatch {
            writes: with_external_source(injection_writes(id, "", &forged), &source),
            injected_commands: vec![binding.clone()],
        })
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not match its user MessageEnd"),
        "{error:#}"
    );
    let rendered = render_event_content(
        "Original event stays unchanged",
        Some("May matter after the current response"),
    );
    writer
        .apply(EventBatch {
            writes: with_external_source(injection_writes(id, "", &rendered), &source),
            injected_commands: vec![binding],
        })
        .await
        .unwrap();
    let mut transaction = store.pool().begin().await.unwrap();
    let original = load_authenticated_command(&store, &mut transaction, id, 1, "user_message")
        .await
        .unwrap();
    assert_eq!(original, command.command);
    transaction.commit().await.unwrap();
}
