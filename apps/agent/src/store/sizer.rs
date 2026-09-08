use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use serde_json::json;
use zeroize::Zeroizing;

pub(crate) const STEER_GROUP_MAX_COMMANDS: usize = 16;
pub(crate) const STEER_GROUP_MAX_BYTES: usize = 1024 * 1024;
pub(crate) const EVENT_BATCH_MAX_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const DURABLE_ROW_OVERHEAD_BYTES: usize = 256;

const ENVELOPE_OVERHEAD: usize = 1 + 24 + 16;

use crate::{
    gateway::CommandId,
    provider::types::{IncomingEventTiming, PublicMessage, UserContent, UserMessage},
    runtime::contracts::{IncomingProvenance, OutputAudience},
};

use super::{Redactor, event_writer::DurableEventMetadata, redactor::search_text_from_projection};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct BatchBounds {
    pub command_count: usize,
    pub command_plaintext_bytes: usize,
}

#[allow(dead_code, reason = "T12 boundary is consumed by the T15 run loop")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BatchSize {
    pub command_count: usize,
    pub command_plaintext_bytes: usize,
    pub transaction_bytes: usize,
}

pub(crate) struct EventBatchSizer;

#[allow(dead_code, reason = "T12 boundary is consumed by the T15 run loop")]
#[derive(Clone, Copy)]
pub(crate) struct CommandSizeInput<'a> {
    pub canonical_payload: &'a [u8],
    pub message_id: &'a str,
    pub text: &'a str,
    pub timestamp: &'a DateTime<Utc>,
}

#[allow(dead_code, reason = "T12 boundary is consumed by the T15 run loop")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InjectionApplication {
    IdleRun,
    HardSteer,
    SoftSteer,
    RetrySteer,
}

#[allow(dead_code, reason = "T12 boundary is consumed by the T15 run loop")]
#[derive(Clone, Copy)]
pub(crate) struct InjectionCommandSizeInput<'a> {
    pub command_id: &'a CommandId,
    pub canonical_payload: &'a [u8],
    pub message_id: &'a str,
    pub text: &'a str,
    pub timestamp: &'a DateTime<Utc>,
    pub provenance: &'a IncomingProvenance,
    pub incoming_timing: Option<&'a IncomingEventTiming>,
}

#[allow(dead_code, reason = "T12 boundary is consumed by the T15 run loop")]
#[derive(Clone, Copy)]
pub(crate) struct InjectionBatchSizeInput<'a> {
    pub application: InjectionApplication,
    pub run_id: &'a str,
    pub turn_id: &'a str,
    pub previous_owner_command_id: Option<&'a CommandId>,
    pub commands: &'a [InjectionCommandSizeInput<'a>],
}

pub(crate) fn canonical_user_message(
    text: &str,
    timestamp: DateTime<Utc>,
    incoming_timing: Option<IncomingEventTiming>,
    incoming_source: Option<IncomingProvenance>,
) -> PublicMessage {
    PublicMessage::User(UserMessage {
        content: vec![UserContent::Text {
            text: text.to_owned(),
        }],
        timestamp,
        incoming_timing,
        incoming_source,
    })
}

pub(crate) fn serialize_message_event(
    event_type: &'static str,
    message_id: &str,
    message: &PublicMessage,
) -> Result<Vec<u8>> {
    if !matches!(event_type, "message_start" | "message_end") {
        bail!("unsupported canonical message event type {event_type}");
    }
    serde_json::to_vec(&json!({
        "type": event_type,
        "message_id": message_id,
        "message": message,
    }))
    .map_err(Into::into)
}

impl EventBatchSizer {
    /// Sizes the complete T12 injection write-set: lifecycle/Steered events,
    /// phase and owner-transfer projections, and every user message row/event.
    #[allow(dead_code, reason = "T12 boundary is consumed by the T15 run loop")]
    pub(crate) fn injection_batch(
        redactor: &Redactor,
        input: InjectionBatchSizeInput<'_>,
    ) -> Result<BatchSize> {
        if input.commands.is_empty() {
            bail!("injection batch must contain at least one command");
        }
        if input.application == InjectionApplication::IdleRun && input.commands.len() != 1 {
            bail!("idle_run injection must contain exactly one command");
        }
        let mut size = Self::command_window_with_timing(
            redactor,
            input.commands.iter().map(|command| {
                (
                    CommandSizeInput {
                        canonical_payload: command.canonical_payload,
                        message_id: command.message_id,
                        text: command.text,
                        timestamp: command.timestamp,
                    },
                    command.incoming_timing,
                    Some(command.provenance),
                )
            }),
        )?;
        for command in input.commands {
            let audience = command.provenance.output_audience();
            let empty_metadata_bytes = serde_json::to_vec(&DurableEventMetadata {
                audience: Some(audience),
                ..DurableEventMetadata::default()
            })?
            .len();
            let authenticated_metadata_bytes = serde_json::to_vec(&DurableEventMetadata {
                direct_chat_provenance: Some(command.provenance.clone()),
                audience: Some(audience),
                ..DurableEventMetadata::default()
            })
            .map_err(|error| {
                anyhow::anyhow!("failed to size authenticated user event metadata: {error}")
            })?
            .len();
            let metadata_growth = authenticated_metadata_bytes
                .checked_sub(empty_metadata_bytes)
                .and_then(|bytes| bytes.checked_mul(2))
                .ok_or_else(|| anyhow::anyhow!("user event metadata byte count overflow"))?;
            size.transaction_bytes = size
                .transaction_bytes
                .checked_add(metadata_growth)
                .ok_or_else(|| anyhow::anyhow!("injection transaction byte count overflow"))?;
        }

        let event_bytes =
            |value: serde_json::Value, mut metadata: serde_json::Value| -> Result<usize> {
                let audience = input.commands[0].provenance.output_audience();
                metadata["audience"] = serde_json::to_value(audience)?;
                let event_type = value
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("sized event has no type"))?;
                let raw = Zeroizing::new(serde_json::to_vec(&value)?);
                let projected = redactor.redact_serialized(&raw)?;
                let metadata = serde_json::to_vec(&metadata)?;
                let wrapped_bytes =
                    serde_json::to_vec(&json!({"event":value,"audience":audience}))?.len();
                Ok(wrapped_bytes
                    .saturating_add(ENVELOPE_OVERHEAD)
                    .saturating_add(projected.len())
                    .saturating_add(event_type.len())
                    .saturating_add(metadata.len())
                    .saturating_add(DURABLE_ROW_OVERHEAD_BYTES))
            };
        let projection_bytes = |identifiers: usize| identifiers.saturating_add(512);

        match input.application {
            InjectionApplication::IdleRun => {
                let command = &input.commands[0];
                size.transaction_bytes = size
                    .transaction_bytes
                    .saturating_add(event_bytes(
                        json!({"type":"agent_start"}),
                        json!({"run_id":input.run_id}),
                    )?)
                    .saturating_add(projection_bytes(
                        command
                            .command_id
                            .as_str()
                            .len()
                            .saturating_add(input.run_id.len()),
                    ))
                    .saturating_add(event_bytes(
                        json!({"type":"turn_start"}),
                        json!({"run_id":input.run_id,"turn_id":input.turn_id}),
                    )?)
                    .saturating_add(projection_bytes(
                        command
                            .command_id
                            .as_str()
                            .len()
                            .saturating_add(input.run_id.len()),
                    ));
            }
            InjectionApplication::HardSteer
            | InjectionApplication::SoftSteer
            | InjectionApplication::RetrySteer => {
                for command in input.commands {
                    size.transaction_bytes = size
                        .transaction_bytes
                        .saturating_add(event_bytes(
                            json!({
                                "type":"steered",
                                "mode":if input.application == InjectionApplication::HardSteer {
                                    "hard"
                                } else {
                                    "soft"
                                },
                            }),
                            json!({
                                "command_id":command.command_id,
                                "run_id":input.run_id,
                                "turn_id":input.turn_id,
                            }),
                        )?)
                        .saturating_add(projection_bytes(
                            command
                                .command_id
                                .as_str()
                                .len()
                                .saturating_add(input.run_id.len()),
                        ));
                }
                if input.application != InjectionApplication::RetrySteer {
                    size.transaction_bytes = size.transaction_bytes.saturating_add(event_bytes(
                        json!({"type":"turn_start"}),
                        json!({"run_id":input.run_id,"turn_id":input.turn_id}),
                    )?);
                }
            }
        }

        let mut owner = input.previous_owner_command_id;
        for command in input.commands {
            if let Some(owner_command_id) = owner {
                size.transaction_bytes = size.transaction_bytes.saturating_add(projection_bytes(
                    owner_command_id
                        .as_str()
                        .len()
                        .saturating_add(input.run_id.len()),
                ));
            }
            // MessageStart and MessageEnd phase projections.
            size.transaction_bytes = size
                .transaction_bytes
                .saturating_add(projection_bytes(
                    command
                        .command_id
                        .as_str()
                        .len()
                        .saturating_add(input.run_id.len()),
                ))
                .saturating_add(projection_bytes(
                    command
                        .command_id
                        .as_str()
                        .len()
                        .saturating_add(input.run_id.len()),
                ));
            owner = Some(command.command_id);
        }
        Ok(size)
    }

    #[cfg(test)]
    pub(crate) fn command_window<'a>(
        redactor: &Redactor,
        commands: impl IntoIterator<Item = CommandSizeInput<'a>>,
    ) -> Result<BatchSize> {
        Self::command_window_with_timing(
            redactor,
            commands.into_iter().map(|command| (command, None, None)),
        )
    }

    fn command_window_with_timing<'a>(
        redactor: &Redactor,
        commands: impl IntoIterator<
            Item = (
                CommandSizeInput<'a>,
                Option<&'a IncomingEventTiming>,
                Option<&'a IncomingProvenance>,
            ),
        >,
    ) -> Result<BatchSize> {
        let mut size = BatchSize {
            command_count: 0,
            command_plaintext_bytes: 0,
            transaction_bytes: 0,
        };
        for (command, incoming_timing, provenance) in commands {
            let audience = provenance.map_or(
                OutputAudience::DirectChat,
                IncomingProvenance::output_audience,
            );
            size.command_count = size.command_count.saturating_add(1);
            size.command_plaintext_bytes = size
                .command_plaintext_bytes
                .saturating_add(command.canonical_payload.len());

            let message = canonical_user_message(
                command.text,
                *command.timestamp,
                incoming_timing.cloned(),
                provenance.filter(|source| source.is_external()).cloned(),
            );
            let raw_message = Zeroizing::new(
                serde_json::to_vec(&message)
                    .map_err(|error| anyhow::anyhow!("failed to size user message: {error}"))?,
            );
            let message_projection = redactor.redact_serialized(&raw_message)?;
            let search_text = search_text_from_projection(&message_projection)?;
            size.transaction_bytes = size
                .transaction_bytes
                .saturating_add(raw_message.len())
                .saturating_add(ENVELOPE_OVERHEAD)
                .saturating_add(message_projection.len())
                .saturating_add(search_text.len())
                .saturating_add(DURABLE_ROW_OVERHEAD_BYTES);

            for event_type in ["message_start", "message_end"] {
                let raw_event = Zeroizing::new(serialize_message_event(
                    event_type,
                    command.message_id,
                    &message,
                )?);
                let event_projection = redactor.redact_serialized(&raw_event)?;
                let event: serde_json::Value = serde_json::from_slice(&raw_event)?;
                let wrapped_bytes =
                    serde_json::to_vec(&json!({"event":event,"audience":audience}))?.len();
                let metadata_bytes = serde_json::to_vec(&json!({"audience":audience}))?.len();
                size.transaction_bytes = size
                    .transaction_bytes
                    .saturating_add(wrapped_bytes)
                    .saturating_add(ENVELOPE_OVERHEAD)
                    .saturating_add(event_projection.len())
                    .saturating_add(event_type.len())
                    .saturating_add(metadata_bytes)
                    .saturating_add(DURABLE_ROW_OVERHEAD_BYTES);
            }
        }
        Ok(size)
    }

    pub(crate) fn validate(bounds: BatchBounds, transaction_bytes: usize) -> Result<BatchSize> {
        let size = BatchSize {
            command_count: bounds.command_count,
            command_plaintext_bytes: bounds.command_plaintext_bytes,
            transaction_bytes,
        };
        if size.command_count > STEER_GROUP_MAX_COMMANDS {
            bail!(
                "command window has {} commands, limit is {}",
                size.command_count,
                STEER_GROUP_MAX_COMMANDS
            );
        }
        if size.command_plaintext_bytes > STEER_GROUP_MAX_BYTES {
            bail!(
                "command window has {} plaintext bytes, limit is {}",
                size.command_plaintext_bytes,
                STEER_GROUP_MAX_BYTES
            );
        }
        if size.transaction_bytes > EVENT_BATCH_MAX_BYTES {
            bail!(
                "event batch has {} durable bytes, limit is {}",
                size.transaction_bytes,
                EVENT_BATCH_MAX_BYTES
            );
        }
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::*;
    use crate::{
        gateway::Command,
        provider::types::{PublicMessage, UserContent, UserMessage},
    };

    fn timestamp() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 20, 12, 34, 56)
            .single()
            .expect("fixed timestamp")
    }

    fn canonical_payload(text: &str) -> Vec<u8> {
        serde_json::to_vec(&Command::UserMessage {
            text: text.to_owned(),
            attachments: Vec::new(),
        })
        .expect("canonical command")
    }

    fn size_one(text: &str) -> BatchSize {
        let payload = canonical_payload(text);
        let timestamp = timestamp();
        EventBatchSizer::command_window(
            &Redactor::v1(),
            [CommandSizeInput {
                canonical_payload: &payload,
                message_id: "018f0000-0000-7000-8000-000000000001",
                text,
                timestamp: &timestamp,
            }],
        )
        .expect("size command")
    }

    fn independent_transaction_bytes(text: &str) -> usize {
        independent_transaction_bytes_with_timing(text, None)
    }

    fn independent_transaction_bytes_with_timing(
        text: &str,
        incoming_timing: Option<IncomingEventTiming>,
    ) -> usize {
        let redactor = Redactor::v1();
        let message = PublicMessage::User(UserMessage {
            incoming_source: None,
            content: vec![UserContent::Text {
                text: text.to_owned(),
            }],
            timestamp: timestamp(),
            incoming_timing,
        });
        let raw_message = serde_json::to_vec(&message).expect("serialize message");
        let message_projection = redactor
            .redact_serialized(&raw_message)
            .expect("redact message");
        let mut bytes = raw_message.len()
            + ENVELOPE_OVERHEAD
            + message_projection.len()
            + search_text_from_projection(&message_projection)
                .expect("message search text")
                .len()
            + DURABLE_ROW_OVERHEAD_BYTES;
        for event_type in ["message_start", "message_end"] {
            let raw_event = serde_json::to_vec(&json!({
                "type":event_type,
                "message_id":"018f0000-0000-7000-8000-000000000001",
                "message":message.clone(),
            }))
            .expect("serialize event");
            let event: crate::agent::AgentEvent =
                serde_json::from_slice(&raw_event).expect("typed event");
            let persisted =
                crate::store::event_payload::encode_event(&event, OutputAudience::DirectChat)
                    .expect("production encrypted envelope plaintext");
            let metadata = serde_json::to_vec(&json!({"audience":"direct_chat"}))
                .expect("persisted audience metadata");
            bytes += persisted.len()
                + ENVELOPE_OVERHEAD
                + redactor
                    .redact_serialized(&raw_event)
                    .expect("redact event")
                    .len()
                + event_type.len()
                + metadata.len()
                + DURABLE_ROW_OVERHEAD_BYTES;
        }
        bytes
    }

    #[test]
    fn independently_rejects_command_count_limit() {
        assert!(
            EventBatchSizer::validate(
                BatchBounds {
                    command_count: STEER_GROUP_MAX_COMMANDS + 1,
                    command_plaintext_bytes: 0,
                },
                0,
            )
            .unwrap_err()
            .to_string()
            .contains("commands")
        );
        EventBatchSizer::validate(
            BatchBounds {
                command_count: STEER_GROUP_MAX_COMMANDS,
                command_plaintext_bytes: 0,
            },
            0,
        )
        .expect("exact command count limit");
    }

    #[test]
    fn independently_rejects_plaintext_limit() {
        assert!(
            EventBatchSizer::validate(
                BatchBounds {
                    command_count: 1,
                    command_plaintext_bytes: STEER_GROUP_MAX_BYTES + 1,
                },
                0,
            )
            .unwrap_err()
            .to_string()
            .contains("plaintext")
        );
        EventBatchSizer::validate(
            BatchBounds {
                command_count: 1,
                command_plaintext_bytes: STEER_GROUP_MAX_BYTES,
            },
            0,
        )
        .expect("exact plaintext limit");
    }

    #[test]
    fn independently_rejects_transaction_limit() {
        assert!(
            EventBatchSizer::validate(
                BatchBounds {
                    command_count: 0,
                    command_plaintext_bytes: 0,
                },
                EVENT_BATCH_MAX_BYTES + 1,
            )
            .unwrap_err()
            .to_string()
            .contains("durable bytes")
        );
        EventBatchSizer::validate(
            BatchBounds {
                command_count: 0,
                command_plaintext_bytes: 0,
            },
            EVENT_BATCH_MAX_BYTES,
        )
        .expect("exact transaction limit");
    }

    #[test]
    fn canonical_dry_run_matches_real_serialization_and_redaction() {
        let cases = vec![
            "plain ASCII".to_owned(),
            "\"quoted\" \\\\ slash\nline\tend".to_owned(),
            "秘密🔐と改行\nを含む".to_owned(),
            "Bearer abcdefgh ".repeat(4096),
        ];
        for text in cases {
            let api_preflight = size_one(&text);
            let durable_recheck = size_one(&text);
            assert_eq!(api_preflight, durable_recheck);
            assert_eq!(
                api_preflight.transaction_bytes,
                independent_transaction_bytes(&text),
                "dry-run drift for {text:?}"
            );
            assert_eq!(
                api_preflight.command_plaintext_bytes,
                canonical_payload(&text).len()
            );
        }
    }

    #[test]
    fn full_injection_builder_accounts_for_application_specific_write_sets() {
        let text = "normal idle injection";
        let payload = canonical_payload(text);
        let timestamp = timestamp();
        let command_id =
            CommandId::parse("00000000-0000-4000-8000-000000000001").expect("canonical UUID");
        let message_id = "018f0000-0000-7000-8000-000000000001";
        let commands = [InjectionCommandSizeInput {
            command_id: &command_id,
            canonical_payload: &payload,
            message_id,
            text,
            timestamp: &timestamp,
            provenance: &crate::gateway::test_direct_chat_provenance(),
            incoming_timing: None,
        }];
        let message_only = size_one(text);
        let idle = EventBatchSizer::injection_batch(
            &Redactor::v1(),
            InjectionBatchSizeInput {
                application: InjectionApplication::IdleRun,
                run_id: "run-1",
                turn_id: "turn-1",
                previous_owner_command_id: None,
                commands: &commands,
            },
        )
        .expect("size idle injection");
        let soft = EventBatchSizer::injection_batch(
            &Redactor::v1(),
            InjectionBatchSizeInput {
                application: InjectionApplication::SoftSteer,
                run_id: "run-1",
                turn_id: "turn-2",
                previous_owner_command_id: Some(&command_id),
                commands: &commands,
            },
        )
        .expect("size soft-steer injection");
        let retry = EventBatchSizer::injection_batch(
            &Redactor::v1(),
            InjectionBatchSizeInput {
                application: InjectionApplication::RetrySteer,
                run_id: "run-1",
                turn_id: "turn-1",
                previous_owner_command_id: Some(&command_id),
                commands: &commands,
            },
        )
        .expect("size retry-steer injection");

        assert!(idle.transaction_bytes > message_only.transaction_bytes);
        assert!(soft.transaction_bytes > retry.transaction_bytes);
        assert_eq!(idle.command_count, 1);
        assert_eq!(idle.command_plaintext_bytes, payload.len());
    }

    #[test]
    fn injection_timing_growth_matches_serialized_durable_rows() {
        let text = "incoming event";
        let payload = canonical_payload(text);
        let timestamp = timestamp();
        let timing = IncomingEventTiming {
            previous_receipt: Some(crate::provider::types::IncomingEventReceipt {
                command_seq: 41,
                received_at: timestamp - chrono::Duration::seconds(90),
            }),
        };
        let command_id =
            CommandId::parse("00000000-0000-4000-8000-000000000001").expect("canonical UUID");
        let provenance = crate::gateway::test_direct_chat_provenance();
        let size = |incoming_timing| {
            EventBatchSizer::injection_batch(
                &Redactor::v1(),
                InjectionBatchSizeInput {
                    application: InjectionApplication::IdleRun,
                    run_id: "run-1",
                    turn_id: "turn-1",
                    previous_owner_command_id: None,
                    commands: &[InjectionCommandSizeInput {
                        command_id: &command_id,
                        canonical_payload: &payload,
                        message_id: "018f0000-0000-7000-8000-000000000001",
                        text,
                        timestamp: &timestamp,
                        provenance: &provenance,
                        incoming_timing,
                    }],
                },
            )
            .expect("size injection")
        };
        let without_timing = size(None);
        let with_timing = size(Some(&timing));
        let expected_growth = independent_transaction_bytes_with_timing(text, Some(timing))
            - independent_transaction_bytes(text);
        assert!(expected_growth > 0);
        assert_eq!(
            with_timing.transaction_bytes - without_timing.transaction_bytes,
            expected_growth
        );
        assert_eq!(
            with_timing.command_plaintext_bytes,
            without_timing.command_plaintext_bytes
        );
    }

    #[test]
    fn maximal_redaction_expansion_uses_actual_replacement_bytes() {
        let text = "Bearer abcdefgh ".repeat(4096);
        let redacted = size_one(&text);
        let same_length_public = size_one(&"x".repeat(text.len()));
        assert!(redacted.transaction_bytes > same_length_public.transaction_bytes);
        EventBatchSizer::validate(
            BatchBounds {
                command_count: redacted.command_count,
                command_plaintext_bytes: redacted.command_plaintext_bytes,
            },
            redacted.transaction_bytes,
        )
        .expect("actual v1 redaction expansion remains within the batch limit");
    }

    #[test]
    fn canonical_command_boundaries_are_exact_for_all_encoding_classes() {
        for unit in ["x", "\"\\\n", "秘密🔐", "Bearer abcdefgh "] {
            let empty_bytes = canonical_payload("").len();
            let unit_bytes = canonical_payload(unit).len() - empty_bytes;
            let admitted_text = unit.repeat((STEER_GROUP_MAX_BYTES - empty_bytes) / unit_bytes);
            let admitted = size_one(&admitted_text);
            let rejected_text = format!("{admitted_text}{unit}");
            let rejected = size_one(&rejected_text);

            assert!(admitted.command_plaintext_bytes <= STEER_GROUP_MAX_BYTES);
            assert!(rejected.command_plaintext_bytes > STEER_GROUP_MAX_BYTES);
            assert_eq!(
                admitted.transaction_bytes,
                independent_transaction_bytes(&admitted_text),
                "admitted dry-run drift for unit {unit:?}"
            );
            assert_eq!(
                rejected.transaction_bytes,
                independent_transaction_bytes(&rejected_text),
                "rejected dry-run drift for unit {unit:?}"
            );
            EventBatchSizer::validate(
                BatchBounds {
                    command_count: admitted.command_count,
                    command_plaintext_bytes: admitted.command_plaintext_bytes,
                },
                admitted.transaction_bytes,
            )
            .expect("largest whole-unit canonical command is admitted");
            assert!(
                EventBatchSizer::validate(
                    BatchBounds {
                        command_count: rejected.command_count,
                        command_plaintext_bytes: rejected.command_plaintext_bytes,
                    },
                    rejected.transaction_bytes,
                )
                .expect_err("next whole unit above 1MiB must fail")
                .to_string()
                .contains("plaintext")
            );
        }
    }
}
