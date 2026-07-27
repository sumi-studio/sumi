//! Encrypted transcript message records.
//!
//! This module owns the durable `messages` row shape and the single public
//! constructor that turns a `PublicMessage` into an encrypted record plus a
//! redacted projection.  It deliberately does not expose the plaintext after
//! construction so that repair/provider-context plaintext cannot cross into
//! transcript types.

#![allow(dead_code)]

use anyhow::{Context, Result, bail};
use chrono::Utc;

use crate::provider::types::{PublicAssistantMessage, PublicMessage};

use super::redactor::search_text_from_projection;
use super::{AgentScope, DataKeyMaterial, DataKeyPurpose, PublicProjectionBuilder, Redactor};

pub(crate) fn message_interrupted(message: &PublicMessage) -> bool {
    match message {
        PublicMessage::Assistant(PublicAssistantMessage { interrupted, .. }) => *interrupted,
        _ => false,
    }
}

pub(crate) struct TranscriptRecord {
    id: String,
    seq: u64,
    role: &'static str,
    raw_key_ref: String,
    raw_ciphertext: Vec<u8>,
    payload: String,
    search_text: String,
    redaction_version: u32,
    interrupted: bool,
    created_at: String,
}

impl TranscriptRecord {
    pub(crate) fn encrypt(
        message: &PublicMessage,
        id: impl Into<String>,
        seq: u64,
        data_key: &DataKeyMaterial,
        scope: &AgentScope,
        redactor: &Redactor,
    ) -> Result<Self> {
        if data_key.purpose != DataKeyPurpose::Transcript {
            bail!("transcript records must be encrypted with a transcript data key");
        }

        let id = id.into();
        let role = public_message_role(message);
        let interrupted = message_interrupted(message);
        let aad = scope.row_aad("messages", &id, DataKeyPurpose::Transcript);
        let protected = PublicProjectionBuilder::new(redactor, data_key)
            .build(message, &aad)
            .context("failed to build transcript projection")?;
        let search_text = search_text_from_projection(&protected.projection)
            .context("failed to derive transcript search text")?;

        Ok(Self {
            id,
            seq,
            role,
            raw_key_ref: data_key.key_ref.clone(),
            raw_ciphertext: protected.ciphertext,
            payload: protected.projection,
            search_text,
            redaction_version: protected.redaction_version,
            interrupted,
            created_at: Utc::now().to_rfc3339(),
        })
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    pub(crate) fn role(&self) -> &'static str {
        self.role
    }

    pub(crate) async fn insert<'e, E>(&self, executor: E) -> Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        sqlx::query(
            "INSERT INTO messages(
                id, seq, role, raw_key_ref, raw_ciphertext, payload, search_text,
                redaction_version, interrupted, created_at
             ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&self.id)
        .bind(i64::try_from(self.seq).context("transcript seq out of SQLite range")?)
        .bind(self.role)
        .bind(&self.raw_key_ref)
        .bind(&self.raw_ciphertext)
        .bind(&self.payload)
        .bind(&self.search_text)
        .bind(i64::from(self.redaction_version))
        .bind(i64::from(self.interrupted))
        .bind(&self.created_at)
        .execute(executor)
        .await
        .context("failed to insert transcript record")?;
        Ok(())
    }
}

pub(crate) fn public_message_role(message: &PublicMessage) -> &'static str {
    match message {
        PublicMessage::User(_) => "user",
        PublicMessage::Assistant(_) => "assistant",
        PublicMessage::ToolResult(_) => "tool_result",
    }
}

/// Search the redacted transcript projection without exposing encrypted raw
/// content. FTS5's trigram tokenizer cannot match queries shorter than three
/// Unicode scalar values, so those queries use a correctness-preserving LIKE
/// fallback over the same redacted column.
pub(crate) async fn search_message_ids(
    pool: &sqlx::SqlitePool,
    query: &str,
) -> Result<Vec<String>> {
    if query.is_empty() {
        bail!("transcript search query must not be empty");
    }
    if query.chars().any(char::is_control) {
        bail!("transcript search query must not contain control characters");
    }

    if query.chars().count() < 3 {
        let escaped = query
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        return sqlx::query_scalar(
            "SELECT id FROM messages
             WHERE search_text LIKE ? ESCAPE '\\'
             ORDER BY seq",
        )
        .bind(format!("%{escaped}%"))
        .fetch_all(pool)
        .await
        .context("failed to search short transcript query");
    }

    let phrase = format!("\"{}\"", query.replace('"', "\"\""));
    sqlx::query_scalar(
        "SELECT messages.id
         FROM messages_fts
         JOIN messages ON messages.rowid = messages_fts.rowid
         WHERE messages_fts MATCH ?
         ORDER BY messages.seq",
    )
    .bind(phrase)
    .fetch_all(pool)
    .await
    .context("failed to search transcript FTS")
}

#[cfg(test)]
mod tests {
    use sqlx::Row;

    use super::*;
    use crate::provider::types::{
        ProviderOrigin, PublicAssistantContent, PublicAssistantMessage, Usage,
    };
    use crate::store::AgentScope;
    use crate::store::crypto::DataKeyPurpose;

    fn scope() -> AgentScope {
        AgentScope {
            tenant_id: "tenant-1".to_owned(),
            agent_id: "agent-1".to_owned(),
            conversation_id: "conversation-1".to_owned(),
        }
    }

    async fn store() -> crate::store::Store {
        crate::store::Store::session_test_store("conversation-1")
            .await
            .expect("open test store")
    }

    fn message_fixture() -> PublicMessage {
        PublicMessage::Assistant(PublicAssistantMessage {
            content: vec![PublicAssistantContent::Text {
                text: "visible answer".to_owned(),
                wire_item_index: 0,
            }],
            model: "model-1".to_owned(),
            provider: "provider-1".to_owned(),
            origin: ProviderOrigin {
                provider_instance_id: "instance-1".to_owned(),
                protocol: crate::provider::types::ApiProtocol::OpenAiChatCompletions,
                model: "model-1".to_owned(),
            },
            usage: Usage::default(),
            stop_reason: crate::provider::types::StopReason::Stop,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: Utc::now(),
        })
    }

    fn interrupted_message_fixture() -> PublicMessage {
        let mut message = message_fixture();
        if let PublicMessage::Assistant(ref mut assistant) = message {
            assistant.interrupted = true;
        }
        message
    }

    #[tokio::test]
    async fn transcript_record_rejects_non_transcript_key() {
        let store = store().await;
        let event_key = store
            .conversation_key(DataKeyPurpose::Event)
            .await
            .expect("mint event key");
        let redactor = Redactor::v1();
        let result = TranscriptRecord::encrypt(
            &message_fixture(),
            "message-1",
            1,
            &event_key,
            &scope(),
            &redactor,
        );
        match result {
            Err(error) => {
                assert!(error.to_string().contains("transcript data key"));
            }
            Ok(_) => panic!("expected encryption to fail with a non-transcript key"),
        }
    }

    #[tokio::test]
    async fn transcript_round_trip_stores_encrypted_record_and_projection() {
        let store = store().await;
        let transcript_key = store
            .conversation_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint transcript key");
        let redactor = Redactor::v1();
        let record = TranscriptRecord::encrypt(
            &message_fixture(),
            "message-1",
            7,
            &transcript_key,
            &scope(),
            &redactor,
        )
        .expect("encrypt transcript");

        assert_eq!(record.id(), "message-1");
        assert_eq!(record.seq(), 7);
        assert_eq!(record.role(), "assistant");

        record
            .insert(store.pool())
            .await
            .expect("insert transcript");

        let row = sqlx::query(
            "SELECT id, seq, role, raw_key_ref, payload, search_text, redaction_version
             FROM messages WHERE id = ?",
        )
        .bind("message-1")
        .fetch_one(store.pool())
        .await
        .expect("fetch transcript row");
        assert_eq!(row.get::<String, _>("id"), "message-1");
        assert_eq!(row.get::<i64, _>("seq"), 7);
        assert_eq!(row.get::<String, _>("role"), "assistant");
        assert_eq!(row.get::<String, _>("raw_key_ref"), transcript_key.key_ref);
        assert_eq!(row.get::<String, _>("search_text"), "visible answer");
        assert_eq!(row.get::<i64, _>("redaction_version"), 1);

        let payload: serde_json::Value =
            serde_json::from_str(&row.get::<String, _>("payload")).expect("parse payload");
        assert_eq!(payload["role"], "assistant");
    }

    #[tokio::test]
    async fn transcript_search_text_is_redacted_before_storage() {
        let fine_grained = format!("github_pat_{}", "x".repeat(82));
        let mut message = message_fixture();
        if let PublicMessage::Assistant(assistant) = &mut message {
            assistant.content = vec![PublicAssistantContent::Text {
                text: format!("use sk-abcdefghijklmnop and {fine_grained}"),
                wire_item_index: 0,
            }];
        }
        let store = store().await;
        let key = store
            .conversation_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint key");
        let redactor = Redactor::v1();
        let record =
            TranscriptRecord::encrypt(&message, "message-secret", 1, &key, &scope(), &redactor)
                .expect("encrypt secret transcript");
        record.insert(store.pool()).await.expect("insert");

        let search: String = sqlx::query_scalar("SELECT search_text FROM messages WHERE id = ?")
            .bind("message-secret")
            .fetch_one(store.pool())
            .await
            .expect("fetch search text");
        assert!(!search.contains("sk-abcdefghijklmnop"));
        assert!(!search.contains(&fine_grained));
        assert!(search.contains("[REDACTED:api_key]"));
        assert!(search.contains("[REDACTED:github_token]"));
    }

    #[tokio::test]
    async fn fts_trigger_maintains_external_content_index() {
        let store = store().await;
        let key = store
            .conversation_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint key");
        let redactor = Redactor::v1();
        let record = TranscriptRecord::encrypt(
            &message_fixture(),
            "message-fts",
            1,
            &key,
            &scope(),
            &redactor,
        )
        .expect("encrypt fts fixture");
        record.insert(store.pool()).await.expect("insert");

        let message_rowid: i64 = sqlx::query_scalar("SELECT rowid FROM messages WHERE id = ?")
            .bind("message-fts")
            .fetch_one(store.pool())
            .await
            .expect("fetch message rowid");

        let indexed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages_fts WHERE rowid = ?")
            .bind(message_rowid)
            .fetch_one(store.pool())
            .await
            .expect("probe fts index");
        assert_eq!(indexed, 1);

        // Update search_text and verify the external-content index is replaced.
        sqlx::query("UPDATE messages SET search_text = 'updated searchable text' WHERE id = ?")
            .bind("message-fts")
            .execute(store.pool())
            .await
            .expect("update search text");

        let fts_text: String =
            sqlx::query_scalar("SELECT search_text FROM messages_fts WHERE rowid = ?")
                .bind(message_rowid)
                .fetch_one(store.pool())
                .await
                .expect("fetch updated fts text");
        assert_eq!(fts_text, "updated searchable text");

        sqlx::query(
            "UPDATE messages SET search_text = '再起動後も過去の発言を検索できる' WHERE id = ?",
        )
        .bind("message-fts")
        .execute(store.pool())
        .await
        .expect("update search text with Japanese");

        let japanese_matches: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM messages_fts
             WHERE messages_fts MATCH '過去の発言'",
        )
        .fetch_one(store.pool())
        .await
        .expect("search Japanese substring");
        assert_eq!(japanese_matches, 1);

        assert_eq!(
            search_message_ids(store.pool(), "過去")
                .await
                .expect("search two-character Japanese substring"),
            vec!["message-fts"]
        );
        assert_eq!(
            search_message_ids(store.pool(), "過去の発言")
                .await
                .expect("search Japanese trigram substring"),
            vec!["message-fts"]
        );

        for query in ["\0", "a\0", "ab\0"] {
            let error = search_message_ids(store.pool(), query)
                .await
                .expect_err("control characters must fail closed");
            assert!(error.to_string().contains("control"));
        }

        sqlx::query("DELETE FROM messages WHERE id = ?")
            .bind("message-fts")
            .execute(store.pool())
            .await
            .expect("delete message");

        let remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM messages_fts WHERE rowid = ?")
                .bind(message_rowid)
                .fetch_one(store.pool())
                .await
                .expect("probe fts after delete");
        assert_eq!(remaining, 0);
    }

    #[tokio::test]
    async fn transcript_record_derives_interrupted_from_public_message() {
        let store = store().await;
        let key = store
            .conversation_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint key");
        let redactor = Redactor::v1();
        let record = TranscriptRecord::encrypt(
            &interrupted_message_fixture(),
            "message-interrupted",
            1,
            &key,
            &scope(),
            &redactor,
        )
        .expect("encrypt interrupted transcript");
        record.insert(store.pool()).await.expect("insert");

        let interrupted: bool = sqlx::query_scalar("SELECT interrupted FROM messages WHERE id = ?")
            .bind("message-interrupted")
            .fetch_one(store.pool())
            .await
            .expect("fetch interrupted flag");
        assert!(
            interrupted,
            "interrupted must be derived from PublicMessage"
        );
    }
}
