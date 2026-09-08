//! Durable audience is authenticated with the event body. The sole legacy
//! exception is an explicit, offline-authorized pre-external sequence boundary.
use super::{DataKeyPurpose, Store, crypto::decrypt_content};
use crate::{
    agent::AgentEvent,
    runtime::contracts::{IncomingProvenance, OutputAudience},
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sqlx::{Row, Sqlite, Transaction};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventPayload {
    event: AgentEvent,
    audience: OutputAudience,
}

pub(super) fn encode_event(event: &AgentEvent, audience: OutputAudience) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&EventPayload {
        event: event.clone(),
        audience,
    })?)
}

pub(super) async fn decode_event(
    store: &Store,
    transaction: &mut Transaction<'_, Sqlite>,
    seq: u64,
    plaintext: &[u8],
    metadata_json: &str,
) -> Result<(AgentEvent, OutputAudience)> {
    let metadata: serde_json::Value =
        serde_json::from_str(metadata_json).context("durable event metadata is invalid")?;
    // Distinguish shape before decoding so a malformed wrapper cannot fall
    // through to the legacy interpretation.
    let shape: serde_json::Value =
        serde_json::from_slice(plaintext).context("durable event plaintext is invalid")?;
    if shape.get("event").is_some() || shape.get("audience").is_some() {
        let payload: EventPayload = serde_json::from_slice(plaintext)
            .context("durable event audience wrapper is invalid")?;
        let projected: OutputAudience = serde_json::from_value(
            metadata
                .get("audience")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("durable event audience metadata is missing"))?,
        )?;
        if projected != payload.audience {
            bail!("durable event audience metadata disagrees with authenticated payload");
        }
        if let Some(value) = metadata.get("direct_chat_provenance") {
            let provenance: IncomingProvenance = serde_json::from_value(value.clone())?;
            provenance.validate(&store.scope().personality_agent_id)?;
            if provenance.output_audience() != payload.audience {
                bail!("event source audience disagrees with authenticated payload");
            }
            if let AgentEvent::MessageStart { message, .. }
            | AgentEvent::MessageEnd { message, .. } = &payload.event
            {
                if let crate::provider::types::PublicMessage::User(user) = message.as_ref() {
                    let expected_source = provenance.is_external().then_some(provenance);
                    if user.incoming_source != expected_source {
                        bail!("event source metadata disagrees with authenticated incoming source");
                    }
                }
            }
        }
        return Ok((payload.event, payload.audience));
    }
    let boundary: Option<i64> = sqlx::query_scalar(
        "SELECT through_seq FROM legacy_event_audience WHERE personality_agent_id=?",
    )
    .bind(store.scope().personality_agent_id.as_str())
    .fetch_optional(&mut **transaction)
    .await?;
    if !boundary.is_some_and(|boundary| boundary >= 0 && seq <= boundary as u64)
        || metadata.get("audience").is_some()
    {
        bail!("raw legacy event {seq} has no verified pre-external audience boundary");
    }
    if let Some(provenance) = metadata.get("direct_chat_provenance") {
        let provenance: IncomingProvenance = serde_json::from_value(provenance.clone())?;
        provenance.validate(&store.scope().personality_agent_id)?;
        if provenance.authenticated_direct_chat_human().is_none() {
            bail!("legacy event {seq} has non-direct provenance");
        }
    }
    Ok((
        serde_json::from_slice(plaintext)?,
        OutputAudience::DirectChat,
    ))
}

impl Store {
    /// Read the route from the exact authenticated persisted event, never from
    /// the currently active command (which may have changed during catch-up).
    pub(crate) async fn authenticated_event_audience(&self, seq: u64) -> Result<OutputAudience> {
        let mut transaction = self.pool().begin().await?;
        let row = sqlx::query(
            "SELECT raw_key_ref, raw_ciphertext, internal_metadata FROM agent_events WHERE seq=?",
        )
        .bind(i64::try_from(seq)?)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| anyhow::anyhow!("durable event {seq} is missing"))?;
        let key_ref: String = row.try_get("raw_key_ref")?;
        let key = self
            .data_key_by_ref_in_transaction(&mut transaction, &key_ref)
            .await?;
        if key.purpose != DataKeyPurpose::Event {
            bail!("event audience references a non-event key");
        }
        let aad = self
            .scope()
            .row_aad("agent_events", seq.to_string(), DataKeyPurpose::Event);
        let raw = zeroize::Zeroizing::new(decrypt_content(
            &key,
            &row.try_get::<Vec<u8>, _>("raw_ciphertext")?,
            &aad,
        )?);
        let (_, audience) = decode_event(
            self,
            &mut transaction,
            seq,
            &raw,
            &row.try_get::<String, _>("internal_metadata")?,
        )
        .await?;
        transaction.commit().await?;
        Ok(audience)
    }

    /// Only an offline cutover operator may call this after verifying the
    /// deployed pre-external image and v1-only command inventory. This private
    /// SQL authorization is not an independent cryptographic attestation.
    #[allow(dead_code)]
    pub(crate) async fn authorize_pre_external_event_boundary(
        &self,
        through_seq: u64,
    ) -> Result<()> {
        let mut transaction = self.pool().begin().await?;
        let head: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(seq),0) FROM agent_events")
            .fetch_one(&mut *transaction)
            .await?;
        if i64::try_from(through_seq)? != head {
            bail!("legacy audience cutover head changed");
        }
        let external: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM inbound_commands WHERE COALESCE(json_extract(provenance_json,'$.version'),0)<>1 OR COALESCE(json_extract(provenance_json,'$.source.surface'),'')<>'direct_chat')")
            .fetch_one(&mut *transaction).await?;
        if external != 0 {
            bail!("legacy audience cutover contains non-direct command provenance");
        }
        sqlx::query(
            "INSERT INTO legacy_event_audience(personality_agent_id,through_seq) VALUES(?,?)",
        )
        .bind(self.scope().personality_agent_id.as_str())
        .bind(head)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }
}
