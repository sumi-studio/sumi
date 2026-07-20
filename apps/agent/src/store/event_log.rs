use anyhow::{Result, anyhow, bail};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use super::{AgentScope, DataKeyMaterial, DataKeyPurpose};

const EVENT_CHAIN_DOMAIN: &[u8] = b"sumi-event-log-chain/v1";
const EVENT_HEAD_DOMAIN: &[u8] = b"sumi-event-log-head/v1";
pub(super) const EVENT_DIGEST_BYTES: usize = 32;

pub(super) struct EventChainEntry<'a> {
    pub(super) seq: u64,
    pub(super) event_type: &'a str,
    pub(super) internal_metadata: &'a str,
    pub(super) key_ref: &'a str,
    pub(super) ciphertext: &'a [u8],
    pub(super) envelope: &'a str,
    pub(super) redaction_version: u32,
}

pub(super) fn extend_event_chain(
    previous: &[u8; EVENT_DIGEST_BYTES],
    entry: EventChainEntry<'_>,
) -> [u8; EVENT_DIGEST_BYTES] {
    let mut hash = Sha256::new();
    hash.update(EVENT_CHAIN_DOMAIN);
    append_field(&mut hash, previous);
    append_field(&mut hash, &entry.seq.to_be_bytes());
    append_field(&mut hash, entry.event_type.as_bytes());
    append_field(&mut hash, entry.internal_metadata.as_bytes());
    append_field(&mut hash, entry.key_ref.as_bytes());
    append_field(&mut hash, entry.ciphertext);
    append_field(&mut hash, entry.envelope.as_bytes());
    append_field(&mut hash, &entry.redaction_version.to_be_bytes());
    hash.finalize().into()
}

pub(super) fn authenticate_event_head(
    scope: &AgentScope,
    key: &DataKeyMaterial,
    last_seq: u64,
    event_count: u64,
    chain_digest: &[u8; EVENT_DIGEST_BYTES],
) -> Result<Vec<u8>> {
    if key.purpose != DataKeyPurpose::Event {
        bail!("event-log head requires an event data key");
    }
    let aad = scope
        .row_aad(
            "event_log_heads",
            &scope.conversation_id,
            DataKeyPurpose::Event,
        )
        .canonical_bytes();
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key.bytes())
        .expect("HMAC accepts keys of every length");
    mac.update(EVENT_HEAD_DOMAIN);
    update_mac_field(&mut mac, &aad);
    update_mac_field(&mut mac, &last_seq.to_be_bytes());
    update_mac_field(&mut mac, &event_count.to_be_bytes());
    update_mac_field(&mut mac, chain_digest);
    update_mac_field(&mut mac, key.key_ref.as_bytes());
    Ok(mac.finalize().into_bytes().to_vec())
}

pub(super) fn verify_event_head(
    scope: &AgentScope,
    key: &DataKeyMaterial,
    last_seq: u64,
    event_count: u64,
    chain_digest: &[u8],
    expected_hmac: &[u8],
) -> Result<[u8; EVENT_DIGEST_BYTES]> {
    let chain_digest: [u8; EVENT_DIGEST_BYTES] = chain_digest
        .try_into()
        .map_err(|_| anyhow!("event-log head has an invalid chain digest length"))?;
    let actual = authenticate_event_head(scope, key, last_seq, event_count, &chain_digest)?;
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key.bytes())
        .expect("HMAC accepts keys of every length");
    mac.update(EVENT_HEAD_DOMAIN);
    let aad = scope
        .row_aad(
            "event_log_heads",
            &scope.conversation_id,
            DataKeyPurpose::Event,
        )
        .canonical_bytes();
    update_mac_field(&mut mac, &aad);
    update_mac_field(&mut mac, &last_seq.to_be_bytes());
    update_mac_field(&mut mac, &event_count.to_be_bytes());
    update_mac_field(&mut mac, &chain_digest);
    update_mac_field(&mut mac, key.key_ref.as_bytes());
    mac.verify_slice(expected_hmac)
        .map_err(|_| anyhow!("event-log head HMAC mismatch"))?;
    debug_assert_eq!(actual.as_slice(), expected_hmac);
    Ok(chain_digest)
}

fn append_field(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

fn update_mac_field(mac: &mut Hmac<Sha256>, value: &[u8]) {
    mac.update(&(value.len() as u64).to_be_bytes());
    mac.update(value);
}
