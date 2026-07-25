use std::{env, fmt};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use chacha20poly1305::{
    KeyInit, XChaCha20Poly1305, XNonce,
    aead::{Aead, Payload},
};
use hmac::{Hmac, Mac};
use rand::TryRngCore;
use sha2::Sha256;
use zeroize::Zeroize;

use crate::gateway::{CommandDigestFactory, IncrementalCommandDigest, KeyedCommandDigest};

pub const CONTENT_ENVELOPE_VERSION: u8 = 1;
pub const CONTENT_NONCE_BYTES: usize = 24;
pub const DATA_KEY_BYTES: usize = 32;
pub const WRAP_ALGORITHM: &str = "xchacha20-poly1305/v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataKeyScope {
    Conversation,
    Agent,
}

impl DataKeyScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Conversation => "conversation",
            Self::Agent => "agent",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DataKeyPurpose {
    Transcript,
    Event,
    MemorySummary,
    ProviderContext,
    Command,
    Mutation,
    Artifact,
    Workspace,
}

impl DataKeyPurpose {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transcript => "transcript",
            Self::Event => "event",
            Self::MemorySummary => "memory_summary",
            Self::ProviderContext => "provider_context",
            Self::Command => "command",
            Self::Mutation => "mutation",
            Self::Artifact => "artifact",
            Self::Workspace => "workspace",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "transcript" => Ok(Self::Transcript),
            "event" => Ok(Self::Event),
            "memory_summary" => Ok(Self::MemorySummary),
            "provider_context" => Ok(Self::ProviderContext),
            "command" => Ok(Self::Command),
            "mutation" => Ok(Self::Mutation),
            "artifact" => Ok(Self::Artifact),
            "workspace" => Ok(Self::Workspace),
            _ => bail!("unknown data-key purpose {value}"),
        }
    }
}

pub struct WrappingKey {
    key_id: String,
    bytes: [u8; DATA_KEY_BYTES],
}

impl WrappingKey {
    pub fn new(key_id: impl Into<String>, bytes: [u8; DATA_KEY_BYTES]) -> Self {
        Self {
            key_id: key_id.into(),
            bytes,
        }
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub(crate) fn bytes(&self) -> &[u8; DATA_KEY_BYTES] {
        &self.bytes
    }
}

impl Clone for WrappingKey {
    fn clone(&self) -> Self {
        Self::new(self.key_id.clone(), self.bytes)
    }
}

impl fmt::Debug for WrappingKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WrappingKey")
            .field("key_id", &self.key_id)
            .field("bytes", &"[REDACTED]")
            .finish()
    }
}

impl Drop for WrappingKey {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

#[async_trait]
pub trait KeyProvider: Send + Sync {
    async fn current_key(&self) -> Result<WrappingKey>;
    async fn key_by_id(&self, key_id: &str) -> Result<WrappingKey>;
}

#[derive(Clone)]
pub struct EnvironmentKeyProvider {
    key: WrappingKey,
}

impl EnvironmentKeyProvider {
    pub fn from_env(variable: &str, key_id: impl Into<String>) -> Result<Self> {
        let mut encoded = env::var(variable)
            .with_context(|| format!("required local test wrapping key {variable} is not set"))?;
        let decoded = decode_hex_key(&encoded)
            .with_context(|| format!("{variable} must contain exactly 64 hexadecimal characters"));
        encoded.zeroize();
        let bytes = decoded?;
        Ok(Self {
            key: WrappingKey::new(key_id, bytes),
        })
    }
}

#[async_trait]
impl KeyProvider for EnvironmentKeyProvider {
    async fn current_key(&self) -> Result<WrappingKey> {
        Ok(self.key.clone())
    }

    async fn key_by_id(&self, key_id: &str) -> Result<WrappingKey> {
        if key_id != self.key.key_id() {
            bail!("wrapping key {key_id} is unavailable");
        }
        Ok(self.key.clone())
    }
}

pub(crate) struct DataKeyMaterial {
    pub key_ref: String,
    pub purpose: DataKeyPurpose,
    bytes: [u8; DATA_KEY_BYTES],
}

impl DataKeyMaterial {
    pub(crate) fn generate(key_ref: impl Into<String>, purpose: DataKeyPurpose) -> Result<Self> {
        let mut bytes = [0_u8; DATA_KEY_BYTES];
        rand::rngs::OsRng
            .try_fill_bytes(&mut bytes)
            .map_err(|error| anyhow!("operating-system random source failed: {error}"))?;
        Ok(Self {
            key_ref: key_ref.into(),
            purpose,
            bytes,
        })
    }

    pub(crate) fn from_bytes(
        key_ref: impl Into<String>,
        purpose: DataKeyPurpose,
        bytes: [u8; DATA_KEY_BYTES],
    ) -> Self {
        Self {
            key_ref: key_ref.into(),
            purpose,
            bytes,
        }
    }

    pub(crate) fn bytes(&self) -> &[u8; DATA_KEY_BYTES] {
        &self.bytes
    }
}

impl fmt::Debug for DataKeyMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DataKeyMaterial")
            .field("key_ref", &self.key_ref)
            .field("purpose", &self.purpose)
            .field("bytes", &"[REDACTED]")
            .finish()
    }
}

impl Drop for DataKeyMaterial {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

pub(crate) struct ConversationCommandDigestFactory {
    key_ref: String,
    key: [u8; DATA_KEY_BYTES],
}

impl ConversationCommandDigestFactory {
    pub(crate) fn new(data_key: &DataKeyMaterial) -> Result<Self> {
        if data_key.purpose != DataKeyPurpose::Command {
            bail!("command digest factory requires a command data key");
        }
        Ok(Self {
            key_ref: data_key.key_ref.clone(),
            key: *data_key.bytes(),
        })
    }
}

impl Drop for ConversationCommandDigestFactory {
    fn drop(&mut self) {
        self.key.zeroize();
    }
}

impl CommandDigestFactory for ConversationCommandDigestFactory {
    fn start(&self) -> Box<dyn IncrementalCommandDigest> {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.key)
            .expect("HMAC accepts keys of every length");
        mac.update(COMMAND_PAYLOAD_DIGEST_DOMAIN);
        Box::new(CommandDigestAccumulator {
            key_ref: self.key_ref.clone(),
            mac,
            payload_bytes: 0,
        })
    }
}

const COMMAND_PAYLOAD_DIGEST_DOMAIN: &[u8] = b"sumi-command-payload/v1";

struct CommandDigestAccumulator {
    key_ref: String,
    mac: Hmac<Sha256>,
    payload_bytes: u64,
}

impl IncrementalCommandDigest for CommandDigestAccumulator {
    fn update(&mut self, bytes: &[u8]) {
        self.mac.update(bytes);
        self.payload_bytes = self.payload_bytes.saturating_add(bytes.len() as u64);
    }

    fn finish(self: Box<Self>) -> KeyedCommandDigest {
        let Self {
            key_ref,
            mut mac,
            payload_bytes,
        } = *self;
        mac.update(&payload_bytes.to_be_bytes());
        KeyedCommandDigest::new(key_ref, mac.finalize().into_bytes().into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowAad {
    pub tenant_id: String,
    pub agent_id: String,
    pub conversation_id: String,
    pub table: String,
    pub row_id: String,
    pub purpose: String,
    pub schema_version: u32,
}

impl RowAad {
    pub fn canonical_bytes(&self) -> Vec<u8> {
        canonical_fields(
            b"sumi-row-aad/v1",
            [
                self.tenant_id.as_bytes(),
                self.agent_id.as_bytes(),
                self.conversation_id.as_bytes(),
                self.table.as_bytes(),
                self.row_id.as_bytes(),
                self.purpose.as_bytes(),
                &self.schema_version.to_be_bytes(),
            ],
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KeyWrapAad {
    pub key_ref: String,
    pub scope: DataKeyScope,
    pub purpose: DataKeyPurpose,
    pub conversation_id: Option<String>,
    pub wrap_key_id: String,
}

impl KeyWrapAad {
    fn canonical_bytes(&self) -> Vec<u8> {
        canonical_fields(
            b"sumi-key-wrap-aad/v1",
            [
                self.key_ref.as_bytes(),
                self.scope.as_str().as_bytes(),
                self.purpose.as_str().as_bytes(),
                self.conversation_id.as_deref().unwrap_or("").as_bytes(),
                self.wrap_key_id.as_bytes(),
            ],
        )
    }
}

pub(crate) fn wrap_data_key(
    data_key: &DataKeyMaterial,
    wrapping_key: &WrappingKey,
    aad: &KeyWrapAad,
) -> Result<([u8; CONTENT_NONCE_BYTES], Vec<u8>)> {
    if aad.key_ref != data_key.key_ref
        || aad.purpose != data_key.purpose
        || aad.wrap_key_id != wrapping_key.key_id()
    {
        bail!("data-key wrap metadata does not match key material");
    }
    let nonce = random_nonce()?;
    let ciphertext = aead_encrypt(
        wrapping_key.bytes(),
        &nonce,
        data_key.bytes(),
        &aad.canonical_bytes(),
    )?;
    Ok((nonce, ciphertext))
}

pub(crate) fn unwrap_data_key(
    key_ref: impl Into<String>,
    purpose: DataKeyPurpose,
    wrapped_key: &[u8],
    wrap_nonce: &[u8],
    wrapping_key: &WrappingKey,
    aad: &KeyWrapAad,
) -> Result<DataKeyMaterial> {
    let key_ref = key_ref.into();
    if aad.key_ref != key_ref || aad.purpose != purpose || aad.wrap_key_id != wrapping_key.key_id()
    {
        bail!("data-key unwrap metadata does not match stored key");
    }
    let nonce: [u8; CONTENT_NONCE_BYTES] = wrap_nonce
        .try_into()
        .map_err(|_| anyhow!("invalid data-key wrap nonce length"))?;
    let mut plaintext = aead_decrypt(
        wrapping_key.bytes(),
        &nonce,
        wrapped_key,
        &aad.canonical_bytes(),
    )?;
    let bytes: Result<[u8; DATA_KEY_BYTES]> = plaintext
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("invalid unwrapped data-key length"));
    plaintext.zeroize();
    let bytes = bytes?;
    Ok(DataKeyMaterial::from_bytes(key_ref, purpose, bytes))
}

pub(crate) fn encrypt_content(
    data_key: &DataKeyMaterial,
    plaintext: &[u8],
    aad: &RowAad,
) -> Result<Vec<u8>> {
    if aad.purpose != data_key.purpose.as_str() {
        bail!("row AAD purpose does not match data key");
    }
    let nonce = random_nonce()?;
    let ciphertext = aead_encrypt(data_key.bytes(), &nonce, plaintext, &aad.canonical_bytes())?;
    let mut envelope = Vec::with_capacity(1 + nonce.len() + ciphertext.len());
    envelope.push(CONTENT_ENVELOPE_VERSION);
    envelope.extend_from_slice(&nonce);
    envelope.extend_from_slice(&ciphertext);
    Ok(envelope)
}

pub(crate) fn decrypt_content(
    data_key: &DataKeyMaterial,
    envelope: &[u8],
    aad: &RowAad,
) -> Result<Vec<u8>> {
    if aad.purpose != data_key.purpose.as_str() {
        bail!("row AAD purpose does not match data key");
    }
    let minimum = 1 + CONTENT_NONCE_BYTES + 16;
    if envelope.len() < minimum {
        bail!("content envelope is truncated");
    }
    if envelope[0] != CONTENT_ENVELOPE_VERSION {
        bail!("unsupported content envelope version");
    }
    let nonce: [u8; CONTENT_NONCE_BYTES] = envelope[1..1 + CONTENT_NONCE_BYTES]
        .try_into()
        .map_err(|_| anyhow!("invalid content nonce length"))?;
    aead_decrypt(
        data_key.bytes(),
        &nonce,
        &envelope[1 + CONTENT_NONCE_BYTES..],
        &aad.canonical_bytes(),
    )
}

pub(crate) fn command_payload_digest(
    data_key: &DataKeyMaterial,
    canonical_payload: &[u8],
) -> Vec<u8> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(data_key.bytes())
        .expect("HMAC accepts keys of every length");
    mac.update(COMMAND_PAYLOAD_DIGEST_DOMAIN);
    mac.update(canonical_payload);
    mac.update(&(canonical_payload.len() as u64).to_be_bytes());
    mac.finalize().into_bytes().to_vec()
}

pub(crate) fn verify_command_payload_digest(
    data_key: &DataKeyMaterial,
    canonical_payload: &[u8],
    expected: &[u8],
) -> Result<()> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(data_key.bytes())
        .expect("HMAC accepts keys of every length");
    mac.update(COMMAND_PAYLOAD_DIGEST_DOMAIN);
    mac.update(canonical_payload);
    mac.update(&(canonical_payload.len() as u64).to_be_bytes());
    mac.verify_slice(expected)
        .map_err(|_| anyhow!("command payload digest mismatch"))
}

pub(super) fn keyed_proof(data_key: &DataKeyMaterial, domain: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(data_key.bytes())
        .expect("HMAC accepts keys of every length");
    mac.update(domain);
    mac.update(&(payload.len() as u64).to_be_bytes());
    mac.update(payload);
    mac.finalize().into_bytes().to_vec()
}

pub(super) fn verify_keyed_proof(
    data_key: &DataKeyMaterial,
    domain: &[u8],
    payload: &[u8],
    expected: &[u8],
) -> Result<()> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(data_key.bytes())
        .expect("HMAC accepts keys of every length");
    mac.update(domain);
    mac.update(&(payload.len() as u64).to_be_bytes());
    mac.update(payload);
    mac.verify_slice(expected)
        .map_err(|_| anyhow!("keyed proof mismatch"))
}

pub(crate) fn aead_encrypt(
    key: &[u8; DATA_KEY_BYTES],
    nonce: &[u8; CONTENT_NONCE_BYTES],
    plaintext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>> {
    let cipher =
        XChaCha20Poly1305::new_from_slice(key).map_err(|_| anyhow!("invalid AEAD key length"))?;
    cipher
        .encrypt(
            XNonce::from_slice(nonce),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| anyhow!("AEAD encryption failed"))
}

pub(crate) fn aead_decrypt(
    key: &[u8; DATA_KEY_BYTES],
    nonce: &[u8; CONTENT_NONCE_BYTES],
    ciphertext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>> {
    let cipher =
        XChaCha20Poly1305::new_from_slice(key).map_err(|_| anyhow!("invalid AEAD key length"))?;
    cipher
        .decrypt(
            XNonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map_err(|_| anyhow!("AEAD authentication failed"))
}

pub(crate) fn random_nonce() -> Result<[u8; CONTENT_NONCE_BYTES]> {
    let mut nonce = [0_u8; CONTENT_NONCE_BYTES];
    rand::rngs::OsRng
        .try_fill_bytes(&mut nonce)
        .map_err(|error| anyhow!("operating-system random source failed: {error}"))?;
    Ok(nonce)
}

pub(crate) fn canonical_fields<const N: usize>(domain: &[u8], fields: [&[u8]; N]) -> Vec<u8> {
    let capacity = domain.len()
        + fields
            .iter()
            .map(|field| 8_usize.saturating_add(field.len()))
            .sum::<usize>();
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(domain);
    for field in fields {
        output.extend_from_slice(&(field.len() as u64).to_be_bytes());
        output.extend_from_slice(field);
    }
    output
}

pub(crate) fn decode_hex_key(encoded: &str) -> Result<[u8; DATA_KEY_BYTES]> {
    if encoded.len() != DATA_KEY_BYTES * 2 {
        bail!("invalid key length");
    }
    let mut bytes = [0_u8; DATA_KEY_BYTES];
    for (slot, pair) in bytes.iter_mut().zip(encoded.as_bytes().chunks_exact(2)) {
        let high = decode_hex_nibble(pair[0]).ok_or_else(|| anyhow!("invalid hexadecimal key"))?;
        let low = decode_hex_nibble(pair[1]).ok_or_else(|| anyhow!("invalid hexadecimal key"))?;
        *slot = (high << 4) | low;
    }
    Ok(bytes)
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    use super::*;

    #[test]
    fn content_envelope_binds_every_row_aad_field() {
        let data_key =
            DataKeyMaterial::from_bytes("key-1", DataKeyPurpose::Transcript, [7; DATA_KEY_BYTES]);
        let aad = RowAad {
            tenant_id: "tenant-1".to_owned(),
            agent_id: "agent-1".to_owned(),
            conversation_id: "conversation-1".to_owned(),
            table: "messages".to_owned(),
            row_id: "message-1".to_owned(),
            purpose: "transcript".to_owned(),
            schema_version: 1,
        };
        let encrypted = encrypt_content(&data_key, b"private text", &aad).expect("encrypt");
        assert_eq!(encrypted[0], CONTENT_ENVELOPE_VERSION);
        assert_eq!(
            decrypt_content(&data_key, &encrypted, &aad).expect("decrypt"),
            b"private text"
        );

        let mutations = [
            RowAad {
                tenant_id: "tenant-2".to_owned(),
                ..aad.clone()
            },
            RowAad {
                agent_id: "agent-2".to_owned(),
                ..aad.clone()
            },
            RowAad {
                conversation_id: "conversation-2".to_owned(),
                ..aad.clone()
            },
            RowAad {
                table: "agent_events".to_owned(),
                ..aad.clone()
            },
            RowAad {
                row_id: "message-2".to_owned(),
                ..aad.clone()
            },
            RowAad {
                schema_version: 2,
                ..aad.clone()
            },
        ];
        for wrong_aad in mutations {
            assert!(decrypt_content(&data_key, &encrypted, &wrong_aad).is_err());
        }
    }

    #[test]
    fn swapping_ciphertext_between_rows_is_rejected() {
        let data_key =
            DataKeyMaterial::from_bytes("key-1", DataKeyPurpose::Transcript, [3; DATA_KEY_BYTES]);
        let first = RowAad {
            tenant_id: "tenant".to_owned(),
            agent_id: "agent".to_owned(),
            conversation_id: "conversation".to_owned(),
            table: "messages".to_owned(),
            row_id: "message-1".to_owned(),
            purpose: "transcript".to_owned(),
            schema_version: 1,
        };
        let second = RowAad {
            row_id: "message-2".to_owned(),
            ..first.clone()
        };
        let first_ciphertext = encrypt_content(&data_key, b"first", &first).expect("encrypt first");
        let second_ciphertext =
            encrypt_content(&data_key, b"second", &second).expect("encrypt second");

        assert!(decrypt_content(&data_key, &second_ciphertext, &first).is_err());
        assert!(decrypt_content(&data_key, &first_ciphertext, &second).is_err());
    }

    #[test]
    fn command_digest_rejects_payload_replacement() {
        let data_key =
            DataKeyMaterial::from_bytes("key-1", DataKeyPurpose::Command, [9; DATA_KEY_BYTES]);
        let digest = command_payload_digest(&data_key, br#"{"type":"abort"}"#);
        verify_command_payload_digest(&data_key, br#"{"type":"abort"}"#, &digest)
            .expect("same payload");
        assert!(
            verify_command_payload_digest(&data_key, br#"{"type":"user_message"}"#, &digest)
                .is_err()
        );
    }

    #[test]
    fn command_digest_framing_is_domain_separated_and_length_unambiguous() {
        let data_key =
            DataKeyMaterial::from_bytes("key-1", DataKeyPurpose::Command, [9; DATA_KEY_BYTES]);
        let payload = b"same bytes";
        let command_digest = command_payload_digest(&data_key, payload);
        let internal_proof = keyed_proof(&data_key, b"sumi-internal-proof/v1", payload);
        assert_ne!(command_digest, internal_proof);

        let mut payload_with_suffix_like_bytes = payload.to_vec();
        payload_with_suffix_like_bytes.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        assert_ne!(
            command_digest,
            command_payload_digest(&data_key, &payload_with_suffix_like_bytes)
        );
    }

    #[test]
    fn rustcrypto_hmac_matches_rfc4231_vectors_and_chunk_boundaries() {
        let vectors = [
            (
                vec![0x0b; 20],
                b"Hi There".to_vec(),
                "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7",
            ),
            (
                b"Jefe".to_vec(),
                b"what do ya want for nothing?".to_vec(),
                "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843",
            ),
            (
                vec![0xaa; 20],
                vec![0xdd; 50],
                "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe",
            ),
        ];
        for (key, data, expected) in vectors {
            let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&key).expect("valid RFC HMAC key");
            mac.update(&data);
            assert_eq!(
                mac.finalize().into_bytes().as_slice(),
                decode_hex_key(expected).expect("valid RFC digest")
            );
        }

        let key = [0x4b; DATA_KEY_BYTES];
        let data_key = DataKeyMaterial::from_bytes("command-key", DataKeyPurpose::Command, key);
        let factory = ConversationCommandDigestFactory::new(&data_key).expect("digest factory");
        let payload: Vec<u8> = (0_u16..=511).map(|value| value as u8).collect();
        let expected = command_payload_digest(&data_key, &payload);
        for chunk_size in [1, 31, 63, 64, 65, 127, 128, 129, 511, 512] {
            let mut incremental = factory.start();
            for chunk in payload.chunks(chunk_size) {
                incremental.update(chunk);
            }
            assert_eq!(incremental.finish().hmac(), expected.as_slice());
        }

        let payload = br#"{"type":"abort"}"#;
        let actual = command_payload_digest(&data_key, payload);
        verify_command_payload_digest(&data_key, payload, &actual)
            .expect("one-shot digest verifies");
        assert!(verify_command_payload_digest(&data_key, b"replacement", &actual).is_err());
    }

    #[test]
    fn non_ascii_hex_key_with_valid_byte_length_returns_an_error_without_panicking() {
        let encoded = "é".repeat(DATA_KEY_BYTES);
        assert_eq!(encoded.len(), DATA_KEY_BYTES * 2);
        assert_eq!(
            decode_hex_key(&encoded)
                .expect_err("non-ASCII bytes are not hexadecimal")
                .to_string(),
            "invalid hexadecimal key"
        );
    }
}
