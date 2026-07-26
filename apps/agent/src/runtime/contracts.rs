//! Checked runtime identities.
//!
//! This module only validates caller-supplied identities. Allocation,
//! issuance, persistence, lease acquisition, and bootstrap belong to later
//! production-runtime tasks.

use std::fmt;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub const MAX_PROCESS_GENERATION: u64 = i64::MAX as u64;
pub const MAX_OPAQUE_ID_BYTES: usize = 128;

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum RuntimeContractError {
    #[error("process generation must be in 0..={MAX_PROCESS_GENERATION}")]
    ProcessGenerationOutOfRange,
    #[error("SQLite process generation must not be negative: {0}")]
    NegativeSqliteProcessGeneration(i64),
    #[error("{kind} must contain 1..={MAX_OPAQUE_ID_BYTES} bytes")]
    InvalidOpaqueIdentity { kind: &'static str },
    #[error("RPC generation or boot nonce mismatch")]
    RpcIdentityMismatch,
    #[error("process generation lease generation or opaque identity mismatch")]
    ProcessGenerationLeaseMismatch,
    #[error("generation recovery fence lease/generation or opaque identity mismatch")]
    GenerationRecoveryFenceMismatch,
}

/// A process generation that is exactly representable by SQLite `INTEGER`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessGeneration(i64);

impl ProcessGeneration {
    pub const MIN: Self = Self(0);
    pub const MAX: Self = Self(i64::MAX);

    pub fn from_wire(value: u64) -> Result<Self, RuntimeContractError> {
        Self::try_from(value)
    }

    pub fn from_sqlite(value: i64) -> Result<Self, RuntimeContractError> {
        Self::try_from(value)
    }

    pub fn as_u64(self) -> u64 {
        u64::try_from(self.0).expect("validated process generation is nonnegative")
    }

    pub const fn as_i64(self) -> i64 {
        self.0
    }

    pub fn to_wire(self) -> u64 {
        self.as_u64()
    }
}

impl TryFrom<u64> for ProcessGeneration {
    type Error = RuntimeContractError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        i64::try_from(value)
            .map(Self)
            .map_err(|_| RuntimeContractError::ProcessGenerationOutOfRange)
    }
}

impl TryFrom<i64> for ProcessGeneration {
    type Error = RuntimeContractError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value < 0 {
            return Err(RuntimeContractError::NegativeSqliteProcessGeneration(value));
        }
        Ok(Self(value))
    }
}

impl From<ProcessGeneration> for u64 {
    fn from(value: ProcessGeneration) -> Self {
        value.as_u64()
    }
}

impl From<ProcessGeneration> for i64 {
    fn from(value: ProcessGeneration) -> Self {
        value.as_i64()
    }
}

impl fmt::Display for ProcessGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for ProcessGeneration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.as_u64())
    }
}

impl<'de> Deserialize<'de> for ProcessGeneration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ProcessGenerationVisitor;

        impl<'de> Visitor<'de> for ProcessGenerationVisitor {
            type Value = ProcessGeneration;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(
                    formatter,
                    "a process generation in 0..={MAX_PROCESS_GENERATION}"
                )
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                ProcessGeneration::from_sqlite(value).map_err(|e| E::custom(e.to_string()))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                ProcessGeneration::from_wire(value).map_err(|e| E::custom(e.to_string()))
            }
        }

        deserializer.deserialize_u64(ProcessGenerationVisitor)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RpcBootNonce(String);

impl RpcBootNonce {
    pub fn new(value: impl Into<String>) -> Result<Self, RuntimeContractError> {
        Ok(Self(validate_opaque(value.into(), "RPC nonce")?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RpcIdentity {
    generation: ProcessGeneration,
    nonce: RpcBootNonce,
}

impl RpcIdentity {
    pub const fn new(generation: ProcessGeneration, nonce: RpcBootNonce) -> Self {
        Self { generation, nonce }
    }

    pub fn from_wire(
        generation: u64,
        nonce: impl Into<String>,
    ) -> Result<Self, RuntimeContractError> {
        Ok(Self::new(
            ProcessGeneration::from_wire(generation)?,
            RpcBootNonce::new(nonce)?,
        ))
    }

    pub const fn generation(&self) -> ProcessGeneration {
        self.generation
    }

    pub fn nonce(&self) -> &RpcBootNonce {
        &self.nonce
    }

    pub fn validate_wire(&self, generation: u64, nonce: &str) -> Result<(), RuntimeContractError> {
        let generation = ProcessGeneration::from_wire(generation)?;
        let nonce = RpcBootNonce::new(nonce)?;
        if generation != self.generation || nonce != self.nonce {
            return Err(RuntimeContractError::RpcIdentityMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessGenerationLease {
    generation: ProcessGeneration,
    lease_id: String,
}

impl ProcessGenerationLease {
    pub fn new(
        generation: ProcessGeneration,
        lease_id: impl Into<String>,
    ) -> Result<Self, RuntimeContractError> {
        Ok(Self {
            generation,
            lease_id: validate_opaque(lease_id.into(), "process generation lease identity")?,
        })
    }

    pub const fn generation(&self) -> ProcessGeneration {
        self.generation
    }

    pub fn lease_id(&self) -> &str {
        &self.lease_id
    }

    pub fn validate_exact(
        &self,
        generation: ProcessGeneration,
        lease_id: &str,
    ) -> Result<(), RuntimeContractError> {
        let lease_id = validate_opaque(lease_id.to_owned(), "process generation lease identity")?;
        if generation != self.generation || lease_id != self.lease_id {
            return Err(RuntimeContractError::ProcessGenerationLeaseMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenerationRecoveryFence {
    generation: ProcessGeneration,
    lease_id: String,
    fence_id: String,
}

impl GenerationRecoveryFence {
    pub fn new(
        lease: &ProcessGenerationLease,
        fence_id: impl Into<String>,
    ) -> Result<Self, RuntimeContractError> {
        Ok(Self {
            generation: lease.generation,
            lease_id: lease.lease_id.clone(),
            fence_id: validate_opaque(fence_id.into(), "generation recovery fence identity")?,
        })
    }

    pub const fn generation(&self) -> ProcessGeneration {
        self.generation
    }

    pub fn lease_id(&self) -> &str {
        &self.lease_id
    }

    pub fn fence_id(&self) -> &str {
        &self.fence_id
    }

    pub fn validate_exact(
        &self,
        lease: &ProcessGenerationLease,
        fence_id: &str,
    ) -> Result<(), RuntimeContractError> {
        let fence_id = validate_opaque(fence_id.to_owned(), "generation recovery fence identity")?;
        if self.generation != lease.generation
            || self.lease_id != lease.lease_id
            || self.fence_id != fence_id
        {
            return Err(RuntimeContractError::GenerationRecoveryFenceMismatch);
        }
        Ok(())
    }
}

fn validate_opaque(value: String, kind: &'static str) -> Result<String, RuntimeContractError> {
    if value.is_empty() || value.len() > MAX_OPAQUE_ID_BYTES {
        return Err(RuntimeContractError::InvalidOpaqueIdentity { kind });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_generation_accepts_exact_domain_and_converts_losslessly() {
        for raw in [0, MAX_PROCESS_GENERATION] {
            let generation = ProcessGeneration::from_wire(raw).expect("valid generation");
            assert_eq!(generation.as_u64(), raw);
            assert_eq!(generation.to_wire(), raw);
            assert_eq!(generation.as_i64(), raw as i64);
            assert_eq!(ProcessGeneration::from_sqlite(raw as i64), Ok(generation));
        }
        assert_eq!(
            ProcessGeneration::from_wire(MAX_PROCESS_GENERATION + 1),
            Err(RuntimeContractError::ProcessGenerationOutOfRange)
        );
        assert_eq!(
            ProcessGeneration::from_sqlite(-1),
            Err(RuntimeContractError::NegativeSqliteProcessGeneration(-1))
        );
    }

    #[test]
    fn rpc_nonce_preserves_existing_nonempty_128_byte_contract() {
        assert!(RpcBootNonce::new("n").is_ok());
        assert!(RpcBootNonce::new("n".repeat(MAX_OPAQUE_ID_BYTES)).is_ok());
        assert!(RpcBootNonce::new("").is_err());
        assert!(RpcBootNonce::new("n".repeat(MAX_OPAQUE_ID_BYTES + 1)).is_err());
    }

    #[test]
    fn rpc_identity_requires_exact_typed_wire_identity() {
        let identity = RpcIdentity::new(
            ProcessGeneration::from_wire(7).unwrap(),
            RpcBootNonce::new("boot-nonce").unwrap(),
        );
        assert!(identity.validate_wire(7, "boot-nonce").is_ok());
        assert!(identity.validate_wire(8, "boot-nonce").is_err());
        assert!(identity.validate_wire(7, "stale-nonce").is_err());
        assert!(
            identity
                .validate_wire(MAX_PROCESS_GENERATION + 1, "boot-nonce")
                .is_err()
        );
    }

    #[test]
    fn lease_and_fence_require_exact_generation_and_opaque_identities() {
        let generation = ProcessGeneration::from_wire(7).unwrap();
        let other_generation = ProcessGeneration::from_wire(8).unwrap();
        let lease = ProcessGenerationLease::new(generation, "lease-1").unwrap();
        let other_generation_lease =
            ProcessGenerationLease::new(other_generation, "lease-1").unwrap();
        let other_identity_lease = ProcessGenerationLease::new(generation, "lease-2").unwrap();
        assert!(lease.validate_exact(generation, "lease-1").is_ok());
        assert!(lease.validate_exact(other_generation, "lease-1").is_err());
        assert!(lease.validate_exact(generation, "lease-2").is_err());

        let fence = GenerationRecoveryFence::new(&lease, "fence-1").unwrap();
        assert!(fence.validate_exact(&lease, "fence-1").is_ok());
        assert!(
            fence
                .validate_exact(&other_generation_lease, "fence-1")
                .is_err()
        );
        assert!(
            fence
                .validate_exact(&other_identity_lease, "fence-1")
                .is_err()
        );
        assert!(fence.validate_exact(&lease, "fence-2").is_err());
        assert!(ProcessGenerationLease::new(generation, "").is_err());
        assert!(GenerationRecoveryFence::new(&lease, "").is_err());
    }
}
