//! Checked runtime identities.
//!
//! This module only validates caller-supplied identities. Allocation,
//! issuance, persistence, lease acquisition, and bootstrap belong to later
//! production-runtime tasks.

use std::fmt;

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
    #[error("hydration receipt identity cannot be empty or exceed {MAX_OPAQUE_ID_BYTES} bytes")]
    InvalidHydrationReceiptIdentity,
    #[error("hydration ready is already latched for generation {generation}")]
    HydrationAlreadyLatched { generation: u64 },
    #[error("hydration ready latch rejected a stale or mismatched generation")]
    HydrationGenerationMismatch,
}

/// Stable identity attached to a hydration receipt.  T17 owns the durable
/// receipt; T26 generates the identity for clean conversations and binds it to
/// the `Ready` state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HydrationReceiptIdentity(String);

impl HydrationReceiptIdentity {
    pub fn new(value: impl Into<String>) -> Result<Self, RuntimeContractError> {
        Ok(Self(validate_opaque(
            value.into(),
            "hydration receipt identity",
        )?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Per-`ProcessGeneration` hydration latch.  T26 initializes this as `NotReady`,
/// then transitions it exactly once to `Ready { generation, ... }` after the
/// production `RunCore` composition has completed for a clean generation.
///
/// Rollover must invalidate the old `Ready` before the new generation is made
/// visible to any caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HydrationReady {
    NotReady,
    Ready {
        generation: ProcessGeneration,
        hydration_receipt_identity: HydrationReceiptIdentity,
    },
}

impl HydrationReady {
    pub const fn not_ready() -> Self {
        Self::NotReady
    }

    /// Latch `Ready` exactly once.  Rejects re-latching, generation mismatches,
    /// and attempts to move from an already-latched state for a different
    /// generation without first invalidating it.
    pub fn latch(
        &mut self,
        generation: ProcessGeneration,
        identity: HydrationReceiptIdentity,
    ) -> Result<(), RuntimeContractError> {
        if let Self::Ready {
            generation: latched,
            ..
        } = self
        {
            if *latched == generation {
                return Err(RuntimeContractError::HydrationAlreadyLatched {
                    generation: generation.as_u64(),
                });
            }
            return Err(RuntimeContractError::HydrationGenerationMismatch);
        }
        *self = Self::Ready {
            generation,
            hydration_receipt_identity: identity,
        };
        Ok(())
    }

    /// Rollover helper: invalidate an existing `Ready` and reset to `NotReady`.
    pub fn invalidate(&mut self) {
        *self = Self::NotReady;
    }
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
        let fence_id = validate_opaque(fence_id.into(), "generation recovery fence identity")?;
        let expected = format!("fence-for-{}", lease.lease_id());
        if fence_id != expected {
            return Err(RuntimeContractError::GenerationRecoveryFenceMismatch);
        }
        Ok(Self {
            generation: lease.generation,
            lease_id: lease.lease_id.clone(),
            fence_id,
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

        let fence_id = format!("fence-for-{}", lease.lease_id());
        let other_lease_id = format!("fence-for-{}", other_identity_lease.lease_id());
        let fence = GenerationRecoveryFence::new(&lease, &fence_id).unwrap();
        assert!(fence.validate_exact(&lease, &fence_id).is_ok());
        assert!(
            fence
                .validate_exact(&other_generation_lease, &fence_id)
                .is_err()
        );
        assert!(
            fence
                .validate_exact(&other_identity_lease, &other_lease_id)
                .is_err()
        );
        assert!(fence.validate_exact(&lease, "fence-for-lease-2").is_err());
        assert!(ProcessGenerationLease::new(generation, "").is_err());
        assert!(GenerationRecoveryFence::new(&lease, "").is_err());
    }
}
