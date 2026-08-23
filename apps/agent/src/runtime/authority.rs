//! One authenticated runtime epoch assembled from the neutral identity values.
//!
//! `contracts` deliberately validates individual values without issuing or
//! composing them.  This type is the narrow composition proof that the RPC
//! process identity, generation lease, and recovery fence all describe the
//! same personality-agent process.  It carries no administrative context and
//! grants no authority by itself.

use crate::runtime::contracts::{
    GenerationRecoveryFence, PersonalityAgentId, ProcessGeneration, ProcessGenerationLease,
    RpcBootNonce, RpcIdentity,
};

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[expect(
    clippy::enum_variant_names,
    reason = "the Mismatch suffix is the stable error taxonomy at this authority boundary"
)]
pub(crate) enum RuntimeEpochAuthorityError {
    #[error("runtime RPC identity is not bound to the supplied process-generation lease")]
    LeaseMismatch,
    #[error("runtime recovery fence is not bound to the supplied process-generation lease")]
    FenceMismatch,
    #[error("runtime RPC identity does not match the authenticated runtime epoch")]
    RpcIdentityMismatch,
    #[error("process generation does not match the authenticated runtime epoch")]
    GenerationMismatch,
}

/// Exact authority tuple for one personality-agent process boot.
///
/// The boot nonce distinguishes two processes that happen to present the same
/// PAID and generation.  The lease and fence keep hydration and readiness tied
/// to the independently supplied durable generation authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RuntimeEpochAuthority {
    rpc_identity: RpcIdentity,
    lease: ProcessGenerationLease,
    fence: GenerationRecoveryFence,
}

impl RuntimeEpochAuthority {
    pub(crate) fn new(
        rpc_identity: RpcIdentity,
        lease: ProcessGenerationLease,
        fence: GenerationRecoveryFence,
    ) -> Result<Self, RuntimeEpochAuthorityError> {
        if rpc_identity.personality_agent_id() != lease.personality_agent_id()
            || rpc_identity.generation() != lease.generation()
        {
            return Err(RuntimeEpochAuthorityError::LeaseMismatch);
        }
        fence
            .validate_exact(&lease, fence.fence_id())
            .map_err(|_| RuntimeEpochAuthorityError::FenceMismatch)?;
        Ok(Self {
            rpc_identity,
            lease,
            fence,
        })
    }

    pub(crate) const fn personality_agent_id(&self) -> &PersonalityAgentId {
        self.rpc_identity.personality_agent_id()
    }

    pub(crate) const fn generation(&self) -> ProcessGeneration {
        self.rpc_identity.generation()
    }

    pub(crate) fn nonce(&self) -> &RpcBootNonce {
        self.rpc_identity.nonce()
    }

    pub(crate) const fn rpc_identity(&self) -> &RpcIdentity {
        &self.rpc_identity
    }

    pub(crate) const fn lease(&self) -> &ProcessGenerationLease {
        &self.lease
    }

    pub(crate) const fn fence(&self) -> &GenerationRecoveryFence {
        &self.fence
    }

    pub(crate) fn validate_rpc_identity(
        &self,
        candidate: &RpcIdentity,
    ) -> Result<(), RuntimeEpochAuthorityError> {
        if candidate != &self.rpc_identity {
            return Err(RuntimeEpochAuthorityError::RpcIdentityMismatch);
        }
        Ok(())
    }

    pub(crate) fn validate_generation(
        &self,
        generation: ProcessGeneration,
    ) -> Result<(), RuntimeEpochAuthorityError> {
        if generation != self.generation() {
            return Err(RuntimeEpochAuthorityError::GenerationMismatch);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";
    const OTHER_PAID: &str = "0198f0f4-9b72-7000-8000-000000000002";

    fn identity(personality_agent_id: &str, generation: u64, nonce: &str) -> RpcIdentity {
        RpcIdentity::from_wire(personality_agent_id, generation, nonce).unwrap()
    }

    fn authority() -> RuntimeEpochAuthority {
        let rpc = identity(PAID, 7, "boot-a");
        let lease = ProcessGenerationLease::new(
            rpc.personality_agent_id().clone(),
            rpc.generation(),
            "lease-a",
        )
        .unwrap();
        let fence = GenerationRecoveryFence::new(&lease, "fence-a").unwrap();
        RuntimeEpochAuthority::new(rpc, lease, fence).unwrap()
    }

    #[test]
    fn construction_requires_one_exact_personality_generation_and_fence() {
        let expected = authority();
        assert_eq!(expected.personality_agent_id().as_str(), PAID);
        assert_eq!(expected.generation().as_u64(), 7);
        assert_eq!(expected.nonce().as_str(), "boot-a");

        let other_rpc = identity(OTHER_PAID, 7, "boot-a");
        assert_eq!(
            RuntimeEpochAuthority::new(
                other_rpc,
                expected.lease().clone(),
                expected.fence().clone(),
            ),
            Err(RuntimeEpochAuthorityError::LeaseMismatch)
        );

        let stale_rpc = identity(PAID, 8, "boot-a");
        assert_eq!(
            RuntimeEpochAuthority::new(
                stale_rpc,
                expected.lease().clone(),
                expected.fence().clone(),
            ),
            Err(RuntimeEpochAuthorityError::LeaseMismatch)
        );

        let other_lease = ProcessGenerationLease::new(
            expected.personality_agent_id().clone(),
            expected.generation(),
            "lease-b",
        )
        .unwrap();
        assert_eq!(
            RuntimeEpochAuthority::new(
                expected.rpc_identity().clone(),
                other_lease,
                expected.fence().clone(),
            ),
            Err(RuntimeEpochAuthorityError::FenceMismatch)
        );
    }

    #[test]
    fn rpc_validation_includes_boot_nonce() {
        let expected = authority();
        assert!(
            expected
                .validate_rpc_identity(expected.rpc_identity())
                .is_ok()
        );
        assert_eq!(
            expected.validate_rpc_identity(&identity(PAID, 7, "boot-b")),
            Err(RuntimeEpochAuthorityError::RpcIdentityMismatch)
        );
    }
}
