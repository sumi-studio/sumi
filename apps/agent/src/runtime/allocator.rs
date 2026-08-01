//! Supervisor-issued runtime allocation parsing for the T26 composition root.
//!
//! The Rust agent does not allocate or infer a generation.  It accepts one
//! complete allocation from its supervisor and turns it into the exact typed
//! authority used by hydration, executor RPC, local control, and Session.

use anyhow::{Context, Result};

use super::{
    authority::RuntimeEpochAuthority,
    contracts::{
        GenerationRecoveryFence, PersonalityAgentId, ProcessGeneration, ProcessGenerationLease,
        RpcBootNonce, RpcIdentity,
    },
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SupervisorAllocation {
    authority: RuntimeEpochAuthority,
}

impl SupervisorAllocation {
    pub(crate) fn from_wire(
        personality_agent_id: &str,
        generation: &str,
        nonce: String,
        lease_id: String,
        fence_id: String,
    ) -> Result<Self> {
        let personality_agent_id = PersonalityAgentId::parse(personality_agent_id)
            .context("SUMI_PERSONALITY_AGENT_ID must be the canonical global PAID")?;
        let generation = generation
            .parse::<u64>()
            .context("SUMI_RPC_GENERATION must be a base-10 integer")
            .and_then(|value| {
                ProcessGeneration::from_wire(value)
                    .context("SUMI_RPC_GENERATION is outside the process-generation domain")
            })?;
        let nonce = RpcBootNonce::new(nonce).context("SUMI_RPC_NONCE is not a valid boot nonce")?;
        let rpc_identity = RpcIdentity::new(personality_agent_id.clone(), generation, nonce);
        let lease = ProcessGenerationLease::new(personality_agent_id, generation, lease_id)
            .context("SUMI_PROCESS_GENERATION_LEASE_ID is invalid")?;
        let fence = GenerationRecoveryFence::new(&lease, fence_id)
            .context("SUMI_GENERATION_RECOVERY_FENCE_ID is invalid")?;
        let authority = RuntimeEpochAuthority::new(rpc_identity, lease, fence)
            .context("supervisor runtime allocation is internally inconsistent")?;
        Ok(Self { authority })
    }

    #[cfg(test)]
    pub(crate) const fn authority(&self) -> &RuntimeEpochAuthority {
        &self.authority
    }

    pub(crate) fn into_authority(self) -> RuntimeEpochAuthority {
        self.authority
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";

    #[test]
    fn allocation_binds_global_paid_generation_nonce_lease_and_fence() {
        let allocation = SupervisorAllocation::from_wire(
            PAID,
            "7",
            "boot-a".to_owned(),
            "lease-a".to_owned(),
            "fence-a".to_owned(),
        )
        .unwrap();
        let authority = allocation.authority();
        assert_eq!(authority.personality_agent_id().as_str(), PAID);
        assert_eq!(authority.generation().as_u64(), 7);
        assert_eq!(authority.nonce().as_str(), "boot-a");
        assert_eq!(authority.lease().lease_id(), "lease-a");
        assert_eq!(authority.fence().fence_id(), "fence-a");
    }

    #[test]
    fn allocation_rejects_noncanonical_paid_and_invalid_generation() {
        assert!(
            SupervisorAllocation::from_wire(
                "agent-local",
                "7",
                "boot".to_owned(),
                "lease".to_owned(),
                "fence".to_owned(),
            )
            .is_err()
        );
        assert!(
            SupervisorAllocation::from_wire(
                PAID,
                &u64::MAX.to_string(),
                "boot".to_owned(),
                "lease".to_owned(),
                "fence".to_owned(),
            )
            .is_err()
        );
    }
}
