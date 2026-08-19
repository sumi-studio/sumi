-- The control plane's observed-empty proof is durable receipt evidence, not
-- an inference from the replacement runtime generation. Existing receipt rows
-- predate this contract and intentionally have no fabricated attestation row.
CREATE TABLE physical_recovery_receipt_reap_attestations (
  receipt_id TEXT NOT NULL PRIMARY KEY,
  attestation_digest TEXT NOT NULL,
  personality_agent_id TEXT NOT NULL REFERENCES agent_scope(personality_agent_id)
    CHECK (sumi_is_canonical_uuid_v7(personality_agent_id) = 1),
  epoch_generation INTEGER NOT NULL CHECK (epoch_generation >= 0),
  rpc_boot_nonce TEXT NOT NULL CHECK (length(rpc_boot_nonce) > 0),
  reaped_through_generation INTEGER NOT NULL CHECK (reaped_through_generation >= 0),
  CHECK (reaped_through_generation < epoch_generation),
  FOREIGN KEY(receipt_id) REFERENCES physical_recovery_receipt_applications(receipt_id)
    ON DELETE RESTRICT
);
