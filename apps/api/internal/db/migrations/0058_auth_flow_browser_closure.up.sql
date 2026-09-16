-- 0058_auth_flow_browser_closure: bind an auth flow to the browser epoch that
-- started it, and mark flows whose session-issuance authority has been
-- revoked. browser_epoch_hash is SHA-256 of the opaque browser epoch cookie;
-- closed_at is mirrored from the durable session store so status and proof
-- paths report the closure and refuse late proofs. Issuance itself is fenced
-- by the session store's closed-flow and closed-epoch barriers.
ALTER TABLE auth_flows
    ADD COLUMN browser_epoch_hash text
        CHECK (browser_epoch_hash IS NULL OR char_length(browser_epoch_hash) = 43),
    ADD COLUMN closed_at timestamptz;

CREATE INDEX auth_flows_browser_epoch ON auth_flows (browser_epoch_hash)
    WHERE browser_epoch_hash IS NOT NULL AND closed_at IS NULL AND status <> 'completed';
