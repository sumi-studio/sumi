-- Extend the durable UID fence to nonce-conflict updates. Migration 0005
-- covered new inserts, but BeginProviderOperation intentionally implements
-- idempotency with INSERT ... ON CONFLICT DO UPDATE; the update path must not
-- bypass a pending unlink.
CREATE OR REPLACE FUNCTION enforce_provider_unlink_uid_fence() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    -- Expired browser link intents cannot be completed by the server and do
    -- not retain the unlink fence. Their same-nonce replay is rejected by the
    -- owning store as expired after this no-op update returns.
    IF NEW.operation = 'link' AND NEW.expires_at <= now() THEN
        RETURN NEW;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('provider-unlink:' || NEW.firebase_uid, 0));
    IF EXISTS (
        SELECT 1 FROM provider_operations p
        WHERE p.firebase_uid = NEW.firebase_uid
          AND p.status = 'pending'
          AND p.operation_id <> NEW.operation_id
          AND (
              p.operation = 'unlink'
              OR (NEW.operation = 'unlink' AND p.operation = 'link' AND p.expires_at > now())
          )
    ) THEN
        RAISE EXCEPTION 'provider operation conflicts with pending unlink fence'
            USING ERRCODE = '23505', CONSTRAINT = 'provider_operations_pending_unlink_uid_fence';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER provider_operations_pending_unlink_uid_fence ON provider_operations;
CREATE TRIGGER provider_operations_pending_unlink_uid_fence
    AFTER INSERT OR UPDATE ON provider_operations
    FOR EACH ROW WHEN (NEW.status = 'pending')
    EXECUTE FUNCTION enforce_provider_unlink_uid_fence();
