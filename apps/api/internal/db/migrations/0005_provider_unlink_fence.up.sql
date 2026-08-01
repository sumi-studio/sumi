-- Only one backend-owned provider unlink may be unresolved for a Firebase
-- principal at a time. This fence serializes last-method decisions across API
-- replicas and remains held when a remote Firebase response is ambiguous.
-- Fail closed if the former browser-owned contract left a live link intent
-- beside a pending unlink; an operator must reconcile that principal before
-- this invariant can be installed safely.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_operations unlink_operation
        JOIN provider_operations link_operation
          ON link_operation.firebase_uid = unlink_operation.firebase_uid
        WHERE unlink_operation.operation = 'unlink'
          AND unlink_operation.status = 'pending'
          AND link_operation.operation = 'link'
          AND link_operation.status = 'pending'
          AND link_operation.expires_at > now()
    ) THEN
        RAISE EXCEPTION 'existing provider operations violate the pending unlink UID fence';
    END IF;
END;
$$;

CREATE UNIQUE INDEX provider_operations_one_pending_unlink_per_firebase_uid
    ON provider_operations (firebase_uid)
    WHERE operation = 'unlink' AND status = 'pending';

-- A provider link is browser-owned and may remain pending while its OAuth UI
-- runs. Serialize the start of every link/unlink boundary against a durable
-- unlink fence so a relink cannot land between the Admin postcheck and the
-- local unlink commit. The server no longer accepts expired link completions,
-- so they do not retain this fence. The transaction advisory lock closes the
-- insert/insert race.
CREATE FUNCTION enforce_provider_unlink_uid_fence() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
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

CREATE TRIGGER provider_operations_pending_unlink_uid_fence
    AFTER INSERT ON provider_operations
    FOR EACH ROW WHEN (NEW.status = 'pending')
    EXECUTE FUNCTION enforce_provider_unlink_uid_fence();

-- Email profile data is not a login capability. A completed Sumi email-link
-- proof is the durable half of the unlink guard; it counts only when paired
-- with the live Firebase email/password provider family.
CREATE INDEX auth_flows_completed_email_link_proof
    ON auth_flows (human_id, firebase_uid)
    WHERE channel = 'email_link' AND status = 'completed';
