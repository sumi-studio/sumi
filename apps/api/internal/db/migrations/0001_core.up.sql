-- Initial Core-only schema. Requires an empty database.
-- Product state before this foundation is intentionally not migrated.

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';

CREATE DOMAIN public.uuidv7 AS text
	CONSTRAINT uuidv7_check CHECK ((VALUE ~ '^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'::text));

CREATE FUNCTION public.enforce_provider_unlink_uid_fence() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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

CREATE FUNCTION public.messaging_increment_participant_status_revision() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.revision := OLD.revision + 1;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.messaging_increment_place_revision() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.revision := OLD.revision + 1;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.prevent_app_installation_address_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.owner_kind IS DISTINCT FROM OLD.owner_kind
       OR NEW.owner_id IS DISTINCT FROM OLD.owner_id
       OR NEW.app_id IS DISTINCT FROM OLD.app_id THEN
        RAISE EXCEPTION 'app installation address is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.prevent_app_workspace_role_capability_identity_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.capability_id IS DISTINCT FROM OLD.capability_id
       OR NEW.app_id IS DISTINCT FROM OLD.app_id
       OR NEW.capability_ref IS DISTINCT FROM OLD.capability_ref THEN
        RAISE EXCEPTION 'app Workspace-role capability identity is immutable';
    END IF;
    IF OLD.retired_at IS NOT NULL
       AND NEW.retired_at IS DISTINCT FROM OLD.retired_at THEN
        RAISE EXCEPTION 'retired app Workspace-role capability cannot be reactivated or rewritten';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.prevent_credential_delete() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION 'credential history cannot be deleted';
END;
$$;

CREATE FUNCTION public.prevent_credential_rebinding() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.human_id IS DISTINCT FROM OLD.human_id THEN
        RAISE EXCEPTION 'credential is permanently bound to one Human and cannot be rebound';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.prevent_human_identity_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.human_id IS DISTINCT FROM OLD.human_id OR
       NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'human identity columns are immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.prevent_personality_agent_identity_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.personality_agent_id IS DISTINCT FROM OLD.personality_agent_id OR
       NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'PersonalityAgent identity columns are immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.prevent_security_event_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION 'credential security events are append-only';
END;
$$;

CREATE FUNCTION public.prevent_workspace_owner_membership_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE' AND EXISTS (
        SELECT 1 FROM workspaces w
        WHERE w.owner_workspace_member_id = OLD.workspace_member_id
    ) THEN
        RAISE EXCEPTION 'workspace owner membership is immutable';
    END IF;
    IF TG_OP = 'UPDATE' AND EXISTS (
        SELECT 1 FROM workspaces w
        WHERE w.owner_workspace_member_id = OLD.workspace_member_id
    ) AND (
        NEW.workspace_id IS DISTINCT FROM OLD.workspace_id
        OR NEW.member_kind IS DISTINCT FROM OLD.member_kind
        OR NEW.member_id IS DISTINCT FROM OLD.member_id
        OR NEW.left_at IS DISTINCT FROM OLD.left_at
    ) THEN
        RAISE EXCEPTION 'workspace owner membership is immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.protect_credential_history() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF OLD.provider IS DISTINCT FROM NEW.provider
       OR OLD.external_subject IS DISTINCT FROM NEW.external_subject
       OR OLD.human_id IS DISTINCT FROM NEW.human_id
       OR OLD.bound_at IS DISTINCT FROM NEW.bound_at THEN
        RAISE EXCEPTION 'credential identity and historical binding are immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.reject_workspace_tenure_close_with_active_places() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF OLD.left_at IS NULL AND NEW.left_at IS NOT NULL AND EXISTS (
        SELECT 1 FROM place_members pm
        WHERE pm.workspace_id = OLD.workspace_id
          AND pm.workspace_member_id = OLD.workspace_member_id
          AND pm.left_at IS NULL
    ) THEN
        RAISE EXCEPTION 'close active place membership tenures before the workspace membership tenure';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.require_active_workspace_tenure_for_place_member() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    parent_id uuidv7;
BEGIN
    IF NEW.left_at IS NULL THEN
        SELECT wm.workspace_member_id INTO parent_id
        FROM workspace_members wm
        WHERE wm.workspace_id = NEW.workspace_id
          AND wm.workspace_member_id = NEW.workspace_member_id
          AND wm.member_kind = NEW.member_kind
          AND wm.member_id = NEW.member_id
          AND wm.left_at IS NULL
        FOR SHARE;
        IF parent_id IS NULL THEN
            RAISE EXCEPTION 'active place membership requires an active workspace membership tenure';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.require_attachment_for_empty_message() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.deleted_at IS NULL AND NEW.content = ''
       AND NOT EXISTS (
           SELECT 1 FROM message_attachments a
           WHERE a.workspace_id = NEW.workspace_id
             AND a.place_id = NEW.place_id
             AND a.message_id = NEW.message_id
       )
       AND NOT EXISTS (
           SELECT 1 FROM message_polls p
           WHERE p.message_id = NEW.message_id
       ) THEN
        RAISE EXCEPTION 'a message with empty content must bind an attachment or poll';
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.validate_app_installation_owner() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.owner_kind = 'workspace' AND NOT EXISTS (
        SELECT 1 FROM workspaces WHERE workspace_id = NEW.owner_id
    ) THEN
        RAISE EXCEPTION 'unknown workspace installation owner';
    ELSIF NEW.owner_kind = 'human' AND NOT EXISTS (
        SELECT 1 FROM humans WHERE human_id = NEW.owner_id
    ) THEN
        RAISE EXCEPTION 'unknown Human installation owner';
    ELSIF NEW.owner_kind = 'personality_agent' AND NOT EXISTS (
        SELECT 1 FROM agents WHERE personality_agent_id = NEW.owner_id
    ) THEN
        RAISE EXCEPTION 'unknown PersonalityAgent installation owner';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.validate_workspace_member_participant() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.member_kind = 'human' AND NOT EXISTS (
        SELECT 1 FROM humans WHERE human_id = NEW.member_id
    ) THEN
        RAISE EXCEPTION 'unknown Human workspace member';
    ELSIF NEW.member_kind = 'personality_agent' AND NOT EXISTS (
        SELECT 1 FROM agents WHERE personality_agent_id = NEW.member_id
    ) THEN
        RAISE EXCEPTION 'unknown PersonalityAgent workspace member';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.validate_workspace_owner_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.owner_workspace_member_id IS DISTINCT FROM OLD.owner_workspace_member_id
       AND NOT EXISTS (
           SELECT 1
           FROM workspace_members wm
           WHERE wm.workspace_id = NEW.workspace_id
             AND wm.workspace_member_id = NEW.owner_workspace_member_id
             AND wm.left_at IS NULL
       ) THEN
        RAISE EXCEPTION 'workspace owner must be an active membership tenure';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TABLE public.agent_attention_deliveries (
    event_id public.uuidv7 NOT NULL,
    personality_agent_id public.uuidv7 NOT NULL,
    source_kind text NOT NULL,
    source_id public.uuidv7 NOT NULL,
    source_revision bigint NOT NULL,
    workspace_id public.uuidv7 NOT NULL,
    installation_id public.uuidv7 NOT NULL,
    authority_epoch bigint NOT NULL,
    workspace_member_id public.uuidv7 NOT NULL,
    place_member_id public.uuidv7,
    place_id public.uuidv7 NOT NULL,
    message_id public.uuidv7 NOT NULL,
    payload jsonb NOT NULL,
    available_at timestamp with time zone NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    admitted_command_id uuid,
    admitted_command_seq bigint,
    admitted_at timestamp with time zone,
    cancellation_requested_at timestamp with time zone,
    suppressed_at timestamp with time zone,
    suppression_reason text,
    CONSTRAINT agent_attention_deliveries_admitted_command_seq_check CHECK (((admitted_command_seq IS NULL) OR (admitted_command_seq > 0))),
    CONSTRAINT agent_attention_deliveries_attempt_count_check CHECK ((attempt_count >= 0)),
    CONSTRAINT agent_attention_deliveries_authority_epoch_check CHECK ((authority_epoch > 0)),
    CONSTRAINT agent_attention_deliveries_check CHECK (((admitted_at IS NULL) = (admitted_command_id IS NULL))),
    CONSTRAINT agent_attention_deliveries_check1 CHECK (((admitted_at IS NULL) = (admitted_command_seq IS NULL))),
    CONSTRAINT agent_attention_deliveries_check2 CHECK (((suppressed_at IS NULL) = (suppression_reason IS NULL))),
    CONSTRAINT agent_attention_deliveries_check3 CHECK (((admitted_at IS NULL) OR (suppressed_at IS NULL))),
    CONSTRAINT agent_attention_deliveries_source_kind_check CHECK ((source_kind = ANY (ARRAY['messaging_message'::text, 'messaging_mention'::text, 'reply_later_due'::text, 'messaging_poll_vote'::text]))),
    CONSTRAINT agent_attention_deliveries_source_revision_check CHECK ((source_revision > 0))
);

CREATE TABLE public.agents (
    personality_agent_id public.uuidv7 NOT NULL,
    human_id public.uuidv7 NOT NULL,
    display_name text DEFAULT 'Sumi'::text NOT NULL,
    warmth text DEFAULT 'cold'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agents_warmth_check CHECK ((warmth = ANY (ARRAY['cold'::text, 'warm'::text])))
);

CREATE TABLE public.app_catalog (
    app_id text NOT NULL,
    display_name text NOT NULL,
    workspace_owner_allowed boolean NOT NULL,
    participant_owner_allowed boolean NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT app_catalog_app_id_check CHECK ((app_id ~ '^[a-z][a-z0-9-]{0,63}$'::text)),
    CONSTRAINT app_catalog_check CHECK ((workspace_owner_allowed OR participant_owner_allowed)),
    CONSTRAINT app_catalog_display_name_check CHECK (((char_length(display_name) >= 1) AND (char_length(display_name) <= 100)))
);

CREATE TABLE public.app_install_operation_receipts (
    owner_kind text NOT NULL,
    owner_id public.uuidv7 NOT NULL,
    operation_id uuid NOT NULL,
    app_id text NOT NULL,
    status text NOT NULL,
    installation_id public.uuidv7,
    enabled boolean,
    authority_epoch bigint,
    installed_at timestamp with time zone,
    updated_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT app_install_operation_receipts_app_id_check CHECK ((app_id ~ '^[a-z][a-z0-9-]{0,63}$'::text)),
    CONSTRAINT app_install_operation_receipts_check CHECK ((((status = 'pending'::text) AND (installation_id IS NULL) AND (enabled IS NULL) AND (authority_epoch IS NULL) AND (installed_at IS NULL) AND (updated_at IS NULL) AND (completed_at IS NULL)) OR ((status = 'already_installed'::text) AND (installation_id IS NULL) AND (enabled IS NULL) AND (authority_epoch IS NULL) AND (installed_at IS NULL) AND (updated_at IS NULL) AND (completed_at IS NOT NULL)) OR ((status = 'installed'::text) AND (installation_id IS NOT NULL) AND (enabled IS TRUE) AND (authority_epoch = 1) AND (installed_at IS NOT NULL) AND (updated_at = installed_at) AND (completed_at IS NOT NULL)))),
    CONSTRAINT app_install_operation_receipts_check1 CHECK (((completed_at IS NULL) OR (completed_at >= created_at))),
    CONSTRAINT app_install_operation_receipts_owner_kind_check CHECK ((owner_kind = ANY (ARRAY['workspace'::text, 'human'::text, 'personality_agent'::text]))),
    CONSTRAINT app_install_operation_receipts_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'installed'::text, 'already_installed'::text]))),
    CONSTRAINT app_install_operation_receipts_uuidv4_or_uuidv5 CHECK (((operation_id)::text ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[45][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'::text))
);

CREATE TABLE public.app_installations (
    installation_id public.uuidv7 NOT NULL,
    owner_kind text NOT NULL,
    owner_id public.uuidv7 NOT NULL,
    app_id text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    installed_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    authority_epoch bigint DEFAULT 1 NOT NULL,
    CONSTRAINT app_installations_authority_epoch_check CHECK ((authority_epoch >= 1)),
    CONSTRAINT app_installations_owner_kind_check CHECK ((owner_kind = ANY (ARRAY['workspace'::text, 'human'::text, 'personality_agent'::text])))
);

CREATE TABLE public.app_workspace_role_capabilities (
    capability_id public.uuidv7 NOT NULL,
    app_id text NOT NULL,
    capability_ref text NOT NULL,
    label text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    retired_at timestamp with time zone,
    CONSTRAINT app_workspace_role_capabilities_capability_ref_check CHECK ((capability_ref ~ '^app\.[a-z][a-z0-9-]{0,63}\.[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT app_workspace_role_capabilities_check CHECK ((capability_ref ~~ (('app.'::text || app_id) || '.%'::text))),
    CONSTRAINT app_workspace_role_capabilities_check1 CHECK (((retired_at IS NULL) OR (retired_at >= created_at))),
    CONSTRAINT app_workspace_role_capabilities_label_check CHECK (((char_length(label) >= 1) AND (char_length(label) <= 100)))
);

CREATE TABLE public.auth_email_challenges (
    challenge_id public.uuidv7 NOT NULL,
    flow_id public.uuidv7 NOT NULL,
    normalized_email text NOT NULL,
    key_id text NOT NULL,
    failed_code_attempts integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    superseded_at timestamp with time zone,
    consumed_at timestamp with time zone,
    consumed_method text,
    CONSTRAINT auth_email_challenges_check CHECK ((expires_at > created_at)),
    CONSTRAINT auth_email_challenges_check1 CHECK (((consumed_at IS NULL) = (consumed_method IS NULL))),
    CONSTRAINT auth_email_challenges_check2 CHECK (((consumed_at IS NULL) OR (superseded_at IS NULL))),
    CONSTRAINT auth_email_challenges_consumed_method_check CHECK ((consumed_method = ANY (ARRAY['code'::text, 'link'::text]))),
    CONSTRAINT auth_email_challenges_failed_code_attempts_check CHECK (((failed_code_attempts >= 0) AND (failed_code_attempts <= 5))),
    CONSTRAINT auth_email_challenges_key_id_check CHECK (((char_length(key_id) >= 1) AND (char_length(key_id) <= 64)))
);

CREATE TABLE public.auth_email_deliveries (
    delivery_id public.uuidv7 NOT NULL,
    challenge_id public.uuidv7 NOT NULL,
    normalized_email text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    lease_expires_at timestamp with time zone,
    finished_at timestamp with time zone,
    failure_class text,
    CONSTRAINT auth_email_deliveries_attempts_check CHECK ((attempts >= 0)),
    CONSTRAINT auth_email_deliveries_check CHECK (((status = 'sending'::text) = (lease_expires_at IS NOT NULL))),
    CONSTRAINT auth_email_deliveries_check1 CHECK (((status = ANY (ARRAY['sent'::text, 'failed'::text, 'cancelled'::text])) = (finished_at IS NOT NULL))),
    CONSTRAINT auth_email_deliveries_check2 CHECK (((status = 'failed'::text) = (failure_class IS NOT NULL))),
    CONSTRAINT auth_email_deliveries_failure_class_check CHECK ((failure_class = ANY (ARRAY['permanent'::text, 'retries_exhausted'::text]))),
    CONSTRAINT auth_email_deliveries_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'sending'::text, 'sent'::text, 'failed'::text, 'cancelled'::text])))
);

CREATE TABLE public.auth_flows (
    flow_id public.uuidv7 NOT NULL,
    nonce_hash bytea NOT NULL,
    intent text NOT NULL,
    channel text NOT NULL,
    expected_provider text NOT NULL,
    normalized_email text,
    continuation text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    confirmation_action text,
    firebase_uid text,
    provider_subject text,
    human_id public.uuidv7,
    personality_agent_id public.uuidv7,
    terminal_outcome text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    proved_at timestamp with time zone,
    completed_at timestamp with time zone,
    expires_at timestamp with time zone NOT NULL,
    verified_display_name text,
    enrollment_invite_id public.uuidv7,
    verified_email text,
    email_verified boolean DEFAULT false NOT NULL,
    email_proved_at timestamp with time zone,
    email_proof_method text,
    email_proof_uid text,
    email_proof_uid_bound_at timestamp with time zone,
    adopted_from_nonce_hash bytea,
    browser_epoch_hash text,
    closed_at timestamp with time zone,
    CONSTRAINT auth_flows_adopted_from_nonce_hash_check CHECK (((adopted_from_nonce_hash IS NULL) OR (octet_length(adopted_from_nonce_hash) = 32))),
    CONSTRAINT auth_flows_browser_epoch_hash_check CHECK (((browser_epoch_hash IS NULL) OR (char_length(browser_epoch_hash) = 43))),
    CONSTRAINT auth_flows_channel_check CHECK ((channel = ANY (ARRAY['email_link'::text, 'email_code'::text, 'provider'::text]))),
    CONSTRAINT auth_flows_check CHECK ((((channel = ANY (ARRAY['email_link'::text, 'email_code'::text])) AND (normalized_email IS NOT NULL)) OR ((channel = 'provider'::text) AND (normalized_email IS NULL)))),
    CONSTRAINT auth_flows_check1 CHECK ((((status = 'confirmation_required'::text) AND (confirmation_action IS NOT NULL) AND (firebase_uid IS NOT NULL) AND (proved_at IS NOT NULL)) OR ((status <> 'confirmation_required'::text) AND (confirmation_action IS NULL)))),
    CONSTRAINT auth_flows_check2 CHECK ((((status = 'completed'::text) AND (terminal_outcome IS NOT NULL) AND (human_id IS NOT NULL) AND (personality_agent_id IS NOT NULL) AND (completed_at IS NOT NULL)) OR ((status <> 'completed'::text) AND (terminal_outcome IS NULL) AND (completed_at IS NULL)))),
    CONSTRAINT auth_flows_confirmation_action_check CHECK ((confirmation_action = ANY (ARRAY['create_account'::text, 'sign_in'::text]))),
    CONSTRAINT auth_flows_email_proof_method_check CHECK ((email_proof_method = ANY (ARRAY['code'::text, 'link'::text]))),
    CONSTRAINT auth_flows_email_proof_state CHECK ((((channel = 'email_code'::text) OR ((email_proved_at IS NULL) AND (email_proof_uid IS NULL) AND (adopted_from_nonce_hash IS NULL))) AND ((email_proved_at IS NULL) = (email_proof_method IS NULL)) AND ((email_proof_uid IS NULL) = (email_proof_uid_bound_at IS NULL)) AND ((email_proof_uid IS NULL) OR (email_proved_at IS NOT NULL)) AND ((adopted_from_nonce_hash IS NULL) OR (email_proof_method = 'link'::text)) AND ((channel <> 'email_code'::text) OR (status = 'pending'::text) OR (email_proof_uid IS NOT NULL)) AND ((channel <> 'email_code'::text) OR (firebase_uid IS NULL) OR (firebase_uid = email_proof_uid)))),
    CONSTRAINT auth_flows_email_proof_uid_check CHECK (((email_proof_uid IS NULL) OR ((char_length(email_proof_uid) >= 1) AND (char_length(email_proof_uid) <= 128)))),
    CONSTRAINT auth_flows_intent_check CHECK ((intent = ANY (ARRAY['sign_in'::text, 'sign_up'::text]))),
    CONSTRAINT auth_flows_nonce_hash_check CHECK ((octet_length(nonce_hash) = 32)),
    CONSTRAINT auth_flows_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'confirmation_required'::text, 'completed'::text]))),
    CONSTRAINT auth_flows_terminal_outcome_check CHECK ((terminal_outcome = ANY (ARRAY['signed_in'::text, 'account_created'::text]))),
    CONSTRAINT auth_flows_verified_display_name_check CHECK (((verified_display_name IS NULL) OR ((char_length(verified_display_name) >= 1) AND (char_length(verified_display_name) <= 80))))
);

CREATE TABLE public.browser_tab_attachments (
    attachment_id uuid NOT NULL,
    human_id public.uuidv7 NOT NULL,
    persona_id public.uuidv7 NOT NULL,
    name text NOT NULL,
    tab jsonb NOT NULL,
    host_token_hash bytea NOT NULL,
    allow_actions boolean DEFAULT false NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    last_seen_at timestamp with time zone DEFAULT '1970-01-01 00:00:00+00'::timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    jev_available boolean DEFAULT false NOT NULL
);

CREATE TABLE public.call_sessions (
    session_id public.uuidv7 NOT NULL,
    workspace_id public.uuidv7 NOT NULL,
    place_id public.uuidv7 NOT NULL,
    personality_agent_id public.uuidv7 NOT NULL,
    room_sid text,
    status text NOT NULL,
    epoch bigint DEFAULT 0 NOT NULL,
    claimed_by text,
    claim_expires_at timestamp with time zone,
    requested_by text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    ended_at timestamp with time zone,
    end_reason text,
    CONSTRAINT call_sessions_epoch_check CHECK ((epoch >= 0)),
    CONSTRAINT call_sessions_status_check CHECK ((status = ANY (ARRAY['requested'::text, 'claimed'::text, 'active'::text, 'ending'::text, 'ended'::text, 'interrupted'::text, 'revoked'::text, 'failed'::text])))
);

CREATE TABLE public.call_utterances (
    utterance_id public.uuidv7 NOT NULL,
    session_id public.uuidv7 NOT NULL,
    session_epoch bigint NOT NULL,
    seq bigint NOT NULL,
    text text NOT NULL,
    status text NOT NULL,
    detail jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT call_utterances_seq_check CHECK ((seq > 0)),
    CONSTRAINT call_utterances_session_epoch_check CHECK ((session_epoch >= 0)),
    CONSTRAINT call_utterances_status_check CHECK ((status = ANY (ARRAY['intended'::text, 'dequeued'::text, 'emitting'::text, 'emitted'::text, 'interrupted'::text, 'expired'::text, 'failed'::text, 'unknown'::text]))),
    CONSTRAINT call_utterances_text_check CHECK (((length(text) >= 1) AND (length(text) <= 4000)))
);

CREATE TABLE public.chatgpt_connections (
    human_id public.uuidv7 NOT NULL,
    connection_id uuid NOT NULL,
    account_id text NOT NULL,
    credential_ciphertext bytea NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    model text NOT NULL,
    effort text NOT NULL,
    reconnect_required boolean DEFAULT false NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.cloud_browser_jev_credentials (
    human_id public.uuidv7 NOT NULL,
    sealed bytea NOT NULL,
    rejected boolean DEFAULT false NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.cloud_browser_profiles (
    profile_id uuid NOT NULL,
    human_id public.uuidv7 NOT NULL,
    persona_id public.uuidv7 NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    incarnation bigint DEFAULT 0 NOT NULL,
    state text DEFAULT 'sleeping'::text NOT NULL,
    state_at timestamp with time zone DEFAULT now() NOT NULL,
    tab_ids jsonb DEFAULT '[]'::jsonb NOT NULL,
    snapshot_seq bigint DEFAULT 0 NOT NULL,
    snapshot_version integer,
    snapshot_incarnation bigint,
    snapshot bytea,
    snapshot_bytes integer,
    snapshot_at timestamp with time zone,
    refresh_requested_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT cloud_browser_profiles_state_check CHECK ((state = ANY (ARRAY['sleeping'::text, 'live'::text, 'lost'::text])))
);

CREATE TABLE public.core_budget_waits (
    persona_id public.uuidv7 NOT NULL,
    input_id text NOT NULL,
    turn_id text NOT NULL,
    funding_kind text NOT NULL,
    funding_id text NOT NULL,
    needed_minor bigint NOT NULL,
    currency text NOT NULL,
    est_input_tokens bigint NOT NULL,
    est_output_bound bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT core_budget_waits_est_input_tokens_check CHECK ((est_input_tokens >= 0)),
    CONSTRAINT core_budget_waits_est_output_bound_check CHECK ((est_output_bound >= 0))
);

CREATE TABLE public.core_events (
    persona_id public.uuidv7 NOT NULL,
    seq bigint NOT NULL,
    turn_id text NOT NULL,
    kind text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.core_inputs (
    persona_id public.uuidv7 NOT NULL,
    input_id text NOT NULL,
    kind text NOT NULL,
    payload jsonb NOT NULL,
    actor_kind text DEFAULT ''::text NOT NULL,
    actor_id text DEFAULT ''::text NOT NULL,
    source_surface text DEFAULT ''::text NOT NULL,
    thread_id text DEFAULT ''::text NOT NULL,
    occurred_at timestamp with time zone,
    attention text DEFAULT 'reply'::text NOT NULL,
    status text NOT NULL,
    claimed_generation bigint,
    turn_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    done_at timestamp with time zone,
    not_before timestamp with time zone,
    received_seq bigint,
    admission_seq bigint NOT NULL,
    waiting_since timestamp with time zone,
    waited_ms bigint DEFAULT 0 NOT NULL,
    CONSTRAINT core_inputs_attention_check CHECK ((attention = ANY (ARRAY['reply'::text, 'observe'::text, 'defer'::text]))),
    CONSTRAINT core_inputs_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'claimed'::text, 'waiting'::text, 'done'::text])))
);

ALTER TABLE public.core_inputs ALTER COLUMN admission_seq ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.core_inputs_admission_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

CREATE TABLE public.core_job_file_ops (
    persona_id public.uuidv7 NOT NULL,
    job_id text NOT NULL,
    op_seq bigint NOT NULL,
    op_id text NOT NULL,
    op text NOT NULL,
    scope text NOT NULL,
    path text NOT NULL,
    request jsonb NOT NULL,
    body bytea,
    status text NOT NULL,
    result jsonb,
    error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    resolved_at timestamp with time zone,
    CONSTRAINT core_job_file_ops_op_check CHECK ((op = ANY (ARRAY['write'::text, 'mkdir'::text, 'remove'::text]))),
    CONSTRAINT core_job_file_ops_status_check CHECK ((status = ANY (ARRAY['admitted'::text, 'settled'::text, 'refused'::text, 'unknown'::text, 'diverged'::text])))
);

CREATE TABLE public.core_jobs (
    persona_id public.uuidv7 NOT NULL,
    job_id text NOT NULL,
    kind text NOT NULL,
    request jsonb NOT NULL,
    status text NOT NULL,
    claimed_by text,
    claim_expires_at timestamp with time zone,
    created_by text DEFAULT 'api'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    cancel_requested_at timestamp with time zone,
    result jsonb,
    error text,
    notified_at timestamp with time zone,
    CONSTRAINT core_jobs_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'running'::text, 'cancel_requested'::text, 'done'::text, 'failed'::text, 'cancelled'::text, 'lost'::text])))
);

CREATE TABLE public.core_memory_chunks (
    persona_id public.uuidv7 NOT NULL,
    chunk_seq bigint NOT NULL,
    layer smallint DEFAULT 1 NOT NULL,
    first_seq bigint NOT NULL,
    last_seq bigint NOT NULL,
    est_tokens bigint NOT NULL,
    status text NOT NULL,
    replacement text,
    replacement_est_tokens bigint,
    attempts integer DEFAULT 0 NOT NULL,
    interruptions integer DEFAULT 0 NOT NULL,
    last_error text,
    claimed_generation bigint,
    claimed_at timestamp with time zone,
    not_before timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    prepared_at timestamp with time zone,
    applied_at timestamp with time zone,
    sources bigint[],
    CONSTRAINT core_memory_chunks_check CHECK ((first_seq <= last_seq)),
    CONSTRAINT core_memory_chunks_layer_check CHECK ((layer >= 1)),
    CONSTRAINT core_memory_chunks_sources_check CHECK (
CASE
    WHEN (layer = 1) THEN (sources IS NULL)
    ELSE ((sources IS NOT NULL) AND (cardinality(sources) >= 1))
END),
    CONSTRAINT core_memory_chunks_status_check CHECK ((status = ANY (ARRAY['sealed'::text, 'preparing'::text, 'prepared'::text, 'applied'::text, 'kept'::text, 'failed'::text, 'superseded'::text])))
);

CREATE TABLE public.core_operations (
    persona_id public.uuidv7 NOT NULL,
    operation_id text NOT NULL,
    turn_id text NOT NULL,
    tool text NOT NULL,
    idempotency_key text NOT NULL,
    request jsonb NOT NULL,
    status text NOT NULL,
    response jsonb,
    claimed_generation bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT core_operations_status_check CHECK ((status = ANY (ARRAY['running'::text, 'awaiting_approval'::text, 'done'::text, 'failed'::text])))
);

CREATE TABLE public.core_outbox (
    persona_id public.uuidv7 NOT NULL,
    seq bigint NOT NULL,
    kind text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    delivered_at timestamp with time zone
);

CREATE TABLE public.core_personas (
    persona_id public.uuidv7 NOT NULL,
    human_id public.uuidv7,
    display_name text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    authority text DEFAULT 'active'::text NOT NULL,
    transfer_id text,
    model_intent jsonb,
    CONSTRAINT core_personas_authority_check CHECK ((authority = ANY (ARRAY['active'::text, 'sealed'::text, 'staged'::text, 'transferred'::text])))
);

CREATE TABLE public.core_placement (
    singleton boolean DEFAULT true NOT NULL,
    placement_id public.uuidv7 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT core_placement_singleton_check CHECK (singleton)
);

CREATE TABLE public.core_schedules (
    persona_id public.uuidv7 NOT NULL,
    schedule_id text NOT NULL,
    wake_at timestamp with time zone NOT NULL,
    payload jsonb NOT NULL,
    miss_policy text DEFAULT 'fire_late'::text NOT NULL,
    status text NOT NULL,
    claimed_generation bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    fired_at timestamp with time zone,
    CONSTRAINT core_schedules_miss_policy_check CHECK ((miss_policy = ANY (ARRAY['fire_late'::text, 'coalesce'::text, 'expire'::text, 'report_missed'::text]))),
    CONSTRAINT core_schedules_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'claimed'::text, 'fired'::text, 'cancelled'::text, 'expired'::text])))
);

CREATE TABLE public.core_terminal_inputs (
    input_id public.uuidv7 NOT NULL,
    session_id public.uuidv7 NOT NULL,
    session_epoch bigint NOT NULL,
    seq bigint NOT NULL,
    kind text NOT NULL,
    payload jsonb NOT NULL,
    source text NOT NULL,
    status text NOT NULL,
    detail jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT core_terminal_inputs_kind_check CHECK ((kind = ANY (ARRAY['stdin'::text, 'resize'::text, 'signal'::text, 'eof'::text]))),
    CONSTRAINT core_terminal_inputs_seq_check CHECK ((seq > 0)),
    CONSTRAINT core_terminal_inputs_source_check CHECK ((source = ANY (ARRAY['agent'::text, 'human'::text]))),
    CONSTRAINT core_terminal_inputs_status_check CHECK ((status = ANY (ARRAY['intended'::text, 'dequeued'::text, 'written'::text, 'interrupted'::text, 'expired'::text, 'failed'::text, 'unknown'::text])))
);

CREATE TABLE public.core_terminal_output (
    session_id public.uuidv7 NOT NULL,
    seq bigint NOT NULL,
    kind text NOT NULL,
    base bigint NOT NULL,
    gap_to bigint,
    data bytea DEFAULT '\x'::bytea NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT core_terminal_output_base_check CHECK ((base >= 0)),
    CONSTRAINT core_terminal_output_check CHECK (((gap_to IS NULL) OR (gap_to >= base))),
    CONSTRAINT core_terminal_output_kind_check CHECK ((kind = ANY (ARRAY['data'::text, 'gap'::text]))),
    CONSTRAINT core_terminal_output_seq_check CHECK ((seq > 0))
);

CREATE TABLE public.core_terminal_sessions (
    session_id public.uuidv7 NOT NULL,
    persona_id public.uuidv7 NOT NULL,
    name text DEFAULT ''::text NOT NULL,
    mode text NOT NULL,
    backend text NOT NULL,
    operation_id text,
    status text NOT NULL,
    epoch bigint DEFAULT 0 NOT NULL,
    claimed_by text,
    claim_expires_at timestamp with time zone,
    control_holder text,
    control_until timestamp with time zone,
    requested_by text NOT NULL,
    created_by text NOT NULL,
    exit_code integer,
    exit_signal text,
    end_reason text,
    output_bytes bigint DEFAULT 0 NOT NULL,
    output_base bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    ended_at timestamp with time zone,
    CONSTRAINT core_terminal_sessions_backend_check CHECK ((backend = ANY (ARRAY['local'::text, 'cloud'::text]))),
    CONSTRAINT core_terminal_sessions_control_holder_check CHECK (((control_holder IS NULL) OR (control_holder = 'human'::text))),
    CONSTRAINT core_terminal_sessions_epoch_check CHECK ((epoch >= 0)),
    CONSTRAINT core_terminal_sessions_mode_check CHECK ((mode = 'pty'::text)),
    CONSTRAINT core_terminal_sessions_name_check CHECK ((char_length(name) <= 80)),
    CONSTRAINT core_terminal_sessions_output_base_check CHECK ((output_base >= 0)),
    CONSTRAINT core_terminal_sessions_output_bytes_check CHECK ((output_bytes >= 0)),
    CONSTRAINT core_terminal_sessions_requested_by_check CHECK ((requested_by = ANY (ARRAY['agent'::text, 'human'::text]))),
    CONSTRAINT core_terminal_sessions_status_check CHECK ((status = ANY (ARRAY['requested'::text, 'claimed'::text, 'active'::text, 'ending'::text, 'ended'::text, 'interrupted'::text, 'lost'::text])))
);

CREATE TABLE public.core_tool_approvals (
    approval_id text NOT NULL,
    persona_id public.uuidv7 NOT NULL,
    input_id text NOT NULL,
    call_index integer NOT NULL,
    operation_id text NOT NULL,
    turn_id text NOT NULL,
    tool text NOT NULL,
    route text NOT NULL,
    required_by text NOT NULL,
    request jsonb NOT NULL,
    action_digest text NOT NULL,
    status text NOT NULL,
    decision text,
    decision_id text,
    decided_by_kind text,
    decided_by_id text,
    provenance text,
    decided_at timestamp with time zone,
    consumed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    prior_decision text,
    prior_decided_by_kind text,
    prior_decided_by_id text,
    prior_decided_at timestamp with time zone,
    CONSTRAINT core_tool_approvals_decision_check CHECK ((decision = ANY (ARRAY['approve_once'::text, 'deny_once'::text]))),
    CONSTRAINT core_tool_approvals_prior_decision_check CHECK ((prior_decision = ANY (ARRAY['approve_once'::text, 'deny_once'::text]))),
    CONSTRAINT core_tool_approvals_provenance_check CHECK ((provenance = 'agent_own_with_human_consent'::text)),
    CONSTRAINT core_tool_approvals_required_by_check CHECK ((required_by = ANY (ARRAY['intrinsic'::text, 'route'::text]))),
    CONSTRAINT core_tool_approvals_route_check CHECK ((route = ANY (ARRAY['normal'::text, 'elevated'::text]))),
    CONSTRAINT core_tool_approvals_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'approved'::text, 'denied'::text])))
);

CREATE TABLE public.core_transfers (
    direction text NOT NULL,
    transfer_id text NOT NULL,
    persona_id public.uuidv7 NOT NULL,
    status text NOT NULL,
    format_version integer NOT NULL,
    destination_id public.uuidv7,
    content_sha256 text,
    proof_key text,
    receipt jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT core_transfers_direction_check CHECK ((direction = ANY (ARRAY['export'::text, 'import'::text]))),
    CONSTRAINT core_transfers_status_check CHECK ((status = ANY (ARRAY['sealed'::text, 'completed'::text, 'aborted'::text, 'staged'::text, 'activated'::text, 'retired'::text])))
);

CREATE TABLE public.core_turn_plans (
    persona_id public.uuidv7 NOT NULL,
    input_id text NOT NULL,
    turn_id text NOT NULL,
    generation bigint NOT NULL,
    plan jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.core_turns (
    persona_id public.uuidv7 NOT NULL,
    turn_id text NOT NULL,
    input_id text NOT NULL,
    generation bigint NOT NULL,
    attempt integer NOT NULL,
    status text NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    output jsonb,
    usage jsonb,
    error text,
    commit_request jsonb,
    CONSTRAINT core_turns_status_check CHECK ((status = ANY (ARRAY['running'::text, 'awaiting'::text, 'done'::text, 'interrupted'::text, 'failed'::text])))
);

CREATE TABLE public.core_writer_leases (
    persona_id public.uuidv7 NOT NULL,
    generation bigint NOT NULL,
    holder_id text NOT NULL,
    acquired_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL
);

CREATE TABLE public.credential_security_events (
    event_id bigint NOT NULL,
    operation_id public.uuidv7,
    human_id public.uuidv7 NOT NULL,
    provider text NOT NULL,
    event_type text NOT NULL,
    decision_path text NOT NULL,
    terminal_outcome text NOT NULL,
    occurred_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT credential_security_events_event_type_check CHECK ((event_type = ANY (ARRAY['provider_linked'::text, 'provider_unlinked'::text, 'provider_link_failed'::text, 'provider_unlink_failed'::text])))
);

CREATE SEQUENCE public.credential_security_events_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.credential_security_events_event_id_seq OWNED BY public.credential_security_events.event_id;

CREATE TABLE public.credentials (
    credential_id bigint NOT NULL,
    provider text NOT NULL,
    external_subject text NOT NULL,
    human_id public.uuidv7 NOT NULL,
    bound_at timestamp with time zone DEFAULT now() NOT NULL,
    active boolean DEFAULT true NOT NULL,
    unlinked_at timestamp with time zone,
    CONSTRAINT credentials_unlink_state CHECK (((active AND (unlinked_at IS NULL)) OR ((NOT active) AND (unlinked_at IS NOT NULL))))
);

CREATE SEQUENCE public.credentials_credential_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.credentials_credential_id_seq OWNED BY public.credentials.credential_id;

CREATE TABLE public.employments (
    employment_id bigint NOT NULL,
    agent_id public.uuidv7 NOT NULL,
    employer_type text NOT NULL,
    employer_id public.uuidv7 NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    ended_at timestamp with time zone,
    CONSTRAINT employments_check CHECK (((ended_at IS NULL) OR (ended_at >= started_at))),
    CONSTRAINT employments_employer_type_check CHECK ((employer_type = ANY (ARRAY['human'::text, 'workspace'::text])))
);

CREATE SEQUENCE public.employments_employment_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.employments_employment_id_seq OWNED BY public.employments.employment_id;

CREATE TABLE public.enrollment_invites (
    invite_id public.uuidv7 NOT NULL,
    token_hash bytea NOT NULL,
    issued_by public.uuidv7,
    email text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    revoked_at timestamp with time zone,
    consumed_at timestamp with time zone,
    consumed_by public.uuidv7,
    workspace_invite_id public.uuidv7,
    CONSTRAINT enrollment_invites_check CHECK (((consumed_at IS NULL) = (consumed_by IS NULL))),
    CONSTRAINT enrollment_invites_token_hash_check CHECK ((octet_length(token_hash) = 32))
);

CREATE TABLE public.feedback_attachments (
    attachment_id public.uuidv7 NOT NULL,
    author_key text NOT NULL,
    thread_id public.uuidv7,
    name text NOT NULL,
    mime_type text NOT NULL,
    content bytea NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone DEFAULT (now() + '24:00:00'::interval) NOT NULL,
    "position" integer,
    CONSTRAINT feedback_attachments_content_check CHECK (((octet_length(content) >= 1) AND (octet_length(content) <= 20971520))),
    CONSTRAINT feedback_attachments_mime_type_check CHECK ((mime_type = ANY (ARRAY['image/png'::text, 'image/jpeg'::text, 'image/webp'::text, 'video/webm'::text, 'video/mp4'::text]))),
    CONSTRAINT feedback_attachments_name_check CHECK (((octet_length(name) >= 1) AND (octet_length(name) <= 255)))
);

CREATE TABLE public.feedback_attention_outbox (
    event_id public.uuidv7 NOT NULL,
    recipient_paid public.uuidv7 NOT NULL,
    thread_id public.uuidv7 NOT NULL,
    payload jsonb NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    outcome text,
    CONSTRAINT feedback_attention_outbox_outcome_check CHECK ((outcome = ANY (ARRAY['admitted'::text, 'suppressed'::text])))
);

CREATE TABLE public.feedback_events (
    event_id public.uuidv7 NOT NULL,
    thread_id public.uuidv7 NOT NULL,
    revision bigint NOT NULL,
    kind text NOT NULL,
    author jsonb NOT NULL,
    body text,
    status text,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT feedback_events_check CHECK ((((kind = 'message'::text) AND (body IS NOT NULL) AND ((char_length(body) >= 1) AND (char_length(body) <= 20000)) AND (status IS NULL)) OR ((kind = 'status'::text) AND (body IS NULL) AND (status = ANY (ARRAY['open'::text, 'resolved'::text]))))),
    CONSTRAINT feedback_events_kind_check CHECK ((kind = ANY (ARRAY['message'::text, 'status'::text]))),
    CONSTRAINT feedback_events_revision_check CHECK ((revision > 1))
);

CREATE TABLE public.feedback_reads (
    thread_id public.uuidv7 NOT NULL,
    reader_key text NOT NULL,
    revision bigint NOT NULL,
    CONSTRAINT feedback_reads_revision_check CHECK ((revision > 0))
);

CREATE TABLE public.feedback_requests (
    actor_key text NOT NULL,
    request_id uuid NOT NULL,
    fingerprint text NOT NULL,
    response jsonb NOT NULL
);

CREATE TABLE public.feedback_threads (
    thread_id public.uuidv7 NOT NULL,
    author_key text NOT NULL,
    author jsonb NOT NULL,
    title text NOT NULL,
    body text NOT NULL,
    status text DEFAULT 'open'::text NOT NULL,
    revision bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    diagnostics jsonb,
    CONSTRAINT feedback_diagnostics_size CHECK (((diagnostics IS NULL) OR ((jsonb_typeof(diagnostics) = 'object'::text) AND (octet_length((diagnostics)::text) <= 40960)))),
    CONSTRAINT feedback_threads_body_check CHECK (((char_length(body) >= 1) AND (char_length(body) <= 20000))),
    CONSTRAINT feedback_threads_revision_check CHECK ((revision > 0)),
    CONSTRAINT feedback_threads_status_check CHECK ((status = ANY (ARRAY['open'::text, 'resolved'::text]))),
    CONSTRAINT feedback_threads_title_check CHECK (((char_length(title) >= 1) AND (char_length(title) <= 160)))
);

CREATE TABLE public.humans (
    human_id public.uuidv7 NOT NULL,
    display_name text DEFAULT 'Sumi'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    display_name_customized boolean DEFAULT false NOT NULL,
    display_name_initialized boolean DEFAULT true NOT NULL,
    CONSTRAINT humans_display_name_length CHECK (((char_length(display_name) >= 1) AND (char_length(display_name) <= 80)))
);

CREATE TABLE public.local_mcp_connections (
    host_id uuid NOT NULL,
    persona_id public.uuidv7 NOT NULL,
    connection_id uuid NOT NULL,
    name text NOT NULL,
    transport text NOT NULL,
    endpoint text DEFAULT ''::text NOT NULL,
    enabled boolean DEFAULT false NOT NULL,
    configuration_ciphertext bytea NOT NULL,
    version uuid NOT NULL,
    CONSTRAINT local_mcp_connections_transport_check CHECK ((transport = ANY (ARRAY['https'::text, 'stdio'::text])))
);

CREATE TABLE public.mcp_connections (
    human_id public.uuidv7 NOT NULL,
    connection_id uuid NOT NULL,
    name text NOT NULL,
    endpoint text NOT NULL,
    enabled boolean DEFAULT false NOT NULL,
    credential_ciphertext bytea NOT NULL,
    version uuid NOT NULL
);

CREATE TABLE public.message_attachments (
    attachment_id public.uuidv7 NOT NULL,
    workspace_id public.uuidv7 NOT NULL,
    place_id public.uuidv7 NOT NULL,
    message_id public.uuidv7,
    uploader_kind text NOT NULL,
    uploader_id public.uuidv7 NOT NULL,
    client_nonce text NOT NULL,
    filename text NOT NULL,
    mime text NOT NULL,
    size_bytes bigint NOT NULL,
    sha256 bytea NOT NULL,
    "position" integer DEFAULT 0 NOT NULL,
    blob_state text DEFAULT 'stored'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    bound_at timestamp with time zone,
    blob_deleted_at timestamp with time zone,
    spoiler boolean DEFAULT false NOT NULL,
    alt text DEFAULT ''::text NOT NULL,
    CONSTRAINT message_attachments_alt_check CHECK ((octet_length(alt) <= 4000)),
    CONSTRAINT message_attachments_blob_state_check CHECK ((blob_state = ANY (ARRAY['stored'::text, 'deleting'::text, 'deleted'::text]))),
    CONSTRAINT message_attachments_check CHECK (((message_id IS NULL) = (bound_at IS NULL))),
    CONSTRAINT message_attachments_check1 CHECK (((blob_state = 'deleted'::text) = (blob_deleted_at IS NOT NULL))),
    CONSTRAINT message_attachments_client_nonce_check CHECK (((octet_length(client_nonce) >= 1) AND (octet_length(client_nonce) <= 128))),
    CONSTRAINT message_attachments_filename_check CHECK (((octet_length(filename) >= 1) AND (octet_length(filename) <= 255))),
    CONSTRAINT message_attachments_mime_check CHECK (((octet_length(mime) >= 1) AND (octet_length(mime) <= 255))),
    CONSTRAINT message_attachments_position_check CHECK ((("position" >= 0) AND ("position" < 10))),
    CONSTRAINT message_attachments_sha256_check CHECK ((octet_length(sha256) = 32)),
    CONSTRAINT message_attachments_size_bytes_check CHECK (((size_bytes > 0) AND (size_bytes <= 20971520))),
    CONSTRAINT message_attachments_uploader_kind_check CHECK ((uploader_kind = ANY (ARRAY['human'::text, 'personality_agent'::text])))
);

CREATE VIEW public.message_attachment_blob_inventory AS
 SELECT attachment_id,
    size_bytes
   FROM public.message_attachments
  WHERE (blob_state = ANY (ARRAY['stored'::text, 'deleting'::text]));

CREATE TABLE public.message_attachment_quotas (
    workspace_id public.uuidv7 NOT NULL,
    used_bytes bigint DEFAULT 0 NOT NULL,
    object_count bigint DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT message_attachment_quotas_object_count_check CHECK ((object_count >= 0)),
    CONSTRAINT message_attachment_quotas_used_bytes_check CHECK ((used_bytes >= 0))
);

CREATE TABLE public.message_attachment_store_usage (
    singleton boolean DEFAULT true NOT NULL,
    used_bytes bigint DEFAULT 0 NOT NULL,
    object_count bigint DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT message_attachment_store_usage_object_count_check CHECK ((object_count >= 0)),
    CONSTRAINT message_attachment_store_usage_singleton_check CHECK (singleton),
    CONSTRAINT message_attachment_store_usage_used_bytes_check CHECK ((used_bytes >= 0))
);

CREATE TABLE public.message_attachment_uploads (
    upload_id public.uuidv7 NOT NULL,
    workspace_id public.uuidv7 NOT NULL,
    place_id public.uuidv7 NOT NULL,
    uploader_kind text NOT NULL,
    uploader_id public.uuidv7 NOT NULL,
    client_nonce text NOT NULL,
    installation_id text NOT NULL,
    authority_epoch bigint NOT NULL,
    declared_bytes bigint NOT NULL,
    state text DEFAULT 'reserved'::text NOT NULL,
    attachment_id public.uuidv7,
    staging_token public.uuidv7,
    staging_expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    settled_at timestamp with time zone,
    CONSTRAINT message_attachment_uploads_authority_epoch_check CHECK ((authority_epoch >= 1)),
    CONSTRAINT message_attachment_uploads_check CHECK (((state = 'finalized'::text) = (attachment_id IS NOT NULL))),
    CONSTRAINT message_attachment_uploads_check1 CHECK (((state = 'reserved'::text) = (settled_at IS NULL))),
    CONSTRAINT message_attachment_uploads_check2 CHECK (((staging_token IS NULL) = (staging_expires_at IS NULL))),
    CONSTRAINT message_attachment_uploads_check3 CHECK ((expires_at > created_at)),
    CONSTRAINT message_attachment_uploads_client_nonce_check CHECK (((octet_length(client_nonce) >= 1) AND (octet_length(client_nonce) <= 128))),
    CONSTRAINT message_attachment_uploads_declared_bytes_check CHECK (((declared_bytes > 0) AND (declared_bytes <= 20971520))),
    CONSTRAINT message_attachment_uploads_installation_id_check CHECK (((octet_length(installation_id) >= 1) AND (octet_length(installation_id) <= 128))),
    CONSTRAINT message_attachment_uploads_state_check CHECK ((state = ANY (ARRAY['reserved'::text, 'finalized'::text, 'released'::text]))),
    CONSTRAINT message_attachment_uploads_uploader_kind_check CHECK ((uploader_kind = ANY (ARRAY['human'::text, 'personality_agent'::text])))
);

CREATE TABLE public.message_mentions (
    message_id public.uuidv7 NOT NULL,
    member_kind text NOT NULL,
    member_id public.uuidv7 NOT NULL,
    CONSTRAINT message_mentions_member_kind_check CHECK ((member_kind = ANY (ARRAY['human'::text, 'personality_agent'::text])))
);

CREATE TABLE public.message_notification_intents (
    message_id public.uuidv7 NOT NULL,
    recipient_kind text NOT NULL,
    recipient_id public.uuidv7 NOT NULL,
    reason text NOT NULL,
    issued_at timestamp with time zone DEFAULT now() NOT NULL,
    recipient_workspace_member_id public.uuidv7 NOT NULL,
    recipient_place_member_id public.uuidv7,
    CONSTRAINT message_notification_intents_reason_check CHECK ((reason = ANY (ARRAY['dm'::text, 'mention'::text, 'keyword'::text, 'all'::text]))),
    CONSTRAINT message_notification_intents_recipient_kind_check CHECK ((recipient_kind = ANY (ARRAY['human'::text, 'personality_agent'::text])))
);

CREATE TABLE public.message_poll_options (
    option_id public.uuidv7 NOT NULL,
    message_id public.uuidv7 NOT NULL,
    text text NOT NULL,
    ord smallint NOT NULL,
    CONSTRAINT message_poll_options_ord_check CHECK (((ord >= 0) AND (ord <= 9))),
    CONSTRAINT message_poll_options_text_check CHECK (((char_length(text) >= 1) AND (char_length(text) <= 200)))
);

CREATE TABLE public.message_poll_votes (
    option_id public.uuidv7 NOT NULL,
    voter_kind text NOT NULL,
    voter_id public.uuidv7 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT message_poll_votes_voter_kind_check CHECK ((voter_kind = ANY (ARRAY['human'::text, 'personality_agent'::text])))
);

CREATE TABLE public.message_polls (
    message_id public.uuidv7 NOT NULL,
    question text NOT NULL,
    allow_multi boolean DEFAULT false NOT NULL,
    closes_at timestamp with time zone,
    revision bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT message_polls_question_check CHECK (((char_length(question) >= 1) AND (char_length(question) <= 500))),
    CONSTRAINT message_polls_revision_check CHECK ((revision >= 0))
);

CREATE TABLE public.message_reaction_mutations (
    workspace_id public.uuidv7 NOT NULL,
    member_kind text NOT NULL,
    member_id public.uuidv7 NOT NULL,
    client_nonce text NOT NULL,
    message_id public.uuidv7 NOT NULL,
    emoji text NOT NULL,
    reacted boolean NOT NULL,
    CONSTRAINT message_reaction_mutations_client_nonce_check CHECK (((length(client_nonce) >= 1) AND (length(client_nonce) <= 128))),
    CONSTRAINT message_reaction_mutations_emoji_check CHECK (((length(emoji) >= 1) AND (length(emoji) <= 32))),
    CONSTRAINT message_reaction_mutations_member_kind_check CHECK ((member_kind = ANY (ARRAY['human'::text, 'personality_agent'::text])))
);

CREATE TABLE public.message_reactions (
    message_id public.uuidv7 NOT NULL,
    member_kind text NOT NULL,
    member_id public.uuidv7 NOT NULL,
    emoji text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT message_reactions_emoji_check CHECK (((length(emoji) >= 1) AND (length(emoji) <= 32))),
    CONSTRAINT message_reactions_member_kind_check CHECK ((member_kind = ANY (ARRAY['human'::text, 'personality_agent'::text])))
);

CREATE TABLE public.messages (
    message_id public.uuidv7 NOT NULL,
    workspace_id public.uuidv7 NOT NULL,
    place_id public.uuidv7 NOT NULL,
    seq bigint NOT NULL,
    author_kind text NOT NULL,
    author_id public.uuidv7 NOT NULL,
    content text,
    urgency text DEFAULT 'normal'::text NOT NULL,
    reply_to public.uuidv7,
    client_nonce text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    edited_at timestamp with time zone,
    deleted_at timestamp with time zone,
    request_digest bytea NOT NULL,
    revision bigint DEFAULT 1 NOT NULL,
    CONSTRAINT messages_author_kind_check CHECK ((author_kind = ANY (ARRAY['human'::text, 'personality_agent'::text]))),
    CONSTRAINT messages_check CHECK (((content IS NULL) = (deleted_at IS NOT NULL))),
    CONSTRAINT messages_client_nonce_check CHECK (((octet_length(client_nonce) >= 1) AND (octet_length(client_nonce) <= 128))),
    CONSTRAINT messages_content_check CHECK ((octet_length(content) <= 65536)),
    CONSTRAINT messages_request_digest_check CHECK ((octet_length(request_digest) = 32)),
    CONSTRAINT messages_revision_check CHECK ((revision > 0)),
    CONSTRAINT messages_seq_check CHECK (((seq > 0) AND (seq <= '9007199254740991'::bigint))),
    CONSTRAINT messages_urgency_check CHECK ((urgency = ANY (ARRAY['urgent'::text, 'normal'::text, 'fyi'::text])))
);

CREATE TABLE public.messaging_place_creation_receipts (
    workspace_id public.uuidv7 NOT NULL,
    workspace_member_id public.uuidv7 NOT NULL,
    member_kind text NOT NULL,
    member_id public.uuidv7 NOT NULL,
    operation text NOT NULL,
    client_nonce text NOT NULL,
    request_digest bytea NOT NULL,
    place_id public.uuidv7 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT messaging_place_creation_receipts_client_nonce_check CHECK (((length(client_nonce) >= 1) AND (length(client_nonce) <= 128))),
    CONSTRAINT messaging_place_creation_receipts_member_kind_check CHECK ((member_kind = ANY (ARRAY['human'::text, 'personality_agent'::text]))),
    CONSTRAINT messaging_place_creation_receipts_operation_check CHECK ((operation = ANY (ARRAY['create_channel'::text, 'duplicate_channel'::text, 'create_group_dm'::text, 'create_thread'::text]))),
    CONSTRAINT messaging_place_creation_receipts_request_digest_check CHECK ((octet_length(request_digest) = 32))
);

CREATE TABLE public.model_api_connections (
    human_id public.uuidv7 NOT NULL,
    connection_id uuid NOT NULL,
    name text NOT NULL,
    preset text NOT NULL,
    base_url text NOT NULL,
    model text NOT NULL,
    credential_ciphertext bytea NOT NULL,
    version uuid NOT NULL,
    max_output_tokens integer,
    CONSTRAINT model_api_connections_max_output_tokens_check CHECK (((max_output_tokens IS NULL) OR (max_output_tokens > 0)))
);

CREATE TABLE public.model_connection_selections (
    human_id public.uuidv7 NOT NULL,
    kind text NOT NULL,
    connection_id uuid,
    CONSTRAINT model_connection_selections_check CHECK (((kind = 'api'::text) = (connection_id IS NOT NULL))),
    CONSTRAINT model_connection_selections_kind_check CHECK ((kind = ANY (ARRAY['api'::text, 'chatgpt'::text, 'none'::text])))
);

CREATE TABLE public.notification_setting_places (
    workspace_id public.uuidv7 NOT NULL,
    member_kind text NOT NULL,
    member_id public.uuidv7 NOT NULL,
    place_id public.uuidv7 NOT NULL,
    level text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT notification_setting_places_level_check CHECK ((level = ANY (ARRAY['all'::text, 'mentions'::text, 'mute'::text]))),
    CONSTRAINT notification_setting_places_member_kind_check CHECK ((member_kind = ANY (ARRAY['human'::text, 'personality_agent'::text])))
);

CREATE TABLE public.notification_settings (
    workspace_id public.uuidv7 NOT NULL,
    member_kind text NOT NULL,
    member_id public.uuidv7 NOT NULL,
    defaults_level text DEFAULT 'all'::text NOT NULL,
    keywords text[] DEFAULT '{}'::text[] NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT notification_settings_defaults_level_check CHECK ((defaults_level = ANY (ARRAY['all'::text, 'mentions'::text, 'mute'::text]))),
    CONSTRAINT notification_settings_keywords_check CHECK ((cardinality(keywords) <= 32)),
    CONSTRAINT notification_settings_member_kind_check CHECK ((member_kind = ANY (ARRAY['human'::text, 'personality_agent'::text])))
);

CREATE TABLE public.participant_profiles (
    member_kind text NOT NULL,
    member_id public.uuidv7 NOT NULL,
    tagline text DEFAULT ''::text NOT NULL,
    CONSTRAINT participant_profiles_member_kind_check CHECK ((member_kind = ANY (ARRAY['human'::text, 'personality_agent'::text]))),
    CONSTRAINT participant_profiles_tagline_check CHECK ((char_length(tagline) <= 100))
);

CREATE TABLE public.participant_statuses (
    member_kind text NOT NULL,
    member_id public.uuidv7 NOT NULL,
    status text,
    note text DEFAULT ''::text NOT NULL,
    expires_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    base_status text,
    base_note text DEFAULT ''::text NOT NULL,
    revision bigint DEFAULT 1 NOT NULL,
    CONSTRAINT participant_statuses_base_needs_expiry CHECK (((base_status IS NULL) OR (expires_at IS NOT NULL))),
    CONSTRAINT participant_statuses_base_note_check CHECK ((length(base_note) <= 200)),
    CONSTRAINT participant_statuses_base_status_check CHECK (((base_status IS NULL) OR (base_status = ANY (ARRAY['available'::text, 'busy'::text, 'away'::text])))),
    CONSTRAINT participant_statuses_member_kind_check CHECK ((member_kind = ANY (ARRAY['human'::text, 'personality_agent'::text]))),
    CONSTRAINT participant_statuses_note_check CHECK ((length(note) <= 200)),
    CONSTRAINT participant_statuses_revision_check CHECK (((revision >= 1) AND (revision <= '9007199254740991'::bigint))),
    CONSTRAINT participant_statuses_status_check CHECK ((status = ANY (ARRAY['available'::text, 'busy'::text, 'away'::text])))
);

CREATE TABLE public.persona_file_tokens (
    token_hash bytea NOT NULL,
    persona_id public.uuidv7 NOT NULL,
    session_id public.uuidv7 NOT NULL,
    destination_placement_id public.uuidv7 NOT NULL,
    file_epoch bigint DEFAULT 0 NOT NULL,
    scope text NOT NULL,
    status text NOT NULL,
    issued_at timestamp with time zone DEFAULT now() NOT NULL,
    resolved_at timestamp with time zone,
    CONSTRAINT persona_file_tokens_check CHECK (((status = 'active'::text) = (resolved_at IS NULL))),
    CONSTRAINT persona_file_tokens_status_check CHECK ((status = ANY (ARRAY['active'::text, 'superseded'::text, 'revoked'::text]))),
    CONSTRAINT persona_file_tokens_token_hash_check CHECK ((octet_length(token_hash) = 32))
);

CREATE TABLE public.place_members (
    place_member_id public.uuidv7 NOT NULL,
    workspace_id public.uuidv7 NOT NULL,
    place_id public.uuidv7 NOT NULL,
    workspace_member_id public.uuidv7 NOT NULL,
    member_kind text NOT NULL,
    member_id public.uuidv7 NOT NULL,
    visible_from_seq bigint DEFAULT 1 NOT NULL,
    joined_at timestamp with time zone DEFAULT now() NOT NULL,
    left_at timestamp with time zone,
    CONSTRAINT place_members_check CHECK (((left_at IS NULL) OR (left_at >= joined_at))),
    CONSTRAINT place_members_member_kind_check CHECK ((member_kind = ANY (ARRAY['human'::text, 'personality_agent'::text]))),
    CONSTRAINT place_members_visible_from_seq_check CHECK (((visible_from_seq > 0) AND (visible_from_seq <= '9007199254740991'::bigint)))
);

CREATE TABLE public.places (
    place_id public.uuidv7 NOT NULL,
    kind text NOT NULL,
    workspace_id public.uuidv7 NOT NULL,
    name text,
    topic text DEFAULT ''::text NOT NULL,
    visibility text DEFAULT 'public'::text NOT NULL,
    dm_key text,
    last_seq bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    voice boolean DEFAULT false NOT NULL,
    revision bigint DEFAULT 1 NOT NULL,
    parent_place_id public.uuidv7,
    parent_message_id public.uuidv7,
    CONSTRAINT places_dm_keyed CHECK (((kind = 'dm'::text) = (dm_key IS NOT NULL))),
    CONSTRAINT places_kind_known CHECK ((kind = ANY (ARRAY['channel'::text, 'dm'::text, 'group_dm'::text, 'thread'::text]))),
    CONSTRAINT places_last_seq_check CHECK (((last_seq >= 0) AND (last_seq <= '9007199254740991'::bigint))),
    CONSTRAINT places_name_check CHECK (((name IS NULL) OR ((length(name) >= 1) AND (length(name) <= 200)))),
    CONSTRAINT places_named CHECK (((kind = ANY (ARRAY['channel'::text, 'thread'::text])) = (name IS NOT NULL))),
    CONSTRAINT places_revision_check CHECK (((revision >= 1) AND (revision <= '9007199254740991'::bigint))),
    CONSTRAINT places_thread_name_length CHECK (((kind <> 'thread'::text) OR (char_length(name) <= 100))),
    CONSTRAINT places_thread_origin CHECK (((parent_message_id IS NULL) OR (kind = 'thread'::text))),
    CONSTRAINT places_thread_parented CHECK (((kind = 'thread'::text) = (parent_place_id IS NOT NULL))),
    CONSTRAINT places_visibility_check CHECK ((visibility = ANY (ARRAY['public'::text, 'private'::text]))),
    CONSTRAINT places_voice_is_channel_only CHECK (((NOT voice) OR (kind = 'channel'::text)))
);

CREATE TABLE public.provider_operations (
    operation_id public.uuidv7 NOT NULL,
    nonce_hash bytea NOT NULL,
    human_id public.uuidv7 NOT NULL,
    firebase_uid text NOT NULL,
    provider text NOT NULL,
    operation text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    decision_path text NOT NULL,
    terminal_outcome text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT provider_operations_check CHECK ((((status = 'pending'::text) AND (terminal_outcome IS NULL) AND (completed_at IS NULL)) OR ((status <> 'pending'::text) AND (terminal_outcome IS NOT NULL) AND (completed_at IS NOT NULL)))),
    CONSTRAINT provider_operations_nonce_hash_check CHECK ((octet_length(nonce_hash) = 32)),
    CONSTRAINT provider_operations_operation_check CHECK ((operation = ANY (ARRAY['link'::text, 'unlink'::text]))),
    CONSTRAINT provider_operations_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'completed'::text, 'failed'::text])))
);

CREATE TABLE public.push_devices (
    device_id text NOT NULL,
    human_id public.uuidv7 NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    CONSTRAINT push_devices_device_id_check CHECK ((length(device_id) = 43))
);

CREATE TABLE public.push_subscriptions (
    subscription_id public.uuidv7 NOT NULL,
    human_id public.uuidv7 NOT NULL,
    endpoint text NOT NULL,
    p256dh text NOT NULL,
    auth text NOT NULL,
    owner_generation bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    device_id text NOT NULL,
    CONSTRAINT push_subscriptions_auth_check CHECK (((length(auth) >= 1) AND (length(auth) <= 100))),
    CONSTRAINT push_subscriptions_endpoint_check CHECK (((length(endpoint) >= 1) AND (length(endpoint) <= 2000))),
    CONSTRAINT push_subscriptions_owner_generation_check CHECK ((owner_generation > 0)),
    CONSTRAINT push_subscriptions_p256dh_check CHECK (((length(p256dh) >= 1) AND (length(p256dh) <= 200)))
);

CREATE TABLE public.push_vapid_keys (
    singleton boolean DEFAULT true NOT NULL,
    public_key text NOT NULL,
    private_key text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT push_vapid_keys_private_key_check CHECK (((length(private_key) >= 1) AND (length(private_key) <= 200))),
    CONSTRAINT push_vapid_keys_public_key_check CHECK (((length(public_key) >= 1) AND (length(public_key) <= 200))),
    CONSTRAINT push_vapid_keys_singleton_check CHECK (singleton)
);

CREATE TABLE public.read_markers (
    place_id public.uuidv7 NOT NULL,
    workspace_member_id public.uuidv7 NOT NULL,
    last_read_seq bigint NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT read_markers_last_read_seq_check CHECK (((last_read_seq >= 0) AND (last_read_seq <= '9007199254740991'::bigint)))
);

CREATE TABLE public.reply_later_markers (
    marker_id public.uuidv7 NOT NULL,
    member_kind text NOT NULL,
    member_id public.uuidv7 NOT NULL,
    place_id public.uuidv7 NOT NULL,
    message_id public.uuidv7 NOT NULL,
    note text DEFAULT ''::text NOT NULL,
    remind_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    resolved_at timestamp with time zone,
    CONSTRAINT reply_later_markers_check CHECK (((resolved_at IS NULL) OR (resolved_at >= created_at))),
    CONSTRAINT reply_later_markers_member_kind_check CHECK ((member_kind = ANY (ARRAY['human'::text, 'personality_agent'::text]))),
    CONSTRAINT reply_later_markers_note_check CHECK ((length(note) <= 500))
);

CREATE TABLE public.research_consents (
    consent_id bigint NOT NULL,
    human_id public.uuidv7 NOT NULL,
    granted_at timestamp with time zone DEFAULT now() NOT NULL,
    revoked_at timestamp with time zone,
    CONSTRAINT research_consents_check CHECK (((revoked_at IS NULL) OR (revoked_at >= granted_at)))
);

CREATE SEQUENCE public.research_consents_consent_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.research_consents_consent_id_seq OWNED BY public.research_consents.consent_id;

CREATE SEQUENCE public.return_file_epoch_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE public.return_sessions (
    session_id public.uuidv7 NOT NULL,
    human_id public.uuidv7 NOT NULL,
    persona_id public.uuidv7 NOT NULL,
    grant_hash bytea NOT NULL,
    status text NOT NULL,
    destination_placement_id public.uuidv7,
    destination_persona_id public.uuidv7,
    destination_slot_state text,
    destination_bound_at timestamp with time zone,
    prior_transfer_id text,
    admit_until timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    file_mode text,
    file_epoch bigint DEFAULT nextval('public.return_file_epoch_seq'::regclass) NOT NULL,
    capture_id text,
    capture_scope_id text,
    capture_manifest_sha text,
    CONSTRAINT return_sessions_capture_binding CHECK ((((capture_id IS NULL) = (capture_manifest_sha IS NULL)) AND ((capture_id IS NULL) OR (capture_scope_id IS NOT NULL)))),
    CONSTRAINT return_sessions_check CHECK (((destination_placement_id IS NULL) = (destination_persona_id IS NULL))),
    CONSTRAINT return_sessions_check1 CHECK (((destination_placement_id IS NULL) = (destination_slot_state IS NULL))),
    CONSTRAINT return_sessions_check2 CHECK (((destination_placement_id IS NULL) = (destination_bound_at IS NULL))),
    CONSTRAINT return_sessions_check3 CHECK (((status <> ALL (ARRAY['sealed'::text, 'cancelling'::text, 'completed'::text, 'aborted'::text])) OR (destination_placement_id IS NOT NULL))),
    CONSTRAINT return_sessions_destination_slot_state_check CHECK ((destination_slot_state = ANY (ARRAY['absent'::text, 'surrendered'::text]))),
    CONSTRAINT return_sessions_file_mode_check CHECK ((file_mode = ANY (ARRAY['local'::text, 'cloud'::text]))),
    CONSTRAINT return_sessions_grant_hash_check CHECK ((octet_length(grant_hash) = 32)),
    CONSTRAINT return_sessions_status_check CHECK ((status = ANY (ARRAY['awaiting_destination'::text, 'sealed'::text, 'cancelling'::text, 'completed'::text, 'aborted'::text, 'cancelled'::text, 'expired'::text])))
);

CREATE TABLE public.transfer_sessions (
    session_id public.uuidv7 NOT NULL,
    claim_provider text NOT NULL,
    claim_subject text NOT NULL,
    grant_hash bytea NOT NULL,
    status text NOT NULL,
    human_id public.uuidv7,
    source_placement_id public.uuidv7,
    source_persona_id public.uuidv7,
    source_bound_at timestamp with time zone,
    admit_until timestamp with time zone NOT NULL,
    claim_until timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT transfer_sessions_check CHECK (((status = ANY (ARRAY['provisioned'::text, 'activated'::text])) = (human_id IS NOT NULL))),
    CONSTRAINT transfer_sessions_check1 CHECK (((status <> 'staged'::text) OR (claim_until IS NOT NULL))),
    CONSTRAINT transfer_sessions_check2 CHECK (((source_placement_id IS NULL) = (source_persona_id IS NULL))),
    CONSTRAINT transfer_sessions_check3 CHECK (((source_placement_id IS NULL) = (source_bound_at IS NULL))),
    CONSTRAINT transfer_sessions_claim_provider_check CHECK ((claim_provider = 'firebase'::text)),
    CONSTRAINT transfer_sessions_claim_subject_check CHECK (((char_length(claim_subject) >= 1) AND (char_length(claim_subject) <= 128))),
    CONSTRAINT transfer_sessions_grant_hash_check CHECK ((octet_length(grant_hash) = 32)),
    CONSTRAINT transfer_sessions_status_check CHECK ((status = ANY (ARRAY['awaiting_bundle'::text, 'staged'::text, 'provisioned'::text, 'activated'::text, 'cancelled'::text, 'expired'::text])))
);

CREATE TABLE public.usage_budgets (
    funding_kind text NOT NULL,
    funding_id text NOT NULL,
    limit_minor bigint NOT NULL,
    currency text NOT NULL,
    rate_input_per_mtok bigint NOT NULL,
    rate_output_per_mtok bigint NOT NULL,
    rate_cached_per_mtok bigint,
    pricing_revision text DEFAULT ''::text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT usage_budgets_currency_check CHECK ((char_length(currency) = 3)),
    CONSTRAINT usage_budgets_funding_kind_check CHECK ((funding_kind = ANY (ARRAY['connection'::text, 'sumi'::text, 'operator'::text]))),
    CONSTRAINT usage_budgets_limit_minor_check CHECK ((limit_minor >= 0)),
    CONSTRAINT usage_budgets_rate_cached_per_mtok_check CHECK ((rate_cached_per_mtok >= 0)),
    CONSTRAINT usage_budgets_rate_input_per_mtok_check CHECK ((rate_input_per_mtok >= 0)),
    CONSTRAINT usage_budgets_rate_output_per_mtok_check CHECK ((rate_output_per_mtok >= 0))
);

CREATE TABLE public.usage_facts (
    persona_id public.uuidv7 NOT NULL,
    fact_id text NOT NULL,
    kind text NOT NULL,
    phase text NOT NULL,
    turn_id text,
    input_id text,
    round integer,
    funding_kind text NOT NULL,
    funding_id text NOT NULL,
    funding jsonb NOT NULL,
    status text NOT NULL,
    input_tokens bigint,
    output_tokens bigint,
    cached_tokens bigint,
    quantities jsonb DEFAULT '{}'::jsonb NOT NULL,
    cost_minor bigint,
    currency text,
    cost_basis text,
    pricing_revision text,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT usage_facts_funding_kind_check CHECK ((funding_kind = ANY (ARRAY['connection'::text, 'operator'::text, 'sumi'::text]))),
    CONSTRAINT usage_facts_status_check CHECK ((status = ANY (ARRAY['reported'::text, 'unknown'::text, 'not_sent'::text, 'unrecorded'::text])))
);

CREATE TABLE public.usage_funding_grants (
    funding_id text NOT NULL,
    human_id public.uuidv7 NOT NULL,
    label text DEFAULT ''::text NOT NULL,
    revoked_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.usage_reservations (
    persona_id public.uuidv7 NOT NULL,
    fact_id text NOT NULL,
    kind text NOT NULL,
    phase text NOT NULL,
    turn_id text,
    input_id text,
    round integer,
    funding_kind text NOT NULL,
    funding_id text NOT NULL,
    funding jsonb NOT NULL,
    reserved_minor bigint NOT NULL,
    currency text,
    bounded boolean DEFAULT false NOT NULL,
    est_input_tokens bigint,
    est_output_bound bigint,
    rate_input_per_mtok bigint,
    rate_output_per_mtok bigint,
    rate_cached_per_mtok bigint,
    pricing_revision text,
    generation bigint NOT NULL,
    status text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    settled_at timestamp with time zone,
    CONSTRAINT usage_reservations_reserved_minor_check CHECK ((reserved_minor >= 0)),
    CONSTRAINT usage_reservations_status_check CHECK ((status = ANY (ARRAY['held'::text, 'settled'::text, 'released'::text])))
);

CREATE TABLE public.workspace_invites (
    invite_id public.uuidv7 NOT NULL,
    workspace_id public.uuidv7 NOT NULL,
    created_by_workspace_member_id public.uuidv7 NOT NULL,
    code_hash bytea,
    expires_at timestamp with time zone NOT NULL,
    redeemed_by_kind text,
    redeemed_by_id public.uuidv7,
    redeemed_workspace_member_id public.uuidv7,
    redeemed_at timestamp with time zone,
    revoked_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    invite_kind text DEFAULT 'share_code'::text NOT NULL,
    target_kind text,
    target_id public.uuidv7,
    reserved_human_id public.uuidv7,
    CONSTRAINT workspace_invites_check CHECK ((((redeemed_by_kind IS NULL) AND (redeemed_by_id IS NULL) AND (redeemed_workspace_member_id IS NULL) AND (redeemed_at IS NULL)) OR ((redeemed_by_kind IS NOT NULL) AND (redeemed_by_id IS NOT NULL) AND (redeemed_workspace_member_id IS NOT NULL) AND (redeemed_at IS NOT NULL)))),
    CONSTRAINT workspace_invites_code_hash_check CHECK ((octet_length(code_hash) = 32)),
    CONSTRAINT workspace_invites_invite_kind_check CHECK ((invite_kind = ANY (ARRAY['share_code'::text, 'targeted_personality_agent'::text]))),
    CONSTRAINT workspace_invites_redeemed_by_kind_check CHECK ((redeemed_by_kind = ANY (ARRAY['human'::text, 'personality_agent'::text]))),
    CONSTRAINT workspace_invites_strict_variant CHECK ((((invite_kind = 'share_code'::text) AND (code_hash IS NOT NULL) AND (target_kind IS NULL) AND (target_id IS NULL)) OR ((invite_kind = 'targeted_personality_agent'::text) AND (code_hash IS NULL) AND (target_kind = 'personality_agent'::text) AND (target_id IS NOT NULL)))),
    CONSTRAINT workspace_invites_target_kind_check CHECK ((target_kind = 'personality_agent'::text)),
    CONSTRAINT workspace_invites_target_redeemer CHECK (((invite_kind <> 'targeted_personality_agent'::text) OR (redeemed_by_kind IS NULL) OR ((redeemed_by_kind = target_kind) AND ((redeemed_by_id)::text = (target_id)::text))))
);

CREATE TABLE public.workspace_members (
    workspace_member_id public.uuidv7 NOT NULL,
    workspace_id public.uuidv7 NOT NULL,
    member_kind text NOT NULL,
    member_id public.uuidv7 NOT NULL,
    joined_at timestamp with time zone DEFAULT now() NOT NULL,
    left_at timestamp with time zone,
    CONSTRAINT workspace_members_check CHECK (((left_at IS NULL) OR (left_at >= joined_at))),
    CONSTRAINT workspace_members_member_kind_check CHECK ((member_kind = ANY (ARRAY['human'::text, 'personality_agent'::text])))
);

CREATE TABLE public.workspace_role_app_capability_grants (
    workspace_id public.uuidv7 NOT NULL,
    role_id public.uuidv7 NOT NULL,
    capability_id public.uuidv7 NOT NULL,
    capability_ref_snapshot text NOT NULL,
    granted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT workspace_role_app_capability_gra_capability_ref_snapshot_check CHECK ((capability_ref_snapshot ~ '^app\.[a-z][a-z0-9-]{0,63}\.[a-z][a-z0-9_]{0,63}$'::text))
);

CREATE TABLE public.workspace_role_assignments (
    workspace_id public.uuidv7 NOT NULL,
    role_id public.uuidv7 NOT NULL,
    workspace_member_id public.uuidv7 NOT NULL,
    granted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.workspace_roles (
    role_id public.uuidv7 NOT NULL,
    workspace_id public.uuidv7 NOT NULL,
    name text NOT NULL,
    color text,
    "position" integer DEFAULT 0 NOT NULL,
    permissions jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT workspace_roles_color_check CHECK (((color IS NULL) OR (color ~ '^#[0-9a-f]{6}$'::text))),
    CONSTRAINT workspace_roles_name_check CHECK (((char_length(name) >= 1) AND (char_length(name) <= 60))),
    CONSTRAINT workspace_roles_permissions_check CHECK ((jsonb_typeof(permissions) = 'object'::text)),
    CONSTRAINT workspace_roles_position_check CHECK ((("position" >= 0) AND ("position" <= 1000000)))
);

CREATE TABLE public.workspaces (
    workspace_id public.uuidv7 NOT NULL,
    name text NOT NULL,
    owner_workspace_member_id public.uuidv7 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT workspaces_name_check CHECK (((char_length(name) >= 1) AND (char_length(name) <= 200)))
);

ALTER TABLE ONLY public.credential_security_events ALTER COLUMN event_id SET DEFAULT nextval('public.credential_security_events_event_id_seq'::regclass);

ALTER TABLE ONLY public.credentials ALTER COLUMN credential_id SET DEFAULT nextval('public.credentials_credential_id_seq'::regclass);

ALTER TABLE ONLY public.employments ALTER COLUMN employment_id SET DEFAULT nextval('public.employments_employment_id_seq'::regclass);

ALTER TABLE ONLY public.research_consents ALTER COLUMN consent_id SET DEFAULT nextval('public.research_consents_consent_id_seq'::regclass);

ALTER TABLE ONLY public.agent_attention_deliveries
    ADD CONSTRAINT agent_attention_deliveries_personality_agent_id_source_kind_key UNIQUE (personality_agent_id, source_kind, source_id, source_revision);

ALTER TABLE ONLY public.agent_attention_deliveries
    ADD CONSTRAINT agent_attention_deliveries_pkey PRIMARY KEY (event_id);

ALTER TABLE ONLY public.agents
    ADD CONSTRAINT agents_pkey PRIMARY KEY (personality_agent_id);

ALTER TABLE ONLY public.app_catalog
    ADD CONSTRAINT app_catalog_pkey PRIMARY KEY (app_id);

ALTER TABLE ONLY public.app_install_operation_receipts
    ADD CONSTRAINT app_install_operation_receipts_pkey PRIMARY KEY (owner_kind, owner_id, operation_id);

ALTER TABLE ONLY public.app_installations
    ADD CONSTRAINT app_installations_owner_kind_owner_id_app_id_key UNIQUE (owner_kind, owner_id, app_id);

ALTER TABLE ONLY public.app_installations
    ADD CONSTRAINT app_installations_pkey PRIMARY KEY (installation_id);

ALTER TABLE ONLY public.app_workspace_role_capabilities
    ADD CONSTRAINT app_workspace_role_capabilitie_capability_id_capability_ref_key UNIQUE (capability_id, capability_ref);

ALTER TABLE ONLY public.app_workspace_role_capabilities
    ADD CONSTRAINT app_workspace_role_capabilities_pkey PRIMARY KEY (capability_id);

ALTER TABLE ONLY public.auth_email_challenges
    ADD CONSTRAINT auth_email_challenges_pkey PRIMARY KEY (challenge_id);

ALTER TABLE ONLY public.auth_email_deliveries
    ADD CONSTRAINT auth_email_deliveries_pkey PRIMARY KEY (delivery_id);

ALTER TABLE ONLY public.auth_flows
    ADD CONSTRAINT auth_flows_adopted_from_nonce_hash_key UNIQUE (adopted_from_nonce_hash);

ALTER TABLE ONLY public.auth_flows
    ADD CONSTRAINT auth_flows_nonce_hash_key UNIQUE (nonce_hash);

ALTER TABLE ONLY public.auth_flows
    ADD CONSTRAINT auth_flows_pkey PRIMARY KEY (flow_id);

ALTER TABLE ONLY public.browser_tab_attachments
    ADD CONSTRAINT browser_tab_attachments_pkey PRIMARY KEY (attachment_id);

ALTER TABLE ONLY public.call_sessions
    ADD CONSTRAINT call_sessions_pkey PRIMARY KEY (session_id);

ALTER TABLE ONLY public.call_utterances
    ADD CONSTRAINT call_utterances_pkey PRIMARY KEY (utterance_id);

ALTER TABLE ONLY public.call_utterances
    ADD CONSTRAINT call_utterances_session_id_seq_key UNIQUE (session_id, seq);

ALTER TABLE ONLY public.chatgpt_connections
    ADD CONSTRAINT chatgpt_connections_pkey PRIMARY KEY (human_id);

ALTER TABLE ONLY public.cloud_browser_jev_credentials
    ADD CONSTRAINT cloud_browser_jev_credentials_pkey PRIMARY KEY (human_id);

ALTER TABLE ONLY public.cloud_browser_profiles
    ADD CONSTRAINT cloud_browser_profiles_pkey PRIMARY KEY (profile_id);

ALTER TABLE ONLY public.core_budget_waits
    ADD CONSTRAINT core_budget_waits_pkey PRIMARY KEY (persona_id, input_id);

ALTER TABLE ONLY public.core_events
    ADD CONSTRAINT core_events_pkey PRIMARY KEY (persona_id, seq);

ALTER TABLE ONLY public.core_inputs
    ADD CONSTRAINT core_inputs_pkey PRIMARY KEY (persona_id, input_id);

ALTER TABLE ONLY public.core_job_file_ops
    ADD CONSTRAINT core_job_file_ops_op_id_key UNIQUE (op_id);

ALTER TABLE ONLY public.core_job_file_ops
    ADD CONSTRAINT core_job_file_ops_pkey PRIMARY KEY (persona_id, job_id, op_seq);

ALTER TABLE ONLY public.core_jobs
    ADD CONSTRAINT core_jobs_pkey PRIMARY KEY (persona_id, job_id);

ALTER TABLE ONLY public.core_memory_chunks
    ADD CONSTRAINT core_memory_chunks_pkey PRIMARY KEY (persona_id, chunk_seq);

ALTER TABLE ONLY public.core_operations
    ADD CONSTRAINT core_operations_persona_id_tool_idempotency_key_key UNIQUE (persona_id, tool, idempotency_key);

ALTER TABLE ONLY public.core_operations
    ADD CONSTRAINT core_operations_pkey PRIMARY KEY (persona_id, operation_id);

ALTER TABLE ONLY public.core_outbox
    ADD CONSTRAINT core_outbox_pkey PRIMARY KEY (persona_id, seq);

ALTER TABLE ONLY public.core_personas
    ADD CONSTRAINT core_personas_pkey PRIMARY KEY (persona_id);

ALTER TABLE ONLY public.core_placement
    ADD CONSTRAINT core_placement_pkey PRIMARY KEY (singleton);

ALTER TABLE ONLY public.core_schedules
    ADD CONSTRAINT core_schedules_pkey PRIMARY KEY (persona_id, schedule_id);

ALTER TABLE ONLY public.core_terminal_inputs
    ADD CONSTRAINT core_terminal_inputs_pkey PRIMARY KEY (input_id);

ALTER TABLE ONLY public.core_terminal_inputs
    ADD CONSTRAINT core_terminal_inputs_session_id_seq_key UNIQUE (session_id, seq);

ALTER TABLE ONLY public.core_terminal_output
    ADD CONSTRAINT core_terminal_output_session_id_base_kind_key UNIQUE (session_id, base, kind);

ALTER TABLE ONLY public.core_terminal_output
    ADD CONSTRAINT core_terminal_output_session_id_seq_key UNIQUE (session_id, seq);

ALTER TABLE ONLY public.core_terminal_sessions
    ADD CONSTRAINT core_terminal_sessions_pkey PRIMARY KEY (session_id);

ALTER TABLE ONLY public.core_tool_approvals
    ADD CONSTRAINT core_tool_approvals_persona_id_input_id_call_index_key UNIQUE (persona_id, input_id, call_index);

ALTER TABLE ONLY public.core_tool_approvals
    ADD CONSTRAINT core_tool_approvals_pkey PRIMARY KEY (persona_id, approval_id);

ALTER TABLE ONLY public.core_transfers
    ADD CONSTRAINT core_transfers_pkey PRIMARY KEY (direction, transfer_id);

ALTER TABLE ONLY public.core_turn_plans
    ADD CONSTRAINT core_turn_plans_pkey PRIMARY KEY (persona_id, input_id);

ALTER TABLE ONLY public.core_turns
    ADD CONSTRAINT core_turns_pkey PRIMARY KEY (persona_id, turn_id);

ALTER TABLE ONLY public.core_writer_leases
    ADD CONSTRAINT core_writer_leases_pkey PRIMARY KEY (persona_id);

ALTER TABLE ONLY public.credential_security_events
    ADD CONSTRAINT credential_security_events_pkey PRIMARY KEY (event_id);

ALTER TABLE ONLY public.credentials
    ADD CONSTRAINT credentials_pkey PRIMARY KEY (credential_id);

ALTER TABLE ONLY public.credentials
    ADD CONSTRAINT credentials_provider_external_subject_key UNIQUE (provider, external_subject);

ALTER TABLE ONLY public.employments
    ADD CONSTRAINT employments_pkey PRIMARY KEY (employment_id);

ALTER TABLE ONLY public.enrollment_invites
    ADD CONSTRAINT enrollment_invites_pkey PRIMARY KEY (invite_id);

ALTER TABLE ONLY public.enrollment_invites
    ADD CONSTRAINT enrollment_invites_token_hash_key UNIQUE (token_hash);

ALTER TABLE ONLY public.enrollment_invites
    ADD CONSTRAINT enrollment_invites_workspace_invite_id_key UNIQUE (workspace_invite_id);

ALTER TABLE ONLY public.feedback_attachments
    ADD CONSTRAINT feedback_attachments_pkey PRIMARY KEY (attachment_id);

ALTER TABLE ONLY public.feedback_attention_outbox
    ADD CONSTRAINT feedback_attention_outbox_pkey PRIMARY KEY (event_id, recipient_paid);

ALTER TABLE ONLY public.feedback_events
    ADD CONSTRAINT feedback_events_pkey PRIMARY KEY (event_id);

ALTER TABLE ONLY public.feedback_events
    ADD CONSTRAINT feedback_events_thread_id_revision_key UNIQUE (thread_id, revision);

ALTER TABLE ONLY public.feedback_reads
    ADD CONSTRAINT feedback_reads_pkey PRIMARY KEY (thread_id, reader_key);

ALTER TABLE ONLY public.feedback_requests
    ADD CONSTRAINT feedback_requests_pkey PRIMARY KEY (actor_key, request_id);

ALTER TABLE ONLY public.feedback_threads
    ADD CONSTRAINT feedback_threads_pkey PRIMARY KEY (thread_id);

ALTER TABLE ONLY public.humans
    ADD CONSTRAINT humans_pkey PRIMARY KEY (human_id);

ALTER TABLE ONLY public.local_mcp_connections
    ADD CONSTRAINT local_mcp_connections_pkey PRIMARY KEY (host_id, persona_id, connection_id);

ALTER TABLE ONLY public.mcp_connections
    ADD CONSTRAINT mcp_connections_pkey PRIMARY KEY (human_id, connection_id);

ALTER TABLE ONLY public.message_attachment_quotas
    ADD CONSTRAINT message_attachment_quotas_pkey PRIMARY KEY (workspace_id);

ALTER TABLE ONLY public.message_attachment_store_usage
    ADD CONSTRAINT message_attachment_store_usage_pkey PRIMARY KEY (singleton);

ALTER TABLE ONLY public.message_attachment_uploads
    ADD CONSTRAINT message_attachment_uploads_pkey PRIMARY KEY (upload_id);

ALTER TABLE ONLY public.message_attachment_uploads
    ADD CONSTRAINT message_attachment_uploads_place_uploader_nonce UNIQUE (workspace_id, place_id, uploader_kind, uploader_id, client_nonce);

ALTER TABLE ONLY public.message_attachments
    ADD CONSTRAINT message_attachments_pkey PRIMARY KEY (attachment_id);

ALTER TABLE ONLY public.message_attachments
    ADD CONSTRAINT message_attachments_place_uploader_nonce UNIQUE (workspace_id, place_id, uploader_kind, uploader_id, client_nonce);

ALTER TABLE ONLY public.message_attachments
    ADD CONSTRAINT message_attachments_workspace_id_place_id_attachment_id_key UNIQUE (workspace_id, place_id, attachment_id);

ALTER TABLE ONLY public.message_mentions
    ADD CONSTRAINT message_mentions_pkey PRIMARY KEY (message_id, member_kind, member_id);

ALTER TABLE ONLY public.message_notification_intents
    ADD CONSTRAINT message_notification_intents_pkey PRIMARY KEY (message_id, recipient_kind, recipient_id);

ALTER TABLE ONLY public.message_poll_options
    ADD CONSTRAINT message_poll_options_message_id_ord_key UNIQUE (message_id, ord);

ALTER TABLE ONLY public.message_poll_options
    ADD CONSTRAINT message_poll_options_message_id_text_key UNIQUE (message_id, text);

ALTER TABLE ONLY public.message_poll_options
    ADD CONSTRAINT message_poll_options_pkey PRIMARY KEY (option_id);

ALTER TABLE ONLY public.message_poll_votes
    ADD CONSTRAINT message_poll_votes_pkey PRIMARY KEY (option_id, voter_kind, voter_id);

ALTER TABLE ONLY public.message_polls
    ADD CONSTRAINT message_polls_pkey PRIMARY KEY (message_id);

ALTER TABLE ONLY public.message_reaction_mutations
    ADD CONSTRAINT message_reaction_mutations_pkey PRIMARY KEY (workspace_id, member_kind, member_id, client_nonce);

ALTER TABLE ONLY public.message_reactions
    ADD CONSTRAINT message_reactions_pkey PRIMARY KEY (message_id, member_kind, member_id, emoji);

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_pkey PRIMARY KEY (message_id);

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_place_id_message_id_key UNIQUE (place_id, message_id);

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_place_id_seq_key UNIQUE (place_id, seq);

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_workspace_id_message_id_key UNIQUE (workspace_id, message_id);

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_workspace_id_place_id_message_id_key UNIQUE (workspace_id, place_id, message_id);

ALTER TABLE ONLY public.messaging_place_creation_receipts
    ADD CONSTRAINT messaging_place_creation_receipts_pkey PRIMARY KEY (workspace_id, workspace_member_id, operation, client_nonce);

ALTER TABLE ONLY public.model_api_connections
    ADD CONSTRAINT model_api_connections_pkey PRIMARY KEY (human_id, connection_id);

ALTER TABLE ONLY public.model_connection_selections
    ADD CONSTRAINT model_connection_selections_pkey PRIMARY KEY (human_id);

ALTER TABLE ONLY public.notification_setting_places
    ADD CONSTRAINT notification_setting_places_pkey PRIMARY KEY (workspace_id, member_kind, member_id, place_id);

ALTER TABLE ONLY public.notification_settings
    ADD CONSTRAINT notification_settings_pkey PRIMARY KEY (workspace_id, member_kind, member_id);

ALTER TABLE ONLY public.participant_profiles
    ADD CONSTRAINT participant_profiles_pkey PRIMARY KEY (member_kind, member_id);

ALTER TABLE ONLY public.participant_statuses
    ADD CONSTRAINT participant_statuses_pkey PRIMARY KEY (member_kind, member_id);

ALTER TABLE ONLY public.persona_file_tokens
    ADD CONSTRAINT persona_file_tokens_pkey PRIMARY KEY (token_hash);

ALTER TABLE ONLY public.place_members
    ADD CONSTRAINT place_members_pkey PRIMARY KEY (place_member_id);

ALTER TABLE ONLY public.place_members
    ADD CONSTRAINT place_members_place_id_place_member_id_key UNIQUE (place_id, place_member_id);

ALTER TABLE ONLY public.places
    ADD CONSTRAINT places_pkey PRIMARY KEY (place_id);

ALTER TABLE ONLY public.places
    ADD CONSTRAINT places_workspace_id_dm_key_key UNIQUE (workspace_id, dm_key);

ALTER TABLE ONLY public.places
    ADD CONSTRAINT places_workspace_id_place_id_key UNIQUE (workspace_id, place_id);

ALTER TABLE ONLY public.provider_operations
    ADD CONSTRAINT provider_operations_nonce_hash_key UNIQUE (nonce_hash);

ALTER TABLE ONLY public.provider_operations
    ADD CONSTRAINT provider_operations_pkey PRIMARY KEY (operation_id);

ALTER TABLE ONLY public.push_devices
    ADD CONSTRAINT push_devices_pkey PRIMARY KEY (device_id);

ALTER TABLE ONLY public.push_subscriptions
    ADD CONSTRAINT push_subscriptions_endpoint_key UNIQUE (endpoint);

ALTER TABLE ONLY public.push_subscriptions
    ADD CONSTRAINT push_subscriptions_pkey PRIMARY KEY (subscription_id);

ALTER TABLE ONLY public.push_vapid_keys
    ADD CONSTRAINT push_vapid_keys_pkey PRIMARY KEY (singleton);

ALTER TABLE ONLY public.read_markers
    ADD CONSTRAINT read_markers_pkey PRIMARY KEY (place_id, workspace_member_id);

ALTER TABLE ONLY public.reply_later_markers
    ADD CONSTRAINT reply_later_markers_pkey PRIMARY KEY (marker_id);

ALTER TABLE ONLY public.research_consents
    ADD CONSTRAINT research_consents_pkey PRIMARY KEY (consent_id);

ALTER TABLE ONLY public.return_sessions
    ADD CONSTRAINT return_sessions_grant_hash_key UNIQUE (grant_hash);

ALTER TABLE ONLY public.return_sessions
    ADD CONSTRAINT return_sessions_pkey PRIMARY KEY (session_id);

ALTER TABLE ONLY public.transfer_sessions
    ADD CONSTRAINT transfer_sessions_grant_hash_key UNIQUE (grant_hash);

ALTER TABLE ONLY public.transfer_sessions
    ADD CONSTRAINT transfer_sessions_pkey PRIMARY KEY (session_id);

ALTER TABLE ONLY public.usage_budgets
    ADD CONSTRAINT usage_budgets_pkey PRIMARY KEY (funding_kind, funding_id);

ALTER TABLE ONLY public.usage_facts
    ADD CONSTRAINT usage_facts_pkey PRIMARY KEY (persona_id, fact_id);

ALTER TABLE ONLY public.usage_funding_grants
    ADD CONSTRAINT usage_funding_grants_pkey PRIMARY KEY (funding_id);

ALTER TABLE ONLY public.usage_reservations
    ADD CONSTRAINT usage_reservations_pkey PRIMARY KEY (persona_id, fact_id);

ALTER TABLE ONLY public.workspace_invites
    ADD CONSTRAINT workspace_invites_code_hash_key UNIQUE (code_hash);

ALTER TABLE ONLY public.workspace_invites
    ADD CONSTRAINT workspace_invites_pkey PRIMARY KEY (invite_id);

ALTER TABLE ONLY public.workspace_members
    ADD CONSTRAINT workspace_members_pkey PRIMARY KEY (workspace_member_id);

ALTER TABLE ONLY public.workspace_members
    ADD CONSTRAINT workspace_members_workspace_id_workspace_member_id_key UNIQUE (workspace_id, workspace_member_id);

ALTER TABLE ONLY public.workspace_members
    ADD CONSTRAINT workspace_members_workspace_id_workspace_member_id_member_k_key UNIQUE (workspace_id, workspace_member_id, member_kind, member_id);

ALTER TABLE ONLY public.workspace_role_app_capability_grants
    ADD CONSTRAINT workspace_role_app_capability_grants_pkey PRIMARY KEY (role_id, capability_id);

ALTER TABLE ONLY public.workspace_role_app_capability_grants
    ADD CONSTRAINT workspace_role_app_capability_role_id_capability_ref_snapsh_key UNIQUE (role_id, capability_ref_snapshot);

ALTER TABLE ONLY public.workspace_role_assignments
    ADD CONSTRAINT workspace_role_assignments_pkey PRIMARY KEY (role_id, workspace_member_id);

ALTER TABLE ONLY public.workspace_roles
    ADD CONSTRAINT workspace_roles_pkey PRIMARY KEY (role_id);

ALTER TABLE ONLY public.workspace_roles
    ADD CONSTRAINT workspace_roles_workspace_id_name_key UNIQUE (workspace_id, name);

ALTER TABLE ONLY public.workspace_roles
    ADD CONSTRAINT workspace_roles_workspace_id_role_id_key UNIQUE (workspace_id, role_id);

ALTER TABLE ONLY public.workspaces
    ADD CONSTRAINT workspaces_owner_workspace_member_id_key UNIQUE (owner_workspace_member_id);

ALTER TABLE ONLY public.workspaces
    ADD CONSTRAINT workspaces_pkey PRIMARY KEY (workspace_id);

ALTER TABLE ONLY public.workspaces
    ADD CONSTRAINT workspaces_workspace_id_owner_workspace_member_id_key UNIQUE (workspace_id, owner_workspace_member_id);

CREATE INDEX agent_attention_ready ON public.agent_attention_deliveries USING btree (next_attempt_at, available_at, event_id) WHERE ((admitted_at IS NULL) AND (suppressed_at IS NULL));

CREATE INDEX app_installations_by_owner ON public.app_installations USING btree (owner_kind, owner_id, app_id);

CREATE UNIQUE INDEX app_workspace_role_capabilities_active_ref ON public.app_workspace_role_capabilities USING btree (capability_ref) WHERE (retired_at IS NULL);

CREATE UNIQUE INDEX auth_email_challenges_one_live ON public.auth_email_challenges USING btree (flow_id) WHERE ((superseded_at IS NULL) AND (consumed_at IS NULL));

CREATE INDEX auth_email_deliveries_address_window ON public.auth_email_deliveries USING btree (normalized_email, created_at);

CREATE INDEX auth_email_deliveries_challenge ON public.auth_email_deliveries USING btree (challenge_id, created_at);

CREATE INDEX auth_email_deliveries_due ON public.auth_email_deliveries USING btree (next_attempt_at) WHERE (status = ANY (ARRAY['pending'::text, 'sending'::text]));

CREATE INDEX auth_flows_browser_epoch ON public.auth_flows USING btree (browser_epoch_hash) WHERE ((browser_epoch_hash IS NOT NULL) AND (closed_at IS NULL) AND (status <> 'completed'::text));

CREATE INDEX auth_flows_completed_email_code_proof ON public.auth_flows USING btree (human_id, firebase_uid) WHERE ((channel = 'email_code'::text) AND (status = 'completed'::text));

CREATE INDEX auth_flows_completed_email_link_proof ON public.auth_flows USING btree (human_id, firebase_uid) WHERE ((channel = 'email_link'::text) AND (status = 'completed'::text));

CREATE INDEX auth_flows_expiry ON public.auth_flows USING btree (expires_at) WHERE (status <> 'completed'::text);

CREATE UNIQUE INDEX browser_tab_identity ON public.browser_tab_attachments USING btree (human_id, ((tab ->> 'runtimeId'::text)), ((tab ->> 'tabId'::text))) WHERE enabled;

CREATE INDEX browser_tabs_persona ON public.browser_tab_attachments USING btree (persona_id) WHERE enabled;

CREATE INDEX call_sessions_claimable ON public.call_sessions USING btree (personality_agent_id, claim_expires_at) WHERE (status = ANY (ARRAY['requested'::text, 'claimed'::text, 'active'::text, 'ending'::text, 'interrupted'::text]));

CREATE UNIQUE INDEX call_sessions_one_live_per_place ON public.call_sessions USING btree (personality_agent_id, place_id) WHERE (status = ANY (ARRAY['requested'::text, 'claimed'::text, 'active'::text, 'ending'::text, 'interrupted'::text]));

CREATE INDEX call_utterances_pending ON public.call_utterances USING btree (session_id, seq) WHERE (status = 'intended'::text);

CREATE INDEX cloud_browser_profile_human ON public.cloud_browser_profiles USING btree (human_id) WHERE enabled;

CREATE UNIQUE INDEX cloud_browser_profile_persona ON public.cloud_browser_profiles USING btree (persona_id) WHERE enabled;

CREATE INDEX core_inputs_pending ON public.core_inputs USING btree (persona_id, admission_seq) WHERE (status = 'queued'::text);

CREATE INDEX core_job_file_ops_pending ON public.core_job_file_ops USING btree (persona_id, job_id) WHERE (status = ANY (ARRAY['admitted'::text, 'unknown'::text]));

CREATE INDEX core_jobs_claimed ON public.core_jobs USING btree (persona_id, claim_expires_at) WHERE (status = ANY (ARRAY['running'::text, 'cancel_requested'::text]));

CREATE INDEX core_jobs_queued ON public.core_jobs USING btree (persona_id, created_at) WHERE (status = 'queued'::text);

CREATE INDEX core_memory_chunks_cover ON public.core_memory_chunks USING btree (persona_id, last_seq);

CREATE UNIQUE INDEX core_memory_chunks_l1_first_seq ON public.core_memory_chunks USING btree (persona_id, first_seq) WHERE (layer = 1);

CREATE INDEX core_schedules_due ON public.core_schedules USING btree (wake_at) WHERE (status = 'pending'::text);

CREATE INDEX core_tool_approvals_status ON public.core_tool_approvals USING btree (persona_id, status);

CREATE INDEX core_transfers_by_persona ON public.core_transfers USING btree (persona_id, created_at);

CREATE INDEX core_turns_by_input ON public.core_turns USING btree (input_id);

CREATE UNIQUE INDEX core_turns_one_running ON public.core_turns USING btree (persona_id) WHERE (status = 'running'::text);

CREATE UNIQUE INDEX employments_one_active_employer_per_agent ON public.employments USING btree (agent_id) WHERE (ended_at IS NULL);

CREATE INDEX feedback_attachments_expiry ON public.feedback_attachments USING btree (expires_at) WHERE (thread_id IS NULL);

CREATE INDEX feedback_attachments_thread ON public.feedback_attachments USING btree (thread_id, "position");

CREATE INDEX feedback_attention_pending ON public.feedback_attention_outbox USING btree (next_attempt_at, event_id) WHERE (finished_at IS NULL);

CREATE INDEX feedback_threads_author ON public.feedback_threads USING btree (author_key, updated_at DESC, thread_id DESC);

CREATE INDEX feedback_threads_updated ON public.feedback_threads USING btree (updated_at DESC, thread_id DESC);

CREATE INDEX idx_core_terminal_inputs_pending ON public.core_terminal_inputs USING btree (session_id, session_epoch, seq) WHERE (status = ANY (ARRAY['intended'::text, 'dequeued'::text]));

CREATE INDEX idx_core_terminal_output_read ON public.core_terminal_output USING btree (session_id, seq);

CREATE INDEX idx_core_terminal_sessions_claim_expiry ON public.core_terminal_sessions USING btree (claim_expires_at) WHERE ((status = ANY (ARRAY['claimed'::text, 'active'::text, 'ending'::text])) AND (claimed_by IS NOT NULL));

CREATE INDEX idx_core_terminal_sessions_claimable ON public.core_terminal_sessions USING btree (status, backend) WHERE (status = ANY (ARRAY['requested'::text, 'interrupted'::text]));

CREATE INDEX idx_core_terminal_sessions_persona ON public.core_terminal_sessions USING btree (persona_id, created_at);

CREATE INDEX message_attachment_uploads_reserved ON public.message_attachment_uploads USING btree (expires_at, upload_id) WHERE (state = 'reserved'::text);

CREATE INDEX message_attachment_uploads_settled ON public.message_attachment_uploads USING btree (settled_at, upload_id) WHERE (state <> 'reserved'::text);

CREATE INDEX message_attachments_deleting ON public.message_attachments USING btree (created_at, attachment_id) WHERE (blob_state = 'deleting'::text);

CREATE UNIQUE INDEX message_attachments_message_position ON public.message_attachments USING btree (message_id, "position") WHERE (message_id IS NOT NULL);

CREATE INDEX message_attachments_unbound_drafts ON public.message_attachments USING btree (workspace_id, place_id, uploader_kind, uploader_id, created_at) WHERE ((message_id IS NULL) AND (blob_state = 'stored'::text));

CREATE INDEX message_mentions_by_participant ON public.message_mentions USING btree (member_kind, member_id);

CREATE INDEX message_notification_intents_by_recipient ON public.message_notification_intents USING btree (recipient_kind, recipient_id, issued_at);

CREATE INDEX messages_content_trgm ON public.messages USING gin (content public.gin_trgm_ops) WHERE (deleted_at IS NULL);

CREATE UNIQUE INDEX messages_idempotent_send ON public.messages USING btree (place_id, author_kind, author_id, client_nonce);

CREATE INDEX notification_setting_places_by_place ON public.notification_setting_places USING btree (workspace_id, place_id);

CREATE INDEX participant_statuses_expiring ON public.participant_statuses USING btree (expires_at) WHERE (expires_at IS NOT NULL);

CREATE INDEX persona_file_tokens_active ON public.persona_file_tokens USING btree (persona_id, destination_placement_id) WHERE (status = 'active'::text);

CREATE INDEX place_members_by_participant ON public.place_members USING btree (member_kind, member_id, workspace_id, place_id) WHERE (left_at IS NULL);

CREATE UNIQUE INDEX place_members_one_active_per_participant ON public.place_members USING btree (place_id, member_kind, member_id) WHERE (left_at IS NULL);

CREATE INDEX places_by_parent ON public.places USING btree (workspace_id, parent_place_id, created_at, place_id) WHERE (parent_place_id IS NOT NULL);

CREATE INDEX places_by_workspace ON public.places USING btree (workspace_id, created_at, place_id);

CREATE UNIQUE INDEX places_one_thread_per_origin ON public.places USING btree (parent_message_id) WHERE (parent_message_id IS NOT NULL);

CREATE UNIQUE INDEX provider_operations_one_pending_unlink_per_firebase_uid ON public.provider_operations USING btree (firebase_uid) WHERE ((operation = 'unlink'::text) AND (status = 'pending'::text));

CREATE INDEX push_devices_by_human ON public.push_devices USING btree (human_id);

CREATE INDEX push_subscriptions_by_device ON public.push_subscriptions USING btree (device_id);

CREATE INDEX push_subscriptions_by_human ON public.push_subscriptions USING btree (human_id, updated_at DESC);

CREATE INDEX reply_later_markers_active_by_place ON public.reply_later_markers USING btree (place_id) WHERE (resolved_at IS NULL);

CREATE UNIQUE INDEX reply_later_one_active_per_message ON public.reply_later_markers USING btree (message_id, member_kind, member_id) WHERE (resolved_at IS NULL);

CREATE UNIQUE INDEX research_consents_one_active_per_human ON public.research_consents USING btree (human_id) WHERE (revoked_at IS NULL);

CREATE UNIQUE INDEX return_sessions_open_persona ON public.return_sessions USING btree (persona_id) WHERE (status = ANY (ARRAY['awaiting_destination'::text, 'sealed'::text, 'cancelling'::text]));

CREATE UNIQUE INDEX transfer_sessions_open_subject ON public.transfer_sessions USING btree (claim_provider, claim_subject) WHERE (status = ANY (ARRAY['awaiting_bundle'::text, 'staged'::text, 'provisioned'::text]));

CREATE INDEX usage_facts_funding ON public.usage_facts USING btree (funding_kind, funding_id, recorded_at);

CREATE INDEX usage_reservations_held ON public.usage_reservations USING btree (funding_kind, funding_id) WHERE (status = 'held'::text);

CREATE INDEX workspace_invites_by_workspace ON public.workspace_invites USING btree (workspace_id, created_at DESC);

CREATE UNIQUE INDEX workspace_invites_one_pending_targeted_pa ON public.workspace_invites USING btree (workspace_id, target_id) WHERE ((invite_kind = 'targeted_personality_agent'::text) AND (revoked_at IS NULL) AND (redeemed_at IS NULL));

CREATE INDEX workspace_invites_pending_targeted_pa_by_invite ON public.workspace_invites USING btree (target_id, invite_id) WHERE ((invite_kind = 'targeted_personality_agent'::text) AND (revoked_at IS NULL) AND (redeemed_at IS NULL));

CREATE UNIQUE INDEX workspace_members_one_active_tenure ON public.workspace_members USING btree (workspace_id, member_kind, member_id) WHERE (left_at IS NULL);

CREATE INDEX workspace_memberships_by_participant ON public.workspace_members USING btree (member_kind, member_id, workspace_id) WHERE (left_at IS NULL);

CREATE INDEX workspace_role_app_capability_grants_by_role ON public.workspace_role_app_capability_grants USING btree (workspace_id, role_id);

CREATE INDEX workspace_role_assignments_by_member ON public.workspace_role_assignments USING btree (workspace_member_id, role_id);

CREATE TRIGGER agents_identity_immutable BEFORE UPDATE OF personality_agent_id, created_at ON public.agents FOR EACH ROW EXECUTE FUNCTION public.prevent_personality_agent_identity_change();

CREATE TRIGGER app_installation_address_immutable BEFORE UPDATE OF owner_kind, owner_id, app_id ON public.app_installations FOR EACH ROW EXECUTE FUNCTION public.prevent_app_installation_address_mutation();

CREATE TRIGGER app_installation_owner_exists BEFORE INSERT OR UPDATE OF owner_kind, owner_id ON public.app_installations FOR EACH ROW EXECUTE FUNCTION public.validate_app_installation_owner();

CREATE TRIGGER app_workspace_role_capability_identity_immutable BEFORE UPDATE OF capability_id, app_id, capability_ref, retired_at ON public.app_workspace_role_capabilities FOR EACH ROW EXECUTE FUNCTION public.prevent_app_workspace_role_capability_identity_mutation();

CREATE TRIGGER credential_history_immutable BEFORE UPDATE ON public.credentials FOR EACH ROW EXECUTE FUNCTION public.protect_credential_history();

CREATE TRIGGER credential_no_delete BEFORE DELETE ON public.credentials FOR EACH ROW EXECUTE FUNCTION public.prevent_credential_delete();

CREATE TRIGGER credential_no_rebind BEFORE UPDATE OF human_id ON public.credentials FOR EACH ROW EXECUTE FUNCTION public.prevent_credential_rebinding();

CREATE TRIGGER credential_security_events_append_only BEFORE DELETE OR UPDATE ON public.credential_security_events FOR EACH ROW EXECUTE FUNCTION public.prevent_security_event_mutation();

CREATE TRIGGER humans_identity_immutable BEFORE UPDATE OF human_id, created_at ON public.humans FOR EACH ROW EXECUTE FUNCTION public.prevent_human_identity_change();

CREATE CONSTRAINT TRIGGER message_empty_content_requires_attachment AFTER INSERT OR UPDATE OF content, deleted_at ON public.messages DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.require_attachment_for_empty_message();

CREATE TRIGGER participant_statuses_increment_revision BEFORE UPDATE ON public.participant_statuses FOR EACH ROW EXECUTE FUNCTION public.messaging_increment_participant_status_revision();

CREATE TRIGGER place_member_requires_active_workspace_tenure BEFORE INSERT OR UPDATE OF workspace_id, workspace_member_id, member_kind, member_id, left_at ON public.place_members FOR EACH ROW EXECUTE FUNCTION public.require_active_workspace_tenure_for_place_member();

CREATE TRIGGER places_increment_revision BEFORE UPDATE ON public.places FOR EACH ROW EXECUTE FUNCTION public.messaging_increment_place_revision();

CREATE TRIGGER provider_operations_pending_unlink_uid_fence AFTER INSERT OR UPDATE ON public.provider_operations FOR EACH ROW WHEN ((new.status = 'pending'::text)) EXECUTE FUNCTION public.enforce_provider_unlink_uid_fence();

CREATE TRIGGER workspace_member_participant_exists BEFORE INSERT OR UPDATE OF member_kind, member_id ON public.workspace_members FOR EACH ROW EXECUTE FUNCTION public.validate_workspace_member_participant();

CREATE TRIGGER workspace_owner_membership_immutable BEFORE DELETE OR UPDATE ON public.workspace_members FOR EACH ROW EXECUTE FUNCTION public.prevent_workspace_owner_membership_mutation();

CREATE TRIGGER workspace_owner_valid BEFORE UPDATE OF owner_workspace_member_id ON public.workspaces FOR EACH ROW EXECUTE FUNCTION public.validate_workspace_owner_change();

CREATE TRIGGER workspace_tenure_close_requires_closed_places BEFORE UPDATE OF left_at ON public.workspace_members FOR EACH ROW EXECUTE FUNCTION public.reject_workspace_tenure_close_with_active_places();

ALTER TABLE ONLY public.agents
    ADD CONSTRAINT agents_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.app_installations
    ADD CONSTRAINT app_installations_app_id_fkey FOREIGN KEY (app_id) REFERENCES public.app_catalog(app_id);

ALTER TABLE ONLY public.app_workspace_role_capabilities
    ADD CONSTRAINT app_workspace_role_capabilities_app_id_fkey FOREIGN KEY (app_id) REFERENCES public.app_catalog(app_id);

ALTER TABLE ONLY public.auth_email_challenges
    ADD CONSTRAINT auth_email_challenges_flow_id_fkey FOREIGN KEY (flow_id) REFERENCES public.auth_flows(flow_id);

ALTER TABLE ONLY public.auth_email_deliveries
    ADD CONSTRAINT auth_email_deliveries_challenge_id_fkey FOREIGN KEY (challenge_id) REFERENCES public.auth_email_challenges(challenge_id);

ALTER TABLE ONLY public.auth_flows
    ADD CONSTRAINT auth_flows_enrollment_invite_id_fkey FOREIGN KEY (enrollment_invite_id) REFERENCES public.enrollment_invites(invite_id);

ALTER TABLE ONLY public.auth_flows
    ADD CONSTRAINT auth_flows_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.auth_flows
    ADD CONSTRAINT auth_flows_personality_agent_id_fkey FOREIGN KEY (personality_agent_id) REFERENCES public.agents(personality_agent_id);

ALTER TABLE ONLY public.browser_tab_attachments
    ADD CONSTRAINT browser_tab_attachments_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.browser_tab_attachments
    ADD CONSTRAINT browser_tab_attachments_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id);

ALTER TABLE ONLY public.call_sessions
    ADD CONSTRAINT call_sessions_personality_agent_id_fkey FOREIGN KEY (personality_agent_id) REFERENCES public.agents(personality_agent_id);

ALTER TABLE ONLY public.call_sessions
    ADD CONSTRAINT call_sessions_workspace_id_place_id_fkey FOREIGN KEY (workspace_id, place_id) REFERENCES public.places(workspace_id, place_id);

ALTER TABLE ONLY public.call_utterances
    ADD CONSTRAINT call_utterances_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.call_sessions(session_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.chatgpt_connections
    ADD CONSTRAINT chatgpt_connections_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.cloud_browser_jev_credentials
    ADD CONSTRAINT cloud_browser_jev_credentials_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.cloud_browser_profiles
    ADD CONSTRAINT cloud_browser_profiles_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.cloud_browser_profiles
    ADD CONSTRAINT cloud_browser_profiles_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id);

ALTER TABLE ONLY public.core_budget_waits
    ADD CONSTRAINT core_budget_waits_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.core_events
    ADD CONSTRAINT core_events_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.core_inputs
    ADD CONSTRAINT core_inputs_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.core_job_file_ops
    ADD CONSTRAINT core_job_file_ops_persona_id_job_id_fkey FOREIGN KEY (persona_id, job_id) REFERENCES public.core_jobs(persona_id, job_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.core_jobs
    ADD CONSTRAINT core_jobs_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.core_memory_chunks
    ADD CONSTRAINT core_memory_chunks_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.core_operations
    ADD CONSTRAINT core_operations_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.core_outbox
    ADD CONSTRAINT core_outbox_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.core_personas
    ADD CONSTRAINT core_personas_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.core_schedules
    ADD CONSTRAINT core_schedules_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.core_terminal_inputs
    ADD CONSTRAINT core_terminal_inputs_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.core_terminal_sessions(session_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.core_terminal_output
    ADD CONSTRAINT core_terminal_output_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.core_terminal_sessions(session_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.core_terminal_sessions
    ADD CONSTRAINT core_terminal_sessions_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.core_tool_approvals
    ADD CONSTRAINT core_tool_approvals_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.core_turn_plans
    ADD CONSTRAINT core_turn_plans_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.core_turn_plans
    ADD CONSTRAINT core_turn_plans_persona_id_input_id_fkey FOREIGN KEY (persona_id, input_id) REFERENCES public.core_inputs(persona_id, input_id);

ALTER TABLE ONLY public.core_turns
    ADD CONSTRAINT core_turns_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.core_turns
    ADD CONSTRAINT core_turns_persona_id_input_id_fkey FOREIGN KEY (persona_id, input_id) REFERENCES public.core_inputs(persona_id, input_id);

ALTER TABLE ONLY public.core_writer_leases
    ADD CONSTRAINT core_writer_leases_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.credential_security_events
    ADD CONSTRAINT credential_security_events_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.credential_security_events
    ADD CONSTRAINT credential_security_events_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES public.provider_operations(operation_id);

ALTER TABLE ONLY public.credentials
    ADD CONSTRAINT credentials_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.employments
    ADD CONSTRAINT employments_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(personality_agent_id);

ALTER TABLE ONLY public.enrollment_invites
    ADD CONSTRAINT enrollment_invites_consumed_by_fkey FOREIGN KEY (consumed_by) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.enrollment_invites
    ADD CONSTRAINT enrollment_invites_issued_by_fkey FOREIGN KEY (issued_by) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.enrollment_invites
    ADD CONSTRAINT enrollment_invites_workspace_invite_id_fkey FOREIGN KEY (workspace_invite_id) REFERENCES public.workspace_invites(invite_id);

ALTER TABLE ONLY public.feedback_attachments
    ADD CONSTRAINT feedback_attachments_thread_id_fkey FOREIGN KEY (thread_id) REFERENCES public.feedback_threads(thread_id);

ALTER TABLE ONLY public.feedback_attention_outbox
    ADD CONSTRAINT feedback_attention_outbox_recipient_paid_fkey FOREIGN KEY (recipient_paid) REFERENCES public.agents(personality_agent_id);

ALTER TABLE ONLY public.feedback_attention_outbox
    ADD CONSTRAINT feedback_attention_outbox_thread_id_fkey FOREIGN KEY (thread_id) REFERENCES public.feedback_threads(thread_id);

ALTER TABLE ONLY public.feedback_events
    ADD CONSTRAINT feedback_events_thread_id_fkey FOREIGN KEY (thread_id) REFERENCES public.feedback_threads(thread_id);

ALTER TABLE ONLY public.feedback_reads
    ADD CONSTRAINT feedback_reads_thread_id_fkey FOREIGN KEY (thread_id) REFERENCES public.feedback_threads(thread_id);

ALTER TABLE ONLY public.local_mcp_connections
    ADD CONSTRAINT local_mcp_connections_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id);

ALTER TABLE ONLY public.mcp_connections
    ADD CONSTRAINT mcp_connections_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.message_attachment_quotas
    ADD CONSTRAINT message_attachment_quotas_workspace_id_fkey FOREIGN KEY (workspace_id) REFERENCES public.workspaces(workspace_id);

ALTER TABLE ONLY public.message_attachment_uploads
    ADD CONSTRAINT message_attachment_uploads_attachment_id_fkey FOREIGN KEY (attachment_id) REFERENCES public.message_attachments(attachment_id);

ALTER TABLE ONLY public.message_attachment_uploads
    ADD CONSTRAINT message_attachment_uploads_workspace_id_place_id_fkey FOREIGN KEY (workspace_id, place_id) REFERENCES public.places(workspace_id, place_id);

ALTER TABLE ONLY public.message_attachments
    ADD CONSTRAINT message_attachments_workspace_id_place_id_fkey FOREIGN KEY (workspace_id, place_id) REFERENCES public.places(workspace_id, place_id);

ALTER TABLE ONLY public.message_attachments
    ADD CONSTRAINT message_attachments_workspace_id_place_id_message_id_fkey FOREIGN KEY (workspace_id, place_id, message_id) REFERENCES public.messages(workspace_id, place_id, message_id);

ALTER TABLE ONLY public.message_mentions
    ADD CONSTRAINT message_mentions_message_id_fkey FOREIGN KEY (message_id) REFERENCES public.messages(message_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.message_notification_intents
    ADD CONSTRAINT message_notification_intents_message_id_fkey FOREIGN KEY (message_id) REFERENCES public.messages(message_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.message_notification_intents
    ADD CONSTRAINT message_notification_intents_recipient_place_member_id_fkey FOREIGN KEY (recipient_place_member_id) REFERENCES public.place_members(place_member_id);

ALTER TABLE ONLY public.message_notification_intents
    ADD CONSTRAINT message_notification_intents_recipient_workspace_member_id_fkey FOREIGN KEY (recipient_workspace_member_id) REFERENCES public.workspace_members(workspace_member_id);

ALTER TABLE ONLY public.message_poll_options
    ADD CONSTRAINT message_poll_options_message_id_fkey FOREIGN KEY (message_id) REFERENCES public.message_polls(message_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.message_poll_votes
    ADD CONSTRAINT message_poll_votes_option_id_fkey FOREIGN KEY (option_id) REFERENCES public.message_poll_options(option_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.message_polls
    ADD CONSTRAINT message_polls_message_id_fkey FOREIGN KEY (message_id) REFERENCES public.messages(message_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.message_reaction_mutations
    ADD CONSTRAINT message_reaction_mutations_workspace_id_message_id_fkey FOREIGN KEY (workspace_id, message_id) REFERENCES public.messages(workspace_id, message_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.message_reactions
    ADD CONSTRAINT message_reactions_message_id_fkey FOREIGN KEY (message_id) REFERENCES public.messages(message_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_workspace_id_place_id_fkey FOREIGN KEY (workspace_id, place_id) REFERENCES public.places(workspace_id, place_id);

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_workspace_id_place_id_reply_to_fkey FOREIGN KEY (workspace_id, place_id, reply_to) REFERENCES public.messages(workspace_id, place_id, message_id);

ALTER TABLE ONLY public.messaging_place_creation_receipts
    ADD CONSTRAINT messaging_place_creation_receipts_workspace_id_place_id_fkey FOREIGN KEY (workspace_id, place_id) REFERENCES public.places(workspace_id, place_id);

ALTER TABLE ONLY public.messaging_place_creation_receipts
    ADD CONSTRAINT messaging_place_creation_receipts_workspace_member_identity FOREIGN KEY (workspace_id, workspace_member_id, member_kind, member_id) REFERENCES public.workspace_members(workspace_id, workspace_member_id, member_kind, member_id);

ALTER TABLE ONLY public.model_api_connections
    ADD CONSTRAINT model_api_connections_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.model_connection_selections
    ADD CONSTRAINT model_connection_selections_human_id_connection_id_fkey FOREIGN KEY (human_id, connection_id) REFERENCES public.model_api_connections(human_id, connection_id);

ALTER TABLE ONLY public.model_connection_selections
    ADD CONSTRAINT model_connection_selections_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.notification_setting_places
    ADD CONSTRAINT notification_setting_places_workspace_id_member_kind_membe_fkey FOREIGN KEY (workspace_id, member_kind, member_id) REFERENCES public.notification_settings(workspace_id, member_kind, member_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.notification_setting_places
    ADD CONSTRAINT notification_setting_places_workspace_id_place_id_fkey FOREIGN KEY (workspace_id, place_id) REFERENCES public.places(workspace_id, place_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.notification_settings
    ADD CONSTRAINT notification_settings_workspace_id_fkey FOREIGN KEY (workspace_id) REFERENCES public.workspaces(workspace_id);

ALTER TABLE ONLY public.persona_file_tokens
    ADD CONSTRAINT persona_file_tokens_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.return_sessions(session_id);

ALTER TABLE ONLY public.place_members
    ADD CONSTRAINT place_members_workspace_id_place_id_fkey FOREIGN KEY (workspace_id, place_id) REFERENCES public.places(workspace_id, place_id);

ALTER TABLE ONLY public.place_members
    ADD CONSTRAINT place_members_workspace_id_workspace_member_id_member_kind_fkey FOREIGN KEY (workspace_id, workspace_member_id, member_kind, member_id) REFERENCES public.workspace_members(workspace_id, workspace_member_id, member_kind, member_id);

ALTER TABLE ONLY public.places
    ADD CONSTRAINT places_parent_message_fk FOREIGN KEY (workspace_id, parent_place_id, parent_message_id) REFERENCES public.messages(workspace_id, place_id, message_id);

ALTER TABLE ONLY public.places
    ADD CONSTRAINT places_parent_place_fk FOREIGN KEY (workspace_id, parent_place_id) REFERENCES public.places(workspace_id, place_id);

ALTER TABLE ONLY public.places
    ADD CONSTRAINT places_workspace_id_fkey FOREIGN KEY (workspace_id) REFERENCES public.workspaces(workspace_id);

ALTER TABLE ONLY public.provider_operations
    ADD CONSTRAINT provider_operations_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.push_devices
    ADD CONSTRAINT push_devices_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.push_subscriptions
    ADD CONSTRAINT push_subscriptions_device_id_fkey FOREIGN KEY (device_id) REFERENCES public.push_devices(device_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.push_subscriptions
    ADD CONSTRAINT push_subscriptions_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.read_markers
    ADD CONSTRAINT read_markers_place_id_fkey FOREIGN KEY (place_id) REFERENCES public.places(place_id);

ALTER TABLE ONLY public.read_markers
    ADD CONSTRAINT read_markers_workspace_member_id_fkey FOREIGN KEY (workspace_member_id) REFERENCES public.workspace_members(workspace_member_id);

ALTER TABLE ONLY public.reply_later_markers
    ADD CONSTRAINT reply_later_markers_message_id_fkey FOREIGN KEY (message_id) REFERENCES public.messages(message_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.reply_later_markers
    ADD CONSTRAINT reply_later_markers_place_id_fkey FOREIGN KEY (place_id) REFERENCES public.places(place_id);

ALTER TABLE ONLY public.research_consents
    ADD CONSTRAINT research_consents_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.return_sessions
    ADD CONSTRAINT return_sessions_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.transfer_sessions
    ADD CONSTRAINT transfer_sessions_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.usage_facts
    ADD CONSTRAINT usage_facts_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.usage_funding_grants
    ADD CONSTRAINT usage_funding_grants_human_id_fkey FOREIGN KEY (human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.usage_reservations
    ADD CONSTRAINT usage_reservations_persona_id_fkey FOREIGN KEY (persona_id) REFERENCES public.core_personas(persona_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.workspace_invites
    ADD CONSTRAINT workspace_invites_reserved_human_id_fkey FOREIGN KEY (reserved_human_id) REFERENCES public.humans(human_id);

ALTER TABLE ONLY public.workspace_invites
    ADD CONSTRAINT workspace_invites_target_id_fkey FOREIGN KEY (target_id) REFERENCES public.agents(personality_agent_id);

ALTER TABLE ONLY public.workspace_invites
    ADD CONSTRAINT workspace_invites_workspace_id_created_by_workspace_member_fkey FOREIGN KEY (workspace_id, created_by_workspace_member_id) REFERENCES public.workspace_members(workspace_id, workspace_member_id);

ALTER TABLE ONLY public.workspace_invites
    ADD CONSTRAINT workspace_invites_workspace_id_fkey FOREIGN KEY (workspace_id) REFERENCES public.workspaces(workspace_id);

ALTER TABLE ONLY public.workspace_invites
    ADD CONSTRAINT workspace_invites_workspace_id_redeemed_workspace_member_i_fkey FOREIGN KEY (workspace_id, redeemed_workspace_member_id, redeemed_by_kind, redeemed_by_id) REFERENCES public.workspace_members(workspace_id, workspace_member_id, member_kind, member_id);

ALTER TABLE ONLY public.workspace_members
    ADD CONSTRAINT workspace_members_workspace_id_fkey FOREIGN KEY (workspace_id) REFERENCES public.workspaces(workspace_id);

ALTER TABLE ONLY public.workspaces
    ADD CONSTRAINT workspace_owner_is_own_membership FOREIGN KEY (workspace_id, owner_workspace_member_id) REFERENCES public.workspace_members(workspace_id, workspace_member_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY public.workspace_role_app_capability_grants
    ADD CONSTRAINT workspace_role_app_capability_capability_id_capability_ref_fkey FOREIGN KEY (capability_id, capability_ref_snapshot) REFERENCES public.app_workspace_role_capabilities(capability_id, capability_ref);

ALTER TABLE ONLY public.workspace_role_app_capability_grants
    ADD CONSTRAINT workspace_role_app_capability_grants_workspace_id_role_id_fkey FOREIGN KEY (workspace_id, role_id) REFERENCES public.workspace_roles(workspace_id, role_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.workspace_role_assignments
    ADD CONSTRAINT workspace_role_assignments_workspace_id_role_id_fkey FOREIGN KEY (workspace_id, role_id) REFERENCES public.workspace_roles(workspace_id, role_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.workspace_role_assignments
    ADD CONSTRAINT workspace_role_assignments_workspace_id_workspace_member_i_fkey FOREIGN KEY (workspace_id, workspace_member_id) REFERENCES public.workspace_members(workspace_id, workspace_member_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.workspace_roles
    ADD CONSTRAINT workspace_roles_workspace_id_fkey FOREIGN KEY (workspace_id) REFERENCES public.workspaces(workspace_id);

-- Built-in application catalog.
INSERT INTO public.app_catalog (app_id, display_name, workspace_owner_allowed, participant_owner_allowed, created_at) VALUES ('messaging', 'Messaging', true, false, now());
INSERT INTO public.app_catalog (app_id, display_name, workspace_owner_allowed, participant_owner_allowed, created_at) VALUES ('alarm', 'Alarm', false, true, now());
INSERT INTO public.app_catalog (app_id, display_name, workspace_owner_allowed, participant_owner_allowed, created_at) VALUES ('direct-chat', 'Direct Chat', false, true, now());
INSERT INTO public.app_catalog (app_id, display_name, workspace_owner_allowed, participant_owner_allowed, created_at) VALUES ('life-log', 'Life Log', false, true, now());
INSERT INTO public.app_catalog (app_id, display_name, workspace_owner_allowed, participant_owner_allowed, created_at) VALUES ('terminal', 'Terminal', false, true, now());
INSERT INTO public.app_workspace_role_capabilities (capability_id, app_id, capability_ref, label, created_at, retired_at) VALUES ('0198f0f4-9b72-7000-8000-0000000008c1', 'messaging', 'app.messaging.manage_channels', 'Manage channels', now(), NULL);

CREATE UNIQUE INDEX enrollment_invites_one_open_bootstrap ON public.enrollment_invites ((true)) WHERE issued_by IS NULL AND consumed_at IS NULL AND revoked_at IS NULL;
