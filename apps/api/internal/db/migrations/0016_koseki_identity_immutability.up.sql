-- 0016_koseki_identity_immutability: keep minted identity and history anchors
-- immutable at the database boundary. Human-facing names, initialization
-- state, warmth, and the current agents.human_id relation remain mutable.

CREATE OR REPLACE FUNCTION prevent_human_identity_change() RETURNS trigger
LANGUAGE plpgsql AS $$
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

CREATE TRIGGER humans_identity_immutable
    BEFORE UPDATE OF human_id, created_at ON humans
    FOR EACH ROW
    EXECUTE FUNCTION prevent_human_identity_change();

CREATE OR REPLACE FUNCTION prevent_personality_agent_identity_change() RETURNS trigger
LANGUAGE plpgsql AS $$
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

CREATE TRIGGER agents_identity_immutable
    BEFORE UPDATE OF personality_agent_id, created_at ON agents
    FOR EACH ROW
    EXECUTE FUNCTION prevent_personality_agent_identity_change();
