DROP TRIGGER IF EXISTS agents_identity_immutable ON agents;
DROP FUNCTION IF EXISTS prevent_personality_agent_identity_change();
DROP TRIGGER IF EXISTS humans_identity_immutable ON humans;
DROP FUNCTION IF EXISTS prevent_human_identity_change();
