-- 0002_koseki_schema down: drop the 戸籍 tables in reverse dependency order.
DROP INDEX IF EXISTS research_consents_one_active_per_human;
DROP TABLE IF EXISTS research_consents;
DROP INDEX IF EXISTS employments_one_active_employer_per_agent;
DROP TABLE IF EXISTS employments;
DROP TABLE IF EXISTS agents;
DROP TRIGGER IF EXISTS credential_no_rebind ON credentials;
DROP FUNCTION IF EXISTS prevent_credential_rebinding();
DROP TABLE IF EXISTS credentials;
DROP TABLE IF EXISTS humans;
DROP DOMAIN IF EXISTS uuidv7;
