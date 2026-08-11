DROP TRIGGER IF EXISTS workspace_tenure_close_requires_closed_places ON workspace_members;
DROP FUNCTION IF EXISTS reject_workspace_tenure_close_with_active_places();
DROP TRIGGER IF EXISTS place_member_requires_active_workspace_tenure ON place_members;
DROP FUNCTION IF EXISTS require_active_workspace_tenure_for_place_member();
DROP TABLE IF EXISTS read_markers;
DROP TABLE IF EXISTS message_mentions;
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS place_members;
DROP TABLE IF EXISTS places;
