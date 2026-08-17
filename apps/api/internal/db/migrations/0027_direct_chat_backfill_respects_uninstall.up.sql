-- 0027_direct_chat_backfill_respects_uninstall: 0022 backfilled a default
-- Direct Chat installation for every Human without one. Because uninstall
-- removes the binding while keeping the durable install receipt, that also
-- recreated Direct Chat for Humans who had explicitly uninstalled it. Remove
-- only those unchanged, deterministic 0022 bindings when a successful Direct
-- Chat install receipt exists and the latest such receipt predates the backfill.
DELETE FROM app_installations ai
USING humans h
WHERE ai.owner_kind = 'human'
  AND ai.owner_id = h.human_id
  AND ai.app_id = 'direct-chat'
  AND ai.authority_epoch = 1
  AND ai.installed_at = ai.updated_at
  AND ai.installation_id =
      substr(h.human_id, 1, 14) || '7' ||
      substr(md5('sumi:direct-chat-backfill:v1:' || h.human_id), 1, 3) ||
      '-8' ||
      substr(md5('sumi:direct-chat-backfill:v1:' || h.human_id), 4, 3) ||
      '-' ||
      substr(md5('sumi:direct-chat-backfill:v1:' || h.human_id), 7, 12)
  AND (
      SELECT max(r.completed_at)
      FROM app_install_operation_receipts r
      WHERE r.owner_kind = 'human'
        AND r.owner_id = h.human_id
        AND r.app_id = 'direct-chat'
        AND r.status IN ('installed', 'already_installed')
        AND r.completed_at IS NOT NULL
  ) < ai.installed_at;
