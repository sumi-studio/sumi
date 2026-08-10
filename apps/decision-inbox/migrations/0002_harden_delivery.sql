ALTER TABLE decision_requests ADD COLUMN callback_delivery_id TEXT;
ALTER TABLE decision_requests ADD COLUMN callback_delivery_created_at INTEGER;

CREATE UNIQUE INDEX decision_requests_callback_delivery_id
  ON decision_requests (callback_delivery_id)
  WHERE callback_delivery_id IS NOT NULL;
