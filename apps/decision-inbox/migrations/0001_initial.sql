PRAGMA foreign_keys = ON;

CREATE TABLE decision_requests (
  id TEXT PRIMARY KEY,
  publisher_fingerprint TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  source_label TEXT NOT NULL,
  choices_json TEXT NOT NULL,
  allow_free_text INTEGER NOT NULL CHECK (allow_free_text IN (0, 1)),
  callback_url TEXT,
  correlation_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('pending', 'resolved', 'cancelled', 'expired')),
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  resolved_at INTEGER,
  cancelled_at INTEGER,
  resolution_key TEXT,
  callback_attempted_at INTEGER,
  callback_status INTEGER,
  UNIQUE (publisher_fingerprint, idempotency_key)
);

CREATE INDEX decision_requests_status_created
  ON decision_requests (status, created_at DESC);
CREATE INDEX decision_requests_publisher_status
  ON decision_requests (publisher_fingerprint, status, created_at DESC);

CREATE TABLE decision_responses (
  id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL UNIQUE REFERENCES decision_requests(id) ON DELETE CASCADE,
  choice_id TEXT,
  reply TEXT,
  idempotency_key_hash TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE human_sessions (
  session_hash TEXT PRIMARY KEY,
  created_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  last_seen_at INTEGER NOT NULL
);

CREATE TABLE bootstrap_tokens (
  token_hash TEXT PRIMARY KEY,
  created_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  consumed_at INTEGER,
  source TEXT NOT NULL
);

CREATE TABLE push_subscriptions (
  endpoint_hash TEXT PRIMARY KEY,
  endpoint TEXT NOT NULL,
  expiration_time INTEGER,
  p256dh TEXT NOT NULL,
  auth TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  last_seen_at INTEGER NOT NULL
);

CREATE TABLE rate_limits (
  key TEXT NOT NULL,
  bucket INTEGER NOT NULL,
  count INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (key, bucket)
);
