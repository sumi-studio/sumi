-- Compose-only identity seed. Production identity provisioning owns this table.
CREATE TABLE IF NOT EXISTS users (
  user_id UUID PRIMARY KEY
);

INSERT INTO users (user_id)
VALUES ('019c0000-0000-7000-8000-000000000001')
ON CONFLICT (user_id) DO NOTHING;
