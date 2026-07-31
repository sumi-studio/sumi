CREATE TABLE todos (
  id            UUID PRIMARY KEY,
  owner_user_id UUID NOT NULL REFERENCES users(user_id),
  title         TEXT NOT NULL,
  description   TEXT NOT NULL DEFAULT '',
  status        TEXT NOT NULL DEFAULT 'open',
  priority      TEXT NOT NULL DEFAULT 'none',

  due_kind      TEXT,
  due_on        DATE,
  due_at        TIMESTAMPTZ,
  due_timezone  TEXT,

  version       INTEGER NOT NULL DEFAULT 1,
  via_agent     BOOLEAN NOT NULL DEFAULT false,
  completed_at  TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (char_length(title) BETWEEN 1 AND 200),
  CHECK (status IN ('open','done')),
  CHECK (priority IN ('none','low','medium','high')),
  CHECK (version >= 1),
  CHECK (
    (due_kind IS NULL AND due_on IS NULL AND due_at IS NULL AND due_timezone IS NULL) OR
    (due_kind = 'date' AND due_on IS NOT NULL AND due_at IS NULL AND due_timezone IS NOT NULL) OR
    (due_kind = 'datetime' AND due_on IS NULL AND due_at IS NOT NULL AND due_timezone IS NOT NULL)
  )
);

CREATE INDEX todos_owner_updated_idx ON todos (owner_user_id, updated_at DESC);
