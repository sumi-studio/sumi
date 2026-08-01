-- 0005_todos: personal Todos owned by a canonical Sumi Human.
CREATE TABLE todos (
  id            uuidv7 PRIMARY KEY,
  owner_user_id uuidv7 NOT NULL REFERENCES humans(human_id),
  title         text NOT NULL,
  description   text NOT NULL DEFAULT '',
  status        text NOT NULL DEFAULT 'open',
  priority      text NOT NULL DEFAULT 'none',

  due_kind      text,
  due_on        date,
  due_at        timestamptz,
  due_timezone  text,

  version       integer NOT NULL DEFAULT 1,
  via_agent     boolean NOT NULL DEFAULT false,
  completed_at  timestamptz,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),

  CHECK (char_length(title) BETWEEN 1 AND 200),
  CHECK (status IN ('open','done')),
  CHECK (priority IN ('none','low','medium','high')),
  CHECK (version >= 1),
  CHECK (
    (due_kind IS NULL AND due_on IS NULL AND due_at IS NULL AND due_timezone IS NULL) OR
    (due_kind = 'date' AND due_on IS NOT NULL AND due_at IS NULL AND due_timezone IS NOT NULL) OR
    (due_kind = 'datetime' AND due_on IS NULL AND due_at IS NOT NULL AND due_timezone IS NOT NULL)
  ),
  CHECK (
    (status = 'done' AND completed_at IS NOT NULL) OR
    (status = 'open' AND completed_at IS NULL)
  )
);

CREATE INDEX todos_owner_updated_idx
  ON todos (owner_user_id, updated_at DESC, id DESC);
