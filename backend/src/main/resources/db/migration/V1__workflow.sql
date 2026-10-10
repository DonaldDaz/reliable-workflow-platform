CREATE TABLE practices (
  id UUID PRIMARY KEY,
  title VARCHAR(160) NOT NULL,
  status VARCHAR(24) NOT NULL,
  version INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE documents (
  id UUID PRIMARY KEY,
  practice_id UUID NOT NULL REFERENCES practices(id),
  filename VARCHAR(200) NOT NULL,
  content_type VARCHAR(120) NOT NULL,
  content BYTEA NOT NULL,
  sha256 CHAR(64) NOT NULL,
  size_bytes INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE practice_events (
  event_id UUID PRIMARY KEY,
  practice_id UUID NOT NULL REFERENCES practices(id),
  sequence INTEGER NOT NULL,
  event_type VARCHAR(40) NOT NULL,
  payload JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  UNIQUE (practice_id, sequence)
);

CREATE TABLE outbox (
  event_id UUID PRIMARY KEY REFERENCES practice_events(event_id),
  practice_id UUID NOT NULL,
  sequence INTEGER NOT NULL,
  event_json JSONB NOT NULL,
  traceparent VARCHAR(128),
  published_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX outbox_pending ON outbox (created_at, sequence) WHERE published_at IS NULL;
