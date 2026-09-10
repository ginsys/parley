-- Parley durable state. One sqlite db per bridge instance (dotfile-scale).
-- See the design plan (private) for the full spec, invariants, and probe evidence.

CREATE TABLE IF NOT EXISTS conversations (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

-- Grants are versioned, never mutated in place except for revoked_at/status.
-- Exactly one row per conversation has status='active' at a time; a renewal
-- supersedes the prior active row instead of overwriting it, so history is
-- never destroyed.
CREATE TABLE IF NOT EXISTS grants (
    conversation   TEXT    NOT NULL REFERENCES conversations(id),
    grant_version  INTEGER NOT NULL,
    peer_a_id      TEXT    NOT NULL,
    peer_b_id      TEXT    NOT NULL,
    direction      TEXT    NOT NULL CHECK (direction IN ('bidirectional', 'a_to_b', 'b_to_a')),
    max_exchanges  INTEGER NOT NULL,
    exchanges_used INTEGER NOT NULL DEFAULT 0,
    granted_at     TEXT    NOT NULL,
    expires_at     TEXT,
    status         TEXT    NOT NULL CHECK (status IN ('active', 'revoked', 'superseded')),
    revoked_at     TEXT,
    PRIMARY KEY (conversation, grant_version)
);

-- Enforces "exactly one active grant per conversation" at the schema level,
-- not just by controller discipline.
CREATE UNIQUE INDEX IF NOT EXISTS idx_grants_one_active
    ON grants(conversation)
    WHERE status = 'active';

-- One row per envelope, append-only except for the state/updated_at columns.
-- state machine: queued -> dispatching -> handed_off -> (acked)
--                                       -> failed
--                                       -> uncertain (crash between host call and commit)
--                       -> cancelled (revoked/renewed away while still queued)
CREATE TABLE IF NOT EXISTS envelopes (
    id            TEXT    PRIMARY KEY,
    conversation  TEXT    NOT NULL REFERENCES conversations(id),
    from_peer     TEXT    NOT NULL,
    to_peer       TEXT    NOT NULL,
    text          TEXT    NOT NULL,
    grant_version INTEGER NOT NULL,
    in_reply_to   TEXT,
    -- Set only by the trusted ingestion path (codex.IngestTurn), which
    -- atomically acks the original envelope and queues this reply as one
    -- transaction, having already validated in_reply_to against that exact
    -- original via replymarker.Validate. dispatch.Bridge.Send has no
    -- parameter for this column and always leaves it 0/false — an ordinary
    -- send cannot claim reply status just by naming some other envelope's id
    -- (even a genuinely acked one) in in_reply_to.
    is_trusted_reply INTEGER NOT NULL DEFAULT 0 CHECK (is_trusted_reply IN (0, 1)),
    state         TEXT    NOT NULL CHECK (
        state IN ('queued', 'dispatching', 'handed_off', 'acked', 'failed', 'cancelled', 'uncertain')
    ),
    created_at    TEXT    NOT NULL,
    updated_at    TEXT    NOT NULL,
    FOREIGN KEY (conversation, grant_version) REFERENCES grants(conversation, grant_version)
);

CREATE INDEX IF NOT EXISTS idx_envelopes_conversation_state
    ON envelopes(conversation, state);
