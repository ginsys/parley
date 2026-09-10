package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/store"

	_ "modernc.org/sqlite"
)

// Regression for a finding on PR #3: Open applies schema.sql with
// CREATE TABLE IF NOT EXISTS, which is a no-op against a table that already
// exists — so a database created before is_trusted_reply was added to the
// envelopes table would never gain the column, and every subsequent
// InsertQueued/ListQueued/GetByID call against it would fail with
// "no such column: is_trusted_reply". This constructs exactly that
// pre-upgrade shape by hand (the envelopes table as it existed before this
// column, plus the conversations/grants tables its foreign keys need), then
// verifies Open's migration step adds the column to the existing table
// rather than leaving it untouched.
func TestOpenMigratesPreExistingDatabaseMissingTrustedReplyColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pre-upgrade.sqlite")
	seedDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open seed db: %v", err)
	}
	for _, stmt := range []string{
		`CREATE TABLE conversations (
			id         TEXT PRIMARY KEY,
			name       TEXT NOT NULL UNIQUE,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE grants (
			conversation   TEXT    NOT NULL REFERENCES conversations(id),
			grant_version  INTEGER NOT NULL,
			peer_a_id      TEXT    NOT NULL,
			peer_b_id      TEXT    NOT NULL,
			direction      TEXT    NOT NULL,
			max_exchanges  INTEGER NOT NULL,
			exchanges_used INTEGER NOT NULL DEFAULT 0,
			granted_at     TEXT    NOT NULL,
			expires_at     TEXT,
			status         TEXT    NOT NULL,
			revoked_at     TEXT,
			PRIMARY KEY (conversation, grant_version)
		)`,
		// The envelopes table exactly as it existed before is_trusted_reply.
		`CREATE TABLE envelopes (
			id            TEXT    PRIMARY KEY,
			conversation  TEXT    NOT NULL REFERENCES conversations(id),
			from_peer     TEXT    NOT NULL,
			to_peer       TEXT    NOT NULL,
			text          TEXT    NOT NULL,
			grant_version INTEGER NOT NULL,
			in_reply_to   TEXT,
			state         TEXT    NOT NULL,
			created_at    TEXT    NOT NULL,
			updated_at    TEXT    NOT NULL,
			FOREIGN KEY (conversation, grant_version) REFERENCES grants(conversation, grant_version)
		)`,
		`INSERT INTO conversations (id, name, created_at) VALUES ('conv-1', 'conv-1', '2026-01-01T00:00:00Z')`,
		`INSERT INTO grants (conversation, grant_version, peer_a_id, peer_b_id, direction, max_exchanges, granted_at, status)
			VALUES ('conv-1', 1, 'a', 'b', 'bidirectional', 10, '2026-01-01T00:00:00Z', 'active')`,
	} {
		if _, err := seedDB.Exec(stmt); err != nil {
			t.Fatalf("seed: %v: %s", err, stmt)
		}
	}
	if err := seedDB.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}

	ctx := context.Background()
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatalf("Open on pre-upgrade database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	e := store.Envelope{
		ID: "env-1", Conversation: "conv-1", FromPeer: "a", ToPeer: "b", Text: "hi",
		GrantVersion: 1, TrustedReply: true, State: store.Queued, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.InsertQueued(ctx, tx, e); err != nil {
		t.Fatalf("InsertQueued after migration: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	tx2, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx2.Rollback(ctx)
	got, err := store.GetByID(ctx, tx2, "env-1")
	if err != nil {
		t.Fatalf("GetByID after migration: %v", err)
	}
	if !got.TrustedReply {
		t.Fatalf("want TrustedReply true round-tripped through the migrated column, got false")
	}
}
