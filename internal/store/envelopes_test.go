package store

import (
	"context"
	"testing"
)

// canCarryReplyFixture builds one conversation with a peer-a authored,
// peer-b acked original, plus a queued trusted reply from peer-b back to
// peer-a stamped at grant version 1. versions lists the grant versions the
// caller wants inserted (each active in turn, then immediately superseded
// except the last), letting a test construct either a contiguous history or
// one with a deliberate gap. Every inserted version keeps the same
// bidirectional peer pair, so PermitsDirection always succeeds -- only
// row-count completeness is under test here, not per-row rejection, which
// internal/dispatch/dispatch_test.go already covers extensively.
func canCarryReplyFixture(t *testing.T, versions ...int64) (*DB, *Envelope) {
	t.Helper()
	ctx := context.Background()
	db := commandDB(t)
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := EnsureConversation(ctx, tx, "gap", "gap", "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	for i, v := range versions {
		if err := InsertGrant(ctx, tx, Grant{
			Conversation: "gap", GrantVersion: v, PeerAID: "peer-a", PeerBID: "peer-b",
			Direction: Bidirectional, MaxExchanges: 10, GrantedAt: "2026-01-01T00:00:00Z",
		}); err != nil {
			t.Fatal(err)
		}
		if i < len(versions)-1 {
			if err := SetGrantStatus(ctx, tx, "gap", v, GrantSuperseded, "2026-01-01T00:00:00Z"); err != nil {
				t.Fatal(err)
			}
		}
	}
	original := "80000000-0000-4000-8000-000000000001"
	if err := InsertQueued(ctx, tx, Envelope{
		ID: original, Conversation: "gap", FromPeer: "peer-a", ToPeer: "peer-b",
		Text: "original", GrantVersion: versions[0], CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	if err := SetState(ctx, tx, original, Queued, Acked, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	reply := &Envelope{
		ID: "80000000-0000-4000-8000-000000000002", Conversation: "gap", FromPeer: "peer-b", ToPeer: "peer-a",
		Text: "reply", GrantVersion: versions[0], InReplyTo: &original, TrustedReply: true,
		CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z",
	}
	if err := InsertQueued(ctx, tx, *reply); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return db, reply
}

// TestCanCarryReplyRejectsMissingIntermediateGrantVersion is the MC-04
// regression: versions 1 and 3 exist, 2 does not (SetGrantStatus never runs
// against a nonexistent row, so this is a genuine gap, not a superseded
// row this test forgot to insert). Before the fix, the per-row loop only
// ever inspected the rows the range query actually returned -- with no gap
// check, it would see versions 1 and 3 pass their own direction checks and
// carry the reply across the unaccounted-for version 2 it never examined.
func TestCanCarryReplyRejectsMissingIntermediateGrantVersion(t *testing.T) {
	db, reply := canCarryReplyFixture(t, 1, 3)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ok, err := CanCarryReply(ctx, tx, reply, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("carry must be rejected across a missing intervening grant version")
	}
}

// TestCanCarryReplyAllowsContiguousGrantHistory is the positive control for
// the same fix: 1, 2 and 3 all exist and every one permits the reply's
// direction, so the completeness proof must not itself become a false
// rejection of an ordinary, fully-accounted-for renewal chain.
func TestCanCarryReplyAllowsContiguousGrantHistory(t *testing.T) {
	db, reply := canCarryReplyFixture(t, 1, 2, 3)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ok, err := CanCarryReply(ctx, tx, reply, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("carry must succeed across a complete, permitting grant history")
	}
}
