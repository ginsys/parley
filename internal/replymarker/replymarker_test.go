package replymarker_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/replymarker"
	"github.com/ginsys/parley/internal/store"
)

func openTestDB(t *testing.T) *store.DB {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "parley.db")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func queueOne(t *testing.T, db *store.DB) (conversation string, id string) {
	t.Helper()
	ctx := context.Background()
	ctrl := controller.New(db)
	conversation = "conv-marker"
	if _, err := ctrl.Grant(ctx, controller.GrantParams{
		Conversation: conversation,
		PeerAID:      "codex-thread-b",
		PeerBID:      "claude-session-a",
		Direction:    store.Bidirectional,
		MaxExchanges: 10,
	}); err != nil {
		t.Fatalf("grant: %v", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	g, err := store.CurrentGrant(ctx, tx, conversation)
	if err != nil {
		t.Fatalf("current grant: %v", err)
	}
	e := store.Envelope{
		ID:           "env-1",
		Conversation: conversation,
		FromPeer:     "claude-session-a",
		ToPeer:       "codex-thread-b",
		Text:         "please respond",
		GrantVersion: g.GrantVersion,
		State:        store.Queued,
		CreatedAt:    "2026-01-01T00:00:00Z",
		UpdatedAt:    "2026-01-01T00:00:00Z",
	}
	if err := store.InsertQueued(ctx, tx, e); err != nil {
		t.Fatalf("insert queued: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return conversation, e.ID
}

func TestExtractValidMarker(t *testing.T) {
	turn := "some prose\n```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"here you go\"}\n```\nmore prose"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if m.InReplyTo != "env-1" || m.To != "claude-session-a" || m.Text != "here you go" {
		t.Fatalf("unexpected marker: %+v", m)
	}
}

func TestExtractNoMarker(t *testing.T) {
	if _, err := replymarker.Extract("just ordinary conversation, no marker here"); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker, got %v", err)
	}
}

// Fixture 7: malformed reply marker — invalid syntax must stop delivery, not
// fall back to forwarding raw turn content.
func TestExtractMalformedMarker(t *testing.T) {
	cases := []string{
		"```BRIDGE-REPLY\nnot json at all\n```",
		"```BRIDGE-REPLY\n{\"to\": \"claude-session-a\", \"text\": \"missing in_reply_to\"}\n```",
		"```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"text\": \"missing to\"}\n```",
	}
	for _, turn := range cases {
		if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMalformedMarker) {
			t.Fatalf("turn %q: want ErrMalformedMarker, got %v", turn, err)
		}
	}
}

// Fixture 8: duplicate reply markers — two well-formed markers in one turn
// must stop delivery, not pick one arbitrarily.
func TestExtractDuplicateMarkers(t *testing.T) {
	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"first\"}\n```\n" +
		"and also\n```BRIDGE-REPLY\n{\"in_reply_to\": \"env-2\", \"to\": \"claude-session-a\", \"text\": \"second\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMultipleMarkers) {
		t.Fatalf("want ErrMultipleMarkers, got %v", err)
	}
}

// Fixture 9: wrong-recipient reply — a well-formed, parseable marker whose
// "to" does not match the enrolled peer must be rejected, not forwarded on
// the strength of syntax alone.
func TestValidateWrongRecipient(t *testing.T) {
	db := openTestDB(t)
	conversation, id := queueOne(t, db)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)

	m := &replymarker.Marker{InReplyTo: id, To: "some-other-peer", Text: "hi"}
	_, err = replymarker.Validate(ctx, tx, conversation, "claude-session-a", m)
	if !errors.Is(err, replymarker.ErrWrongRecipient) {
		t.Fatalf("want ErrWrongRecipient, got %v", err)
	}
}

// Fixture 10: stale reply — a well-formed marker whose in_reply_to names an
// envelope that isn't currently queued/handed_off awaiting reply on that
// conversation must be rejected, not forwarded.
func TestValidateStaleReply(t *testing.T) {
	db := openTestDB(t)
	conversation, id := queueOne(t, db)
	ctx := context.Background()

	t.Run("unknown envelope", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer tx.Rollback(ctx)
		m := &replymarker.Marker{InReplyTo: "does-not-exist", To: "claude-session-a", Text: "hi"}
		if _, err := replymarker.Validate(ctx, tx, conversation, "claude-session-a", m); !errors.Is(err, replymarker.ErrStaleReply) {
			t.Fatalf("want ErrStaleReply, got %v", err)
		}
	})

	t.Run("different conversation", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer tx.Rollback(ctx)
		m := &replymarker.Marker{InReplyTo: id, To: "claude-session-a", Text: "hi"}
		if _, err := replymarker.Validate(ctx, tx, "a-different-conversation", "claude-session-a", m); !errors.Is(err, replymarker.ErrStaleReply) {
			t.Fatalf("want ErrStaleReply, got %v", err)
		}
	})

	t.Run("already acked", func(t *testing.T) {
		tx, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if err := store.SetState(ctx, tx, id, store.Acked, "2026-01-01T00:01:00Z"); err != nil {
			tx.Rollback(ctx)
			t.Fatalf("set acked: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}

		tx2, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer tx2.Rollback(ctx)
		m := &replymarker.Marker{InReplyTo: id, To: "claude-session-a", Text: "hi"}
		if _, err := replymarker.Validate(ctx, tx2, conversation, "claude-session-a", m); !errors.Is(err, replymarker.ErrStaleReply) {
			t.Fatalf("want ErrStaleReply, got %v", err)
		}
	})
}

func TestValidateAccepted(t *testing.T) {
	db := openTestDB(t)
	conversation, id := queueOne(t, db)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)

	m := &replymarker.Marker{InReplyTo: id, To: "claude-session-a", Text: "hi"}
	e, err := replymarker.Validate(ctx, tx, conversation, "claude-session-a", m)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if e.ID != id {
		t.Fatalf("want envelope %s, got %s", id, e.ID)
	}
}
