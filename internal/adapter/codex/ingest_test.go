package codex_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/ginsys/parley/internal/adapter/codex"
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

func queueOne(t *testing.T, db *store.DB) (conversation, id string) {
	t.Helper()
	ctx := context.Background()
	ctrl := controller.New(db)
	conversation = "conv-ingest"
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

func TestIngestTurnAccepted(t *testing.T) {
	db := openTestDB(t)
	conversation, id := queueOne(t, db)
	ctx := context.Background()

	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"" + id + "\", \"to\": \"claude-session-a\", \"text\": \"here you go\"}\n```"
	reply, err := codex.IngestTurn(ctx, db, conversation, "codex-thread-b", "claude-session-a", turn)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if reply.Text != "here you go" || reply.ToPeer != "claude-session-a" || reply.FromPeer != "codex-thread-b" {
		t.Fatalf("unexpected reply envelope: %+v", reply)
	}
	if reply.InReplyTo == nil || *reply.InReplyTo != id {
		t.Fatalf("want in_reply_to %s, got %+v", id, reply.InReplyTo)
	}
	if reply.State != store.Queued {
		t.Fatalf("want reply queued, got %s", reply.State)
	}

	// The original envelope must now be acked, atomically with the reply.
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	original, err := store.GetByID(ctx, tx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if original.State != store.Acked {
		t.Fatalf("want original envelope acked, got %s", original.State)
	}
}

func TestIngestTurnNoMarkerIsOrdinaryConversation(t *testing.T) {
	db := openTestDB(t)
	conversation, id := queueOne(t, db)
	ctx := context.Background()

	_, err := codex.IngestTurn(ctx, db, conversation, "codex-thread-b", "claude-session-a", "just chatting, nothing to forward")
	if !codex.IsNoMarker(err) {
		t.Fatalf("want no-marker, got %v", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	original, err := store.GetByID(ctx, tx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if original.State != store.Queued {
		t.Fatalf("ordinary conversation must not touch envelope state, got %s", original.State)
	}
}

func TestIngestTurnMalformedStopsDelivery(t *testing.T) {
	db := openTestDB(t)
	conversation, _ := queueOne(t, db)
	ctx := context.Background()

	_, err := codex.IngestTurn(ctx, db, conversation, "codex-thread-b", "claude-session-a", "```BRIDGE-REPLY\nnot json\n```")
	if !errors.Is(err, replymarker.ErrMalformedMarker) {
		t.Fatalf("want ErrMalformedMarker, got %v", err)
	}
}

func TestIngestTurnWrongRecipientStopsDelivery(t *testing.T) {
	db := openTestDB(t)
	conversation, id := queueOne(t, db)
	ctx := context.Background()

	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"" + id + "\", \"to\": \"someone-else\", \"text\": \"hi\"}\n```"
	_, err := codex.IngestTurn(ctx, db, conversation, "codex-thread-b", "claude-session-a", turn)
	if !errors.Is(err, replymarker.ErrWrongRecipient) {
		t.Fatalf("want ErrWrongRecipient, got %v", err)
	}
}

func TestIngestTurnStaleReplyStopsDelivery(t *testing.T) {
	db := openTestDB(t)
	conversation, _ := queueOne(t, db)
	ctx := context.Background()

	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"does-not-exist\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	_, err := codex.IngestTurn(ctx, db, conversation, "codex-thread-b", "claude-session-a", turn)
	if !errors.Is(err, replymarker.ErrStaleReply) {
		t.Fatalf("want ErrStaleReply, got %v", err)
	}
}
