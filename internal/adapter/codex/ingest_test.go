package codex_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/ginsys/parley/internal/adapter/codex"
	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/dispatch"
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
	// Only a handed-off envelope is eligible for a reply to ack (Validate):
	// replyingPeer cannot have seen a message the bridge never delivered.
	if err := store.SetState(ctx, tx, e.ID, store.Queued, store.HandedOff, "2026-01-01T00:00:01Z"); err != nil {
		t.Fatalf("set handed off: %v", err)
	}
	if err := tx.Commit(); err != nil {
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
	defer tx.Rollback()
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
	defer tx.Rollback()
	original, err := store.GetByID(ctx, tx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if original.State != store.HandedOff {
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

// Regression for a finding on PR #4: IngestTurn fetched the current grant
// only to copy its GrantVersion, never checking that the grant's Direction
// actually permits fromPeer -> marker.To. A reply must be rejected under a
// one-directional grant that doesn't cover that direction, not queued
// silently in the reverse direction from what was granted.
func TestIngestTurnDirectionNotPermittedStopsDelivery(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	ctrl := controller.New(db)
	conversation := "conv-direction"

	// b_to_a only: claude-session-a -> codex-thread-b is permitted, but a
	// reply going codex-thread-b -> claude-session-a (a_to_b) is not.
	if _, err := ctrl.Grant(ctx, controller.GrantParams{
		Conversation: conversation,
		PeerAID:      "codex-thread-b",
		PeerBID:      "claude-session-a",
		Direction:    store.BToA,
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
		ID:           "env-direction",
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
	// Only a handed-off envelope is eligible for a reply to ack (Validate):
	// replyingPeer cannot have seen a message the bridge never delivered.
	if err := store.SetState(ctx, tx, e.ID, store.Queued, store.HandedOff, "2026-01-01T00:00:01Z"); err != nil {
		t.Fatalf("set handed off: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"" + e.ID + "\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	_, err = codex.IngestTurn(ctx, db, conversation, "codex-thread-b", "claude-session-a", turn)
	if !errors.Is(err, codex.ErrDirectionNotPermitted) {
		t.Fatalf("want ErrDirectionNotPermitted, got %v", err)
	}

	tx2, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx2.Rollback()
	original, err := store.GetByID(ctx, tx2, e.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if original.State != store.HandedOff {
		t.Fatalf("rejected direction must not touch the original envelope's state, got %s", original.State)
	}
}

// Verification for a finding on PR #4: IngestTurn's reply-queuing path
// doesn't itself consult the grant's remaining exchange budget -- but this
// isn't a gap unique to replies, since store.InsertQueued (Bridge.Send's own
// path) doesn't either. store.ClaimExchange has exactly one call site,
// dispatch.Bridge's claim(), so budget is enforced uniformly at Dispatch
// time for every queued envelope regardless of how it was queued. This
// proves a reply queued via IngestTurn is rejected at its own later
// Dispatch once the grant's budget is spent, exactly like an ordinary one.
func TestIngestTurnReplyRespectsExchangeBudgetAtDispatch(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	ctrl := controller.New(db)
	conversation := "conv-reply-budget"

	if _, err := ctrl.Grant(ctx, controller.GrantParams{
		Conversation: conversation,
		PeerAID:      "codex-thread-b",
		PeerBID:      "claude-session-a",
		Direction:    store.Bidirectional,
		MaxExchanges: 1,
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
	original := store.Envelope{
		ID:           "env-reply-budget",
		Conversation: conversation,
		FromPeer:     "claude-session-a",
		ToPeer:       "codex-thread-b",
		Text:         "please respond",
		GrantVersion: g.GrantVersion,
		State:        store.Queued,
		CreatedAt:    "2026-01-01T00:00:00Z",
		UpdatedAt:    "2026-01-01T00:00:00Z",
	}
	if err := store.InsertQueued(ctx, tx, original); err != nil {
		t.Fatalf("insert queued: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Spend the grant's only exchange slot dispatching the original
	// envelope, before Codex ever replies.
	bridge := dispatch.New(db, noopTransport{})
	if _, err := bridge.Dispatch(ctx, original.ID); err != nil {
		t.Fatalf("dispatch original: %v", err)
	}

	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"" + original.ID + "\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	reply, err := codex.IngestTurn(ctx, db, conversation, "codex-thread-b", "claude-session-a", turn)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}

	if _, err := bridge.Dispatch(ctx, reply.ID); !errors.Is(err, dispatch.ErrBudgetExhausted) {
		t.Fatalf("want ErrBudgetExhausted dispatching the reply, got %v", err)
	}
}

type noopTransport struct{}

func (noopTransport) Deliver(context.Context, store.Envelope) error { return nil }

func TestIngestHiddenHTMLReplyDoesNotAcknowledgeOrQueue(t *testing.T) {
	db := openTestDB(t)
	conversation, id := queueOne(t, db)
	ctx := context.Background()
	turn := "Heading\n-\n<custom>\n```BRIDGE-REPLY\n{\"in_reply_to\":\"" + id + "\",\"to\":\"claude-session-a\",\"text\":\"hidden\"}\n```\n</custom>"
	if _, err := codex.IngestTurn(ctx, db, conversation, "codex-thread-b", "claude-session-a", turn); !errors.Is(err, codex.ErrNoMarker) {
		t.Fatalf("hidden reply accepted: %v", err)
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	original, err := store.GetByID(ctx, tx, id)
	if err != nil || original.State != store.HandedOff {
		t.Fatalf("original=%+v: %v", original, err)
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM envelopes").Scan(&count); err != nil || count != 1 {
		t.Fatalf("rows=%d: %v", count, err)
	}
}
