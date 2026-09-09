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
		// Regression for a finding on PR #3: a marker with in_reply_to and to
		// but no text must also be rejected, not accepted as an empty reply.
		"```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\"}\n```",
	}
	for _, turn := range cases {
		if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMalformedMarker) {
			t.Fatalf("turn %q: want ErrMalformedMarker, got %v", turn, err)
		}
	}
}

// Regression for a finding on PR #3: encoding/json's plain struct unmarshal
// keeps the last value of a repeated JSON object key, so a marker repeating
// a security-relevant field (e.g. "to") would resolve ambiguously instead
// of being rejected outright.
func TestExtractRejectsDuplicateObjectMember(t *testing.T) {
	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"unexpected\", \"to\": \"expected\", \"text\": \"hi\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMalformedMarker) {
		t.Fatalf("want ErrMalformedMarker, got %v", err)
	}
}

// Regression for a finding on PR #3: encoding/json matches struct fields
// case-insensitively, so a marker using "TO" instead of "to" would silently
// populate the same security-relevant field. JSON member names are
// case-sensitive; only the exact declared names are accepted.
func TestExtractRejectsWrongCaseFieldName(t *testing.T) {
	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"TO\": \"claude-session-a\", \"text\": \"hi\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMalformedMarker) {
		t.Fatalf("want ErrMalformedMarker, got %v", err)
	}
}

// Regression for a finding on PR #3: decodeMarker consumed only the first
// JSON value in the fenced block, so a second value smuggled in right after
// the first object's closing brace was silently discarded rather than
// rejected — the block must contain exactly one JSON value, not merely start
// with one.
func TestExtractRejectsTrailingContent(t *testing.T) {
	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"} {\"to\": \"attacker\"}\n```"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMalformedMarker) {
		t.Fatalf("want ErrMalformedMarker, got %v", err)
	}
}

// Regression for a finding on PR #3: before the fences were anchored to
// their own line, a four-backtick fence like "````BRIDGE-REPLY" could still
// satisfy an unanchored opener/closer match by consuming three of its four
// backticks, letting quoted protocol documentation be forwarded as if it
// were a real marker. The current line-anchored patterns must not match a
// four-backtick fence at all — it should read as ordinary conversation.
func TestExtractIgnoresFourBacktickFence(t *testing.T) {
	turn := "discussing the protocol:\n````BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n````"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a four-backtick fence, got %v", err)
	}
}

// Regression for a finding on PR #3: a valid marker followed by a second,
// truncated opener with no closing fence at all is ambiguous and must stop
// delivery — it must not be treated as exactly one valid marker just
// because only one block happens to close.
func TestExtractRejectsTruncatedSecondOpener(t *testing.T) {
	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"a\"}\n```\n" +
		"and then i started another one but never finished:\n```BRIDGE-REPLY\noops"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrMultipleMarkers) {
		t.Fatalf("want ErrMultipleMarkers, got %v", err)
	}
}

// Regression for a finding on PR #3: quoting the marker's exact syntax
// inside an outer four-backtick (or tilde) fence — e.g. documentation
// showing the protocol by example — must not be extracted as a live marker.
// Markdown fences don't nest, so a shorter same-character fence inside a
// still-open outer one is never itself a real fence boundary; it must be
// treated the same as any other quoted text.
func TestExtractIgnoresMarkerNestedInsideOuterFence(t *testing.T) {
	turn := "discussing the protocol:\n````\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```\n````"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a marker nested inside an outer fence, got %v", err)
	}
}

func TestExtractIgnoresMarkerNestedInsideOuterTildeFence(t *testing.T) {
	turn := "discussing the protocol:\n~~~\n```BRIDGE-REPLY\n" +
		"{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"hi\"}\n```\n~~~"
	if _, err := replymarker.Extract(turn); !errors.Is(err, replymarker.ErrNoMarker) {
		t.Fatalf("want ErrNoMarker for a marker nested inside an outer tilde fence, got %v", err)
	}
}

// Regression for a finding on PR #3: a marker sent with Windows-style CRLF
// line endings must still be recognized — the fence patterns anchor to line
// boundaries, and an un-normalized "\r" before each "$" broke that anchor.
func TestExtractAcceptsCRLFLineEndings(t *testing.T) {
	turn := "some prose\r\n```BRIDGE-REPLY\r\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"here you go\"}\r\n```\r\nmore prose"
	m, err := replymarker.Extract(turn)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if m.InReplyTo != "env-1" || m.To != "claude-session-a" || m.Text != "here you go" {
		t.Fatalf("unexpected marker: %+v", m)
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

// Regression for a finding on PR #3: two markers immediately adjacent, with
// no other content between the first block's closing fence position and the
// second block's opening fence, must still be rejected as a duplicate — the
// second opener must never be consumed as the first block's closer.
func TestExtractAdjacentMarkersRejectedAsDuplicate(t *testing.T) {
	turn := "```BRIDGE-REPLY\n{\"in_reply_to\": \"env-1\", \"to\": \"claude-session-a\", \"text\": \"a\"}\n" +
		"```BRIDGE-REPLY\n{\"in_reply_to\": \"env-2\", \"to\": \"claude-session-a\", \"text\": \"b\"}\n```"
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
	_, err = replymarker.Validate(ctx, tx, conversation, "codex-thread-b", "claude-session-a", m)
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
		if _, err := replymarker.Validate(ctx, tx, conversation, "codex-thread-b", "claude-session-a", m); !errors.Is(err, replymarker.ErrStaleReply) {
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
		if _, err := replymarker.Validate(ctx, tx, "a-different-conversation", "codex-thread-b", "claude-session-a", m); !errors.Is(err, replymarker.ErrStaleReply) {
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
		if _, err := replymarker.Validate(ctx, tx2, conversation, "codex-thread-b", "claude-session-a", m); !errors.Is(err, replymarker.ErrStaleReply) {
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
	e, err := replymarker.Validate(ctx, tx, conversation, "codex-thread-b", "claude-session-a", m)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if e.ID != id {
		t.Fatalf("want envelope %s, got %s", id, e.ID)
	}
}

// Regression for a finding on PR #3: Validate must reject a marker
// referencing an envelope that was never addressed to the peer producing
// the reply — otherwise a peer could name an envelope it sent itself (or
// one sent to a different peer) as long as the conversation and state
// happened to match, forging a reply nobody asked it for.
func TestValidateRejectsReplyFromNonAddressee(t *testing.T) {
	db := openTestDB(t)
	conversation, id := queueOne(t, db)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)

	// queueOne's envelope was sent from claude-session-a to codex-thread-b.
	// claude-session-a (the sender, not the addressee) must not be able to
	// reply to its own outgoing envelope.
	m := &replymarker.Marker{InReplyTo: id, To: "someone-else", Text: "hi"}
	_, err = replymarker.Validate(ctx, tx, conversation, "claude-session-a", "someone-else", m)
	if !errors.Is(err, replymarker.ErrWrongReplier) {
		t.Fatalf("want ErrWrongReplier, got %v", err)
	}
}
