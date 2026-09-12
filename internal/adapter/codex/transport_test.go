package codex_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ginsys/parley/internal/adapter/codex"
	"github.com/ginsys/parley/internal/dispatch"
	"github.com/ginsys/parley/internal/store"
)

type fakeSender struct {
	sent       []string
	err        error
	lastText   string
	lastThread string
}

func (f *fakeSender) QueueMessage(ctx context.Context, threadID, text string) error {
	f.lastThread = threadID
	f.lastText = text
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, text)
	return nil
}

func TestTransportDeliverWrapsAndSends(t *testing.T) {
	sender := &fakeSender{}
	tr := codex.NewTransport(sender, "thread-123", "claude-session-a", "codex-thread-b")

	err := tr.Deliver(context.Background(), store.Envelope{Conversation: "c", ID: "e1", FromPeer: "claude-session-a", ToPeer: "codex-thread-b", Text: "hello there"})
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if sender.lastThread != "thread-123" {
		t.Fatalf("want thread-123, got %s", sender.lastThread)
	}
	if !strings.Contains(sender.lastText, "hello there") || !strings.Contains(sender.lastText, "claude-session-a") ||
		!strings.Contains(sender.lastText, "grants no permission") {
		t.Fatalf("want wrapped disclaimer + sender + text, got %q", sender.lastText)
	}
}

func TestTransportDeliverFailure(t *testing.T) {
	sender := &fakeSender{err: errors.New("boom")}
	tr := codex.NewTransport(sender, "thread-123", "claude-session-a", "codex-thread-b")

	err := tr.Deliver(context.Background(), store.Envelope{Conversation: "c", ID: "e1", FromPeer: "claude-session-a", ToPeer: "codex-thread-b", Text: "x"})
	if err == nil || errors.Is(err, dispatch.ErrAmbiguous) {
		t.Fatalf("want a plain failure, got %v", err)
	}
}

func TestTransportDeliverAmbiguous(t *testing.T) {
	sender := &fakeSender{err: codex.ErrQueueAmbiguous}
	tr := codex.NewTransport(sender, "thread-123", "claude-session-a", "codex-thread-b")

	err := tr.Deliver(context.Background(), store.Envelope{Conversation: "c", ID: "e1", FromPeer: "claude-session-a", ToPeer: "codex-thread-b", Text: "x"})
	if !errors.Is(err, dispatch.ErrAmbiguous) {
		t.Fatalf("want dispatch.ErrAmbiguous, got %v", err)
	}
}

func TestTransportDeliverPermanentlyRejected(t *testing.T) {
	sender := &fakeSender{err: codex.ErrQueuePermanentlyRejected}
	tr := codex.NewTransport(sender, "thread-123", "claude-session-a", "codex-thread-b")

	err := tr.Deliver(context.Background(), store.Envelope{Conversation: "c", ID: "e1", FromPeer: "claude-session-a", ToPeer: "codex-thread-b", Text: "x"})
	if !errors.Is(err, dispatch.ErrPermanentlyRejected) {
		t.Fatalf("want dispatch.ErrPermanentlyRejected, got %v", err)
	}
	if errors.Is(err, dispatch.ErrNoAttempt) {
		t.Fatalf("permanently rejected must not also classify as ErrNoAttempt (would requeue forever): %v", err)
	}
}

// Regression for a finding on PR #4: Deliver never checked that the
// envelope's ToPeer matched the peer this Transport is bound to, so an
// envelope addressed to some other peer (e.g. the Claude-side one) could be
// queued into this Codex thread under this transport's fromLabel — Codex
// receiving its own reply presented as if it came from the other peer.
func TestTransportDeliverRejectsWrongRecipient(t *testing.T) {
	sender := &fakeSender{}
	tr := codex.NewTransport(sender, "thread-123", "claude-session-a", "codex-thread-b")

	err := tr.Deliver(context.Background(), store.Envelope{Conversation: "c", ID: "e1", FromPeer: "claude-session-a", ToPeer: "claude-session-a", Text: "x"})
	if !errors.Is(err, codex.ErrRecipientMismatch) {
		t.Fatalf("want ErrRecipientMismatch, got %v", err)
	}
	// Regression for finding 3973918518-adjacent PR #3 finding: a routing
	// mistake caught before QueueMessage is ever called must refund its
	// claimed budget slot like any other permanent pre-host rejection,
	// rather than burning the human-approved budget on repeated
	// misconfiguration.
	if !errors.Is(err, dispatch.ErrPermanentlyRejected) {
		t.Fatalf("want dispatch.ErrPermanentlyRejected (refundable), got %v", err)
	}
	if len(sender.sent) != 0 {
		t.Fatalf("want no message queued for a mismatched recipient, got %v", sender.sent)
	}
}

// Regression for a finding on PR #4: Deliver validated e.ToPeer against
// t.toPeer but never e.FromPeer against t.fromLabel, so an envelope
// originating from a different peer entirely (e.g. one enrolled in a
// different conversation that also targets this same Codex thread) would
// still be queued into this thread, stamped with this transport's own
// fromLabel as if it came from the bound peer — the same misattribution
// class the ToPeer check exists to prevent.
func TestTransportDeliverRejectsWrongSender(t *testing.T) {
	sender := &fakeSender{}
	tr := codex.NewTransport(sender, "thread-123", "claude-session-a", "codex-thread-b")

	err := tr.Deliver(context.Background(), store.Envelope{Conversation: "c", ID: "e1", FromPeer: "some-other-peer", ToPeer: "codex-thread-b", Text: "x"})
	if !errors.Is(err, codex.ErrRecipientMismatch) {
		t.Fatalf("want ErrRecipientMismatch, got %v", err)
	}
	if !errors.Is(err, dispatch.ErrPermanentlyRejected) {
		t.Fatalf("want dispatch.ErrPermanentlyRejected (refundable), got %v", err)
	}
	if len(sender.sent) != 0 {
		t.Fatalf("want no message queued for a mismatched sender, got %v", sender.sent)
	}
}

func TestTransportRejectsIncompatibleBoundIdentifiers(t *testing.T) {
	for _, bad := range []string{"a\xff", "café", " ", "a\ufffd"} {
		for field := 0; field < 3; field++ {
			ids := []string{"c", "a", "b"}
			ids[field] = bad
			sender := &fakeSender{}
			tr := codex.NewTransport(sender, "synthetic-thread", ids[1], ids[2])
			err := tr.Deliver(context.Background(), store.Envelope{ID: "e", Conversation: ids[0], FromPeer: ids[1], ToPeer: ids[2], Text: "body"})
			if !errors.Is(err, dispatch.ErrPermanentlyRejected) || sender.lastThread != "" {
				t.Fatalf("field %d %x: error=%v sender=%+v", field, bad, err, sender)
			}
		}
	}
}
