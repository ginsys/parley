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

	err := tr.Deliver(context.Background(), store.Envelope{ID: "e1", ToPeer: "codex-thread-b", Text: "hello there"})
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

	err := tr.Deliver(context.Background(), store.Envelope{ID: "e1", ToPeer: "codex-thread-b", Text: "x"})
	if err == nil || errors.Is(err, dispatch.ErrAmbiguous) {
		t.Fatalf("want a plain failure, got %v", err)
	}
}

func TestTransportDeliverAmbiguous(t *testing.T) {
	sender := &fakeSender{err: codex.ErrQueueAmbiguous}
	tr := codex.NewTransport(sender, "thread-123", "claude-session-a", "codex-thread-b")

	err := tr.Deliver(context.Background(), store.Envelope{ID: "e1", ToPeer: "codex-thread-b", Text: "x"})
	if !errors.Is(err, dispatch.ErrAmbiguous) {
		t.Fatalf("want dispatch.ErrAmbiguous, got %v", err)
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

	err := tr.Deliver(context.Background(), store.Envelope{ID: "e1", ToPeer: "claude-session-a", Text: "x"})
	if !errors.Is(err, codex.ErrRecipientMismatch) {
		t.Fatalf("want ErrRecipientMismatch, got %v", err)
	}
	if len(sender.sent) != 0 {
		t.Fatalf("want no message queued for a mismatched recipient, got %v", sender.sent)
	}
}
