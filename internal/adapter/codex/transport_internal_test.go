package codex

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/dispatch"
	"github.com/ginsys/parley/internal/store"
)

// Regression for a finding on PR #4, refined by a later finding on PR #3: a
// context already canceled/expired before the process ever started must not
// be classified as an ambiguous ('uncertain') outcome — nothing could have
// committed. It's not merely "not ambiguous" either: it's definitely never
// attempted, so dispatch.Bridge refunds the budget claim and requeues it,
// rather than leaving it as a terminal failure that loses the message.
func TestRunAndClassifyPreStartCancellationIsNeverAttempted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := exec.CommandContext(ctx, "true")
	err := runAndClassify(ctx, cmd)
	if err == nil {
		t.Fatalf("want an error, got nil")
	}
	if errors.Is(err, ErrQueueAmbiguous) {
		t.Fatalf("want never-attempted for a pre-start cancellation, got ambiguous: %v", err)
	}
	if !errors.Is(err, ErrQueueNotAttempted) {
		t.Fatalf("want ErrQueueNotAttempted for a pre-start cancellation, got %v", err)
	}
	if cmd.Process != nil {
		t.Fatalf("test premise violated: process must not have started")
	}
}

// A context that expires only after the process has started is genuinely
// ambiguous — the process may have partially run or already committed
// something before being killed.
func TestRunAndClassifyMidRunExpiryIsAmbiguous(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sleep", "5")
	err := runAndClassify(ctx, cmd)
	if err == nil {
		t.Fatalf("want an error, got nil")
	}
	if !errors.Is(err, ErrQueueAmbiguous) {
		t.Fatalf("want ambiguous for a mid-run expiry, got %v", err)
	}
	if cmd.Process == nil {
		t.Fatalf("test premise violated: process must have started")
	}
}

// Regression for a finding on the merge-triggered review: a wrapped message
// too large for a single exec argv element (Linux's MAX_ARG_STRLEN, 128 KiB)
// must be rejected with a clear error before ever calling exec, not left to
// fail deep inside it with an opaque E2BIG.
func TestQueueMessageRejectsOversizedText(t *testing.T) {
	oversized := strings.Repeat("x", maxMessageBytes+1)
	err := ExecSender{}.QueueMessage(context.Background(), "thread-1", oversized)
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("want ErrMessageTooLarge, got %v", err)
	}
}

func TestQueueMessageAllowsTextAtLimit(t *testing.T) {
	atLimit := strings.Repeat("x", maxMessageBytes)
	err := ExecSender{}.QueueMessage(context.Background(), "thread-1", atLimit)
	if errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("want text exactly at the limit to pass the size check, got %v", err)
	}
}

// Regression for a finding on review 5160464724's follow-up: an oversized
// message is rejected before exec is ever called, so its budget slot must be
// refunded rather than permanently lost. Refined by a later finding (PR #3):
// unlike a transient never-attempted cause (a canceled context), the size of
// this exact message will never shrink on retry, so classifying it the same
// as ErrQueueNotAttempted would refund-and-requeue it forever instead of
// resolving. It must be its own permanent classification.
func TestQueueMessageOversizedTextIsPermanentlyRejected(t *testing.T) {
	oversized := strings.Repeat("x", maxMessageBytes+1)
	err := ExecSender{}.QueueMessage(context.Background(), "thread-1", oversized)
	if !errors.Is(err, ErrQueuePermanentlyRejected) {
		t.Fatalf("want ErrQueuePermanentlyRejected for an oversized message, got %v", err)
	}
	if errors.Is(err, ErrQueueNotAttempted) {
		t.Fatalf("oversized message must not also classify as ErrQueueNotAttempted (would requeue forever): %v", err)
	}
}

// Regression for a finding on PR #3's third review round: bridgetext.Wrap's
// one failure mode (crypto/rand exhausted while generating the boundary)
// happens before QueueMessage is ever called, but the previous code returned
// it as a plain error, so Dispatch recorded the envelope permanently failed
// while keeping its claimed exchange burned. Unlike the recipient/sender
// mismatch case, nothing about this envelope caused the failure — a retry
// could succeed — so it must classify as dispatch.ErrNoAttempt (refund and
// requeue), not dispatch.ErrPermanentlyRejected or a bare failure.
func TestTransportDeliverClassifiesWrapFailureAsNoAttempt(t *testing.T) {
	orig := wrapMessage
	defer func() { wrapMessage = orig }()
	wrapMessage = func(id, from, text string) (string, error) {
		return "", errors.New("crypto/rand: boom")
	}

	tr := NewTransport(&fakeQueueSender{}, "thread-123", "claude-session-a", "codex-thread-b")
	err := tr.Deliver(context.Background(), store.Envelope{ID: "e1", FromPeer: "claude-session-a", ToPeer: "codex-thread-b", Text: "x"})
	if !errors.Is(err, dispatch.ErrNoAttempt) {
		t.Fatalf("want dispatch.ErrNoAttempt (refundable, requeueable), got %v", err)
	}
	if errors.Is(err, dispatch.ErrPermanentlyRejected) {
		t.Fatalf("a wrap failure is transient, must not also classify as ErrPermanentlyRejected: %v", err)
	}
}

type fakeQueueSender struct{}

func (fakeQueueSender) QueueMessage(ctx context.Context, threadID, text string) error { return nil }
