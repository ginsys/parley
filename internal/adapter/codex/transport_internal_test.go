package codex

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Regression for a finding on PR #4: a context already canceled/expired
// before the process ever started must not be classified as an ambiguous
// ('uncertain') outcome — nothing could have committed, so it's a plain,
// safely retryable failure.
func TestRunAndClassifyPreStartCancellationIsPlainFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := exec.CommandContext(ctx, "true")
	err := runAndClassify(ctx, cmd)
	if err == nil {
		t.Fatalf("want an error, got nil")
	}
	if errors.Is(err, ErrQueueAmbiguous) {
		t.Fatalf("want a plain failure for a pre-start cancellation, got ambiguous: %v", err)
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
