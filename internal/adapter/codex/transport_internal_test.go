package codex

import (
	"context"
	"errors"
	"os/exec"
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
