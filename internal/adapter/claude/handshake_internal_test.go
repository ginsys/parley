package claude

import (
	"testing"
	"time"
)

// Regression for a race an automated reviewer flagged on PR #3: a timeout
// callback already blocked on h.mu when Stop() runs would proceed once
// Stop() released the lock and re-arm a fresh probe — time.Timer.Stop
// cannot cancel a callback that has already started running. Simulates the
// in-flight callback directly rather than relying on a real race window.
func TestStopPreventsRearmFromInFlightTimeout(t *testing.T) {
	calls := 0
	hs := NewHandshake(func(string) error { calls++; return nil }, time.Hour)
	if err := hs.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if calls != 1 {
		t.Fatalf("want 1 probe after start, got %d", calls)
	}

	hs.Stop()
	hs.onTimeout()
	if calls != 1 {
		t.Fatalf("want no additional probe once stopped, got %d calls", calls)
	}
	if hs.Ready() {
		t.Fatalf("Stop must not toggle readiness")
	}
}
