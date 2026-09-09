package claude

import (
	"errors"
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

// Regression for a finding on PR #3: a nonce-generation failure must not
// leave a prior connection's nonce in place and ackable. Simulated via
// generateNonce, since crypto/rand failing is not something a test can
// trigger for real.
func TestNonceGenerationFailureClearsStaleNonce(t *testing.T) {
	orig := generateNonce
	t.Cleanup(func() { generateNonce = orig })

	probe := &recordingProbe{}
	hs := NewHandshake(probe.send, time.Hour)
	if err := hs.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	firstNonce := probe.last()

	boom := errors.New("entropy source down")
	generateNonce = func() (string, error) { return "", boom }

	if err := hs.Reset(); !errors.Is(err, boom) {
		t.Fatalf("want the nonce-generation error propagated, got %v", err)
	}
	if hs.Ready() {
		t.Fatalf("want not ready after a failed reset")
	}
	if hs.Ack(firstNonce) {
		t.Fatalf("the prior connection's nonce must not remain ackable after a failed reset")
	}
}

type recordingProbe struct {
	nonces []string
}

func (p *recordingProbe) send(nonce string) error {
	p.nonces = append(p.nonces, nonce)
	return nil
}

func (p *recordingProbe) last() string {
	if len(p.nonces) == 0 {
		return ""
	}
	return p.nonces[len(p.nonces)-1]
}
