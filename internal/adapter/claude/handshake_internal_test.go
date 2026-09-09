package claude

import (
	"errors"
	"sync/atomic"
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
	hs.onTimeout(hs.generation)
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

// Regression for a finding on PR #3: a timeout retry must resend the same
// nonce, not mint a fresh one. Rotating the nonce on every retry meant a
// genuine but slow acknowledgement for the original probe was rejected as
// stale the moment a retry fired — under a round trip consistently longer
// than the timeout, the handshake would never succeed even though the
// connection was healthy.
func TestTimeoutRetryPreservesNonce(t *testing.T) {
	probe := &recordingProbe{}
	hs := NewHandshake(probe.send, time.Hour)
	if err := hs.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	firstNonce := probe.last()

	hs.onTimeout(hs.generation)
	if got := probe.last(); got != firstNonce {
		t.Fatalf("want the retry to resend nonce %q, got %q", firstNonce, got)
	}
	if !hs.Ack(firstNonce) {
		t.Fatalf("want the original nonce still ackable after a timeout retry")
	}
}

// Regression for a finding on PR #3: a timer callback captured its
// generation at schedule time. If that callback doesn't run until after
// Stop() then a subsequent Reset() have both already happened, the stopped
// flag alone can't tell it's stale — Reset() clears stopped as part of
// starting the new connection. The generation check must still catch it.
func TestStaleTimeoutCallbackIgnoredAfterStopAndReset(t *testing.T) {
	probe := &recordingProbe{}
	hs := NewHandshake(probe.send, time.Hour)
	if err := hs.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	staleGen := hs.generation

	hs.Stop()
	if err := hs.Reset(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	newNonce := probe.last()
	sendsSoFar := len(probe.nonces)

	// The old connection's timer callback, arriving late.
	hs.onTimeout(staleGen)

	if len(probe.nonces) != sendsSoFar {
		t.Fatalf("want the stale callback to send nothing, got %d new sends", len(probe.nonces)-sendsSoFar)
	}
	if !hs.Ack(newNonce) {
		t.Fatalf("want the new connection's nonce still ackable after the stale callback ran")
	}
}

// Regression for a finding on PR #3: if sendProbe blocks longer than
// timeout, the timer fires while the original send is still in flight. The
// retry must not launch a second concurrent send stacked on the first —
// that pileup grows unbounded for as long as the transport stays stalled.
func TestOverlappingTimeoutRetrySkippedWhileSendInFlight(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls int32

	hs := NewHandshake(func(string) error {
		atomic.AddInt32(&calls, 1)
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	}, time.Hour)

	done := make(chan error, 1)
	go func() { done <- hs.Start() }()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatalf("probe never started")
	}

	hs.onTimeout(hs.generation)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("want no overlapping send while one is in flight, got %d total sends", got)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("start: %v", err)
	}
}

// Regression for a finding on PR #3: begin() used to clear a single shared
// "sending" bool unconditionally when starting a new generation, and the
// completing send (from whichever generation) also cleared it
// unconditionally. If a superseded generation's send was still in flight
// when a newer generation started its own send, the older send's eventual
// completion would wrongly clear the newer generation's in-flight flag,
// letting the newer generation's own timeout stack a second concurrent send
// on top of the one still running — the exact overlap the guard exists to
// prevent, now crossing generations instead of staying within one.
func TestStaleGenerationSendCompletionDoesNotCorruptNewGenerationInFlight(t *testing.T) {
	type call struct {
		nonce   string
		release chan struct{}
	}
	calls := make(chan *call, 8)
	probe := func(nonce string) error {
		c := &call{nonce: nonce, release: make(chan struct{})}
		calls <- c
		<-c.release
		return nil
	}

	hs := NewHandshake(probe, time.Hour)

	done1 := make(chan error, 1)
	go func() { done1 <- hs.Start() }()
	call1 := <-calls
	gen1 := hs.generation

	done2 := make(chan error, 1)
	go func() { done2 <- hs.Reset() }()
	call2 := <-calls
	gen2 := hs.generation
	if gen2 == gen1 {
		t.Fatalf("want Reset to bump the generation")
	}

	// gen1's send completes while gen2's send is still in flight.
	close(call1.release)
	if err := <-done1; err != nil {
		t.Fatalf("start: %v", err)
	}

	// A stale/duplicate timeout for gen2 firing while gen2's own send is
	// still in flight must be a no-op, not a second concurrent send.
	hs.onTimeout(gen2)

	select {
	case extra := <-calls:
		t.Fatalf("want no second concurrent send for generation %d while one is in flight, got nonce %q", gen2, extra.nonce)
	case <-time.After(50 * time.Millisecond):
	}

	close(call2.release)
	if err := <-done2; err != nil {
		t.Fatalf("reset: %v", err)
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
