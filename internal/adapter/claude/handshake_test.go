package claude_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	adapterclaude "github.com/ginsys/parley/internal/adapter/claude"
	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/dispatch"
	"github.com/ginsys/parley/internal/store"
)

type fakeTransport struct {
	mu        sync.Mutex
	delivered []string
}

func (f *fakeTransport) Deliver(ctx context.Context, e store.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delivered = append(f.delivered, e.ID)
	return nil
}

func (f *fakeTransport) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.delivered)
}

type probeRecorder struct {
	mu     sync.Mutex
	nonces []string
}

func (p *probeRecorder) send(nonce string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nonces = append(p.nonces, nonce)
	return nil
}

func (p *probeRecorder) last() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.nonces) == 0 {
		return ""
	}
	return p.nonces[len(p.nonces)-1]
}

func (p *probeRecorder) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.nonces)
}

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

func grantOne(t *testing.T, ctrl *controller.Controller, conversation string) {
	t.Helper()
	_, err := ctrl.Grant(context.Background(), controller.GrantParams{
		Conversation: conversation,
		PeerAID:      "codex-thread-b",
		PeerBID:      "claude-session-a",
		Direction:    store.Bidirectional,
		MaxExchanges: 10,
	})
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
}

// Fixture 11: delayed readiness — an application message accepted before the
// handshake is acknowledged must stay queued, never advance to dispatching,
// until the handshake's nonce is acked.
func TestDelayedReadinessKeepsQueued(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-readiness")

	transport := &fakeTransport{}
	bridge := dispatch.New(db, transport)
	ctx := context.Background()

	e, err := bridge.Send(ctx, "conv-readiness", "codex-thread-b", "claude-session-a", "hello", nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	probe := &probeRecorder{}
	hs := adapterclaude.NewHandshake(probe.send, time.Hour)
	if err := hs.Start(); err != nil {
		t.Fatalf("start handshake: %v", err)
	}
	poller := adapterclaude.NewPoller(db, bridge, hs, "conv-readiness", "claude-session-a")

	// Not yet acked: Tick must not dispatch anything.
	ids, err := poller.Tick(ctx)
	if err != nil {
		t.Fatalf("tick before ack: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("want no envelopes attempted before ack, got %v", ids)
	}
	if transport.count() != 0 {
		t.Fatalf("transport must not have been called before ack")
	}
	assertState(t, db, e.ID, store.Queued)

	// Ack with the wrong nonce: still not ready, still queued.
	if hs.Ack("not-the-nonce") {
		t.Fatalf("ack with wrong nonce must not succeed")
	}
	if _, err := poller.Tick(ctx); err != nil {
		t.Fatalf("tick after bad ack: %v", err)
	}
	assertState(t, db, e.ID, store.Queued)

	// Ack with the real nonce: now ready, and the queued message dispatches.
	if !hs.Ack(probe.last()) {
		t.Fatalf("ack with correct nonce must succeed")
	}
	ids, err = poller.Tick(ctx)
	if err != nil {
		t.Fatalf("tick after ack: %v", err)
	}
	if len(ids) != 1 || ids[0] != e.ID {
		t.Fatalf("want envelope %s dispatched, got %v", e.ID, ids)
	}
	assertState(t, db, e.ID, store.HandedOff)
}

// Fixture 12: handshake timeout — an unacked probe is retried on its own
// timeout, never an application message; anything queued during the wait
// stays queued, not failed, not silently sent.
func TestHandshakeTimeoutRetriesOnlyHandshake(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-timeout")

	transport := &fakeTransport{}
	bridge := dispatch.New(db, transport)
	ctx := context.Background()

	e, err := bridge.Send(ctx, "conv-timeout", "codex-thread-b", "claude-session-a", "queued during wait", nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	probe := &probeRecorder{}
	hs := adapterclaude.NewHandshake(probe.send, 20*time.Millisecond)
	if err := hs.Start(); err != nil {
		t.Fatalf("start handshake: %v", err)
	}
	defer hs.Stop()

	deadline := time.After(2 * time.Second)
	for probe.count() < 3 {
		select {
		case <-deadline:
			t.Fatalf("want at least 3 handshake probes within deadline, got %d", probe.count())
		case <-time.After(10 * time.Millisecond):
		}
	}

	if hs.Ready() {
		t.Fatalf("handshake must not be ready without a matching ack")
	}
	poller := adapterclaude.NewPoller(db, bridge, hs, "conv-timeout", "claude-session-a")
	ids, err := poller.Tick(ctx)
	if err != nil {
		t.Fatalf("tick during retries: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("want no dispatch attempts while unacked, got %v", ids)
	}
	assertState(t, db, e.ID, store.Queued)

	// The eventual, correct ack against the latest nonce still works.
	if !hs.Ack(probe.last()) {
		t.Fatalf("ack with latest nonce must succeed")
	}
	if _, err := poller.Tick(ctx); err != nil {
		t.Fatalf("tick after eventual ack: %v", err)
	}
	assertState(t, db, e.ID, store.HandedOff)
}

// Fixture 13: reconnect resets readiness — a new connection must not treat a
// prior connection's ack as still valid; a fresh nonce handshake is required
// before dispatching anything on the new connection.
func TestReconnectResetsReadiness(t *testing.T) {
	probe := &probeRecorder{}
	hs := adapterclaude.NewHandshake(probe.send, time.Hour)
	if err := hs.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	firstNonce := probe.last()
	if !hs.Ack(firstNonce) {
		t.Fatalf("ack on first connection must succeed")
	}
	if !hs.Ready() {
		t.Fatalf("want ready after first connection's ack")
	}

	// Simulate a reconnect: a new connection, same Handshake instance reset.
	if err := hs.Reset(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if hs.Ready() {
		t.Fatalf("want not ready immediately after reconnect")
	}

	// The prior connection's nonce must not carry over.
	if hs.Ack(firstNonce) {
		t.Fatalf("ack with the old connection's nonce must not succeed after reset")
	}
	if hs.Ready() {
		t.Fatalf("want still not ready after a stale ack attempt")
	}

	// Only the new connection's own nonce acks it.
	newNonce := probe.last()
	if newNonce == firstNonce {
		t.Fatalf("reset must generate a fresh nonce, got the same one")
	}
	if !hs.Ack(newNonce) {
		t.Fatalf("ack with the new connection's nonce must succeed")
	}
	if !hs.Ready() {
		t.Fatalf("want ready after the new connection's own ack")
	}
}

func TestHandshakeStartPropagatesProbeError(t *testing.T) {
	boom := errors.New("boom")
	hs := adapterclaude.NewHandshake(func(string) error { return boom }, time.Hour)
	if err := hs.Start(); !errors.Is(err, boom) {
		t.Fatalf("want probe error propagated, got %v", err)
	}
}

func assertState(t *testing.T, db *store.DB, id string, want store.EnvelopeState) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	e, err := store.GetByID(ctx, tx, id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	if e.State != want {
		t.Fatalf("envelope %s: want state %s, got %s", id, want, e.State)
	}
}
