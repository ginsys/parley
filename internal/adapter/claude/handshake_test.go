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
	// onDeliver, if set, runs synchronously after recording delivery and
	// before Deliver returns — used to simulate a reconnect happening
	// mid-batch, between two envelopes in the same Poller.Tick call.
	onDeliver func(id string)
}

func (f *fakeTransport) Deliver(ctx context.Context, e store.Envelope) error {
	f.mu.Lock()
	f.delivered = append(f.delivered, e.ID)
	hook := f.onDeliver
	f.mu.Unlock()
	if hook != nil {
		hook(e.ID)
	}
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

// Regression for a finding on PR #3: Stop() must clear readiness, not only
// disarm the timer — a stopped connection is by definition no longer proven
// ready, and a caller that only checks Ready() (Poller) must see that
// immediately rather than keep dispatching into a dead transport.
func TestStopClearsReadiness(t *testing.T) {
	probe := &probeRecorder{}
	hs := adapterclaude.NewHandshake(probe.send, time.Hour)
	if err := hs.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	nonce := probe.last()
	if !hs.Ack(nonce) {
		t.Fatalf("ack must succeed")
	}
	if !hs.Ready() {
		t.Fatalf("want ready after ack")
	}

	hs.Stop()
	if hs.Ready() {
		t.Fatalf("want not ready immediately after Stop")
	}
	if hs.Ack(nonce) {
		t.Fatalf("the stopped connection's own nonce must not re-ack it")
	}
}

// Regression for a finding on PR #3: Tick must re-check readiness before
// each dispatch, not just once before the batch — a reconnect revoking
// readiness mid-batch must stop the remaining envelopes from being
// dispatched, not let the batch finish.
func TestPollerStopsMidBatchWhenReadinessRevoked(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-mid-batch")
	ctx := context.Background()

	e1, err := bridge(t, db).Send(ctx, "conv-mid-batch", "codex-thread-b", "claude-session-a", "first", nil)
	if err != nil {
		t.Fatalf("send 1: %v", err)
	}
	e2, err := bridge(t, db).Send(ctx, "conv-mid-batch", "codex-thread-b", "claude-session-a", "second", nil)
	if err != nil {
		t.Fatalf("send 2: %v", err)
	}

	probe := &probeRecorder{}
	hs := adapterclaude.NewHandshake(probe.send, time.Hour)
	if err := hs.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !hs.Ack(probe.last()) {
		t.Fatalf("ack must succeed")
	}

	transport := &fakeTransport{}
	transport.onDeliver = func(id string) {
		if id == e1.ID {
			hs.Stop() // simulate a reconnect revoking readiness mid-batch
		}
	}
	b := dispatch.New(db, transport)
	poller := adapterclaude.NewPoller(db, b, hs, "conv-mid-batch", "claude-session-a")

	attempted, err := poller.Tick(ctx)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(attempted) != 1 || attempted[0] != e1.ID {
		t.Fatalf("want only %s attempted before readiness was revoked, got %v", e1.ID, attempted)
	}
	assertState(t, db, e1.ID, store.HandedOff)
	assertState(t, db, e2.ID, store.Queued)
}

// Regression for a finding on PR #3: Ready() alone can't distinguish the
// connection that authorized this Tick's batch from a brand new one that
// also happens to reach Ready() by the time the loop gets to a later
// envelope. Without also checking Generation(), a reconnect completing and
// re-acking mid-batch would let the new connection's readiness silently
// authorize the rest of the old batch.
func TestPollerStopsMidBatchWhenGenerationChangesDespiteReady(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-mid-batch-gen")
	ctx := context.Background()

	e1, err := bridge(t, db).Send(ctx, "conv-mid-batch-gen", "codex-thread-b", "claude-session-a", "first", nil)
	if err != nil {
		t.Fatalf("send 1: %v", err)
	}
	e2, err := bridge(t, db).Send(ctx, "conv-mid-batch-gen", "codex-thread-b", "claude-session-a", "second", nil)
	if err != nil {
		t.Fatalf("send 2: %v", err)
	}

	probe := &probeRecorder{}
	hs := adapterclaude.NewHandshake(probe.send, time.Hour)
	if err := hs.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !hs.Ack(probe.last()) {
		t.Fatalf("ack must succeed")
	}

	transport := &fakeTransport{}
	transport.onDeliver = func(id string) {
		if id != e1.ID {
			return
		}
		// Simulate a reconnect completing and re-acking mid-batch: a brand
		// new connection, a distinct generation, that also reaches Ready()
		// before this Tick's loop gets to e2.
		if err := hs.Reset(); err != nil {
			t.Fatalf("reset: %v", err)
		}
		if !hs.Ack(probe.last()) {
			t.Fatalf("ack on the new connection must succeed")
		}
	}
	b := dispatch.New(db, transport)
	poller := adapterclaude.NewPoller(db, b, hs, "conv-mid-batch-gen", "claude-session-a")

	attempted, err := poller.Tick(ctx)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(attempted) != 1 || attempted[0] != e1.ID {
		t.Fatalf("want only %s attempted before the generation changed, got %v", e1.ID, attempted)
	}
	if !hs.Ready() {
		t.Fatalf("the new connection should still be ready after Tick returns")
	}
	assertState(t, db, e1.ID, store.HandedOff)
	assertState(t, db, e2.ID, store.Queued)
}

// Regression for a finding on PR #3: Tick silently continued through every
// remaining candidate after the grant's budget was exhausted, repeating the
// same failing claim transaction for each and reporting them all as
// attempted even though exhaustion is meant to halt delivery pending human
// action.
func TestPollerStopsBatchOnBudgetExhaustion(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	conversation := "conv-budget-batch"
	if _, err := ctrl.Grant(context.Background(), controller.GrantParams{
		Conversation: conversation,
		PeerAID:      "codex-thread-b",
		PeerBID:      "claude-session-a",
		Direction:    store.Bidirectional,
		MaxExchanges: 1,
	}); err != nil {
		t.Fatalf("grant: %v", err)
	}
	ctx := context.Background()

	e1, err := bridge(t, db).Send(ctx, conversation, "codex-thread-b", "claude-session-a", "first", nil)
	if err != nil {
		t.Fatalf("send 1: %v", err)
	}
	e2, err := bridge(t, db).Send(ctx, conversation, "codex-thread-b", "claude-session-a", "second", nil)
	if err != nil {
		t.Fatalf("send 2: %v", err)
	}
	e3, err := bridge(t, db).Send(ctx, conversation, "codex-thread-b", "claude-session-a", "third", nil)
	if err != nil {
		t.Fatalf("send 3: %v", err)
	}

	probe := &probeRecorder{}
	hs := adapterclaude.NewHandshake(probe.send, time.Hour)
	if err := hs.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !hs.Ack(probe.last()) {
		t.Fatalf("ack must succeed")
	}

	transport := &fakeTransport{}
	b := dispatch.New(db, transport)
	poller := adapterclaude.NewPoller(db, b, hs, conversation, "claude-session-a")

	// e1 consumes the only budget slot; e2 discovers the exhaustion (a
	// genuine attempt that fails, so it's correctly counted as attempted);
	// e3 must never be touched at all — that's what stopping the batch
	// protects, not re-litigating the envelope that already found the
	// exhausted budget.
	attempted, err := poller.Tick(ctx)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(attempted) != 2 || attempted[0] != e1.ID || attempted[1] != e2.ID {
		t.Fatalf("want e1 and e2 attempted and the batch to stop before e3, got %v", attempted)
	}
	assertState(t, db, e1.ID, store.HandedOff)
	assertState(t, db, e2.ID, store.Queued)
	assertState(t, db, e3.ID, store.Queued)
	if transport.count() != 1 {
		t.Fatalf("want only 1 transport call (e1), got %d", transport.count())
	}
}

func bridge(t *testing.T, db *store.DB) *dispatch.Bridge {
	t.Helper()
	return dispatch.New(db, &fakeTransport{})
}

// transportFunc adapts a plain function to dispatch.Transport, for tests
// that need to inspect or act on the per-call context Poller passes in.
type transportFunc func(ctx context.Context, e store.Envelope) error

func (f transportFunc) Deliver(ctx context.Context, e store.Envelope) error {
	return f(ctx, e)
}

// Regression for a finding on PR #3: Tick's per-iteration readiness check
// runs before Dispatch, not around it — a Reset/Stop landing after that
// check passes but before (or during) the transport call it just authorized
// used to let that delivery complete as if the connection were still live.
// The check was a time-of-check, not a lease held across the actual I/O.
// This proves the fix: the context passed into the transport is derived
// from the handshake's per-generation context and is observably canceled
// the instant Stop() invalidates the generation this dispatch was
// authorized under, even though Stop() runs strictly after Tick's own
// check already passed.
func TestPollerCancelsInFlightDispatchWhenGenerationInvalidatedMidDelivery(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-cancel-mid-delivery")
	ctx := context.Background()

	e, err := bridge(t, db).Send(ctx, "conv-cancel-mid-delivery", "codex-thread-b", "claude-session-a", "hi", nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	probe := &probeRecorder{}
	hs := adapterclaude.NewHandshake(probe.send, time.Hour)
	if err := hs.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !hs.Ack(probe.last()) {
		t.Fatalf("ack must succeed")
	}

	started := make(chan struct{})
	var sawCancel bool
	transport := transportFunc(func(dctx context.Context, _ store.Envelope) error {
		close(started)
		select {
		case <-dctx.Done():
			sawCancel = true
		case <-time.After(2 * time.Second):
		}
		return dctx.Err()
	})
	b := dispatch.New(db, transport)
	poller := adapterclaude.NewPoller(db, b, hs, "conv-cancel-mid-delivery", "claude-session-a")

	done := make(chan struct{})
	go func() {
		poller.Tick(ctx)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatalf("transport never called")
	}

	// Lands strictly after Tick's own readiness/generation check already
	// passed for this envelope — exactly the window being closed.
	hs.Stop()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Tick did not return after Stop canceled the in-flight dispatch")
	}

	if !sawCancel {
		t.Fatalf("want the in-flight transport call's context canceled when Stop invalidated its generation")
	}
	assertState(t, db, e.ID, store.Failed)
}

// Regression for a finding on PR #3: sendProbe must run outside h.mu. A
// probe that blocks on a slow transport send must not also block
// Ready/Ack/Retries for the duration — reverting the fix (calling sendProbe
// while still holding the lock) makes this test time out.
func TestSendProbeRunsOutsideLock(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	hs := adapterclaude.NewHandshake(func(string) error {
		close(started)
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

	readyDone := make(chan struct{})
	go func() {
		hs.Ready()
		close(readyDone)
	}()
	select {
	case <-readyDone:
	case <-time.After(time.Second):
		t.Fatalf("Ready() blocked while sendProbe was in flight")
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("start: %v", err)
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
