package dispatch_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/dispatch"
	"github.com/ginsys/parley/internal/store"
)

// fakeTransport records every delivered envelope and can be told to fail or
// hang (ambiguous) for specific envelope ids.
type fakeTransport struct {
	mu        sync.Mutex
	delivered []string
	fail      map[string]bool
	ambiguous map[string]bool
	noAttempt map[string]bool
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{fail: map[string]bool{}, ambiguous: map[string]bool{}, noAttempt: map[string]bool{}}
}

func (f *fakeTransport) Deliver(ctx context.Context, e store.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ambiguous[e.ID] {
		return fmt.Errorf("wrap: %w", dispatch.ErrAmbiguous)
	}
	if f.noAttempt[e.ID] {
		return fmt.Errorf("wrap: %w", dispatch.ErrNoAttempt)
	}
	if f.fail[e.ID] {
		return errors.New("delivery refused")
	}
	f.delivered = append(f.delivered, e.ID)
	return nil
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

func grantOne(t *testing.T, ctrl *controller.Controller, conversation string, maxExchanges int64) *store.Grant {
	t.Helper()
	g, err := ctrl.Grant(context.Background(), controller.GrantParams{
		Conversation: conversation,
		PeerAID:      "claude-session-a",
		PeerBID:      "codex-thread-b",
		Direction:    store.Bidirectional,
		MaxExchanges: maxExchanges,
	})
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	return g
}

// Fixture 3: concurrent budget exhaustion — two sends racing against the
// last remaining budget slot must not both succeed.
func TestConcurrentBudgetExhaustion(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-budget", 1)

	transport := newFakeTransport()
	bridge := dispatch.New(db, transport)
	ctx := context.Background()

	e1, err := bridge.Send(ctx, "conv-budget", "a", "b", "first", nil)
	if err != nil {
		t.Fatalf("send 1: %v", err)
	}
	e2, err := bridge.Send(ctx, "conv-budget", "a", "b", "second", nil)
	if err != nil {
		t.Fatalf("send 2: %v", err)
	}

	var wg sync.WaitGroup
	results := make(chan struct {
		state store.EnvelopeState
		err   error
	}, 2)
	for _, id := range []string{e1.ID, e2.ID} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			state, err := bridge.Dispatch(ctx, id)
			results <- struct {
				state store.EnvelopeState
				err   error
			}{state, err}
		}(id)
	}
	wg.Wait()
	close(results)

	handedOff, exhausted := 0, 0
	for r := range results {
		switch {
		case r.err == nil && r.state == store.HandedOff:
			handedOff++
		case errors.Is(r.err, dispatch.ErrBudgetExhausted):
			exhausted++
		default:
			t.Fatalf("unexpected outcome: state=%v err=%v", r.state, r.err)
		}
	}
	if handedOff != 1 || exhausted != 1 {
		t.Fatalf("want exactly 1 handed_off and 1 exhausted, got %d handed_off, %d exhausted", handedOff, exhausted)
	}
	if len(transport.delivered) != 1 {
		t.Fatalf("transport.Deliver called %d times, want 1", len(transport.delivered))
	}
}

// Fixture 4: revoke-vs-dispatch — revoke concurrent with an in-flight send
// must produce exactly one of the three named outcomes, and any other
// queued row must end up cancelled on disk.
func TestRevokeVsDispatch(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-revoke", 10)

	transport := newFakeTransport()
	bridge := dispatch.New(db, transport)
	ctx := context.Background()

	dispatched, err := bridge.Send(ctx, "conv-revoke", "a", "b", "will be dispatching", nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	stillQueued, err := bridge.Send(ctx, "conv-revoke", "a", "b", "will be cancelled", nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	if _, err := bridge.Dispatch(ctx, dispatched.ID); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	result, err := ctrl.Revoke(ctx, "conv-revoke")
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if result.Cancelled != 1 {
		t.Fatalf("want 1 cancelled, got %d", result.Cancelled)
	}
	if result.AlreadyHandedOff != 1 {
		t.Fatalf("want 1 already handed off, got %d", result.AlreadyHandedOff)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	e, err := store.GetByID(ctx, tx, stillQueued.ID)
	if err != nil {
		tx.Rollback(ctx)
		t.Fatalf("get: %v", err)
	}
	tx.Rollback(ctx)
	if e.State != store.Cancelled {
		t.Fatalf("want cancelled on disk, got %s", e.State)
	}

	if _, err := bridge.Send(ctx, "conv-revoke", "a", "b", "after revoke", nil); !errors.Is(err, store.ErrNoActiveGrant) {
		t.Fatalf("send after revoke: want ErrNoActiveGrant, got %v", err)
	}
}

// Fixture 5: crash-after-handoff — an envelope left in 'dispatching' when
// the process died must be recovered as 'uncertain', never silently
// 'queued' or 'handed_off'.
func TestCrashAfterHandoffRecoversUncertain(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-crash", 5)

	transport := newFakeTransport()
	bridge := dispatch.New(db, transport)
	ctx := context.Background()

	e, err := bridge.Send(ctx, "conv-crash", "a", "b", "in flight when we died", nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// Simulate the crash: the pre-dispatch transaction committed
	// ('dispatching'), but the process died before the host call's outcome
	// was recorded. Reach into the row directly, the same state Dispatch's
	// first phase would have left it in.
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := store.SetState(ctx, tx, e.ID, store.Dispatching, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatalf("set dispatching: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	n, err := db.RecoverUncertain(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 row recovered, got %d", n)
	}

	tx2, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx2.Rollback(ctx)
	got, err := store.GetByID(ctx, tx2, e.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.State != store.Uncertain {
		t.Fatalf("want uncertain, got %s", got.State)
	}
}

// Regression for a finding on PR #3 (poller.go's dispatch-lease fix): the
// context passed to Deliver is also reused, unchanged, for the transaction
// that records Deliver's outcome. A caller-side cancellation (e.g. a
// deadline, or a Claude-side reconnect canceling an in-flight dispatch's
// context) landing during Deliver used to also kill that recording
// transaction, stranding the envelope in 'dispatching' forever — the exact
// class of ambiguous, un-recorded outcome this design exists to prevent.
// Deliver has already returned a definite answer by the time the recording
// transaction runs; losing the ability to write it down is a bug, not a
// faithful propagation of the cancellation.
func TestDispatchRecordsOutcomeDespiteContextCanceledDuringDeliver(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-cancel-during-deliver", 5)

	ctx, cancel := context.WithCancel(context.Background())
	transport := transportFunc(func(dctx context.Context, e store.Envelope) error {
		cancel() // simulate the ctx being invalidated while Deliver is running
		return errors.New("delivery refused")
	})
	bridge := dispatch.New(db, transport)

	e, err := bridge.Send(context.Background(), "conv-cancel-during-deliver", "a", "b", "hi", nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	state, err := bridge.Dispatch(ctx, e.ID)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if state != store.Failed {
		t.Fatalf("want failed, got %s", state)
	}

	tx, err := db.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(context.Background())
	got, err := store.GetByID(context.Background(), tx, e.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.State != store.Failed {
		t.Fatalf("want the envelope durably recorded failed, not stuck at %s", got.State)
	}
}

// transportFunc adapts a plain function to dispatch.Transport.
type transportFunc func(ctx context.Context, e store.Envelope) error

func (f transportFunc) Deliver(ctx context.Context, e store.Envelope) error {
	return f(ctx, e)
}

// Fixture 6: stale grant version — a message accepted under version N,
// dispatched after a renewal to N+1, must be cancelled by the renewal, not
// dispatched under the new grant's terms.
func TestStaleGrantVersionCancelledByRenewal(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-renew", 10)

	transport := newFakeTransport()
	bridge := dispatch.New(db, transport)
	ctx := context.Background()

	stale, err := bridge.Send(ctx, "conv-renew", "a", "b", "queued under v1", nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if stale.GrantVersion != 1 {
		t.Fatalf("want grant_version 1, got %d", stale.GrantVersion)
	}

	if _, err := ctrl.Renew(ctx, controller.RenewParams{Conversation: "conv-renew", MaxExchanges: 20}); err != nil {
		t.Fatalf("renew: %v", err)
	}

	state, err := bridge.Dispatch(ctx, stale.ID)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if state != store.Cancelled {
		t.Fatalf("want cancelled (stale grant version), got %s", state)
	}
	if len(transport.delivered) != 0 {
		t.Fatalf("transport must not have been called for a stale-version envelope")
	}
}

// Regression for a finding on the merge-triggered review: a reply queued
// under a grant version that gets renewed before it's dispatched must be
// carried forward to the new version, not cancelled — the reply's own
// originating envelope is already permanently 'acked' by IngestTurn, so
// unlike an ordinary Send, there's no live sender left to resubmit it if
// cancelled.
func TestRenewCarriesForwardQueuedReplyInsteadOfCancelling(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-reply-renew", 10)

	transport := newFakeTransport()
	bridge := dispatch.New(db, transport)
	ctx := context.Background()

	original := "original-envelope-id"
	reply, err := bridge.Send(ctx, "conv-reply-renew", "b", "a", "reply text", &original)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if reply.GrantVersion != 1 {
		t.Fatalf("want grant_version 1, got %d", reply.GrantVersion)
	}

	if _, err := ctrl.Renew(ctx, controller.RenewParams{Conversation: "conv-reply-renew", MaxExchanges: 20}); err != nil {
		t.Fatalf("renew: %v", err)
	}

	state, err := bridge.Dispatch(ctx, reply.ID)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if state != store.HandedOff {
		t.Fatalf("want the reply carried forward and dispatched under the new grant, got %s", state)
	}
	if len(transport.delivered) != 1 || transport.delivered[0] != reply.ID {
		t.Fatalf("want the reply delivered exactly once, got %v", transport.delivered)
	}
}

// Regression for a finding on review 5160464724's follow-up: a reply
// accepted (ingested) while the grant was still valid but not dispatched
// until after ExpiresAt has passed must not be delivered late — expiry must
// be rechecked atomically at claim time, not only at accept/ingest time.
// Unlike budget exhaustion, an expired grant version can never claim again,
// so the envelope is cancelled rather than left queued forever.
func TestGrantExpiryRecheckedAtDispatch(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	ctx := context.Background()

	past := time.Now().UTC().Add(-time.Hour)
	if _, err := ctrl.Grant(ctx, controller.GrantParams{
		Conversation: "conv-expiry-dispatch",
		PeerAID:      "claude-session-a",
		PeerBID:      "codex-thread-b",
		Direction:    store.Bidirectional,
		MaxExchanges: 10,
		ExpiresAt:    &past,
	}); err != nil {
		t.Fatalf("grant: %v", err)
	}

	transport := newFakeTransport()
	bridge := dispatch.New(db, transport)

	e, err := bridge.Send(ctx, "conv-expiry-dispatch", "a", "b", "queued after expiry", nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	state, err := bridge.Dispatch(ctx, e.ID)
	if !errors.Is(err, dispatch.ErrGrantExpired) {
		t.Fatalf("want ErrGrantExpired, got %v", err)
	}
	if state != store.Cancelled {
		t.Fatalf("want cancelled (expired grant), got %s", state)
	}
	if len(transport.delivered) != 0 {
		t.Fatalf("transport must not have been called for an expired-grant envelope")
	}
}

// Regression for a finding on review 5160464724's follow-up: a transport
// outcome that definitely never reached the host (e.g. a canceled context
// before the process started, or a payload rejected before ever calling the
// host) must be requeued with its budget slot refunded, not left as a
// terminal failure that permanently loses the message.
func TestDispatchRequeuesAndRefundsNeverAttemptedDelivery(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-no-attempt", 1)

	transport := newFakeTransport()
	bridge := dispatch.New(db, transport)
	ctx := context.Background()

	e, err := bridge.Send(ctx, "conv-no-attempt", "a", "b", "never attempted", nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	transport.noAttempt[e.ID] = true

	state, err := bridge.Dispatch(ctx, e.ID)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if state != store.Queued {
		t.Fatalf("want the envelope back in queued, got %s", state)
	}
	delete(transport.noAttempt, e.ID)

	got, err := bridge.Dispatch(ctx, e.ID)
	if err != nil {
		t.Fatalf("redispatch after refund: %v", err)
	}
	if got != store.HandedOff {
		t.Fatalf("want the budget refund to allow a real retry to succeed, got %s", got)
	}
	if len(transport.delivered) != 1 || transport.delivered[0] != e.ID {
		t.Fatalf("want exactly one real delivery after the refund, got %v", transport.delivered)
	}
}
