package dispatch_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

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
		tx.Rollback()
		t.Fatalf("get: %v", err)
	}
	tx.Rollback()
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
	if err := tx.Commit(); err != nil {
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
	defer tx2.Rollback()
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
	defer tx.Rollback()
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

// ackEnvelope inserts and immediately acks an envelope directly, standing in
// for the original message a genuine BRIDGE-REPLY responds to — IngestTurn
// always acks the original atomically before queuing its reply, and
// CarryForwardQueuedReplies now requires that provenance (see the finding
// 3973918530 regression test below), so any test exercising a reply's
// carry-forward/rescue path needs a real acked row behind it, not a bare
// InReplyTo string.
func ackEnvelope(t *testing.T, db *store.DB, conversation, id string, grantVersion int64) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	e := store.Envelope{
		ID: id, Conversation: conversation, FromPeer: "claude-session-a", ToPeer: "codex-thread-b",
		Text: "original", GrantVersion: grantVersion, State: store.Queued,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := store.InsertQueued(ctx, tx, e); err != nil {
		t.Fatalf("insert original: %v", err)
	}
	if err := store.SetState(ctx, tx, id, store.Acked, now); err != nil {
		t.Fatalf("ack original: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// sendTrustedReply inserts a queued reply envelope with TrustedReply set,
// standing in for what codex.IngestTurn does atomically (ack the original,
// then queue this reply) — dispatch.Bridge.Send has no parameter for this
// column and can never produce one, by design (see the finding
// 3973918513/3973918530-follow-up regression tests below), so tests that
// need a genuine reply for the carry-forward/rescue paths must construct one
// directly rather than through Send.
func sendTrustedReply(t *testing.T, db *store.DB, conversation, from, to, text, inReplyTo string, grantVersion int64) *store.Envelope {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	e := store.Envelope{
		ID: uuid.NewString(), Conversation: conversation, FromPeer: from, ToPeer: to, Text: text,
		GrantVersion: grantVersion, InReplyTo: &inReplyTo, TrustedReply: true, State: store.Queued,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := store.InsertQueued(ctx, tx, e); err != nil {
		t.Fatalf("insert trusted reply: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	committed = true
	return &e
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
	ackEnvelope(t, db, "conv-reply-renew", "original-envelope-id", 1)

	transport := newFakeTransport()
	bridge := dispatch.New(db, transport)
	ctx := context.Background()

	reply := sendTrustedReply(t, db, "conv-reply-renew", "b", "a", "reply text", "original-envelope-id", 1)
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

// Regression for finding 3973918513 on PR #3: the ErrNoAttempt refund/requeue
// fix above didn't check whether the grant version an envelope was claimed
// under was still the conversation's active one before resurrecting it as
// 'queued'. A renewal that commits while Deliver is in flight tears down
// that old version — resurrecting an ordinary send under it would strand the
// row forever (ClaimExchange never matches a superseded version again, and
// a later renewal's CarryForwardQueuedReplies only ever matches the version
// it is itself superseding, never this stale one). An ordinary send has no
// path back once its grant version is gone — same rule Renew already
// applies to any other row still queued under an old version — so it must
// be cancelled, not resurrected.
func TestDispatchCancelsUnattemptedOrdinarySendWhenGrantRenewedMidFlight(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-no-attempt-renewed", 5)
	ctx := context.Background()

	transport := transportFunc(func(dctx context.Context, e store.Envelope) error {
		if _, err := ctrl.Renew(context.Background(), controller.RenewParams{Conversation: "conv-no-attempt-renewed", MaxExchanges: 5}); err != nil {
			t.Fatalf("renew during deliver: %v", err)
		}
		return dispatch.ErrNoAttempt
	})
	bridge := dispatch.New(db, transport)

	e, err := bridge.Send(ctx, "conv-no-attempt-renewed", "a", "b", "ordinary send", nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	state, err := bridge.Dispatch(ctx, e.ID)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if state != store.Cancelled {
		t.Fatalf("want cancelled once its grant version is torn down mid-flight, got %s", state)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	var used int64
	if err := tx.QueryRowContext(ctx,
		`SELECT exchanges_used FROM grants WHERE conversation = ? AND grant_version = ?`,
		"conv-no-attempt-renewed", int64(1)).Scan(&used); err != nil {
		t.Fatalf("query old grant: %v", err)
	}
	if used != 0 {
		t.Fatalf("want the claimed slot refunded on the superseded grant version, got exchanges_used=%d", used)
	}
}

// Regression for finding 3973918513 on PR #3, reply side: unlike an ordinary
// send, a reply has no live sender left to resubmit it if cancelled (its
// source turn is already permanently 'acked'), so when the grant version it
// was claimed under gets superseded mid-flight, it must be rescued onto
// whatever grant is current now — the same rescue a renewal's own
// CarryForwardQueuedReplies performs for a still-queued reply — rather than
// stranded under the dead version or cancelled outright.
func TestDispatchRescuesUnattemptedReplyOntoRenewedGrantVersion(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-reply-rescue", 5)
	ackEnvelope(t, db, "conv-reply-rescue", "original-envelope-id", 1)
	ctx := context.Background()

	renewed := false
	transport := transportFunc(func(dctx context.Context, e store.Envelope) error {
		if !renewed {
			renewed = true
			if _, err := ctrl.Renew(context.Background(), controller.RenewParams{Conversation: "conv-reply-rescue", MaxExchanges: 5}); err != nil {
				t.Fatalf("renew during deliver: %v", err)
			}
			return dispatch.ErrNoAttempt
		}
		return nil
	})
	bridge := dispatch.New(db, transport)

	reply := sendTrustedReply(t, db, "conv-reply-rescue", "codex-thread-b", "claude-session-a", "reply text", "original-envelope-id", 1)

	state, err := bridge.Dispatch(ctx, reply.ID)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if state != store.Queued {
		t.Fatalf("want the reply rescued back to queued under the renewed grant, got %s", state)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	got, err := store.GetByID(ctx, tx, reply.ID)
	tx.Rollback()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.GrantVersion != 2 {
		t.Fatalf("want the reply re-stamped onto the renewed grant version 2, got %d", got.GrantVersion)
	}

	state2, err := bridge.Dispatch(ctx, reply.ID)
	if err != nil {
		t.Fatalf("redispatch: %v", err)
	}
	if state2 != store.HandedOff {
		t.Fatalf("want the rescued reply to dispatch successfully under the new grant, got %s", state2)
	}
}

// Regression for a finding on PR #3's fourth review round: rescuing a
// trusted reply onto a renewed grant must not itself check the successor
// grant's expiry — an expired-but-otherwise-permitted successor is still a
// valid requeue target, with claim()'s own expiry check (which leaves a
// trusted reply's row untouched rather than cancelling it) governing whether
// it can actually dispatch. Rejecting it here instead would discard the
// reply outright the moment the renewal itself was already-expired at
// commit time, rather than leaving it queued and rescuable by a further
// renewal exactly as TestGrantExpiryPreservesQueuedReplyForRenewal already
// guarantees for a row that never left 'queued' at all.
func TestDispatchRescuesUnattemptedReplyOntoAlreadyExpiredSuccessorGrant(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-reply-rescue-expired", 5)
	ackEnvelope(t, db, "conv-reply-rescue-expired", "original-envelope-id", 1)
	ctx := context.Background()

	renewed := false
	transport := transportFunc(func(dctx context.Context, e store.Envelope) error {
		if !renewed {
			renewed = true
			past := time.Now().UTC().Add(-time.Hour)
			if _, err := ctrl.Renew(context.Background(), controller.RenewParams{
				Conversation: "conv-reply-rescue-expired", MaxExchanges: 5, ExpiresAt: &past,
			}); err != nil {
				t.Fatalf("renew during deliver: %v", err)
			}
			return dispatch.ErrNoAttempt
		}
		return nil
	})
	bridge := dispatch.New(db, transport)

	reply := sendTrustedReply(t, db, "conv-reply-rescue-expired", "codex-thread-b", "claude-session-a", "reply text", "original-envelope-id", 1)

	state, err := bridge.Dispatch(ctx, reply.ID)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if state != store.Queued {
		t.Fatalf("want the reply rescued back to queued under the already-expired successor grant, got %s", state)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	got, err := store.GetByID(ctx, tx, reply.ID)
	tx.Rollback()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.GrantVersion != 2 {
		t.Fatalf("want the reply re-stamped onto the successor grant version 2 despite its expiry, got %d", got.GrantVersion)
	}

	state2, err := bridge.Dispatch(ctx, reply.ID)
	if !errors.Is(err, dispatch.ErrGrantExpired) {
		t.Fatalf("want ErrGrantExpired dispatching against the expired successor, got %v", err)
	}
	if state2 != store.Queued {
		t.Fatalf("want the reply left queued (rescuable by a further renewal), got %s", state2)
	}
}

// Companion to the rescue test above: when the grant is torn down mid-flight
// by a revoke rather than a renewal, there is no successor grant to rescue
// the reply onto — it must be cancelled, the same terminal outcome a
// revoke gives every other undelivered row for that conversation.
func TestDispatchCancelsUnattemptedReplyWhenNoActiveGrantSurvives(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-reply-revoked", 5)
	ackEnvelope(t, db, "conv-reply-revoked", "original-envelope-id", 1)
	ctx := context.Background()

	transport := transportFunc(func(dctx context.Context, e store.Envelope) error {
		if _, err := ctrl.Revoke(context.Background(), "conv-reply-revoked"); err != nil {
			t.Fatalf("revoke during deliver: %v", err)
		}
		return dispatch.ErrNoAttempt
	})
	bridge := dispatch.New(db, transport)

	original := "original-envelope-id"
	reply, err := bridge.Send(ctx, "conv-reply-revoked", "codex-thread-b", "claude-session-a", "reply text", &original)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	state, err := bridge.Dispatch(ctx, reply.ID)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if state != store.Cancelled {
		t.Fatalf("want cancelled once there is no active grant left to rescue it onto, got %s", state)
	}
}

// Regression for finding 3973918534 on PR #3: cancelling any queued
// envelope outright when its grant expires at claim time is correct for an
// ordinary send (TestGrantExpiryRecheckedAtDispatch above), but a reply has
// no live sender left to resubmit it if cancelled — its source turn is
// already permanently 'acked'. It must instead be left 'queued' under the
// expired-but-not-yet-superseded grant version so a future renewal's
// CarryForwardQueuedReplies (keyed on exactly that version, since expiry
// alone never changes the grant row's status) can still rescue it.
func TestGrantExpiryPreservesQueuedReplyForRenewal(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	ctx := context.Background()

	past := time.Now().UTC().Add(-time.Hour)
	if _, err := ctrl.Grant(ctx, controller.GrantParams{
		Conversation: "conv-expiry-reply",
		PeerAID:      "claude-session-a",
		PeerBID:      "codex-thread-b",
		Direction:    store.Bidirectional,
		MaxExchanges: 10,
		ExpiresAt:    &past,
	}); err != nil {
		t.Fatalf("grant: %v", err)
	}
	ackEnvelope(t, db, "conv-expiry-reply", "original-envelope-id", 1)

	transport := newFakeTransport()
	bridge := dispatch.New(db, transport)

	reply := sendTrustedReply(t, db, "conv-expiry-reply", "codex-thread-b", "claude-session-a", "reply after expiry", "original-envelope-id", 1)

	state, err := bridge.Dispatch(ctx, reply.ID)
	if !errors.Is(err, dispatch.ErrGrantExpired) {
		t.Fatalf("want ErrGrantExpired, got %v", err)
	}
	if state != store.Queued {
		t.Fatalf("want the reply left queued (rescuable by a future renewal), got %s", state)
	}
	if len(transport.delivered) != 0 {
		t.Fatalf("transport must not have been called for an expired-grant envelope")
	}

	future := time.Now().UTC().Add(time.Hour)
	if _, err := ctrl.Renew(ctx, controller.RenewParams{Conversation: "conv-expiry-reply", MaxExchanges: 10, ExpiresAt: &future}); err != nil {
		t.Fatalf("renew: %v", err)
	}

	state2, err := bridge.Dispatch(ctx, reply.ID)
	if err != nil {
		t.Fatalf("dispatch after renewal: %v", err)
	}
	if state2 != store.HandedOff {
		t.Fatalf("want the reply carried forward and delivered after renewal, got %s", state2)
	}
}

// Regression for finding 3973918518 on PR #3: an oversized message rejected
// before exec is ever attempted must be refunded (it never spent its budget
// slot) but left terminally 'failed', not requeued — unlike a transient
// never-attempted cause, its size will never shrink on retry, so requeuing
// it would loop forever reproducing the identical rejection.
func TestDispatchFailsPermanentlyRejectedWithoutRequeueLoop(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-permanent-reject", 1)
	ctx := context.Background()

	transport := transportFunc(func(dctx context.Context, e store.Envelope) error {
		return fmt.Errorf("wrap: %w", dispatch.ErrPermanentlyRejected)
	})
	bridge := dispatch.New(db, transport)

	e, err := bridge.Send(ctx, "conv-permanent-reject", "a", "b", "too big", nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	state, err := bridge.Dispatch(ctx, e.ID)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if state != store.Failed {
		t.Fatalf("want failed (permanent rejection), got %s", state)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	g, err := store.CurrentGrant(ctx, tx, "conv-permanent-reject")
	tx.Rollback()
	if err != nil {
		t.Fatalf("current grant: %v", err)
	}
	if g.ExchangesUsed != 0 {
		t.Fatalf("want the claimed budget slot refunded (never actually attempted), got exchanges_used=%d", g.ExchangesUsed)
	}

	state2, err := bridge.Dispatch(ctx, e.ID)
	if err != nil {
		t.Fatalf("second dispatch: %v", err)
	}
	if state2 != store.Failed {
		t.Fatalf("want it to remain failed, not silently re-attempted in a loop, got %s", state2)
	}
}

// Regression for finding 3973918530 on PR #3: CarryForwardQueuedReplies
// previously trusted a bare non-nil in_reply_to as proof of being a genuine
// reply, but dispatch.Bridge.Send takes an arbitrary caller-supplied
// inReplyTo with no validation — an ordinary send naming an unrelated
// envelope (here, one that was never acked at all) must be cancelled by a
// renewal like any other old-grant message, not silently carried forward as
// if it were a real reply.
func TestRenewDoesNotCarryForwardFakeReplyWithUnackedOriginal(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-fake-reply", 10)
	ctx := context.Background()

	transport := newFakeTransport()
	bridge := dispatch.New(db, transport)

	notActuallyAcked := "not-an-acked-envelope"
	forged, err := bridge.Send(ctx, "conv-fake-reply", "a", "b", "pretending to be a reply", &notActuallyAcked)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	if _, err := ctrl.Renew(ctx, controller.RenewParams{Conversation: "conv-fake-reply", MaxExchanges: 10}); err != nil {
		t.Fatalf("renew: %v", err)
	}

	state, err := bridge.Dispatch(ctx, forged.ID)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if state != store.Cancelled {
		t.Fatalf("want a fake reply (no acked original) cancelled by the renewal like any other old-grant message, got %s", state)
	}
}

// Regression for a follow-up finding on PR #3 (3973918513/3973918530's
// re-review): the acked-original check above is still not real provenance —
// it only proved the referenced envelope was acked *somewhere*, not that
// this row was the one atomically produced by IngestTurn for it. When the
// conversation already contains a genuinely acked envelope (e.g. from an
// earlier real exchange), an ordinary Send naming that same id in
// InReplyTo must still be cancelled by a renewal like any other old-grant
// message, not carried forward just because the id happens to resolve.
func TestRenewDoesNotCarryForwardOrdinarySendEvenNamingAGenuinelyAckedOriginal(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-fake-reply-real-target", 10)
	ackEnvelope(t, db, "conv-fake-reply-real-target", "genuinely-acked-envelope", 1)
	ctx := context.Background()

	transport := newFakeTransport()
	bridge := dispatch.New(db, transport)

	target := "genuinely-acked-envelope"
	forged, err := bridge.Send(ctx, "conv-fake-reply-real-target", "a", "b", "pretending to be a reply", &target)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	if _, err := ctrl.Renew(ctx, controller.RenewParams{Conversation: "conv-fake-reply-real-target", MaxExchanges: 10}); err != nil {
		t.Fatalf("renew: %v", err)
	}

	state, err := bridge.Dispatch(ctx, forged.ID)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if state != store.Cancelled {
		t.Fatalf("want an ordinary send cancelled by the renewal even though its InReplyTo names a genuinely acked envelope, got %s", state)
	}
}

// Companion to the above for the never-attempted rescue path
// (resolveRequeueVersion): the same insufficient check — any InReplyTo that
// resolves to an acked envelope — must not let an ordinary send get rescued
// onto a renewed grant version either.
func TestDispatchDoesNotRescueOrdinarySendEvenNamingAGenuinelyAckedOriginal(t *testing.T) {
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "conv-fake-rescue-real-target", 5)
	ackEnvelope(t, db, "conv-fake-rescue-real-target", "genuinely-acked-envelope", 1)
	ctx := context.Background()

	transport := transportFunc(func(dctx context.Context, e store.Envelope) error {
		if _, err := ctrl.Renew(context.Background(), controller.RenewParams{Conversation: "conv-fake-rescue-real-target", MaxExchanges: 5}); err != nil {
			t.Fatalf("renew during deliver: %v", err)
		}
		return dispatch.ErrNoAttempt
	})
	bridge := dispatch.New(db, transport)

	target := "genuinely-acked-envelope"
	forged, err := bridge.Send(ctx, "conv-fake-rescue-real-target", "a", "b", "pretending to be a reply", &target)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	state, err := bridge.Dispatch(ctx, forged.ID)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if state != store.Cancelled {
		t.Fatalf("want an ordinary send cancelled rather than rescued, even though its InReplyTo names a genuinely acked envelope, got %s", state)
	}
}
