// Package dispatch implements Parley's ordinary bridge operations: Send
// (either peer's adapter queues a message) and Dispatch (the single-process
// step that claims budget, hands the message to a Transport, and records
// the outcome). Unlike controller, nothing here writes a grant — Send only
// ever reads the current grant to stamp grant_version and reject a
// revoked/missing one.
package dispatch

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/ginsys/parley/internal/store"
)

// ErrAmbiguous marks a Transport.Deliver outcome where whether the host
// actually accepted the message can't be determined (e.g. a timeout after
// the underlying call may already have committed). Bridge records this as
// 'uncertain', never retries it automatically, and never releases its
// budget claim — see the design plan's Delivery section.
var ErrAmbiguous = errors.New("ambiguous transport outcome")

// ErrNoAttempt marks a Transport.Deliver outcome where the host was
// definitely never invoked for this message — rejected before the host call
// (e.g. an oversized payload) or the underlying process never started (e.g.
// a context canceled before exec forked it). Unlike ErrAmbiguous, this is
// not a maybe: the claimed budget slot was spent for nothing and the
// message itself was never at risk of duplicate delivery, so Dispatch
// refunds the slot and puts the envelope back in 'queued' rather than
// terminally failing it.
var ErrNoAttempt = errors.New("transport never attempted delivery")

// ErrBudgetExhausted is returned by Dispatch when the grant's max_exchanges
// has already been reached. The envelope is left queued, untouched — per
// the design plan, exhaustion halts delivery pending a human renewal, it
// does not fail or cancel the message.
var ErrBudgetExhausted = errors.New("grant budget exhausted")

// ErrGrantExpired is returned by Dispatch when the envelope's grant version
// has passed its ExpiresAt by claim time, even though it hadn't expired when
// the message was accepted (design plan §1's exact-version dispatch check
// covers a stale *version*, not a grant that ages out while the message sat
// queued). Unlike budget exhaustion, an expired grant version can never
// claim again — claim cancels the row rather than leaving it queued forever.
var ErrGrantExpired = errors.New("grant expired before dispatch")

// ErrPermanentlyRejected marks a Transport.Deliver outcome where the host
// was never invoked, and never will be no matter how many times this exact
// envelope is retried — the content itself makes delivery impossible (e.g.
// too large for the transport to ever send). Unlike ErrNoAttempt, refunding
// the budget and requeuing would only reproduce the identical rejection on
// the very next dispatch attempt, forever. Dispatch refunds the claimed
// budget slot (it was never actually spent) but leaves the envelope
// terminally 'failed' rather than requeuing it.
var ErrPermanentlyRejected = errors.New("transport permanently rejected the message; retrying cannot help")

// Transport is the one thing Bridge asks of a concrete adapter: hand this
// envelope's text to the actual host (codex queue, a Channels notification)
// and report whether it was accepted.
type Transport interface {
	Deliver(ctx context.Context, e store.Envelope) error
}

type Bridge struct {
	db        *store.DB
	transport Transport
}

func New(db *store.DB, t Transport) *Bridge {
	return &Bridge{db: db, transport: t}
}

var ErrNotPermitted = errors.New("grant does not permit this peer pair or direction")
var ErrStaleGrantVersion = errors.New("envelope grant version is no longer current")

// Send accepts a new message into the queue. It stamps grant_version from
// whatever is current right now; a later renewal cancels this row if it's
// still queued when the renewal commits (see controller.Renew). Send never
// calls the transport itself — that's Dispatch's job — so a crash between
// accept and delivery leaves the message safely queued, not lost.
func (b *Bridge) Send(ctx context.Context, conversation, from, to, text string, inReplyTo *string) (*store.Envelope, error) {
	tx, err := b.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	g, err := store.CurrentGrant(ctx, tx, conversation)
	if err != nil {
		return nil, err
	}

	if g.Expired(time.Now()) {
		return nil, ErrGrantExpired
	}
	if !g.Permits(from, to, time.Now()) {
		return nil, ErrNotPermitted
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	e := store.Envelope{
		ID:           uuid.NewString(),
		Conversation: conversation,
		FromPeer:     from,
		ToPeer:       to,
		Text:         text,
		GrantVersion: g.GrantVersion,
		InReplyTo:    inReplyTo,
		State:        store.Queued,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := store.InsertQueued(ctx, tx, e); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return &e, nil
}

// Dispatch attempts delivery of one queued envelope: claim its budget slot
// and transition it to 'dispatching' in a single BEGIN IMMEDIATE
// transaction, then call the transport outside that transaction, then
// record the outcome. Returns the envelope's state after the attempt.
//
// If the envelope is no longer 'queued' (already claimed, cancelled by a
// concurrent revoke/renew, or already terminal), Dispatch does nothing and
// returns its current state with no error — this is the serialization the
// design relies on, not a failure.
// Outcome exposes delivery state and bounded diagnostics without treating
// a queued/budget-exhausted candidate as a host attempt.
type Outcome struct {
	ID          string
	State       store.EnvelopeState
	Attempted   bool
	ErrorCode   string
	ErrorDetail string
}

func (b *Bridge) Dispatch(ctx context.Context, envelopeID string) (store.EnvelopeState, error) {
	outcome, err := b.DispatchOutcome(ctx, envelopeID)
	return outcome.State, err
}
func (b *Bridge) DispatchOutcome(ctx context.Context, envelopeID string) (Outcome, error) {
	outcome := Outcome{ID: envelopeID}
	state, err := b.dispatch(ctx, envelopeID, &outcome)
	outcome.State = state
	return outcome, err
}
func (b *Bridge) dispatch(ctx context.Context, envelopeID string, outcome *Outcome) (store.EnvelopeState, error) {
	claimedEnvelope, claimed, err := b.claim(ctx, envelopeID)
	if err != nil {
		if errors.Is(err, ErrBudgetExhausted) {
			return store.Queued, err
		}
		if errors.Is(err, ErrGrantExpired) || errors.Is(err, ErrNotPermitted) || errors.Is(err, ErrStaleGrantVersion) || errors.Is(err, store.ErrNoActiveGrant) {
			// claim() leaves a reply's row untouched on expiry (so a future
			// renewal can still carry it forward) but cancels an ordinary
			// send outright — ask the row itself what actually happened
			// rather than assuming which of the two this envelope was.
			state, stateErr := b.currentState(ctx, envelopeID)
			if stateErr != nil {
				return "", stateErr
			}
			return state, err
		}
		return "", err
	}
	if !claimed {
		return b.currentState(ctx, envelopeID)
	}

	deliverErr := b.transport.Deliver(ctx, *claimedEnvelope)
	settled, err := b.settle(context.WithoutCancel(ctx), claimedEnvelope, deliverErr)
	*outcome = settled
	return settled.State, err
}

func (b *Bridge) settle(ctx context.Context, claimed *store.Envelope, deliverErr error) (Outcome, error) {
	ambiguous := errors.Is(deliverErr, ErrAmbiguous)
	noAttempt := !ambiguous && errors.Is(deliverErr, ErrNoAttempt)
	permanent := !ambiguous && errors.Is(deliverErr, ErrPermanentlyRejected)
	outcome := Outcome{ID: claimed.ID, State: store.HandedOff, Attempted: !noAttempt && !permanent}
	switch {
	case deliverErr == nil:
	case errors.Is(deliverErr, ErrAmbiguous):
		outcome.State = store.Uncertain
		outcome.ErrorCode = "ambiguous"
		outcome.ErrorDetail = "Host acceptance could not be established; automatic retry is disabled."
	case noAttempt:
		outcome.State = store.Queued
		outcome.ErrorCode = "not_attempted"
		outcome.ErrorDetail = "Transport did not attempt host delivery."
	case permanent:
		outcome.State = store.Failed
		outcome.ErrorCode = "rejected"
		outcome.ErrorDetail = "Message was rejected before host delivery."
	default:
		outcome.State = store.Failed
		outcome.ErrorCode = "failed"
		outcome.ErrorDetail = "Transport reported a delivery failure."
	}
	tx, err := b.db.Begin(ctx)
	if err != nil {
		return outcome, fmt.Errorf("record dispatch outcome: %w", err)
	}
	defer tx.Rollback()
	version := claimed.GrantVersion
	if noAttempt {
		target, ok, err := resolveRequeueVersion(ctx, tx, claimed)
		if err != nil {
			return outcome, err
		}
		if ok {
			version = target
		} else {
			outcome.State = store.Cancelled
		}
	}
	changed, err := store.SettleDispatch(ctx, tx, claimed, outcome.State, version, outcome.ErrorCode, outcome.ErrorDetail, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return outcome, err
	}
	if !changed {
		current, err := store.GetByID(ctx, tx, claimed.ID)
		if err != nil {
			return outcome, err
		}
		outcome.State = current.State
		outcome.ErrorCode = current.ErrorCode
		outcome.ErrorDetail = current.ErrorDetail
		return outcome, nil
	}
	if noAttempt || permanent {
		if err := store.RefundExchange(ctx, tx, claimed.Conversation, claimed.GrantVersion); err != nil {
			return outcome, err
		}
	}
	if err := tx.Commit(); err != nil {
		return outcome, err
	}
	return outcome, nil
}

// resolveRequeueVersion decides whether an unattempted envelope can go back
// to 'queued', and under which grant_version. If the version it was claimed
// under is still the conversation's current one, nothing changed underneath
// it — reuse it unchanged. Otherwise a revoke or renewal committed while
// Deliver was in flight: an ordinary send has no path back, the same rule
// Renew already applies to any other row still queued under a superseded
// version (store.CancelQueuedUnderVersion), so it is not requeued. A reply
// is different — its source turn is already permanently 'acked' with no
// live sender left to resubmit it — so it is worth rescuing onto whatever
// grant is current now. "Is a reply" is judged by e.TrustedReply, not merely
// e.InReplyTo != nil: TrustedReply is set only by codex.IngestTurn, which
// validates in_reply_to against the specific original envelope it atomically
// acks before queuing this one. dispatch.Bridge.Send accepts an arbitrary
// caller-supplied inReplyTo with no validation, so an ordinary send naming
// any envelope's id — even a genuinely acked one — must not qualify for this
// rescue path either; it has to be cancelled like any other old-grant
// message.
//
// A trusted reply is re-stamped onto the successor version even if that
// grant is already expired, mirroring CarryForwardQueuedReplies's own
// expiry-agnostic carry-forward for rows still 'queued' under the old
// version: rejecting it here on expiry would discard a genuine reply outright
// when the row could instead sit 'queued' under the new version and let
// claim()'s own expiry check (which leaves a trusted reply's row untouched
// rather than cancelling it) keep it recoverable for whatever grant comes
// next, exactly as it already would have been had it never left 'queued' in
// the first place. Direction permission (peer_a/peer_b, revoked-ness) is
// still checked — an expired-but-otherwise-permitted grant is a valid
// requeue target, a grant that never permitted this direction at all is not.
func resolveRequeueVersion(ctx context.Context, tx *sql.Tx, e *store.Envelope) (int64, bool, error) {
	g, err := store.CurrentGrant(ctx, tx, e.Conversation)
	if err != nil {
		if errors.Is(err, store.ErrNoActiveGrant) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if g.GrantVersion == e.GrantVersion {
		return e.GrantVersion, true, nil
	}
	ok, err := store.CanCarryReply(ctx, tx, e, g.GrantVersion)
	return g.GrantVersion, ok, err
}

// claim runs the atomic budget-claim + dispatching transition. Returns
// claimed=false (no error) for either ErrBudgetExhausted's cause or a
// concurrent state change — callers distinguish by re-reading state.
func (b *Bridge) claim(ctx context.Context, envelopeID string) (*store.Envelope, bool, error) {
	tx, err := b.db.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	e, err := store.GetByID(ctx, tx, envelopeID)
	if err != nil {
		return nil, false, err
	}
	if e.State != store.Queued {
		return nil, false, nil
	}

	g, err := store.CurrentGrant(ctx, tx, e.Conversation)
	authErr := err
	if err == nil {
		authErr = authorizeEnvelope(g, e, time.Now())
	}
	if authErr != nil {
		if !errors.Is(authErr, store.ErrNoActiveGrant) && !errors.Is(authErr, ErrGrantExpired) && !errors.Is(authErr, ErrNotPermitted) && !errors.Is(authErr, ErrStaleGrantVersion) {
			return nil, false, authErr
		}
		if errors.Is(authErr, ErrGrantExpired) && e.TrustedReply {
			return nil, false, authErr
		}
		if err := store.SetState(ctx, tx, e.ID, store.Queued, store.Cancelled, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return nil, false, err
		}
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		committed = true
		return nil, false, authErr
	}

	ok, err := store.ClaimExchange(ctx, tx, e.Conversation, e.GrantVersion)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		current, err := store.CurrentGrant(ctx, tx, e.Conversation)
		if err != nil {
			return nil, false, err
		}
		if err := authorizeEnvelope(current, e, time.Now()); err != nil {
			return nil, false, err
		}
		if current.ExchangesUsed >= current.MaxExchanges {
			return nil, false, ErrBudgetExhausted
		}
		return nil, false, fmt.Errorf("budget claim failed without exhaustion")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	transitioned, err := store.TransitionToDispatching(ctx, tx, envelopeID, now)
	if err != nil {
		return nil, false, err
	}
	if !transitioned {
		// Can't happen under a single BEGIN IMMEDIATE writer without a bug
		// elsewhere, since nothing else could have changed this row between
		// the two statements above within the same transaction.
		return nil, false, fmt.Errorf("dispatch: envelope %s left queued state between claim and transition", envelopeID)
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	committed = true
	e.State = store.Dispatching
	e.DispatchAttempt++
	return e, true, nil
}

func (b *Bridge) currentState(ctx context.Context, envelopeID string) (store.EnvelopeState, error) {
	tx, err := b.db.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	e, err := store.GetByID(ctx, tx, envelopeID)
	if err != nil {
		return "", err
	}
	return e.State, nil
}

func authorizeEnvelope(g *store.Grant, e *store.Envelope, now time.Time) error {
	if e.GrantVersion != g.GrantVersion {
		return ErrStaleGrantVersion
	}
	if !g.PermitsDirection(e.FromPeer, e.ToPeer) {
		return ErrNotPermitted
	}
	if g.Expired(now) {
		return ErrGrantExpired
	}
	return nil
}
