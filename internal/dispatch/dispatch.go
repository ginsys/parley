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
func (b *Bridge) Dispatch(ctx context.Context, envelopeID string) (store.EnvelopeState, error) {
	claimedEnvelope, claimed, err := b.claim(ctx, envelopeID)
	if err != nil {
		if errors.Is(err, ErrBudgetExhausted) {
			return store.Queued, err
		}
		if errors.Is(err, ErrGrantExpired) {
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
	noAttempt := errors.Is(deliverErr, ErrNoAttempt)
	permanentlyRejected := errors.Is(deliverErr, ErrPermanentlyRejected)
	finalState := store.HandedOff
	switch {
	case deliverErr == nil:
		finalState = store.HandedOff
	case errors.Is(deliverErr, ErrAmbiguous):
		finalState = store.Uncertain
	case noAttempt:
		finalState = store.Queued
	case permanentlyRejected:
		finalState = store.Failed
	default:
		finalState = store.Failed
	}

	// Recording the outcome must survive ctx being canceled during Deliver
	// (e.g. a caller-imposed deadline, or a Claude-side reconnect
	// invalidating the connection an in-flight delivery was authorized
	// under) — Deliver has already returned a definite answer by this
	// point, and losing the ability to write it down would strand the
	// envelope in 'dispatching' forever, exactly the ambiguous-outcome
	// class this design exists to avoid. context.WithoutCancel detaches
	// from ctx's cancellation/deadline while keeping any values.
	recordCtx := context.WithoutCancel(ctx)
	tx, err := b.db.Begin(recordCtx)
	if err != nil {
		return "", fmt.Errorf("record dispatch outcome: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	switch {
	case noAttempt:
		// The host was never actually invoked: refund the budget slot
		// claim() consumed. Whether the envelope can go back to 'queued'
		// depends on whether the grant it was claimed under is still this
		// conversation's current one — a revoke or renewal can have torn it
		// down while Deliver was in flight.
		if err := store.RefundExchange(recordCtx, tx, claimedEnvelope.Conversation, claimedEnvelope.GrantVersion); err != nil {
			return "", err
		}
		version, ok, err := resolveRequeueVersion(recordCtx, tx, claimedEnvelope)
		if err != nil {
			return "", err
		}
		if ok {
			if _, err := store.RequeueUnattempted(recordCtx, tx, envelopeID, version, now); err != nil {
				return "", err
			}
			finalState = store.Queued
		} else {
			if err := store.SetState(recordCtx, tx, envelopeID, store.Cancelled, now); err != nil {
				return "", err
			}
			finalState = store.Cancelled
		}
	case permanentlyRejected:
		// Also never attempted, so also refund — but retrying can only ever
		// reproduce the same rejection, so this stays 'failed' rather than
		// going back to 'queued'.
		if err := store.RefundExchange(recordCtx, tx, claimedEnvelope.Conversation, claimedEnvelope.GrantVersion); err != nil {
			return "", err
		}
		if err := store.SetState(recordCtx, tx, envelopeID, finalState, now); err != nil {
			return "", err
		}
	default:
		if err := store.SetState(recordCtx, tx, envelopeID, finalState, now); err != nil {
			return "", err
		}
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	committed = true
	return finalState, nil
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
	if !e.TrustedReply {
		return 0, false, nil
	}
	if !g.PermitsDirection(e.FromPeer, e.ToPeer) {
		return 0, false, nil
	}
	return g.GrantVersion, true, nil
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
	if err != nil {
		return nil, false, err
	}
	if g.Expired(time.Now().UTC()) {
		// A grant that expires while a message sits queued must not deliver
		// it late — recheck at claim time, not only at accept time. Unlike
		// budget exhaustion, expiry is permanent for this grant version.
		//
		// An ordinary send has no path back once cancelled, so cancel it —
		// but a genuine reply's source turn is already permanently 'acked' by
		// IngestTurn, with no live sender left to notice a cancellation and
		// resubmit. Expiry alone doesn't change the grant's row status (still
		// 'active' until a Revoke/Renew says otherwise), so leaving the reply
		// untouched in 'queued' under this same version means a future
		// Renew's CarryForwardQueuedReplies (keyed on exactly this version,
		// since it's still the current one) can still rescue it. This only
		// blocks *this* dispatch attempt — the row itself is left as-is.
		//
		// e.TrustedReply, not e.InReplyTo != nil: only codex.IngestTurn ever
		// sets it, after validating in_reply_to against the exact original it
		// atomically acked. A caller-supplied InReplyTo on an ordinary Send
		// is not proof of a genuine reply and must be cancelled like any
		// other expired ordinary message, not preserved.
		if e.TrustedReply {
			return nil, false, ErrGrantExpired
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if err := store.SetState(ctx, tx, envelopeID, store.Cancelled, now); err != nil {
			return nil, false, err
		}
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		committed = true
		return nil, false, ErrGrantExpired
	}

	ok, err := store.ClaimExchange(ctx, tx, e.Conversation, e.GrantVersion)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		// e.State was already confirmed 'queued' above under this exclusive
		// transaction, so a revoke/renewal can't have raced us — any grant
		// no longer active would already have cancelled this row instead.
		// The only remaining reason ClaimExchange fails is budget
		// exhaustion. Leave the row queued — do not cancel or fail it.
		return nil, false, ErrBudgetExhausted
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
