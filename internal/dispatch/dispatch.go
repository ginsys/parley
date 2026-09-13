package dispatch

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ginsys/parley/internal/store"
)

// ErrAmbiguous marks a Transport.Deliver outcome where whether the host
// actually accepted the message can't be determined (e.g. a timeout after
// the underlying call may already have committed). Bridge records this as
// 'uncertain', never retries it automatically, and never releases its
// budget claim — see docs/architecture.md.
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
// the delivery contract, exhaustion halts delivery pending a human renewal, it
// does not fail or cancel the message.
var ErrBudgetExhausted = errors.New("grant budget exhausted")

// ErrGrantExpired is returned by Dispatch when the envelope's grant version
// has passed its ExpiresAt by claim time, even though it hadn't expired when
// the message was accepted (the exact-version dispatch check
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

type settlementStore struct {
	db      *store.DB
	queries store.Queries
}

func newSettlement(db *store.DB) *settlementStore {
	return &settlementStore{db: db, queries: db.Queries()}
}

var ErrNotPermitted = errors.New("grant does not permit this peer pair or direction")
var ErrStaleGrantVersion = errors.New("envelope grant version is no longer current")

// Outcome exposes delivery state and bounded diagnostics without treating
// a queued/budget-exhausted candidate as a host attempt.
type Outcome struct {
	ID          string
	State       store.EnvelopeState
	Attempted   bool // This call may have reached the host; not historical row state.
	ErrorCode   string
	ErrorDetail string
}

func (b *settlementStore) settle(ctx context.Context, claimed *store.Envelope, deliverErr error) (outcome Outcome, err error) {
	// A failed transaction cannot report its intended state as durable evidence.
	// Preserve attempt information, but leave the stored outcome unknown on error.
	defer func() {
		if err != nil {
			outcome.State = ""
			outcome.ErrorCode = ""
			outcome.ErrorDetail = ""
		}
	}()

	ambiguous := errors.Is(deliverErr, ErrAmbiguous)
	noAttempt := !ambiguous && errors.Is(deliverErr, ErrNoAttempt)
	permanent := !ambiguous && errors.Is(deliverErr, ErrPermanentlyRejected)
	outcome = Outcome{ID: claimed.ID, State: store.HandedOff, Attempted: !noAttempt && !permanent}
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
// e.InReplyTo != nil: TrustedReply is set only by authenticated ingestion, which
// validates in_reply_to against the specific original envelope it atomically
// acks before queuing this one. Historical ordinary messages may contain
// an untrusted correlation ID; those must not qualify for reply carry-forward.
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
	cancelled, err := store.WorkCancelled(ctx, tx, store.WorkRef{Kind: "envelope", ID: e.ID})
	if err != nil || cancelled {
		return 0, false, err
	}
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

func (b *settlementStore) currentOutcome(ctx context.Context, envelopeID string) (Outcome, error) {
	e, err := b.queries.Outcome(ctx, envelopeID)
	if err != nil {
		return Outcome{ID: envelopeID}, err
	}
	return Outcome{ID: e.ID, State: e.State, ErrorCode: e.ErrorCode, ErrorDetail: e.ErrorDetail}, nil
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
