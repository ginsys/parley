package dispatch

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/ginsys/parley/internal/bridgetext"
	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/store"
)

// AuthenticatedBridge is the internal replacement consumed in the mechanical
// caller migration. TransportFor must bind deterministic delivery to the supplied
// exact private Session; it may not resolve a newer connection during Deliver.
type AuthenticatedBridge struct {
	bridge       *Bridge
	manager      *connection.Manager
	transportFor func(*connection.Session) Transport
	now          func() time.Time
}

func NewAuthenticated(db *store.DB, m *connection.Manager, transportFor func(*connection.Session) Transport, now func() time.Time) (*AuthenticatedBridge, error) {
	if db == nil || m == nil || transportFor == nil || !m.OwnsStore(db) {
		return nil, store.InvalidRequest
	}
	if now == nil {
		now = time.Now
	}
	return &AuthenticatedBridge{bridge: New(db, nil), manager: m, transportFor: transportFor, now: now}, nil
}
func (b *AuthenticatedBridge) Send(ctx context.Context, s *connection.Session, r connection.SendRequest) (store.CommandReceipt, error) {
	return b.manager.Send(ctx, s, r)
}
func (b *AuthenticatedBridge) DispatchOutcome(ctx context.Context, id string) (out Outcome, err error) {
	ctx, expiry := store.ObserveExpiries(ctx, b.bridge.db)
	defer func() {
		if persistErr := expiry.Persist(b.bridge.db, b.manager.Invalidate); persistErr != nil {
			out = Outcome{ID: id}
			err = persistErr
		}
	}()
	var claimed *store.Envelope
	var recipient *connection.Session
	var claimErr error
	_, err = b.bridge.db.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		e, err := store.GetByID(ctx, tx, id)
		if err != nil {
			return store.TransitionResult{}, err
		}
		if e.State != store.Queued {
			return store.TransitionResult{}, nil
		}
		for _, key := range []string{e.Conversation, e.FromPeer, e.ToPeer} {
			if bridgetext.ValidateMetadata(key) != nil {
				return store.TransitionResult{}, store.InvalidRequest
			}
		}
		g, err := store.CurrentGrant(ctx, tx, e.Conversation)
		if err == nil {
			err = authorizeEnvelope(g, e, store.AuthorityTime(ctx, b.now))
		}
		if err != nil {
			if !errors.Is(err, store.ErrNoActiveGrant) && !errors.Is(err, ErrGrantExpired) && !errors.Is(err, ErrNotPermitted) && !errors.Is(err, ErrStaleGrantVersion) {
				return store.TransitionResult{}, err
			}
			claimErr = err
			if errors.Is(err, ErrGrantExpired) && e.TrustedReply {
				return store.TransitionResult{}, nil
			}
			err = store.SetState(ctx, tx, e.ID, store.Queued, store.Cancelled, store.AuthorityTime(ctx, b.now).UTC().Format(time.RFC3339Nano))
			return store.TransitionResult{Changed: true}, err
		}
		if err := store.AuthorizeProvenance(ctx, tx, e, store.AuthorityTime(ctx, b.now)); err != nil {
			return store.TransitionResult{}, err
		}
		recipient, err = b.manager.RecipientForClaim(ctx, tx, e.ToPeer)
		if err != nil {
			return store.TransitionResult{}, err
		}
		ok, err := store.ClaimExchange(ctx, tx, e.Conversation, e.GrantVersion)
		if err != nil {
			return store.TransitionResult{}, err
		}
		if !ok {
			claimErr = ErrBudgetExhausted
			return store.TransitionResult{}, nil
		}
		ok, err = store.TransitionToDispatching(ctx, tx, e.ID, store.AuthorityTime(ctx, b.now).UTC().Format(time.RFC3339Nano))
		if err != nil {
			return store.TransitionResult{}, err
		}
		if !ok {
			return store.TransitionResult{}, store.VersionConflict
		}
		e.State = store.Dispatching
		e.DispatchAttempt++
		claimed = e
		return store.TransitionResult{Changed: true}, nil
	}, nil)
	if err != nil || claimed == nil {
		outcome, readErr := b.bridge.currentOutcome(ctx, id)
		if readErr != nil {
			return outcome, readErr
		}
		if err == store.BindingUnavailable || err == store.SecurityHold || err == store.NotReady || err == store.AuthenticationFailed {
			return outcome, nil
		}
		if err != nil {
			return outcome, err
		}
		return outcome, claimErr
	}
	// The captured capability is immutable even if its live slot is replaced.
	// Cooperative cancellation crosses handoff; settlement retains uncertainty.
	attempt, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(recipient.Context(), cancel)
	defer stop()
	transport := b.transportFor(recipient)
	var deliveryErr error
	if transport == nil || recipient.Context().Err() != nil {
		deliveryErr = ErrNoAttempt
	} else {
		deliveryErr = transport.Deliver(attempt, *claimed)
	}
	return b.bridge.settle(context.WithoutCancel(ctx), claimed, deliveryErr)
}
