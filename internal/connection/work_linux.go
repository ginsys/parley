package connection

import (
	"context"
	"database/sql"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/ginsys/parley/internal/bridgetext"
	"github.com/ginsys/parley/internal/store"
	"github.com/google/uuid"
)

// AuthorizeWork is trusted writer-side wiring. Call only inside this manager's
// store coordinator, never before opening a separate claim transaction. The
// private Session pointer, current slot and durable identity all have to match.
func (m *Manager) AuthorizeWork(ctx context.Context, tx *sql.Tx, s *Session, ready bool) error {
	if s == nil || !m.owned(s.socket) || m.slots[s.token.BindingID] != s || m.expired(s.socket, m.now()) {
		return store.AuthenticationFailed
	}
	b, err := store.EnabledPeer(ctx, tx, s.token.PeerID, store.AuthorityTime(ctx, m.now))
	if err != nil {
		return err
	}
	c, err := store.ReadCredential(ctx, tx, s.socket.credentialID)
	if err != nil {
		return err
	}
	if b.ID != s.token.BindingID || b.Generation != s.token.Generation || b.ConnectorUID != s.token.ConnectorUID || c.BindingID != b.ID || c.Version != s.token.CredentialVersion || c.Status != "current" || !store.AuthorityTime(ctx, m.now).Before(time.Unix(0, c.ExpiresAtNS)) {
		return store.AuthenticationFailed
	}
	if !s.hostVerified || (ready && !s.ready) {
		return store.NotReady
	}
	return m.guard(ctx, tx, b.ID)
}

// RecipientForClaim returns the exact capability to which this claim is bound.
// The transport must retain this Session and its context across handoff; it must
// never resolve the current slot again after the claim commits.
func (m *Manager) RecipientForClaim(ctx context.Context, tx *sql.Tx, peer string) (*Session, error) {
	b, err := store.EnabledPeer(ctx, tx, peer, store.AuthorityTime(ctx, m.now))
	if err != nil {
		return nil, err
	}
	s := m.slots[b.ID]
	if err := m.AuthorizeWork(ctx, tx, s, true); err != nil {
		return nil, err
	}
	return s, nil
}
func (s *Session) Context() context.Context { return s.socket.Context() }
func (s *Session) PeerID() string           { return s.token.PeerID }

type SendRequest struct{ OperationID, Conversation, Recipient, Text string }

// Send has no sender-ID or trusted-reply input. An enabled recipient need not be
// attached yet; recipient readiness is checked separately at dispatch claim.
func (m *Manager) Send(ctx context.Context, s *Session, r SendRequest) (receipt store.CommandReceipt, err error) {
	ctx, expiry := store.ObserveExpiries(ctx, m.store)
	defer func() {
		if persistErr := expiry.Persist(m.store, m.Invalidate); persistErr != nil {
			receipt = store.CommandReceipt{}
			err = persistErr
		}
	}()
	if s == nil || s.socket == nil || s.socket.manager != m {
		return store.CommandReceipt{}, store.AuthenticationFailed
	}
	for _, key := range []string{r.Conversation, r.Recipient} {
		if len(key) > store.MaxIdentityBytes || bridgetext.ValidateMetadata(key) != nil {
			return store.CommandReceipt{}, store.InvalidRequest
		}
	}
	if !utf8.ValidString(r.Text) {
		return store.CommandReceipt{}, store.InvalidRequest
	}
	req, err := store.NewCommandRequest("message.send", r.OperationID, store.Field{Name: "conversation", Value: r.Conversation}, store.Field{Name: "recipient", Value: r.Recipient}, store.Field{Name: "text", Value: r.Text})
	if err != nil {
		return store.CommandReceipt{}, err
	}
	// Connection validation persists terminal expiry even when the ordinary
	// command is rejected; the command transaction rechecks identity afterwards.
	if err := m.sessionTransition(ctx, s, func(context.Context, *sql.Tx) (store.TransitionResult, error) { return store.TransitionResult{}, nil }, nil); err != nil {
		return store.CommandReceipt{}, err
	}
	return m.store.Coordinator().Execute(ctx, store.CommandPrincipal{ID: s.token.BindingID, ConnectorUID: s.token.ConnectorUID}, req, func(ctx context.Context, tx *sql.Tx) error {
		return m.AuthorizeWork(ctx, tx, s, false)
	}, func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
		if _, err := store.EnabledPeer(ctx, tx, r.Recipient, store.AuthorityTime(ctx, m.now)); err != nil {
			return domainRejection(err)
		}
		grant, err := store.CurrentGrant(ctx, tx, r.Conversation)
		if err != nil {
			if errors.Is(err, store.ErrNoActiveGrant) {
				return rejection(store.Forbidden)
			}
			return store.CommandResult{}, err
		}
		if !grant.Permits(s.token.PeerID, r.Recipient, store.AuthorityTime(ctx, m.now)) {
			return rejection(store.Forbidden)
		}
		id, err := uuid.NewRandom()
		if err != nil {
			return store.CommandResult{}, store.TemporarilyUnavailable
		}
		now := store.AuthorityTime(ctx, m.now).UTC().Format(time.RFC3339Nano)
		e := store.Envelope{ID: id.String(), Conversation: r.Conversation, FromPeer: s.token.PeerID, ToPeer: r.Recipient, Text: r.Text, GrantVersion: grant.GrantVersion, State: store.Queued, CreatedAt: now, UpdatedAt: now}
		if err := store.InsertQueued(ctx, tx, e); err != nil {
			return store.CommandResult{}, err
		}
		if err := store.RecordAuthenticatedEnvelope(ctx, tx, e.ID, s.token.BindingID, s.token.CredentialVersion); err != nil {
			return store.CommandResult{}, err
		}
		return store.CommandResult{Resources: []store.ResourceChange{{Kind: "envelope", ID: e.ID, After: grant.GrantVersion}}}, nil
	}, nil)
}

// OwnsStore prevents trusted server wiring from mixing capabilities and writers.
func (m *Manager) OwnsStore(db *store.DB) bool { return m != nil && m.store == db }
