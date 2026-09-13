package connection

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/ginsys/parley/internal/bridgetext"
	"github.com/ginsys/parley/internal/replymarker"
	"github.com/ginsys/parley/internal/store"
	"github.com/google/uuid"
)

type NativeEvent struct{ ID, SourceID, Revision, Before, After string }
type IngestRequest struct {
	Event                         NativeEvent
	Conversation, Recipient, Text string
}
type IngestorConfig struct {
	Manager *Manager
	// Verify must establish exact native source identity, revision, interval and
	// final-turn content for this bound host. It runs without a writer transaction.
	Verify func(context.Context, NativeTuple, Token, IngestRequest) error
	Origin func(context.Context, NativeTuple, Token, string, string) error
}
type Ingestor struct{ config IngestorConfig }

func NewIngestor(c IngestorConfig) (*Ingestor, error) {
	if c.Manager == nil || c.Verify == nil || c.Origin == nil {
		return nil, store.InvalidRequest
	}
	return &Ingestor{c}, nil
}
func (i *Ingestor) Initialize(ctx context.Context, s *Session, source, cursor string) (err error) {
	ctx, expiry := store.ObserveExpiries(ctx, i.config.Manager.store)
	defer func() {
		if persistErr := expiry.Persist(i.config.Manager.store, i.config.Manager.Invalidate); persistErr != nil {
			err = persistErr
		}
	}()
	m := i.config.Manager
	if err := m.sessionTransition(ctx, s, func(ctx context.Context, tx *sql.Tx) (store.TransitionResult, error) {
		return store.TransitionResult{}, m.AuthorizeWork(ctx, tx, s, false)
	}, nil); err != nil {
		return err
	}
	verifyCtx, stop := s.hostEvidenceContext(ctx)
	defer stop()
	if err := i.config.Origin(verifyCtx, s.socket.native, s.token, source, cursor); err != nil || verifyCtx.Err() != nil {
		return store.HostUnverified
	}
	return m.sessionTransition(ctx, s, func(ctx context.Context, tx *sql.Tx) (store.TransitionResult, error) {
		if err := m.AuthorizeWork(ctx, tx, s, false); err != nil {
			return store.TransitionResult{}, err
		}
		return store.TransitionResult{Changed: true}, store.InitializeIngestionSource(ctx, tx, s.token.BindingID, source, cursor)
	}, nil)
}
func (i *Ingestor) Ingest(ctx context.Context, s *Session, r IngestRequest) (out store.EventResult, err error) {
	ctx, expiry := store.ObserveExpiries(ctx, i.config.Manager.store)
	defer func() {
		if persistErr := expiry.Persist(i.config.Manager.store, i.config.Manager.Invalidate); persistErr != nil {
			out = store.EventResult{}
			err = persistErr
		}
	}()
	m := i.config.Manager
	if !utf8.ValidString(r.Text) {
		return store.EventResult{}, store.InvalidRequest
	}
	for _, key := range []string{r.Conversation, r.Recipient} {
		if len(key) > store.MaxIdentityBytes || bridgetext.ValidateMetadata(key) != nil {
			return store.EventResult{}, store.InvalidRequest
		}
	}
	if err := m.sessionTransition(ctx, s, func(ctx context.Context, tx *sql.Tx) (store.TransitionResult, error) {
		return store.TransitionResult{}, m.AuthorizeWork(ctx, tx, s, false)
	}, nil); err != nil {
		return store.EventResult{}, err
	}
	// Routing and content are part of retained conflict evidence even when the
	// native revision is accidentally reused. The database retains only this hash.
	data, err := json.Marshal(r)
	if err != nil {
		return store.EventResult{}, store.InvalidRequest
	}
	e := store.SourceEvent{BindingID: s.token.BindingID, EventID: r.Event.ID, SourceID: r.Event.SourceID, Revision: r.Event.Revision, Before: r.Event.Before, After: r.Event.After, Digest: sha256.Sum256(data)}
	var previous store.EventResult
	err = m.sessionTransition(ctx, s, func(ctx context.Context, tx *sql.Tx) (store.TransitionResult, error) {
		if err := m.AuthorizeWork(ctx, tx, s, false); err != nil {
			return store.TransitionResult{}, err
		}
		if err := store.IngestionAllowed(ctx, tx, s.token.BindingID); err != nil {
			return store.TransitionResult{}, err
		}
		var err error
		previous, _, err = store.LookupEvent(ctx, tx, e)
		return store.TransitionResult{}, err
	}, nil)
	if err != nil {
		return store.EventResult{}, err
	}
	if previous.Replayed {
		return previous, nil
	}
	verifyCtx, stop := s.hostEvidenceContext(ctx)
	defer stop()
	if err := i.config.Verify(verifyCtx, s.socket.native, s.token, r); err != nil || verifyCtx.Err() != nil {
		return store.EventResult{}, store.HostUnverified
	}
	marker, parseErr := replymarker.Extract(r.Text)
	var result store.EventResult
	err = m.sessionTransition(ctx, s, func(ctx context.Context, tx *sql.Tx) (store.TransitionResult, error) {
		if err := m.AuthorizeWork(ctx, tx, s, false); err != nil {
			return store.TransitionResult{}, err
		}
		if err := store.IngestionAllowed(ctx, tx, s.token.BindingID); err != nil {
			return store.TransitionResult{}, err
		}
		var err error
		result, err = store.StageEvent(ctx, tx, e)
		if err != nil {
			return store.TransitionResult{}, err
		}
		if result.Replayed {
			return store.TransitionResult{}, nil
		}
		pending := func(code store.Code) (store.TransitionResult, error) {
			result = store.EventResult{Classification: "pending", Code: code}
			return store.TransitionResult{Changed: true}, nil
		}
		if err := store.EventAtCursor(ctx, tx, e); err != nil {
			if err == store.TemporarilyUnavailable || err == store.HostUnverified {
				return pending(store.TemporarilyUnavailable)
			}
			return store.TransitionResult{}, err
		}
		finish := func(classification string, code store.Code, credential int64) (store.TransitionResult, error) {
			result.Classification = classification
			result.Code = code
			return store.TransitionResult{Changed: true}, store.FinishEvent(ctx, tx, e, result, credential)
		}
		if errors.Is(parseErr, replymarker.ErrNoMarker) {
			return finish("no_marker", "", 0)
		}
		if parseErr != nil {
			return finish("malformed", store.InvalidRequest, 0)
		}
		original, err := replymarker.Validate(ctx, tx, r.Conversation, s.token.PeerID, r.Recipient, marker)
		if err != nil {
			if errors.Is(err, replymarker.ErrDeliveryPending) {
				return pending(store.TemporarilyUnavailable)
			}
			if errors.Is(err, bridgetext.ErrInvalidMetadata) {
				return finish("malformed", store.InvalidRequest, 0)
			}
			if errors.Is(err, replymarker.ErrStaleReply) || errors.Is(err, replymarker.ErrWrongRecipient) || errors.Is(err, replymarker.ErrWrongReplier) {
				return finish("rejected", store.Forbidden, 0)
			}
			return store.TransitionResult{}, err
		}
		if original.FromPeer != r.Recipient {
			return finish("rejected", store.Forbidden, 0)
		}
		cancelled, err := store.WorkCancelled(ctx, tx, store.WorkRef{Kind: "envelope", ID: original.ID})
		if err != nil {
			return store.TransitionResult{}, err
		}
		if cancelled {
			return finish("held", store.SecurityHold, 0)
		}
		if err := store.AuthorizeProvenance(ctx, tx, original, store.AuthorityTime(ctx, m.now)); err != nil {
			if err == store.SecurityHold {
				return pending(store.SecurityHold)
			}
			if err == store.BindingUnavailable {
				return pending(store.BindingUnavailable)
			}
			return store.TransitionResult{}, err
		}
		if _, err := store.EnabledPeer(ctx, tx, r.Recipient, store.AuthorityTime(ctx, m.now)); err != nil {
			if err == store.BindingUnavailable {
				return pending(store.BindingUnavailable)
			}
			return store.TransitionResult{}, err
		}
		g, err := store.CurrentGrant(ctx, tx, r.Conversation)
		if errors.Is(err, store.ErrNoActiveGrant) {
			return pending(store.BindingUnavailable)
		}
		if err != nil {
			return store.TransitionResult{}, err
		}
		if !g.Permits(s.token.PeerID, r.Recipient, store.AuthorityTime(ctx, m.now)) {
			return pending(store.BindingUnavailable)
		}
		id, err := uuid.NewRandom()
		if err != nil {
			return store.TransitionResult{}, store.TemporarilyUnavailable
		}
		now := store.AuthorityTime(ctx, m.now).UTC().Format(time.RFC3339Nano)
		if err := store.SetState(ctx, tx, original.ID, store.HandedOff, store.Acked, now); err != nil {
			return store.TransitionResult{}, err
		}
		reply := store.Envelope{ID: id.String(), Conversation: r.Conversation, FromPeer: s.token.PeerID, ToPeer: r.Recipient, Text: marker.Text, GrantVersion: g.GrantVersion, InReplyTo: &original.ID, TrustedReply: true, State: store.Queued, CreatedAt: now, UpdatedAt: now}
		if err := store.InsertQueued(ctx, tx, reply); err != nil {
			return store.TransitionResult{}, err
		}
		if err := store.RecordAuthenticatedEnvelope(ctx, tx, reply.ID, s.token.BindingID, s.token.CredentialVersion); err != nil {
			return store.TransitionResult{}, err
		}
		result.EnvelopeID = reply.ID
		return finish("accepted", "", s.token.CredentialVersion)
	}, nil)
	if err != nil {
		return store.EventResult{}, err
	}
	return result, nil
}

func (s *Session) hostEvidenceContext(ctx context.Context) (context.Context, func()) {
	verified, cancel := context.WithTimeout(ctx, store.ReadinessDeadline)
	stop := context.AfterFunc(s.Context(), cancel)
	if s.Context().Err() != nil {
		cancel()
	}
	return verified, func() { stop(); cancel() }
}
