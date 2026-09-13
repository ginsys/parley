package connection

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/ginsys/parley/internal/store"
	"github.com/google/uuid"
)

// sessionTransition revalidates the exact server-created capability in the same
// transaction as its consumer. No exported Token can satisfy this pointer fence.
func (m *Manager) sessionTransition(ctx context.Context, s *Session,
	change func(context.Context, *sql.Tx) (store.TransitionResult, error), publish func(time.Time) store.Code,
) error {
	ctx, cancel := context.WithTimeout(ctx, store.AuthenticationDeadline)
	defer cancel()
	var credential store.CredentialRecord
	var expired bool
	var publicationCode store.Code
	code, err := m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, view store.CommitView) (store.TransitionResult, error) {
		if s == nil || !m.owned(s.socket) || m.slots[s.token.BindingID] != s || s.token.Epoch != view.Epoch {
			return store.TransitionResult{Code: store.AuthenticationFailed}, nil
		}
		b, err := store.ReadBinding(ctx, tx, s.token.BindingID)
		if err != nil {
			return store.TransitionResult{}, err
		}
		c, err := store.ReadCredential(ctx, tx, s.socket.credentialID)
		if err != nil {
			return store.TransitionResult{}, err
		}
		if b.Status != "enabled" || b.Generation != s.token.Generation || b.ConnectorUID != s.token.ConnectorUID || c.Status != "current" || c.Version != s.token.CredentialVersion {
			return store.TransitionResult{Code: store.AuthenticationFailed}, nil
		}
		credential = c
		expired, err = m.expireCredential(ctx, tx, c)
		if err != nil {
			return store.TransitionResult{}, err
		}
		if expired {
			return store.TransitionResult{Changed: true, Code: store.AuthenticationFailed}, nil
		}
		if m.expired(s.socket, m.now()) {
			return store.TransitionResult{Code: store.AuthenticationFailed}, nil
		}
		if err := m.guard(ctx, tx, b.ID); err != nil {
			return store.TransitionResult{}, err
		}
		expired, err = m.expireCredential(ctx, tx, c)
		if err != nil {
			return store.TransitionResult{}, err
		}
		if expired {
			return store.TransitionResult{Changed: true, Code: store.AuthenticationFailed}, nil
		}
		if !m.owned(s.socket) || m.expired(s.socket, m.now()) {
			return store.TransitionResult{Code: store.AuthenticationFailed}, nil
		}
		return change(ctx, tx)
	}, func(store.CommitView) {
		now := m.now()
		if !expired && m.observeCredentialExpiry(credential, now) {
			expired = true
		}
		if expired {
			m.Invalidate(credential.BindingID)
			publicationCode = store.AuthenticationFailed
			return
		}
		if !m.owned(s.socket) || m.slots[s.token.BindingID] != s || m.expired(s.socket, now) {
			m.remove(s.socket)
			publicationCode = store.AuthenticationFailed
			return
		}
		if publish != nil {
			publicationCode = publish(now)
		}
	})
	if expired {
		s.socket.cancel()
		if persistErr := m.persistCredentialExpiry(credential); persistErr != nil {
			err = persistErr
		}
	}
	if err == nil && publicationCode != "" {
		err = publicationCode
	}
	if err == nil && code != "" {
		err = code
	}
	if err == nil && s != nil && s.socket.ctx.Err() != nil {
		err = store.AuthenticationFailed
	}
	if (expired || err == store.AuthenticationFailed) && s != nil && s.socket != nil && s.socket.manager == m {
		s.socket.Close()
	}
	return err
}
func (m *Manager) Heartbeat(ctx context.Context, s *Session) error {
	return m.sessionTransition(ctx, s, func(context.Context, *sql.Tx) (store.TransitionResult, error) {
		return store.TransitionResult{Changed: true}, nil
	}, func(now time.Time) store.Code {
		s.socket.lastHeartbeat = now
		s.socket.arm(store.LivenessDeadline)
		return ""
	})
}
func (m *Manager) RequireReady(ctx context.Context, s *Session) error {
	return m.sessionTransition(ctx, s, func(context.Context, *sql.Tx) (store.TransitionResult, error) {
		if !s.ready {
			return store.TransitionResult{Code: store.NotReady}, nil
		}
		return store.TransitionResult{}, nil
	}, nil)
}

// Probe is scoped evidence for the deterministic host adapter. An ACK must arrive
// via that adapter's current Session; copying this metadata grants no authority.
type Probe struct {
	Token  Token
	Native NativeTuple
	Nonce  string
}

func (m *Manager) BeginReadiness(ctx context.Context, s *Session) (Probe, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return Probe{}, store.TemporarilyUnavailable
	}
	var probe Probe
	var credential store.CredentialRecord
	err = m.sessionTransition(ctx, s, func(ctx context.Context, tx *sql.Tx) (store.TransitionResult, error) {
		// Retain immutable expiry evidence while the session is authenticated.
		// A later disconnect may remove the slot before the verifier returns.
		var err error
		credential, err = store.ReadCredential(ctx, tx, s.socket.credentialID)
		if err != nil {
			return store.TransitionResult{}, err
		}
		probe = Probe{s.token, s.socket.native, id.String()}
		return store.TransitionResult{Changed: true}, nil
	}, func(now time.Time) store.Code {
		s.ready = false
		s.hostVerified = false
		s.nonce = probe.Nonce
		s.readinessDeadline = now.Add(store.ReadinessDeadline)
		return ""
	})
	if err != nil {
		return Probe{}, err
	}
	verifyCtx, cancel := context.WithTimeout(ctx, store.ReadinessDeadline)
	defer cancel()
	stopLifetime := context.AfterFunc(s.socket.Context(), cancel)
	defer stopLifetime()
	verifyErr := m.verify(verifyCtx, probe.Native, probe.Token)
	if m.observeCredentialExpiry(credential, m.now()) {
		s.socket.cancel()
		defer s.socket.Close()
		if err := m.persistCredentialExpiry(credential); err != nil {
			return Probe{}, err
		}
		return Probe{}, store.AuthenticationFailed
	}
	if s.socket.Context().Err() != nil {
		return Probe{}, store.AuthenticationFailed
	}
	if verifyCtx.Err() != nil || verifyErr != nil {
		// A failed verifier grants no readiness, but elapsed credential expiry
		// still needs observation/persistence, even if the caller cancelled.
		if err := m.sessionTransition(context.WithoutCancel(ctx), s, func(context.Context, *sql.Tx) (store.TransitionResult, error) {
			return store.TransitionResult{Changed: true}, nil
		}, nil); err != nil {
			return Probe{}, err
		}
	}
	if verifyCtx.Err() != nil {
		return Probe{}, store.TemporarilyUnavailable
	}
	if verifyErr != nil {
		if errors.Is(verifyErr, store.HostUnverified) {
			return Probe{}, store.HostUnverified
		}
		return Probe{}, store.TemporarilyUnavailable
	}
	err = m.sessionTransition(ctx, s, func(context.Context, *sql.Tx) (store.TransitionResult, error) {
		if s.nonce != probe.Nonce || !m.now().Before(s.readinessDeadline) {
			return store.TransitionResult{Code: store.NotReady}, nil
		}
		return store.TransitionResult{Changed: true}, nil
	}, func(now time.Time) store.Code {
		if s.nonce != probe.Nonce || !now.Before(s.readinessDeadline) {
			return store.NotReady
		}
		s.hostVerified = true
		return ""
	})
	if err != nil {
		return Probe{}, err
	}
	return probe, nil
}
func (m *Manager) Acknowledge(ctx context.Context, s *Session, p Probe) error {
	return m.sessionTransition(ctx, s, func(context.Context, *sql.Tx) (store.TransitionResult, error) {
		if !s.hostVerified || p.Token != s.token || p.Native != s.socket.native || p.Nonce == "" || p.Nonce != s.nonce || !m.now().Before(s.readinessDeadline) {
			return store.TransitionResult{Code: store.NotReady}, nil
		}
		return store.TransitionResult{Changed: !s.ready}, nil
	}, func(now time.Time) store.Code {
		if !now.Before(s.readinessDeadline) {
			return store.NotReady
		}
		s.ready = true
		return ""
	})
}
