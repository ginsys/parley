package connection

import (
	"context"
	"database/sql"
	"time"

	"github.com/ginsys/parley/internal/store"
	"github.com/google/uuid"
)

// sessionTransition revalidates the exact server-created capability in the same
// transaction as its consumer. No exported Token can satisfy this pointer fence.
func (m *Manager) sessionTransition(ctx context.Context, s *Session,
	change func(context.Context, *sql.Tx) (store.TransitionResult, error), publish func(),
) error {
	ctx, cancel := context.WithTimeout(ctx, store.AuthenticationDeadline)
	defer cancel()
	var expiredBinding, expiredCredential string
	code, err := m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, view store.CommitView) (store.TransitionResult, error) {
		if s == nil || !m.owned(s.socket) || m.slots[s.token.BindingID] != s || s.token.Epoch != view.Epoch || m.expired(s.socket, m.now()) {
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
		if !store.AuthorityTime(ctx, m.now).Before(time.Unix(0, c.ExpiresAtNS)) || m.store.CredentialExpiryObserved(c.ID) {
			m.store.RememberCredentialExpiry(c)
			if _, err := tx.ExecContext(ctx, "UPDATE credentials SET status='expired' WHERE credential_id=? AND status='current'", c.ID); err != nil {
				return store.TransitionResult{}, err
			}
			expiredBinding = b.ID
			expiredCredential = c.ID
			return store.TransitionResult{Changed: true, Code: store.AuthenticationFailed}, nil
		}
		if err := m.guard(ctx, tx, b.ID); err != nil {
			return store.TransitionResult{}, err
		}
		return change(ctx, tx)
	}, func(store.CommitView) {
		if expiredBinding != "" {
			m.Invalidate(expiredBinding)
			m.store.ForgetCredentialExpiry(expiredCredential)
		} else if publish != nil {
			publish()
		}
	})
	if err == nil && code != "" {
		err = code
	}
	if err == nil && s != nil && s.socket.ctx.Err() != nil {
		err = store.AuthenticationFailed
	}
	if err == store.AuthenticationFailed && s != nil && s.socket != nil && s.socket.manager == m {
		s.socket.Close()
	}
	return err
}
func (m *Manager) Heartbeat(ctx context.Context, s *Session) error {
	return m.sessionTransition(ctx, s, func(context.Context, *sql.Tx) (store.TransitionResult, error) {
		return store.TransitionResult{Changed: true}, nil
	}, func() { s.socket.lastHeartbeat = m.now(); s.socket.arm(store.LivenessDeadline) })
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
	err = m.sessionTransition(ctx, s, func(context.Context, *sql.Tx) (store.TransitionResult, error) {
		probe = Probe{s.token, s.socket.native, id.String()}
		return store.TransitionResult{Changed: true}, nil
	}, func() {
		s.ready = false
		s.hostVerified = false
		s.nonce = probe.Nonce
		s.readinessDeadline = m.now().Add(store.ReadinessDeadline)
	})
	if err != nil {
		return Probe{}, err
	}
	verifyCtx, cancel := context.WithTimeout(ctx, store.ReadinessDeadline)
	defer cancel()
	stopLifetime := context.AfterFunc(s.socket.Context(), cancel)
	defer stopLifetime()
	if err := m.verify(verifyCtx, probe.Native, probe.Token); err != nil || verifyCtx.Err() != nil {
		return Probe{}, store.HostUnverified
	}
	err = m.sessionTransition(ctx, s, func(context.Context, *sql.Tx) (store.TransitionResult, error) {
		if s.nonce != probe.Nonce || !m.now().Before(s.readinessDeadline) {
			return store.TransitionResult{Code: store.NotReady}, nil
		}
		return store.TransitionResult{Changed: true}, nil
	}, func() { s.hostVerified = true })
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
	}, func() { s.ready = true })
}
