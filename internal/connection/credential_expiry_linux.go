package connection

import (
	"context"
	"database/sql"
	"time"

	"github.com/ginsys/parley/internal/store"
)

func (m *Manager) observeCredentialExpiry(c store.CredentialRecord, now time.Time) bool {
	if m.store.CredentialExpiryObserved(c.ID) || !now.Before(time.Unix(0, c.ExpiresAtNS)) {
		m.store.RememberCredentialExpiry(c)
		return true
	}
	return false
}

func (m *Manager) expireCredential(ctx context.Context, tx *sql.Tx, c store.CredentialRecord) (bool, error) {
	if !m.observeCredentialExpiry(c, store.AuthorityTime(ctx, m.now)) {
		return false, nil
	}
	_, err := tx.ExecContext(ctx, "UPDATE credentials SET status='expired' WHERE credential_id=? AND status='current'", c.ID)
	return true, err
}

// Publication may observe expiry only after the original transaction committed.
// Persist that evidence independently of caller cancellation; retain the exact
// denial if storage fails. Rotation cannot make this expire a successor identity.
func (m *Manager) persistCredentialExpiry(c store.CredentialRecord) error {
	if !m.store.CredentialExpiryObserved(c.ID) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), store.AuthenticationDeadline)
	defer cancel()
	var changed bool
	_, err := m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		result, err := tx.ExecContext(ctx, "UPDATE credentials SET status='expired' WHERE credential_id=? AND expires_at_ns=? AND status='current'", c.ID, c.ExpiresAtNS)
		if err != nil {
			return store.TransitionResult{}, err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return store.TransitionResult{}, err
		}
		changed = n > 0
		// Commit even when the identity is already terminal, so forgetting the
		// observation happens only after a successful transaction boundary.
		return store.TransitionResult{Changed: true}, nil
	}, func(store.CommitView) {
		m.store.ForgetCredentialExpiry(c.ID)
		if changed {
			m.Invalidate(c.BindingID)
		}
	})
	return err
}
