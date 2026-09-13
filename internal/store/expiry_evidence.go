package store

import (
	"context"
	"database/sql"
	"sync"
)

// ExpiryEvidence captures observations inside a business transaction and commits
// terminal expiry afterwards, including when the business effect was rejected or
// rolled back. It retains credential identity, never a bearer or verifier.
type ExpiryEvidence struct {
	owner    *DB
	mu       sync.Mutex
	observed map[string]CredentialExpiry
}
type CredentialExpiry struct {
	BindingID string
	Deadline  int64
}
type expiryContextKey struct{}

func ObserveExpiries(ctx context.Context, db *DB) (context.Context, *ExpiryEvidence) {
	evidence := &ExpiryEvidence{owner: db, observed: make(map[string]CredentialExpiry)}
	return context.WithValue(ctx, expiryContextKey{}, evidence), evidence
}
func recordExpiry(ctx context.Context, c CredentialRecord) {
	if evidence, ok := ctx.Value(expiryContextKey{}).(*ExpiryEvidence); ok {
		evidence.mu.Lock()
		evidence.observed[c.ID] = CredentialExpiry{c.BindingID, c.ExpiresAtNS}
		evidence.owner.coordinator.credentialExpiries.Store(c.ID, c.ExpiresAtNS)
		evidence.mu.Unlock()
	}
}
func (e *ExpiryEvidence) Persist(db *DB, invalidate func(string)) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if db != e.owner {
		return InvalidRequest
	}
	if len(e.observed) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), AuthenticationDeadline)
	defer cancel()
	var bindings []string
	_, err := db.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ CommitView) (TransitionResult, error) {
		changed := false
		for id, observed := range e.observed {
			result, err := tx.ExecContext(ctx, "UPDATE credentials SET status='expired' WHERE credential_id=? AND expires_at_ns=? AND status='current'", id, observed.Deadline)
			if err != nil {
				return TransitionResult{}, err
			}
			n, err := result.RowsAffected()
			if err != nil {
				return TransitionResult{}, err
			}
			changed = changed || n > 0
			if n > 0 {
				bindings = append(bindings, observed.BindingID)
			}
		}
		return TransitionResult{Changed: changed}, nil
	}, func(CommitView) {
		for id := range e.observed {
			db.coordinator.credentialExpiries.Delete(id)
		}
		if invalidate != nil {
			for _, binding := range bindings {
				invalidate(binding)
			}
		}
	})
	return err
}

// CredentialExpiryObserved denies only the exact immutable credential identity.
// A failed persistence attempt cannot poison an unrelated or rotated credential.
func (d *DB) CredentialExpiryObserved(id string) bool {
	_, denied := d.coordinator.credentialExpiries.Load(id)
	return denied
}
func observedExpiry(ctx context.Context, id string) bool {
	evidence, ok := ctx.Value(expiryContextKey{}).(*ExpiryEvidence)
	return ok && evidence.owner.CredentialExpiryObserved(id)
}

// RememberCredentialExpiry is used before a terminal-expiry write so failure
// cannot erase that observation. Credential identities/deadlines are immutable.
func (d *DB) RememberCredentialExpiry(c CredentialRecord) {
	d.coordinator.credentialExpiries.Store(c.ID, c.ExpiresAtNS)
}

// ForgetCredentialExpiry is only a post-commit publication callback after the
// exact credential became terminal. It never changes durable lifecycle state.
func (d *DB) ForgetCredentialExpiry(id string) { d.coordinator.credentialExpiries.Delete(id) }
