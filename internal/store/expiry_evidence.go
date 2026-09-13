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
	if db != e.owner {
		return InvalidRequest
	}
	// Never hold the evidence mutex while waiting on the writer: collection
	// takes this mutex from inside a writer transaction. Preserve observations
	// for retry if persistence fails, and leave concurrently added ones intact.
	e.mu.Lock()
	observations := make(map[string]CredentialExpiry, len(e.observed))
	for id, observed := range e.observed {
		observations[id] = observed
	}
	e.mu.Unlock()
	if len(observations) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), AuthenticationDeadline)
	defer cancel()
	var bindings []string
	_, err := db.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ CommitView) (TransitionResult, error) {
		changed := false
		for id, observed := range observations {
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
		for id := range observations {
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

func observedExpiry(ctx context.Context, id string) bool {
	evidence, ok := ctx.Value(expiryContextKey{}).(*ExpiryEvidence)
	return ok && evidence.owner.CredentialExpiryObserved(id)
}
