package store

import (
	"context"
	"database/sql"
	"testing"
)

func TestAlreadyTerminalExpiryClearsRememberedEvidenceWithoutRevision(t *testing.T) {
	for _, status := range []string{"expired", "revoked"} {
		t.Run(status, func(t *testing.T) {
			db, b, _, _ := retainedWorkFixture(t)
			ctx := context.Background()
			var credential CredentialRecord
			_, err := db.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ CommitView) (TransitionResult, error) {
				var err error
				credential, err = LatestCredential(ctx, tx, b.ID)
				if err != nil {
					return TransitionResult{}, err
				}
				_, err = tx.ExecContext(ctx, "UPDATE credentials SET status=? WHERE credential_id=?", status, credential.ID)
				return TransitionResult{Changed: true}, err
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			db.RememberCredentialExpiry(credential)
			_, evidence := ObserveExpiries(ctx, db)
			revision := db.coordinator.revision
			if err := evidence.Persist(db, func(string) { t.Error("terminal credential invalidated a newer session") }); err != nil {
				t.Fatal(err)
			}
			if db.CredentialExpiryObserved(credential.ID) {
				t.Fatal("no-op persistence retained global expiry observation")
			}
			if db.coordinator.revision != revision {
				t.Fatal("no-op persistence advanced revision")
			}
		})
	}
}

func TestPersistedExpiryCollectorDoesNotRetryCompletedEvidence(t *testing.T) {
	db, b, _, _ := retainedWorkFixture(t)
	ctx, evidence := ObserveExpiries(context.Background(), db)
	if err := db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		c, err := LatestCredential(ctx, tx, b.ID)
		if err == nil {
			recordExpiry(ctx, c)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := evidence.Persist(db, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := evidence.Persist(db, nil); err != nil {
		t.Fatalf("completed collector reopened writer: %v", err)
	}
}
