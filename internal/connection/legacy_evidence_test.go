package connection

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/store"
)

func legacyDispositionFixture(t *testing.T) (*Lifecycle, LegacyDispositionRequest) {
	t.Helper()
	return legacyDispositionIDFixture(t, "work")
}

func legacyDispositionIDFixture(t *testing.T, workID string) (*Lifecycle, LegacyDispositionRequest) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "legacy.db")
	seed, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := os.ReadFile(filepath.Join("..", "store", "schema.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Exec(string(schema)); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`INSERT INTO conversations VALUES('legacy','legacy','1970-01-01T00:00:00Z')`,
		`INSERT INTO grants(conversation,grant_version,peer_a_id,peer_b_id,direction,max_exchanges,granted_at,status) VALUES('legacy',1,'sender','receiver','bidirectional',10,'1970-01-01T00:00:00Z','active')`,
		`INSERT INTO envelopes(id,conversation,from_peer,to_peer,text,grant_version,state,created_at,updated_at) VALUES('work','legacy','sender','receiver','synthetic',1,'queued','1970-01-01T00:00:00Z','1970-01-01T00:00:00Z')`,
		`PRAGMA user_version=1`,
	} {
		if _, err := seed.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := seed.Exec("UPDATE envelopes SET id=?", workID); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	// The real numbered migrations create provenance and quarantine. No test
	// bypasses the guard that only migration may create legacy provenance.
	db, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	l, err := NewLifecycle(LifecycleConfig{
		Store: db, Now: func() time.Time { return time.Unix(100, 0) },
		Authorize:          func(context.Context, *sql.Tx, store.CommandPrincipal) error { return nil },
		Guard:              func(context.Context, *sql.Tx, string) error { return nil },
		Invalidate:         func(string) {},
		PendingWork:        func(context.Context, *sql.Tx, string) ([]store.WorkRef, error) { return nil, nil },
		PendingDisposition: func(context.Context, *sql.Tx, store.WorkRef, string) error { return store.Forbidden },
		LegacyEvidence:     func(context.Context, LegacyDispositionRequest) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	r := LegacyDispositionRequest{OperationID: registerID, WorkID: workID, ExpectedQuarantineVersion: 1, Action: "cancel", DispositionRef: targetID}
	if err := db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT incident_id FROM migration_quarantine WHERE work_id=?", r.WorkID).Scan(&r.MigrationIncidentID)
	}); err != nil {
		t.Fatal(err)
	}
	return l, r
}

func TestLegacyEvidenceFailureRetainsCorrectOutcome(t *testing.T) {
	for name, failure := range map[string]error{
		"unavailable":         store.TemporarilyUnavailable,
		"wrapped unavailable": fmt.Errorf("synthetic evidence: %w", store.TemporarilyUnavailable),
		"canceled":            context.Canceled,
		"deadline":            context.DeadlineExceeded,
		"provider failure":    errors.New("synthetic private manifest unavailable"),
		"forbidden":           store.Forbidden,
		"wrapped forbidden":   fmt.Errorf("synthetic evidence: %w", store.Forbidden),
	} {
		t.Run(name, func(t *testing.T) {
			l, request := legacyDispositionFixture(t)
			ctx := context.Background()
			actor := store.CommandPrincipal{ID: adminID}
			calls := 0
			l.config.LegacyEvidence = func(context.Context, LegacyDispositionRequest) error { calls++; return failure }
			terminal := errors.Is(failure, store.Forbidden)
			result, err := l.LegacyDisposition(ctx, actor, request)
			if terminal {
				if err != nil || result.Result.Code != store.Forbidden {
					t.Errorf("terminal=%+v err=%v", result, err)
				}
			} else if err != store.TemporarilyUnavailable || result.AuditID != "" {
				t.Errorf("transient=%+v err=%v", result, err)
			}
			if err := l.config.Store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
				for _, table := range []string{"operation_results", "command_audit", "work_dispositions"} {
					var count int
					if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
						return err
					}
					want := 0
					if terminal && table != "work_dispositions" {
						want = 1
					}
					if count != want {
						t.Errorf("%s rows=%d want=%d", table, count, want)
					}
				}
				var status string
				var version int64
				if err := tx.QueryRowContext(ctx, "SELECT status,quarantine_version FROM migration_quarantine WHERE work_id=?", request.WorkID).Scan(&status, &version); err != nil {
					return err
				}
				envelope, err := store.GetByID(ctx, tx, request.WorkID)
				if err != nil {
					return err
				}
				if status != "held" || version != 1 || envelope.State != store.Queued || envelope.UpdatedAt != "1970-01-01T00:00:00Z" {
					t.Error("failed evidence changed quarantined work")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			failure = nil
			retry, err := l.LegacyDisposition(ctx, actor, request)
			if terminal {
				if err != nil || !retry.Replayed || retry.Result.Code != store.Forbidden || calls != 1 {
					t.Fatalf("terminal replay=%+v err=%v calls=%d", retry, err, calls)
				}
				return
			}
			if err != nil || retry.Replayed || retry.Result.Code != "" || calls != 2 {
				t.Fatalf("retry=%+v err=%v calls=%d", retry, err, calls)
			}
			replay, err := l.LegacyDisposition(ctx, actor, request)
			if err != nil || !replay.Replayed || replay.AuditID != retry.AuditID || calls != 2 {
				t.Fatalf("replay=%+v err=%v calls=%d", replay, err, calls)
			}
			if err := l.config.Store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
				envelope, err := store.GetByID(ctx, tx, request.WorkID)
				if err == nil && envelope.State != store.Cancelled {
					t.Error("successful retry did not cancel work")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLegacyDispositionPreservesMalformedIDBytes(t *testing.T) {
	for _, action := range []string{"release", "cancel"} {
		t.Run(action, func(t *testing.T) {
			id := string([]byte{'w', 0xff})
			l, request := legacyDispositionIDFixture(t, id)
			request.Action = action
			ctx := context.Background()
			actor := store.CommandPrincipal{ID: adminID}
			calls := 0
			l.config.LegacyEvidence = func(_ context.Context, r LegacyDispositionRequest) error {
				calls++
				if r.WorkID != id {
					t.Fatalf("evidence ID bytes changed: %x", r.WorkID)
				}
				return nil
			}
			result, err := l.LegacyDisposition(ctx, actor, request)
			if err != nil || result.Result.Code != "" {
				t.Fatalf("disposition=%+v err=%v", result, err)
			}
			replay, err := l.LegacyDisposition(ctx, actor, request)
			if err != nil || !replay.Replayed || replay.AuditID != result.AuditID || calls != 1 {
				t.Fatalf("replay=%+v err=%v calls=%d", replay, err, calls)
			}
			for _, other := range []string{string([]byte{'w', 0xfe}), "w\ufffd"} {
				request.WorkID = other
				if _, err := l.LegacyDisposition(ctx, actor, request); err != store.OperationConflict {
					t.Fatalf("different ID %x replayed: %v", other, err)
				}
			}
			if err := l.config.Store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
				var got, status string
				if err := tx.QueryRowContext(ctx, "SELECT work_id,status FROM migration_quarantine").Scan(&got, &status); err != nil {
					return err
				}
				want := "released"
				if action == "cancel" {
					want = "cancelled"
				}
				if got != id || status != want {
					t.Errorf("retained ID=%x status=%s", got, status)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
