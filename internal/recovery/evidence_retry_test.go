package recovery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/ginsys/parley/internal/store"
)

func TestRecoveryEvidenceFailuresRetainOnlyMismatch(t *testing.T) {
	for _, kind := range []string{"clock", "restore"} {
		for name, failure := range map[string]error{
			"unavailable":         store.TemporarilyUnavailable,
			"wrapped unavailable": fmt.Errorf("provider: %w", store.TemporarilyUnavailable),
			"canceled":            context.Canceled,
			"deadline":            context.DeadlineExceeded,
			"unknown":             errors.New("synthetic evidence unavailable"),
			"mismatch":            store.HostUnverified,
			"wrapped mismatch":    fmt.Errorf("provider: %w", store.HostUnverified),
		} {
			t.Run(kind+"/"+name, func(t *testing.T) {
				ctx := context.Background()
				var s *Service
				var incident string
				calls := 0
				var run func() (store.CommandReceipt, error)
				actor := store.CommandPrincipal{ID: recoveryPrincipal}
				if kind == "clock" {
					a, service, _, r := clockAdministration(t)
					s, incident = service, r.IncidentID
					a.config.VerifyTime = func(context.Context, ClockReconcileRequest, store.RecoveryRecord) error { calls++; return failure }
					run = func() (store.CommandReceipt, error) { return a.ClockReconcile(ctx, actor, r) }
				} else {
					a, service, _, r := restoreAdministration(t)
					s, incident = service, r.IncidentID
					a.resolve = func(context.Context, RecoveryCompleteRequest) (RestoreDisposition, error) {
						calls++
						return RestoreDisposition{}, failure
					}
					run = func() (store.CommandReceipt, error) { return a.Complete(ctx, actor, r) }
				}
				terminal := errors.Is(failure, store.HostUnverified)
				receipt, err := run()
				if terminal {
					if err != nil || receipt.Result.Code != store.HostUnverified {
						t.Fatalf("mismatch=%+v %v", receipt, err)
					}
				} else if err != store.TemporarilyUnavailable || receipt.AuditID != "" {
					t.Errorf("transient=%+v %v", receipt, err)
				}
				if err := s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
					record, err := store.ReadRecovery(ctx, tx, incident)
					if err != nil {
						return err
					}
					if record.Status != "held" || record.Version != 1 {
						t.Errorf("failed evidence changed incident: %+v", record)
					}
					want := 0
					if terminal {
						want = 1
					}
					for _, table := range []string{"operation_results", "command_audit"} {
						var count int
						if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE operation_id=?", recoveryOperation).Scan(&count); err != nil {
							return err
						}
						if count != want {
							t.Errorf("%s rows=%d want=%d", table, count, want)
						}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				failure = nil
				retry, err := run()
				if terminal {
					if err != nil || !retry.Replayed || retry.Result.Code != store.HostUnverified || calls != 1 {
						t.Fatalf("terminal replay=%+v %v calls=%d", retry, err, calls)
					}
					return
				}
				if err != nil || retry.Replayed || retry.Result.Code != "" || calls != 2 {
					t.Fatalf("retry=%+v %v calls=%d", retry, err, calls)
				}
				replay, err := run()
				if err != nil || !replay.Replayed || replay.AuditID != retry.AuditID || calls != 2 {
					t.Fatalf("replay=%+v %v calls=%d", replay, err, calls)
				}
			})
		}
	}
}
