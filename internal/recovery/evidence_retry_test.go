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

func TestMissingRecoveryIncidentRetainsTerminalResult(t *testing.T) {
	for _, kind := range []string{"clock", "restore"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			actor := store.CommandPrincipal{ID: recoveryPrincipal}
			missing := "60000000-0000-4000-8000-000000000077"
			var s *Service
			var run func() (store.CommandReceipt, error)
			if kind == "clock" {
				a, service, _, r := clockAdministration(t)
				s = service
				r.IncidentID = missing
				a.config.VerifyTime = func(context.Context, ClockReconcileRequest, store.RecoveryRecord) error {
					t.Fatal("missing incident triggered verification")
					return nil
				}
				run = func() (store.CommandReceipt, error) { return a.ClockReconcile(ctx, actor, r) }
			} else {
				a, service, _, r := restoreAdministration(t)
				s = service
				r.IncidentID = missing
				run = func() (store.CommandReceipt, error) { return a.Complete(ctx, actor, r) }
			}
			first, err := run()
			if err != nil || first.Result.Code != store.NotFound || first.AuditID == "" {
				t.Fatalf("missing receipt=%+v %v", first, err)
			}
			marker := Marker{IncidentID: missing, ServerID: s.serverID, Kind: kind}
			if kind == "clock" {
				floor, observed := int64(110000000000), int64(100000000000)
				marker.Floor = &floor
				marker.Observed = &observed
			}
			if err := s.config.Markers.Put(ctx, marker); err != nil {
				t.Fatal(err)
			}
			replay, err := run()
			if err != nil || !replay.Replayed || replay.Result.Code != store.NotFound || replay.AuditID != first.AuditID {
				t.Fatalf("late incident changed result=%+v %v", replay, err)
			}
			if err := s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
				r, err := store.ReadRecovery(ctx, tx, missing)
				if err == nil && (r.Status != "held" || r.Version != 1) {
					t.Errorf("replay mutated new incident=%+v", r)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
