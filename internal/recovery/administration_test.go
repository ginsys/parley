package recovery

import (
	"context"
	"database/sql"
	"testing"
	"time"

	runtimeowner "github.com/ginsys/parley/internal/runtime"
	"github.com/ginsys/parley/internal/store"
)

const recoveryPrincipal = "80000000-0000-4000-8000-000000000001"
const recoveryOperation = "80000000-0000-4000-8000-000000000002"
const recoveryEvidence = "80000000-0000-4000-8000-000000000003"

func clockAdministration(t *testing.T) (*Administration, *Service, *time.Time, ClockReconcileRequest) {
	t.Helper()
	s, now, _ := recoveryFixture(t)
	*now = time.Unix(100, 0)
	if err := rejectOrdinary(t, s); err != store.RecoveryRequired {
		t.Fatal(err)
	}
	markers, err := s.config.Markers.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	*now = time.Unix(200, 0)
	monotonic := time.Unix(0, 0)
	a, err := NewAdministration(AdministrationConfig{Service: s, Authorize: func(context.Context, *sql.Tx, store.CommandPrincipal) error { return nil }, VerifyTime: func(context.Context, ClockReconcileRequest, store.RecoveryRecord) error { return nil }, MonotonicNow: func() time.Time { return monotonic }, Wait: func(context.Context, time.Duration) error {
		monotonic = monotonic.Add(time.Second)
		*now = now.Add(time.Second)
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	return a, s, now, ClockReconcileRequest{OperationID: recoveryOperation, IncidentID: markers[0].IncidentID, ExpectedClockVersion: 1, TimeEvidenceRef: recoveryEvidence}
}

type removalFailure struct {
	Markers
	fail bool
}

func (m *removalFailure) Remove(ctx context.Context, marker Marker) error {
	if m.fail {
		return store.TemporarilyUnavailable
	}
	return m.Markers.Remove(ctx, marker)
}
func TestClockReconciliationReplayFinishesOnlyCommittedCleanup(t *testing.T) {
	a, s, _, request := clockAdministration(t)
	ctx := context.Background()
	markers := &removalFailure{Markers: s.config.Markers, fail: true}
	s.config.Markers = markers
	receipt, err := a.ClockReconcile(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, request)
	if err != store.TemporarilyUnavailable || receipt.Result.Code != "" || len(receipt.Result.Resources) != 1 {
		t.Fatalf("committed reconciliation cleanup=%+v %v", receipt, err)
	}
	if mode, err := s.InspectRecovery(ctx, s.config.Store); err != nil || mode != runtimeowner.Held {
		t.Fatalf("premature recovery=%v %v", mode, err)
	}
	markers.fail = false
	a.config.VerifyTime = func(context.Context, ClockReconcileRequest, store.RecoveryRecord) error {
		t.Fatal("replay reran reviewed time source")
		return store.HostUnverified
	}
	replay, err := a.ClockReconcile(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, request)
	if err != nil || !replay.Replayed || replay.AuditID != receipt.AuditID {
		t.Fatalf("cleanup replay=%+v %v", replay, err)
	}
	if mode, err := s.InspectRecovery(ctx, s.config.Store); err != nil || mode != runtimeowner.Normal {
		t.Fatalf("completed recovery=%v %v", mode, err)
	}
	if err := s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		r, err := store.ReadRecovery(ctx, tx, request.IncidentID)
		if err != nil {
			return err
		}
		if r.Status != "cleared" || r.Version != 3 {
			t.Errorf("final record=%+v", r)
		}
		var count int
		err = tx.QueryRowContext(ctx, "SELECT count(*) FROM command_audit WHERE principal_id=? AND operation_id=?", recoveryPrincipal, recoveryOperation).Scan(&count)
		if count != 1 {
			t.Errorf("duplicate audit=%d", count)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
func TestClockReconciliationRequiresSeparatedSamples(t *testing.T) {
	a, s, _, request := clockAdministration(t)
	a.config.Wait = func(context.Context, time.Duration) error { return nil }
	receipt, err := a.ClockReconcile(context.Background(), store.CommandPrincipal{ID: recoveryPrincipal}, request)
	if err != nil || receipt.Result.Code != store.HostUnverified {
		t.Fatalf("unseparated samples=%+v %v", receipt, err)
	}
	if mode, err := s.InspectRecovery(context.Background(), s.config.Store); err != nil || mode != runtimeowner.Held {
		t.Fatalf("invalid evidence cleared hold=%v %v", mode, err)
	}
}
func TestClockCleanupCannotClearAnotherIncidentOrReusedMarker(t *testing.T) {
	a, s, _, request := clockAdministration(t)
	ctx := context.Background()
	other := Marker{IncidentID: "60000000-0000-4000-8000-000000000002", ServerID: s.serverID, Kind: "restore"}
	if err := s.config.Markers.Put(ctx, other); err != nil {
		t.Fatal(err)
	}
	receipt, err := a.ClockReconcile(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, request)
	if err != nil || receipt.Result.Code != "" {
		t.Fatalf("reconcile=%+v %v", receipt, err)
	}
	if mode, err := s.InspectRecovery(ctx, s.config.Store); err != nil || mode != runtimeowner.Held {
		t.Fatalf("other restore incident cleared=%v %v", mode, err)
	}
	var old store.RecoveryRecord
	if err := s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		old, err = store.ReadRecovery(ctx, tx, request.IncidentID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.config.Markers.Put(ctx, markerForRecord(old)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ClockReconcile(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, request); err != store.RecoveryRequired {
		t.Fatalf("reused cleared marker removed=%v", err)
	}
}

func TestClockReconciliationStaleVersionRetainsExactIncident(t *testing.T) {
	a, s, _, request := clockAdministration(t)
	ctx := context.Background()
	request.ExpectedClockVersion = 2
	receipt, err := a.ClockReconcile(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, request)
	if err != nil || receipt.Result.Code != store.VersionConflict {
		t.Fatalf("stale reconciliation=%+v %v", receipt, err)
	}
	markers, err := s.config.Markers.List(ctx)
	if err != nil || len(markers) != 1 || markers[0].IncidentID != request.IncidentID {
		t.Fatalf("stale removed incident=%+v %v", markers, err)
	}
	if err := s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		record, err := store.ReadRecovery(ctx, tx, request.IncidentID)
		if record.Status != "held" || record.Version != 1 {
			t.Errorf("stale mutated record=%+v", record)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
