package store

import (
	"context"
	"database/sql"
	"testing"
)

func TestRecoveryRecordReplayPreservesActiveHoldAndRejectsClearedReuse(t *testing.T) {
	db := commandDB(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	r := RecoveryRecord{ID: "60000000-0000-4000-8000-000000000002", Kind: "restore", Status: "held", Version: 1}
	if err := tx.QueryRowContext(ctx, "SELECT server_id FROM installation").Scan(&r.ServerID); err != nil {
		t.Fatal(err)
	}
	if changed, err := RecordRecovery(ctx, tx, r); err != nil || !changed {
		t.Fatalf("initial record=%v %v", changed, err)
	}
	checkReplay := func(wantStatus string, wantVersion int64, wantHeld bool, wantErr error) {
		t.Helper()
		changed, err := RecordRecovery(ctx, tx, r)
		if changed || err != wantErr {
			t.Errorf("%s replay=%v %v", wantStatus, changed, err)
		}
		current, err := ReadRecovery(ctx, tx, r.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status != wantStatus || current.Version != wantVersion {
			t.Errorf("replay mutated record=%+v", current)
		}
		held, err := RecoveryHeld(ctx, tx)
		if err != nil || held != wantHeld {
			t.Errorf("%s hold=%v %v", wantStatus, held, err)
		}
		if wantStatus != "held" && current.Evidence != (sql.NullString{String: testOperation, Valid: true}) {
			t.Errorf("replay changed reconciliation evidence=%+v", current.Evidence)
		}
	}
	checkReplay("held", 1, true, nil)
	if _, err := ReconcileRecovery(ctx, tx, r.ID, 1, testOperation); err != nil {
		t.Fatal(err)
	}
	// Reconciled still holds until marker removal; retries must stay idempotent.
	checkReplay("reconciled", 2, true, nil)
	if _, err := ClearRecovery(ctx, tx, r.ID, 2, testOperation); err != nil {
		t.Fatal(err)
	}
	checkReplay("cleared", 3, false, RecoveryRequired)
}

func TestNamespaceRetirementRejectsDifferentIncident(t *testing.T) {
	db := commandDB(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var server string
	if err := tx.QueryRowContext(ctx, "SELECT server_id FROM installation").Scan(&server); err != nil {
		t.Fatal(err)
	}
	first, second := "60000000-0000-4000-8000-000000000001", "60000000-0000-4000-8000-000000000002"
	for _, id := range []string{first, second} {
		if _, err := RecordRecovery(ctx, tx, RecoveryRecord{ID: id, ServerID: server, Kind: "restore", Status: "held", Version: 1}); err != nil {
			t.Fatal(err)
		}
	}
	retirement := NamespaceRetirement{PrincipalID: "80000000-0000-4000-8000-000000000001"}
	if err := RetireRecoveryNamespace(ctx, tx, first, retirement, 1); err != nil {
		t.Fatal(err)
	}
	if err := RetireRecoveryNamespace(ctx, tx, first, retirement, 2); err != nil {
		t.Fatalf("exact incident replay=%v", err)
	}
	if err := RetireRecoveryNamespace(ctx, tx, second, retirement, 3); err != VersionConflict {
		t.Errorf("different incident retirement=%v", err)
	}
	var incident string
	var retiredAt int64
	if err := tx.QueryRowContext(ctx, "SELECT incident_id,retired_at_ns FROM retired_namespaces WHERE principal_id=?", retirement.PrincipalID).Scan(&incident, &retiredAt); err != nil {
		t.Fatal(err)
	}
	if incident != first || retiredAt != 1 {
		t.Errorf("original evidence changed: %s %d", incident, retiredAt)
	}
}
