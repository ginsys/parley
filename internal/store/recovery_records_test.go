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
