package store

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
)

func TestHoldDispositionPreservesWorkAndAudit(t *testing.T) {
	db, b, work, _ := retainedWorkFixture(t)
	ctx := context.Background()
	incident := "60000000-0000-4000-8000-000000000001"
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RevokeBinding(ctx, tx, RevocationRequest{BindingID: b.ID, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1, IncidentID: incident}, nil); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	req, err := NewCommandRequest("hold.disposition", testOperation, Field{Name: "work_id", Value: work})
	if err != nil {
		t.Fatal(err)
	}
	disposition := DispositionRequest{Work: WorkRef{"envelope", work}, IncidentID: incident, ExpectedVersion: 1, Action: "release", ReasonCode: "owner_reviewed", PrincipalID: testPrincipal, OperationID: testOperation}
	receipt, err := db.Coordinator().Execute(ctx, CommandPrincipal{ID: testPrincipal}, req, allowed, func(ctx context.Context, tx *sql.Tx) (CommandResult, error) {
		change, err := ApplyWorkDisposition(ctx, tx, disposition, nil)
		return CommandResult{Resources: []ResourceChange{change}}, err
	}, nil)
	if err != nil || receipt.Result.Resources[0].After != 2 {
		t.Fatalf("disposition=%+v %v", receipt, err)
	}
	if err := db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		held, err := WorkHeld(ctx, tx, disposition.Work)
		if err != nil {
			return err
		}
		if held {
			t.Error("released work held")
		}
		e, err := GetByID(ctx, tx, work)
		if err != nil {
			return err
		}
		if e.State != Queued {
			t.Error("release changed delivery state")
		}
		var count int
		err = tx.QueryRowContext(ctx, `SELECT count(*) FROM work_dispositions d JOIN command_audit a ON a.principal_id=d.audit_principal_id AND a.operation_id=d.audit_operation_id WHERE d.hold_id=?`, receipt.Result.Resources[0].ID).Scan(&count)
		if count != 1 {
			t.Errorf("disposition audit links=%d", count)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
func TestLegacyDispositionCannotRetryUncertainOrEraseEvidence(t *testing.T) {
	for _, state := range []EnvelopeState{Queued, HandedOff, Dispatching, Uncertain} {
		for _, action := range []string{"release", "cancel"} {
			t.Run(string(state)+"_"+action, func(t *testing.T) {
				seed, path := seedVersionFive(t)
				ids := seedLegacyWork(t, seed)
				ctx := context.Background()
				db, err := Open(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				work := " legacy " + string(state) + "☃\n"
				var incident string
				if err := seed.QueryRow("SELECT incident_id FROM migration_incidents").Scan(&incident); err != nil {
					t.Fatal(err)
				}
				before := legacySnapshot(t, seed, ids)
				request, err := NewCommandRequest("legacy.disposition", testOperation, Field{Name: "work", Value: work}, Field{Name: "action", Value: action})
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := db.Coordinator().Execute(ctx, CommandPrincipal{ID: testPrincipal}, request, allowed, func(ctx context.Context, tx *sql.Tx) (CommandResult, error) {
					change, err := ApplyWorkDisposition(ctx, tx, DispositionRequest{Legacy: true, Work: WorkRef{"envelope", work}, IncidentID: incident, ExpectedVersion: 1, Action: action, ReasonCode: "owner_reviewed", EvidenceRef: "70000000-0000-4000-8000-000000000001", PrincipalID: testPrincipal, OperationID: testOperation}, nil)
					if err == RequestTerminal {
						return CommandResult{Code: RequestTerminal}, nil
					}
					return CommandResult{Resources: []ResourceChange{change}}, err
				}, nil)
				if err != nil {
					t.Fatal(err)
				}
				rejected := action == "release" && (state == Dispatching || state == Uncertain)
				if (receipt.Result.Code == RequestTerminal) != rejected {
					t.Fatalf("disposition=%+v", receipt)
				}
				after := legacySnapshot(t, seed, ids)
				for i := range before {
					if before[i].ID == work && action == "cancel" && state == Queued {
						before[i].State = Cancelled
						before[i].UpdatedAt = "1970-01-01T00:00:00Z"
					}
				}
				if !reflect.DeepEqual(after, before) {
					t.Fatalf("disposition changed historical evidence: got=%+v want=%+v", after, before)
				}
				var status, tag string
				if err := seed.QueryRow("SELECT q.status,p.provenance FROM migration_quarantine q JOIN work_provenance p USING(work_kind,work_id) WHERE q.work_id=?", work).Scan(&status, &tag); err != nil {
					t.Fatal(err)
				}
				want := "released"
				if action == "cancel" {
					want = "cancelled"
				}
				if rejected {
					want = "held"
				}
				if status != want || tag != "legacy" {
					t.Fatalf("quarantine=%s provenance=%s", status, tag)
				}
			})
		}
	}
}
