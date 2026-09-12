package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"testing"
)

func sourceFixture(t *testing.T) (*DB, SourceEvent) {
	t.Helper()
	db, b, _, _ := retainedWorkFixture(t)
	e := SourceEvent{BindingID: b.ID, EventID: "native-turn-1", SourceID: "70000000-0000-4000-8000-000000000001", Revision: "revision-1", Digest: sha256.Sum256([]byte("synthetic source")), Before: "start", After: "one"}
	tx, err := db.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := InitializeIngestionSource(context.Background(), tx, b.ID, e.SourceID, e.Before); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return db, e
}
func TestEventPendingPreventsCursorJumpAndSurvivesReplay(t *testing.T) {
	db, first := sourceFixture(t)
	ctx := context.Background()
	second := first
	second.EventID = "native-turn-2"
	second.Before = first.After
	second.After = "two"
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []SourceEvent{first, second} {
		if _, err := StageEvent(ctx, tx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := FinishEvent(ctx, tx, second, EventResult{Classification: "no_marker"}, 0); err != TemporarilyUnavailable {
		t.Fatalf("cursor jumped pending event: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	result, err := StageEvent(ctx, tx, first)
	if err != nil || result.Classification != "pending" || result.Replayed {
		t.Fatalf("pending=%+v %v", result, err)
	}
	if err := FinishEvent(ctx, tx, first, EventResult{Classification: "no_marker"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := FinishEvent(ctx, tx, second, EventResult{Classification: "malformed", Code: InvalidRequest}, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		got, err := StageEvent(ctx, tx, first)
		if err != nil || !got.Replayed || got.Classification != "no_marker" {
			t.Fatalf("terminal replay=%+v %v", got, err)
		}
		changed := first
		changed.Digest[0] ^= 1
		if _, err := StageEvent(ctx, tx, changed); err != EventConflict {
			t.Fatalf("changed event=%v", err)
		}
		var cursor string
		err = tx.QueryRowContext(ctx, "SELECT cursor FROM ingestion_cursors WHERE binding_id=?", first.BindingID).Scan(&cursor)
		if cursor != "two" {
			t.Errorf("cursor=%s", cursor)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
func TestEventClassificationAndCursorRollbackTogether(t *testing.T) {
	db, e := sourceFixture(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := StageEvent(ctx, tx, e); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.sql.Exec("CREATE TRIGGER fail_cursor BEFORE UPDATE ON ingestion_cursors BEGIN SELECT RAISE(ABORT,'synthetic'); END"); err != nil {
		t.Fatal(err)
	}
	_, err = db.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ CommitView) (TransitionResult, error) {
		return TransitionResult{Changed: true}, FinishEvent(ctx, tx, e, EventResult{Classification: "no_marker"}, 0)
	}, nil)
	if err != TemporarilyUnavailable {
		t.Fatalf("injected failure=%v", err)
	}
	var classification, cursor string
	if err := db.sql.QueryRow("SELECT classification,cursor FROM ingestion_evidence JOIN ingestion_cursors USING(binding_id) WHERE native_event_id=?", e.EventID).Scan(&classification, &cursor); err != nil {
		t.Fatal(err)
	}
	if classification != "pending" || cursor != e.Before {
		t.Fatalf("partial result=%s/%s", classification, cursor)
	}
}

func TestResumeRetainsReviewedEventsWithoutAcknowledgment(t *testing.T) {
	db, event := sourceFixture(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := StageEvent(ctx, tx, event); err != nil {
		t.Fatal(err)
	}
	if _, err := RevokeBinding(ctx, tx, RevocationRequest{BindingID: event.BindingID, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1, IncidentID: "60000000-0000-4000-8000-000000000001"}, nil); err != nil {
		t.Fatal(err)
	}
	next := CredentialRecord{BindingID: event.BindingID, ID: "40000000-0000-4000-8000-000000000002", Version: 2, Status: "current", ExpiresAtNS: 300000000000}
	if err := ReenrollCredential(ctx, tx, event.BindingID, 2, 1, next); err != nil {
		t.Fatal(err)
	}
	if err := IngestionAllowed(ctx, tx, event.BindingID); err != SecurityHold {
		t.Fatalf("reenrollment bypassed barrier: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	request, err := NewCommandRequest("ingestion.resume", testOperation, Field{Name: "binding_id", Value: event.BindingID})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := db.Coordinator().Execute(ctx, CommandPrincipal{ID: testPrincipal}, request, allowed, func(ctx context.Context, tx *sql.Tx) (CommandResult, error) {
		change, err := ResumeIngestion(ctx, tx, ResumeIngestionRequest{BindingID: event.BindingID, ExpectedBindingVersion: 3, ExpectedBarrierVersion: 1, EvidenceRef: "80000000-0000-4000-8000-000000000001", PrincipalID: testPrincipal, OperationID: testOperation, Interval: ReviewedInterval{SourceID: event.SourceID, Before: event.Before, After: event.After, Events: []SourceEvent{event}}})
		return CommandResult{Resources: []ResourceChange{change}}, err
	}, nil)
	if err != nil || receipt.Result.Code != "" {
		t.Fatalf("resume=%+v %v", receipt, err)
	}
	if err := db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := IngestionAllowed(ctx, tx, event.BindingID); err != nil {
			return err
		}
		replay, err := StageEvent(ctx, tx, event)
		if err != nil || !replay.Replayed || replay.Classification != "held" {
			t.Fatalf("reviewed event laundered=%+v %v", replay, err)
		}
		var acked, dispositions int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM envelopes WHERE state='acked'").Scan(&acked); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM ingestion_dispositions d JOIN command_audit a ON a.principal_id=d.audit_principal_id AND a.operation_id=d.audit_operation_id").Scan(&dispositions); err != nil {
			return err
		}
		if acked != 0 || dispositions != 1 {
			t.Errorf("ACKs=%d dispositions=%d", acked, dispositions)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
