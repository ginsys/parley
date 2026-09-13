package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
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
	if _, err := InitializeIngestionSource(context.Background(), tx, b.ID, e.SourceID, e.Before); err != nil {
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

func TestResumeCannotJumpPastRetainedPendingEvidence(t *testing.T) {
	db, first := sourceFixture(t)
	ctx := context.Background()
	second := first
	second.EventID = "native-turn-2"
	second.Before = "one"
	second.After = "two"
	future := first
	future.EventID = "native-turn-3"
	future.Before = "two"
	future.After = "three"
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, e := range []SourceEvent{second, future} {
		if _, err := StageEvent(ctx, tx, e); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := RevokeBinding(ctx, tx, RevocationRequest{BindingID: first.BindingID, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1, IncidentID: "60000000-0000-4000-8000-000000000001"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := ReenrollCredential(ctx, tx, first.BindingID, 2, 1, CredentialRecord{BindingID: first.BindingID, ID: "40000000-0000-4000-8000-000000000002", Version: 2, Status: "current", ExpiresAtNS: 300000000000}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	request, err := NewCommandRequest("ingestion.resume", testOperation, Field{Name: "binding_id", Value: first.BindingID})
	if err != nil {
		t.Fatal(err)
	}
	run := func(events []SourceEvent) (CommandReceipt, error) {
		return db.Coordinator().Execute(ctx, CommandPrincipal{ID: testPrincipal}, request, allowed, func(ctx context.Context, tx *sql.Tx) (CommandResult, error) {
			change, err := ResumeIngestion(ctx, tx, ResumeIngestionRequest{BindingID: first.BindingID, ExpectedBindingVersion: 3, ExpectedBarrierVersion: 1, EvidenceRef: "80000000-0000-4000-8000-000000000001", PrincipalID: testPrincipal, OperationID: testOperation, Interval: ReviewedInterval{SourceID: first.SourceID, Before: "start", After: "two", Events: events}})
			return CommandResult{Resources: []ResourceChange{change}}, err
		}, nil)
	}
	jump := first
	jump.After = "two"
	if _, err := run([]SourceEvent{jump}); err != EventConflict {
		t.Fatalf("skipped retained event: %v", err)
	}
	if err := db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var cursor string
		var version int64
		if err := tx.QueryRowContext(ctx, "SELECT cursor,cursor_version FROM ingestion_cursors WHERE binding_id=?", first.BindingID).Scan(&cursor, &version); err != nil {
			return err
		}
		if cursor != "start" || version != 1 {
			t.Errorf("rejected interval advanced cursor=%s/v%d", cursor, version)
		}
		if err := IngestionAllowed(ctx, tx, first.BindingID); err != SecurityHold {
			t.Errorf("barrier changed=%v", err)
		}
		var pending, total int
		if err := tx.QueryRowContext(ctx, "SELECT count(*),count(CASE WHEN classification='pending' THEN 1 END) FROM ingestion_evidence").Scan(&total, &pending); err != nil {
			return err
		}
		if total != 2 || pending != 2 {
			t.Errorf("rejected interval changed evidence: %d/%d", pending, total)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if receipt, err := run([]SourceEvent{first, second}); err != nil || receipt.Result.Code != "" {
		t.Fatalf("complete interval=%+v %v", receipt, err)
	}
	if err := db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		pending, _, err := LookupEvent(ctx, tx, future)
		if err != nil {
			return err
		}
		if pending.Classification != "pending" || pending.Replayed {
			t.Errorf("boundary event changed=%+v", pending)
		}
		return EventAtCursor(ctx, tx, future)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestResumePendingEvidenceMaterializationBound(t *testing.T) {
	for _, count := range []int{1000, 1001} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			db, event := sourceFixture(t)
			ctx := context.Background()
			tx, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			for i := 0; i < count; i++ {
				e := event
				e.EventID = fmt.Sprintf("pending-%d", i)
				e.Before = fmt.Sprint(i)
				e.After = fmt.Sprint(i + 1)
				if _, err := StageEvent(ctx, tx, e); err != nil {
					t.Fatal(err)
				}
			}
			err = pendingAfterResume(ctx, tx, event.BindingID, event.SourceID, "0")
			if count == 1000 && err != nil {
				t.Fatalf("bounded future chain=%v", err)
			}
			if count == 1001 && err != CapacityExceeded {
				t.Fatalf("over-capacity future chain=%v", err)
			}
		})
	}
}

func TestInitializationCannotStrandRetainedPendingEvidence(t *testing.T) {
	for _, variant := range []string{"past-event", "different-source", "at-first-edge"} {
		t.Run(variant, func(t *testing.T) {
			db, b, _, _ := retainedWorkFixture(t)
			ctx := context.Background()
			event := SourceEvent{BindingID: b.ID, EventID: "early-event", SourceID: "70000000-0000-4000-8000-000000000001", Revision: "one", Digest: sha256.Sum256([]byte("synthetic")), Before: "start", After: "next"}
			tx, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := StageEvent(ctx, tx, event); err != nil {
				t.Fatal(err)
			}
			if err := EventAtCursor(ctx, tx, event); err != HostUnverified {
				t.Fatalf("uninitialized event=%v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			source, cursor := event.SourceID, event.Before
			want := Code("")
			switch variant {
			case "past-event":
				cursor = event.After
				want = EventConflict
			case "different-source":
				source = "70000000-0000-4000-8000-000000000002"
				want = EventConflict
			}
			tx, err = db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			_, err = InitializeIngestionSource(ctx, tx, b.ID, source, cursor)
			if want != "" {
				if err != want {
					t.Fatalf("initialization bypass=%v want %v", err, want)
				}
				var count int
				if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM ingestion_cursors").Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 0 {
					t.Fatal("rejected initialization inserted cursor")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := FinishEvent(ctx, tx, event, EventResult{Classification: "no_marker"}, 0); err != nil {
					t.Fatalf("retained event unreachable=%v", err)
				}
			}
		})
	}
}

func TestEmptyResumeCannotInitializePastPendingEvidence(t *testing.T) {
	for _, variant := range []string{"past-event", "different-source", "at-first-edge"} {
		t.Run(variant, func(t *testing.T) {
			db, b, _, _ := retainedWorkFixture(t)
			ctx := context.Background()
			e := SourceEvent{BindingID: b.ID, EventID: "early", SourceID: "70000000-0000-4000-8000-000000000001", Revision: "one", Digest: sha256.Sum256([]byte("synthetic")), Before: "start", After: "next"}
			tx, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := StageEvent(ctx, tx, e); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO ingestion_barriers(binding_id,barrier_version,status) VALUES(?,1,'held')", b.ID); err != nil {
				t.Fatal(err)
			}
			source, cursor := e.SourceID, e.Before
			want := Code("")
			switch variant {
			case "past-event":
				cursor = e.After
				want = EventConflict
			case "different-source":
				source = "70000000-0000-4000-8000-000000000002"
				want = EventConflict
			}
			_, err = ResumeIngestion(ctx, tx, ResumeIngestionRequest{BindingID: b.ID, ExpectedBindingVersion: 1, ExpectedBarrierVersion: 1, EvidenceRef: "80000000-0000-4000-8000-000000000010", PrincipalID: testPrincipal, OperationID: testOperation, Interval: ReviewedInterval{SourceID: source, Before: cursor, After: cursor}})
			if want != "" {
				if err != want {
					t.Fatalf("resume stranded evidence=%v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := EventAtCursor(ctx, tx, e); err != nil {
					t.Fatalf("resume lost pending retry=%v", err)
				}
			}
		})
	}
}
