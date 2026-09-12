package store

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const testPrincipal = "10000000-0000-4000-8000-000000000001"
const testOperation = "20000000-0000-4000-8000-000000000001"

func commandDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(context.Background(), filepath.Join(t.TempDir(), "commands.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
func testRequest(t *testing.T, id string, fields ...Field) CommandRequest {
	t.Helper()
	r, err := NewCommandRequest("binding.register", id, fields...)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func allowed(context.Context, *sql.Tx) error { return nil }
func insertSynthetic(ctx context.Context, tx *sql.Tx) (CommandResult, error) {
	_, err := tx.ExecContext(ctx, "INSERT INTO conversations VALUES('synthetic','synthetic','2026-01-01T00:00:00Z')")
	return CommandResult{}, err
}
func TestCommandReplayReauthorizesBeforeReturningReceipt(t *testing.T) {
	db := commandDB(t)
	req := testRequest(t, testOperation, Field{"peer_id", "exact "})
	p := CommandPrincipal{ID: testPrincipal, ConnectorUID: 1000}
	first, err := db.Coordinator().Execute(context.Background(), p, req, allowed, insertSynthetic, nil)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := db.Coordinator().Execute(context.Background(), p, req, allowed, func(context.Context, *sql.Tx) (CommandResult, error) {
		t.Fatal("replayed mutation")
		return CommandResult{}, nil
	}, nil)
	if err != nil || !replay.Replayed || replay.AuditID != first.AuditID {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	_, err = db.Coordinator().Execute(context.Background(), p, req, func(context.Context, *sql.Tx) error { return Forbidden }, insertSynthetic, nil)
	if !errors.Is(err, Forbidden) {
		t.Fatalf("unauthorized replay: %v", err)
	}
	_, err = db.Coordinator().Execute(context.Background(), p, testRequest(t, testOperation, Field{"peer_id", "exact"}), allowed, insertSynthetic, nil)
	if !errors.Is(err, OperationConflict) {
		t.Fatalf("conflicting replay: %v", err)
	}
	var count int
	if err := db.sql.QueryRow("SELECT count(*) FROM command_audit").Scan(&count); err != nil || count != 1 {
		t.Fatalf("audits=%d err=%v", count, err)
	}
}
func TestCommandAuditFailureRollsBackMutation(t *testing.T) {
	db := commandDB(t)
	if _, err := db.sql.Exec("CREATE TRIGGER fail_audit BEFORE INSERT ON command_audit BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	published := false
	_, err := db.Coordinator().Execute(context.Background(), CommandPrincipal{testPrincipal, 1000}, testRequest(t, testOperation), allowed, insertSynthetic, func(CommitView) { published = true })
	if err == nil || published {
		t.Fatalf("err=%v published=%t", err, published)
	}
	for _, table := range []string{"conversations", "operation_results", "command_audit"} {
		var n int
		if err := db.sql.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("%s rows=%d err=%v", table, n, err)
		}
	}
}
func TestTerminalRejectionDiscardsPartialEffectAndIsReplayed(t *testing.T) {
	db := commandDB(t)
	req := testRequest(t, testOperation)
	mutation := func(ctx context.Context, tx *sql.Tx) (CommandResult, error) {
		if _, err := insertSynthetic(ctx, tx); err != nil {
			return CommandResult{}, err
		}
		return CommandResult{Code: VersionConflict}, nil
	}
	p := CommandPrincipal{testPrincipal, 1000}
	got, err := db.Coordinator().Execute(context.Background(), p, req, allowed, mutation, nil)
	if err != nil || got.Result.Code != VersionConflict {
		t.Fatalf("receipt=%+v err=%v", got, err)
	}
	var n int
	if err := db.sql.QueryRow("SELECT count(*) FROM conversations").Scan(&n); err != nil || n != 0 {
		t.Fatalf("partial effects=%d err=%v", n, err)
	}
	got, err = db.Coordinator().Execute(context.Background(), p, req, allowed, insertSynthetic, nil)
	if err != nil || !got.Replayed || got.Result.Code != VersionConflict {
		t.Fatalf("replay=%+v err=%v", got, err)
	}
}
func TestCommandConcurrencyAndOverflow(t *testing.T) {
	db := commandDB(t)
	c := db.Coordinator()
	if c != db.Coordinator() {
		t.Fatal("more than one coordinator per writer")
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := c.Execute(context.Background(), CommandPrincipal{testPrincipal, 1000}, testRequest(t, testOperation), allowed, insertSynthetic, nil); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if _, err := db.sql.Exec("UPDATE installation SET audit_sequence=?", int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	_, err := c.Execute(context.Background(), CommandPrincipal{testPrincipal, 1000}, testRequest(t, "20000000-0000-4000-8000-000000000002"), allowed, func(context.Context, *sql.Tx) (CommandResult, error) {
		t.Fatal("overflow reached mutation")
		return CommandResult{}, nil
	}, nil)
	if !errors.Is(err, InvalidRequest) {
		t.Fatalf("overflow: %v", err)
	}
}
func TestCanonicalCommandRequest(t *testing.T) {
	a := testRequest(t, testOperation, Field{"b", Set{"b", "a"}}, Field{"a", Fields{{"n", int64(2)}}})
	b := testRequest(t, testOperation, Field{"a", Fields{{"n", int64(2)}}}, Field{"b", Set{"a", "b"}})
	if a.digest != b.digest {
		t.Fatal("equivalent logical request differs")
	}
	for _, fields := range []Fields{
		{{"x", true}, {"x", false}}, {{"x", math.MaxFloat64}}, {{"x", Set{"a", "a"}}}, {{"secret", "synthetic-only"}},
	} {
		if _, err := NewCommandRequest("binding.register", testOperation, fields...); err == nil {
			t.Fatalf("accepted invalid fields: %v", fields)
		}
	}
	absent := testRequest(t, testOperation)
	null := testRequest(t, testOperation, Field{"x", nil})
	if absent.digest == null.digest {
		t.Fatal("absent/null conflated")
	}
}

func TestCommandPublicationPrecedesNextAdmissionAndWaitCancels(t *testing.T) {
	db := commandDB(t)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := db.Coordinator().Execute(context.Background(), CommandPrincipal{testPrincipal, 1000}, testRequest(t, testOperation), allowed, insertSynthetic, func(view CommitView) { close(entered); <-release })
		done <- err
	}()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := db.Coordinator().Execute(ctx, CommandPrincipal{testPrincipal, 1000}, testRequest(t, testOperation), func(context.Context, *sql.Tx) error { t.Error("entered while publication pending"); return nil }, insertSynthetic, nil)
	close(release)
	if err != TemporarilyUnavailable {
		t.Fatalf("cancelled wait: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCommandCommitFailurePoisonsCoordinator(t *testing.T) {
	db := commandDB(t)
	if _, err := db.sql.Exec(`CREATE TABLE commit_parent(id INTEGER PRIMARY KEY); CREATE TABLE commit_child(parent INTEGER REFERENCES commit_parent(id) DEFERRABLE INITIALLY DEFERRED)`); err != nil {
		t.Fatal(err)
	}
	published := false
	_, err := db.Coordinator().Execute(context.Background(), CommandPrincipal{testPrincipal, 1000}, testRequest(t, testOperation), allowed, func(ctx context.Context, tx *sql.Tx) (CommandResult, error) {
		_, err := tx.ExecContext(ctx, "INSERT INTO commit_child VALUES(1)")
		return CommandResult{}, err
	}, func(CommitView) { published = true })
	if err != OutcomeUnknown || published {
		t.Fatalf("err=%v published=%t", err, published)
	}
	_, err = db.Coordinator().Execute(context.Background(), CommandPrincipal{testPrincipal, 1000}, testRequest(t, testOperation), allowed, insertSynthetic, nil)
	if err != RecoveryRequired {
		t.Fatalf("continued after unknown commit: %v", err)
	}
	var count int
	if err := db.sql.QueryRow("SELECT count(*) FROM operation_results").Scan(&count); err != nil || count != 0 {
		t.Fatalf("receipts=%d err=%v", count, err)
	}
}

func TestCommandReplaySurvivesReopenAndRecordsArePermanent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "replay.db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	req := testRequest(t, testOperation)
	first, err := db.Coordinator().Execute(ctx, CommandPrincipal{testPrincipal, 1000}, req, allowed, insertSynthetic, nil)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	replay, err := db.Coordinator().Execute(ctx, CommandPrincipal{testPrincipal, 1000}, req, allowed, insertSynthetic, nil)
	if err != nil || !replay.Replayed || first.View != replay.View || first.AuditID != replay.AuditID {
		t.Fatalf("receipt=%+v err=%v", replay, err)
	}
	if db.Coordinator().epoch == first.View.Epoch {
		t.Fatal("startup reused epoch")
	}
	for _, query := range []string{
		"DELETE FROM operation_results", "UPDATE operation_results SET result_json='{}'",
		"DELETE FROM command_audit", "UPDATE command_audit SET audit_sequence=2",
	} {
		if _, err := db.sql.Exec(query); err == nil {
			t.Fatalf("accepted replay-evidence destruction: %s", query)
		}
	}
}

func TestCommandCapacityFailureAndVersionLimits(t *testing.T) {
	db := commandDB(t)
	_, err := db.Coordinator().Execute(context.Background(), CommandPrincipal{testPrincipal, 1000}, testRequest(t, testOperation), allowed, func(ctx context.Context, tx *sql.Tx) (CommandResult, error) {
		_, err := insertSynthetic(ctx, tx)
		if err != nil {
			return CommandResult{}, err
		}
		return CommandResult{}, CapacityExceeded
	}, nil)
	if err != CapacityExceeded {
		t.Fatalf("capacity error: %v", err)
	}
	var n int
	if err := db.sql.QueryRow("SELECT count(*) FROM conversations").Scan(&n); err != nil || n != 0 {
		t.Fatalf("partial effects=%d err=%v", n, err)
	}
	if next, err := NextVersion(math.MaxInt64 - 1); err != nil || next != math.MaxInt64 {
		t.Fatalf("boundary next=%d err=%v", next, err)
	}
	if _, err := NextVersion(math.MaxInt64); err != InvalidRequest {
		t.Fatalf("overflow: %v", err)
	}
	if _, err := InstantNanos(time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)); err != InvalidRequest {
		t.Fatalf("unrepresentable timestamp: %v", err)
	}
}

func TestTransientCommandResultsDoNotBecomePermanentReceipts(t *testing.T) {
	for _, code := range []Code{TemporarilyUnavailable, CapacityExceeded, RecoveryRequired, OutcomeUnknown, AuthenticationFailed} {
		t.Run(string(code), func(t *testing.T) {
			db := commandDB(t)
			req := testRequest(t, testOperation)
			actor := CommandPrincipal{testPrincipal, 1000}
			_, err := db.Coordinator().Execute(context.Background(), actor, req, allowed, func(ctx context.Context, tx *sql.Tx) (CommandResult, error) {
				if _, err := insertSynthetic(ctx, tx); err != nil {
					return CommandResult{}, err
				}
				return CommandResult{Code: code}, nil
			}, nil)
			if err != code {
				t.Fatalf("transient result committed: err=%v want=%s", err, code)
			}
			for _, table := range []string{"conversations", "operation_results", "command_audit"} {
				var n int
				if err := db.sql.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 0 {
					t.Fatalf("%s=%d err=%v", table, n, err)
				}
			}
			receipt, err := db.Coordinator().Execute(context.Background(), actor, req, allowed, insertSynthetic, nil)
			if err != nil || receipt.Replayed || receipt.Result.Code != "" {
				t.Fatalf("same-ID retry failed: %+v %v", receipt, err)
			}
		})
	}
}
func TestCancelledCommitDoesNotPoisonCoordinator(t *testing.T) {
	db := commandDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := insertSynthetic(ctx, tx); err != nil {
		t.Fatal(err)
	}
	// Cancel precisely before database/sql.Commit's context/done check, after SQL
	// completed. The pinned SQLite driver commits with context.Background.
	cancel()
	if err := db.Coordinator().commit(tx); err != TemporarilyUnavailable {
		t.Fatalf("cancelled commit: %v", err)
	}
	tx.Rollback()
	receipt, err := db.Coordinator().Execute(context.Background(), CommandPrincipal{testPrincipal, 1000}, testRequest(t, testOperation), allowed, insertSynthetic, nil)
	if err != nil || receipt.Result.Code != "" {
		t.Fatalf("coordinator poisoned by cancellation: %v", err)
	}
}
