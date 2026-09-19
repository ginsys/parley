package store

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestRecoveryHooksFreezeAuthorityTimeAndCoverReplay(t *testing.T) {
	db := commandDB(t)
	ctx := context.Background()
	var maintenance *RecoveryMaintenance
	held := false
	before, after := 0, 0
	instant := time.Unix(110, 0)
	var err error
	maintenance, err = db.Coordinator().InstallRecovery(RecoveryHooks{
		Before: func(ctx context.Context, kind string) error {
			before++
			if held && kind != "human_inspection" {
				return RecoveryRequired
			}
			// Reentering through maintenance proves no writer gate is held here.
			return maintenance.Inspect(ctx, func(context.Context, *sql.Tx) error { return nil })
		},
		Time: func(context.Context, *sql.Tx, string) (time.Time, error) { return instant, nil },
		After: func() error {
			after++
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			return maintenance.Inspect(ctx, func(context.Context, *sql.Tx) error { return nil })
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest(t, testOperation)
	mutate := func(ctx context.Context, tx *sql.Tx) (CommandResult, error) {
		first := AuthorityTime(ctx, func() time.Time { t.Fatal("resampled authority clock"); return time.Time{} })
		instant = time.Unix(50, 0)
		second := AuthorityTime(ctx, func() time.Time { t.Fatal("resampled authority clock"); return time.Time{} })
		if !first.Equal(time.Unix(110, 0)) || !second.Equal(first) {
			t.Fatalf("authority times=%v / %v", first, second)
		}
		return CommandResult{}, nil
	}
	receipt, err := db.Coordinator().Execute(ctx, CommandPrincipal{ID: testPrincipal}, request, allowed, mutate, nil)
	if err != nil || receipt.Result.Code != "" {
		t.Fatalf("command=%+v %v", receipt, err)
	}
	held = true
	if _, err := db.Coordinator().Execute(ctx, CommandPrincipal{ID: testPrincipal}, request, allowed, mutate, nil); err != RecoveryRequired {
		t.Fatalf("ordinary replay bypassed hold=%v", err)
	}
	replay, found, err := db.Coordinator().LookupCommand(ctx, CommandPrincipal{ID: testPrincipal}, request, allowed)
	if err != nil || !found || !replay.Replayed || replay.AuditID != receipt.AuditID {
		t.Fatalf("human recovery lookup=%+v %v %v", replay, found, err)
	}
	if before != 3 || after != 3 {
		t.Fatalf("hook phases=%d/%d", before, after)
	}
}

// TestExecutePreservesReceiptWhenAfterFailsAfterCommittedSuccess and
// TestExecutePreservesReceiptWhenAfterFailsAfterTerminalRejection are EC-02
// (2026-09-19 review): before this fix, Execute's deferred After-failure
// handler unconditionally zeroed the receipt and reported storageCode(err)
// (usually RecoveryRequired, since that is exactly what
// internal/recovery.Service.after returns) regardless of whether the
// business transaction had already committed -- producing a well-formed
// error response that looked exactly like a proven, never-executed
// rejection, discarding the caller's only evidence (OperationID/AuditID/
// CommitView) of a real, already-durable command. Both tests demonstrably
// commit the business transaction first (LookupCommand below proves it),
// then trigger an After failure, and check that the receipt returned by
// Execute is NOT the zero value and that err is OutcomeUnknown -- the same
// "uncertain, safe to retry with the same operation ID" contract a
// client-side timeout already carries -- never a rejection-looking code.
func TestExecutePreservesReceiptWhenAfterFailsAfterCommittedSuccess(t *testing.T) {
	db := commandDB(t)
	ctx := context.Background()
	afterFails := false
	_, err := db.Coordinator().InstallRecovery(RecoveryHooks{
		Before: func(context.Context, string) error { return nil },
		Time:   func(context.Context, *sql.Tx, string) (time.Time, error) { return time.Unix(1, 0), nil },
		After: func() error {
			if afterFails {
				return RecoveryRequired // exactly what internal/recovery.Service.after actually returns
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(context.Context, *sql.Tx) (CommandResult, error) { return CommandResult{}, nil }

	afterFails = true
	request := testRequest(t, testOperation)
	receipt, err := db.Coordinator().Execute(ctx, CommandPrincipal{ID: testPrincipal}, request, allowed, mutate, nil)
	if err != OutcomeUnknown {
		t.Fatalf("expected OutcomeUnknown, got %v", err)
	}
	if receipt.OperationID == "" || receipt.AuditID == "" {
		t.Fatalf("a post-commit After failure must not discard the committed receipt's evidence: %+v", receipt)
	}
	if receipt.OperationID != testOperation {
		t.Fatalf("receipt.OperationID=%q, want %q", receipt.OperationID, testOperation)
	}

	// Prove the business transaction genuinely committed despite the
	// OutcomeUnknown result -- a same-ID replay must find the durable
	// receipt this very call produced, not re-execute.
	afterFails = false
	replay, found, err := db.Coordinator().LookupCommand(ctx, CommandPrincipal{ID: testPrincipal}, request, allowed)
	if err != nil || !found || !replay.Replayed || replay.AuditID != receipt.AuditID {
		t.Fatalf("the mutation must have actually committed: replay=%+v found=%v err=%v", replay, found, err)
	}
}

func TestExecutePreservesReceiptWhenAfterFailsAfterTerminalRejection(t *testing.T) {
	db := commandDB(t)
	ctx := context.Background()
	afterFails := false
	_, err := db.Coordinator().InstallRecovery(RecoveryHooks{
		Before: func(context.Context, string) error { return nil },
		Time:   func(context.Context, *sql.Tx, string) (time.Time, error) { return time.Unix(1, 0), nil },
		After: func() error {
			if afterFails {
				return RecoveryRequired
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// A terminal domain rejection still records its audit row (see
	// coordinator.go's own doc comments) before this transaction commits --
	// it is just as durably committed as an ordinary success.
	mutate := func(context.Context, *sql.Tx) (CommandResult, error) {
		return CommandResult{Code: NoActiveGrant}, nil
	}

	afterFails = true
	request := testRequest(t, testOperation)
	receipt, err := db.Coordinator().Execute(ctx, CommandPrincipal{ID: testPrincipal}, request, allowed, mutate, nil)
	if err != OutcomeUnknown {
		t.Fatalf("expected OutcomeUnknown, got %v", err)
	}
	if receipt.OperationID == "" || receipt.AuditID == "" {
		t.Fatalf("a post-commit After failure must not discard a committed terminal rejection's evidence: %+v", receipt)
	}

	afterFails = false
	replay, found, err := db.Coordinator().LookupCommand(ctx, CommandPrincipal{ID: testPrincipal}, request, allowed)
	if err != nil || !found || !replay.Replayed || replay.AuditID != receipt.AuditID || replay.Result.Code != NoActiveGrant {
		t.Fatalf("the terminal rejection must have actually committed: replay=%+v found=%v err=%v", replay, found, err)
	}
}

// TestExecutePreservesOutcomeUnknownWhenAfterAlsoFailsAfterAmbiguousCommit is
// round-2 finding 3 (review 5256660570, comment 4053958839): the two tests
// above only special-cased err == nil entering the deferred After-failure
// handler. c.execute's own tx.Commit can independently set err =
// OutcomeUnknown *before* that deferred call ever runs (coordinator.go's
// commit method, on a genuine driver-level commit failure -- forced here the
// same way TestCommandCommitFailurePoisonsCoordinator does, with a
// DEFERRABLE INITIALLY DEFERRED foreign key that only fails at COMMIT, not
// at the INSERT that violates it). Before this fix, an unrelated After
// failure on top of that would downgrade the already-correct "uncertain,
// safe to retry" OutcomeUnknown classification to whatever storageCode(afterErr)
// produced (typically RecoveryRequired, which elsewhere always means
// "provably never committed") -- asserting a stronger, unproven claim about
// a commit whose outcome could never actually be observed. There is no
// receipt to preserve in this path (c.execute returns the zero receipt
// alongside OutcomeUnknown, since the commit was never confirmed) -- only
// the OutcomeUnknown classification itself must survive the After failure.
func TestExecutePreservesOutcomeUnknownWhenAfterAlsoFailsAfterAmbiguousCommit(t *testing.T) {
	db := commandDB(t)
	ctx := context.Background()
	if _, err := db.sql.Exec(`CREATE TABLE commit_parent2(id INTEGER PRIMARY KEY); CREATE TABLE commit_child2(parent INTEGER REFERENCES commit_parent2(id) DEFERRABLE INITIALLY DEFERRED)`); err != nil {
		t.Fatal(err)
	}
	_, err := db.Coordinator().InstallRecovery(RecoveryHooks{
		Before: func(context.Context, string) error { return nil },
		Time:   func(context.Context, *sql.Tx, string) (time.Time, error) { return time.Unix(1, 0), nil },
		After: func() error {
			return RecoveryRequired // an unrelated, independent failure
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(ctx context.Context, tx *sql.Tx) (CommandResult, error) {
		_, err := tx.ExecContext(ctx, "INSERT INTO commit_child2 VALUES(1)")
		return CommandResult{}, err
	}
	request := testRequest(t, testOperation)
	receipt, err := db.Coordinator().Execute(ctx, CommandPrincipal{ID: testPrincipal}, request, allowed, mutate, nil)
	if err != OutcomeUnknown {
		t.Fatalf("an unrelated After failure must not downgrade an already-ambiguous commit's OutcomeUnknown, got %v", err)
	}
	if receipt.OperationID != "" || receipt.AuditID != "" {
		t.Fatalf("an unconfirmed commit has no receipt to preserve, got %+v", receipt)
	}
}

func TestRecoveryHooksCannotBeInstalledAfterAdmission(t *testing.T) {
	db := commandDB(t)
	if err := db.Coordinator().ClaimConnections(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := db.Coordinator().InstallRecovery(RecoveryHooks{Before: func(context.Context, string) error { return nil }, Time: func(context.Context, *sql.Tx, string) (time.Time, error) { return time.Now(), nil }, After: func() error { return nil }})
	if err != InvalidRequest {
		t.Fatalf("late recovery installation=%v", err)
	}
}
