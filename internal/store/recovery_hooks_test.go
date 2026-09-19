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
