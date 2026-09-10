package claude_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ginsys/parley/internal/store"
)

// Regression for a finding on PR #3's fifth review round: store.Tx wraps a
// manually issued BEGIN IMMEDIATE on a pinned connection, not a real
// database/sql.Tx, so the pool has no automatic transaction-abort-on-close
// behavior of its own. Poller.queuedForMe used to defer tx.Rollback(ctx)
// with the same ctx passed into the whole call — if that ctx were canceled
// before the deferred Rollback ran, ROLLBACK's own ExecContext could return
// without the statement reaching sqlite, yet Tx.Rollback still
// unconditionally closes/releases the connection, handing it back to the
// pool (SetMaxOpenConns(1) guarantees the very next Begin reuses this exact
// connection) with its BEGIN IMMEDIATE still open — stalling every
// subsequent Begin until the process restarts. The fix, mirroring
// dispatch.Bridge.Dispatch's existing recordCtx pattern, is
// context.WithoutCancel(ctx). This test exercises that detachment directly
// against store.Tx (the layer the defect actually lives in): reliably
// forcing a real cancellation to land inside the narrow window between
// Begin and a deferred Rollback through the higher-level Poller API would
// require racing an internal timing window rather than asserting the fix's
// actual guarantee, which is that context.WithoutCancel lets Rollback
// complete regardless of the caller's own context.
func TestRollbackWithDetachedContextSurvivesCallerCancellation(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "parley.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	callerCtx, cancel := context.WithCancel(ctx)
	tx, err := db.Begin(callerCtx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	cancel()

	if err := tx.Rollback(context.WithoutCancel(callerCtx)); err != nil {
		t.Fatalf("rollback with a detached context must still succeed despite the caller's context being canceled, got %v", err)
	}

	// If the ROLLBACK above hadn't actually reached sqlite, the pinned
	// connection (SetMaxOpenConns(1) forces reuse) would still hold an open
	// BEGIN IMMEDIATE, and this next Begin would fail.
	tx2, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("want the next Begin to succeed once the prior transaction is truly rolled back, got %v", err)
	}
	if err := tx2.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}
