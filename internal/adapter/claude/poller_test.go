package claude_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/ginsys/parley/internal/store"
)

// A cancelled operation must not leave a transaction open for the next poll.
func TestRollbackSurvivesCallerCancellation(t *testing.T) {
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

	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("rollback must finish despite caller cancellation, got %v", err)
	}

	// If the ROLLBACK above hadn't actually reached sqlite, the pinned
	// connection (SetMaxOpenConns(1) forces reuse) would still hold an open
	// BEGIN IMMEDIATE, and this next Begin would fail.
	tx2, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("want the next Begin to succeed once the prior transaction is truly rolled back, got %v", err)
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}
