package claude_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	adapterclaude "github.com/ginsys/parley/internal/adapter/claude"
	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/dispatch"
	"github.com/ginsys/parley/internal/store"
)

func TestPollerRetryableBatchDoesNotStarveLaterMessages(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if _, err := controller.New(db).Grant(ctx, controller.GrantParams{Conversation: "c", PeerAID: "a", PeerBID: "b", Direction: store.Bidirectional, MaxExchanges: 200}); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	transport := transportFunc(func(_ context.Context, e store.Envelope) error {
		attempts++
		if e.Text == "retry later" {
			return dispatch.ErrNoAttempt
		}
		return nil
	})
	b := dispatch.New(db, transport)
	for i := 0; i < store.MaxQueueBatch; i++ {
		if _, err := b.Send(ctx, "c", "a", "b", "retry later", nil); err != nil {
			t.Fatal(err)
		}
	}
	last, err := b.Send(ctx, "c", "a", "b", "deliverable", nil)
	if err != nil {
		t.Fatal(err)
	}
	probe := &probeRecorder{}
	hs, err := adapterclaude.NewHandshake(probe.send, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer hs.Stop()
	if err := hs.Start(); err != nil {
		t.Fatal(err)
	}
	if !hs.Ack(probe.last()) {
		t.Fatal("ack")
	}
	poller := adapterclaude.NewPoller(db, b, hs, "c", "b")
	outcomes, err := poller.Tick(ctx)
	if err != nil || len(outcomes) != 100 || attempts != 100 {
		t.Fatalf("first batch: %d %d %v", len(outcomes), attempts, err)
	}
	outcomes, err = poller.Tick(ctx)
	if err != nil || len(outcomes) != 1 || outcomes[0].ID != last.ID || outcomes[0].State != store.HandedOff {
		t.Fatalf("starved later message: %+v %v", outcomes, err)
	}
	outcomes, err = poller.Tick(ctx)
	if err != nil || len(outcomes) != 100 {
		t.Fatalf("did not wrap to retry batch: %d %v", len(outcomes), err)
	}
}

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
