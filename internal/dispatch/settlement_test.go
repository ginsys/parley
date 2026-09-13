package dispatch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/store"
)

type testTransport struct{ err error }

func (t testTransport) Deliver(context.Context, store.Envelope) error { return t.err }

func setupSettlement(t *testing.T) (*store.DB, *Bridge, *store.Envelope, string) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "settle.db")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.OpenReaders(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := controller.New(db).Grant(ctx, controller.GrantParams{Conversation: "c", PeerAID: "a", PeerBID: "b", Direction: store.Bidirectional, MaxExchanges: 5}); err != nil {
		t.Fatal(err)
	}
	bridge := New(db, testTransport{})
	e, err := bridge.Send(ctx, "c", "a", "b", "message", nil)
	if err != nil {
		t.Fatal(err)
	}
	return db, bridge, e, path
}

func TestSettlementCannotRefundOrOverwriteANewerAttempt(t *testing.T) {
	db, b, e, _ := setupSettlement(t)
	ctx := context.Background()
	first, ok, err := b.claim(ctx, e.ID)
	if err != nil || !ok {
		t.Fatalf("claim: %v", err)
	}
	if _, err := b.settle(ctx, first, ErrNoAttempt); err != nil {
		t.Fatal(err)
	}
	second, ok, err := b.claim(ctx, e.ID)
	if err != nil || !ok {
		t.Fatalf("claim 2: %v", err)
	}
	if second.DispatchAttempt <= first.DispatchAttempt {
		t.Fatal("attempt token did not advance")
	}
	if _, err := b.settle(ctx, first, ErrNoAttempt); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	g, err := store.CurrentGrant(ctx, tx, "c")
	if err != nil || g.ExchangesUsed != 1 {
		t.Fatalf("new claim refunded: %+v %v", g, err)
	}
	tx.Rollback()
	if _, err := b.settle(ctx, second, nil); err != nil {
		t.Fatal(err)
	}
	outcome, err := b.settle(ctx, second, ErrNoAttempt)
	if err != nil || outcome.State != store.HandedOff || outcome.Attempted {
		t.Fatalf("terminal overwritten: %+v %v", outcome, err)
	}
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	g, err = store.CurrentGrant(ctx, tx, "c")
	if err != nil || g.ExchangesUsed != 1 {
		t.Fatalf("double refund: %+v %v", g, err)
	}
}

func TestRepeatedDispatchReportsOnlyThisCallsAttempt(t *testing.T) {
	_, b, e, _ := setupSettlement(t)
	for _, attempted := range []bool{true, false} {
		outcome, err := b.DispatchOutcome(context.Background(), e.ID)
		if err != nil || outcome.State != store.HandedOff || outcome.Attempted != attempted {
			t.Fatalf("want this call attempted=%t, got %+v: %v", attempted, outcome, err)
		}
	}
}

func TestDiagnosticsPersistWithoutTransportSecrets(t *testing.T) {
	db, b, e, _ := setupSettlement(t)
	ctx := context.Background()
	claimed, _, err := b.claim(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.settle(ctx, claimed, errors.New("token=private-fixture-value\nmessage payload")); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	saved, err := store.GetByID(ctx, tx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.ErrorCode != "failed" || saved.ErrorDetail == "" || strings.Contains(saved.ErrorDetail, "private-fixture") {
		t.Fatalf("diagnostics=%+v", saved)
	}
}

func TestRefundFailureRollsBackSettlement(t *testing.T) {
	db, b, e, _ := setupSettlement(t)
	ctx := context.Background()
	claimed, ok, err := b.claim(ctx, e.ID)
	if err != nil || !ok {
		t.Fatalf("claim: %v", err)
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`CREATE TRIGGER reject_refund BEFORE UPDATE OF exchanges_used ON grants WHEN NEW.exchanges_used<OLD.exchanges_used BEGIN SELECT RAISE(ABORT,'synthetic refund failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	outcome, err := b.settle(ctx, claimed, ErrNoAttempt)
	if err == nil {
		t.Fatal("refund unexpectedly succeeded")
	}
	if outcome.State != "" || outcome.ErrorCode != "" || outcome.ErrorDetail != "" {
		t.Fatalf("reported uncommitted outcome: %+v", outcome)
	}
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	saved, err := store.GetByID(ctx, tx, e.ID)
	if err != nil || saved.State != store.Dispatching || saved.DispatchAttempt != claimed.DispatchAttempt || saved.ErrorCode != "" {
		t.Fatalf("partial settlement: %+v %v", saved, err)
	}
	g, err := store.CurrentGrant(ctx, tx, "c")
	if err != nil || g.ExchangesUsed != 1 {
		t.Fatalf("partial refund: %+v %v", g, err)
	}
}

func TestCrashAfterHandoffRecoversUncertainWithoutReplay(t *testing.T) {
	db, _, e, path := setupSettlement(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestCrashHandoffHelper$")
	cmd.Env = append(os.Environ(), "PARLEY_CRASH_HELPER_DB="+path, "PARLEY_CRASH_HELPER_ID="+e.ID)
	var exitErr *exec.ExitError
	if err := cmd.Run(); !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
		t.Fatalf("helper exit: %v", err)
	}
	if content, err := os.ReadFile(path + ".handoff"); err != nil || string(content) != e.ID {
		t.Fatalf("handoff evidence: %q %v", content, err)
	}
	count, err := db.RecoverUncertain(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("recover=%d %v", count, err)
	}
	b := New(db, testTransport{})
	outcome, err := b.DispatchOutcome(context.Background(), e.ID)
	if err != nil || outcome.State != store.Uncertain || outcome.Attempted || outcome.ErrorCode != "interrupted" {
		t.Fatalf("replayed uncertain: %+v %v", outcome, err)
	}
	tx, err := db.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	grant, err := store.CurrentGrant(context.Background(), tx, "c")
	if err != nil || grant.ExchangesUsed != 1 {
		t.Fatalf("crash accounting=%+v %v", grant, err)
	}
}

func TestCrashHandoffHelper(t *testing.T) {
	path := os.Getenv("PARLEY_CRASH_HELPER_DB")
	if path == "" {
		return
	}
	ctx := context.Background()
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.OpenReaders(context.Background()); err != nil {
		t.Fatal(err)
	}
	b := New(db, testTransport{})
	e, ok, err := b.claim(ctx, os.Getenv("PARLEY_CRASH_HELPER_ID"))
	if err != nil || !ok {
		t.Fatalf("claim: %v", err)
	}
	if err := b.transport.Deliver(ctx, *e); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".handoff", []byte(e.ID), 0600); err != nil {
		t.Fatal(err)
	}
	// Exit after host return, before any outcome transaction or deferred cleanup.
	os.Exit(23)
}

func TestDispatchOutcomeDoesNotReportRolledBackSettlement(t *testing.T) {
	db, b, e, _ := setupSettlement(t)
	ctx := context.Background()
	b.transport = testTransport{err: ErrNoAttempt}
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`CREATE TRIGGER reject_refund BEFORE UPDATE OF exchanges_used ON grants WHEN NEW.exchanges_used<OLD.exchanges_used BEGIN SELECT RAISE(ABORT,'synthetic refund failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	outcome, err := b.DispatchOutcome(ctx, e.ID)
	if err == nil || outcome.ID != e.ID || outcome.State != "" || outcome.ErrorCode != "" || outcome.ErrorDetail != "" || outcome.Attempted {
		t.Fatalf("rolled-back outcome: %+v %v", outcome, err)
	}
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	saved, err := store.GetByID(ctx, tx, e.ID)
	if err != nil || saved.State != store.Dispatching {
		t.Fatalf("stored outcome: %+v %v", saved, err)
	}
}

// A process upgrading validation must still settle attempts made under the old rule.
func TestSettlementOfHistoricalIncompatibleAttempt(t *testing.T) {
	for _, result := range []error{nil, ErrNoAttempt, ErrAmbiguous} {
		t.Run(fmt.Sprint(result), func(t *testing.T) {
			db, b, e, _ := setupSettlement(t)
			ctx := context.Background()
			claimed, ok, err := b.claim(ctx, e.ID)
			if err != nil || !ok {
				t.Fatalf("claim: %v", err)
			}
			tx, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, "UPDATE grants SET peer_a_id=?", "a\xff"); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, "UPDATE envelopes SET from_peer=?", "a\xff"); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			claimed.FromPeer = "a\xff"
			if _, err := b.settle(ctx, claimed, result); err != nil {
				t.Fatal(err)
			}
			// Duplicate settlement must not refund twice, including the compatibility case.
			if _, err := b.settle(ctx, claimed, result); err != nil {
				t.Fatal(err)
			}
			tx, err = db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			g, err := store.CurrentGrant(ctx, tx, "c")
			if err != nil {
				t.Fatal(err)
			}
			stored, err := store.GetByID(ctx, tx, e.ID)
			if err != nil {
				t.Fatal(err)
			}
			wantState, wantUsed := store.HandedOff, int64(1)
			if result == ErrNoAttempt {
				wantState, wantUsed = store.Queued, 0
			}
			if result == ErrAmbiguous {
				wantState = store.Uncertain
			}
			if g.ExchangesUsed != wantUsed || stored.State != wantState || stored.DispatchAttempt != claimed.DispatchAttempt || stored.FromPeer != "a\xff" || g.PeerAID != "a\xff" {
				t.Fatalf("historical settlement: %+v %+v", g, stored)
			}
		})
	}
}

func TestDispatchAttemptOverflowLeavesStateAndBudgetUnchanged(t *testing.T) {
	db, b, e, _ := setupSettlement(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE envelopes SET dispatch_attempt=9223372036854775807 WHERE id=?", e.ID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := b.claim(ctx, e.ID); err != store.InvalidRequest || claimed {
		t.Fatalf("overflow claim=%v %v", claimed, err)
	}
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	current, err := store.GetByID(ctx, tx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	g, err := store.CurrentGrant(ctx, tx, e.Conversation)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != store.Queued || current.DispatchAttempt != 9223372036854775807 || g.ExchangesUsed != 0 {
		t.Fatalf("overflow mutated evidence: %+v %+v", current, g)
	}
}
