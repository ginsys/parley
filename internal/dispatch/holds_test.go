package dispatch

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/store"
)

const holdBinding = "30000000-0000-4000-8000-000000000001"
const holdIncident = "60000000-0000-4000-8000-000000000001"
const holdPrincipal = "70000000-0000-4000-8000-000000000001"
const holdOperation = "80000000-0000-4000-8000-000000000001"

func attributeWork(t *testing.T, db *store.DB, e *store.Envelope) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	b := store.BindingRecord{ID: holdBinding, PeerID: e.FromPeer, HostKind: "codex_cli", NamespaceID: "synthetic", SessionID: "native", ConnectorUID: 1000, Status: "enabled", Version: 1}
	c := store.CredentialRecord{BindingID: b.ID, ID: "40000000-0000-4000-8000-000000000001", Version: 1, Status: "current", ExpiresAtNS: time.Now().Add(time.Hour).UnixNano()}
	if err := store.InsertBindingCredential(ctx, tx, b, c); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordAuthenticatedEnvelope(ctx, tx, e.ID, b.ID, 1); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
func revokeWork(t *testing.T, db *store.DB, e *store.Envelope, cancelWork bool) {
	t.Helper()
	ctx := context.Background()
	req, err := store.NewCommandRequest("binding.revoke", holdOperation, store.Field{Name: "binding_id", Value: holdBinding})
	if err != nil {
		t.Fatal(err)
	}
	result, err := db.Coordinator().Execute(ctx, store.CommandPrincipal{ID: holdPrincipal}, req, func(context.Context, *sql.Tx) error { return nil }, func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
		_, err := store.RevokeBinding(ctx, tx, store.RevocationRequest{BindingID: holdBinding, IncidentID: holdIncident, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1, NowNS: time.Now().UnixNano()}, nil)
		if err == nil && cancelWork {
			_, err = store.ApplyWorkDisposition(ctx, tx, store.DispositionRequest{Work: store.WorkRef{Kind: "envelope", ID: e.ID}, IncidentID: holdIncident, ExpectedVersion: 1, Action: "cancel", ReasonCode: "compromise", PrincipalID: holdPrincipal, OperationID: holdOperation, NowNS: time.Now().UnixNano()}, nil)
		}
		return store.CommandResult{}, err
	}, nil)
	if err != nil || result.Result.Code != "" {
		t.Fatalf("revoke=%+v %v", result, err)
	}
}

// C12: revocation between committed claim and host result must retain provenance,
// prevent further claims, and refund only the exact never-attempted token once.
func TestHeldLateSettlementPreservesEvidenceAndRefundsOnce(t *testing.T) {
	for _, cancelWork := range []bool{false, true} {
		t.Run(map[bool]string{false: "held", true: "cancelled"}[cancelWork], func(t *testing.T) {
			db, b, e, _ := setupSettlement(t)
			ctx := context.Background()
			attributeWork(t, db, e)
			claimed, ok, err := b.claim(ctx, e.ID)
			if err != nil || !ok {
				t.Fatalf("claim=%v %v", ok, err)
			}
			revokeWork(t, db, e, cancelWork)
			want := store.Queued
			if cancelWork {
				want = store.Cancelled
			}
			for i := 0; i < 2; i++ {
				outcome, err := b.settle(ctx, claimed, ErrNoAttempt)
				if err != nil || outcome.State != want {
					t.Fatalf("settle %d=%+v %v", i, outcome, err)
				}
			}
			if _, ok, err := b.claim(ctx, e.ID); err != nil || ok {
				t.Fatalf("held claim=%v %v", ok, err)
			}
			tx, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			g, err := store.CurrentGrant(ctx, tx, e.Conversation)
			if err != nil || g.ExchangesUsed != 0 {
				t.Fatalf("budget=%+v %v", g, err)
			}
			current, err := store.GetByID(ctx, tx, e.ID)
			if err != nil || current.State != want || current.DispatchAttempt != 1 {
				t.Fatalf("evidence=%+v %v", current, err)
			}
			var binding string
			var credential, grant int64
			if err := tx.QueryRowContext(ctx, "SELECT binding_id,credential_version,original_grant_version FROM work_provenance WHERE work_id=?", e.ID).Scan(&binding, &credential, &grant); err != nil {
				t.Fatal(err)
			}
			if binding != holdBinding || credential != 1 || grant != 1 {
				t.Fatal("late settlement rewrote provenance")
			}
		})
	}
}
func TestHeldOriginalCannotBeAcknowledged(t *testing.T) {
	db, b, e, _ := setupSettlement(t)
	ctx := context.Background()
	attributeWork(t, db, e)
	if state, err := b.Dispatch(ctx, e.ID); err != nil || state != store.HandedOff {
		t.Fatalf("dispatch=%s %v", state, err)
	}
	revokeWork(t, db, e, false)
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := store.SetState(ctx, tx, e.ID, store.HandedOff, store.Acked, time.Now().UTC().Format(time.RFC3339Nano)); err != store.SecurityHold {
		t.Fatalf("held ACK=%v", err)
	}
	current, err := store.GetByID(ctx, tx, e.ID)
	if err != nil || current.State != store.HandedOff || current.DispatchAttempt != 1 {
		t.Fatalf("ACK evidence=%+v %v", current, err)
	}
}
