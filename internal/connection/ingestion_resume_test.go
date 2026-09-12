package connection

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ginsys/parley/internal/store"
)

func TestIngestionResumeReauthorizesReplayAndKeepsStaleBarrierClosed(t *testing.T) {
	m, _, recipient, _, event, _ := readyIngestion(t)
	ctx := context.Background()
	binding := recipient.token.BindingID
	_, err := m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		if _, err := store.RevokeBinding(ctx, tx, store.RevocationRequest{BindingID: binding, IncidentID: "60000000-0000-4000-8000-000000000001", ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1}, nil); err != nil {
			return store.TransitionResult{}, err
		}
		credential := store.CredentialRecord{BindingID: binding, ID: "40000000-0000-4000-8000-000000000003", Version: 2, Status: "current", ExpiresAtNS: 300000000000}
		return store.TransitionResult{Changed: true}, store.ReenrollCredential(ctx, tx, binding, 2, 1, credential)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	allowed := true
	resolutions := 0
	service, err := NewIngestionRecovery(IngestionRecoveryConfig{Store: m.store, Authorize: func(context.Context, *sql.Tx, store.CommandPrincipal) error {
		if !allowed {
			return store.Forbidden
		}
		return nil
	}, Guard: func(context.Context, *sql.Tx, string) error { return nil }, Resolve: func(context.Context, IngestionResumeRequest) (store.ReviewedInterval, error) {
		resolutions++
		return store.ReviewedInterval{SourceID: event.Event.SourceID, Before: "start", After: "start"}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	r := IngestionResumeRequest{OperationID: "80000000-0000-4000-8000-000000000001", BindingID: binding, DispositionRef: "80000000-0000-4000-8000-000000000002", ExpectedBindingVersion: 3, ExpectedBarrierVersion: 9}
	p := store.CommandPrincipal{ID: adminID}
	stale, err := service.Resume(ctx, p, r)
	if err != nil || stale.Result.Code != store.VersionConflict {
		t.Fatalf("stale=%+v %v", stale, err)
	}
	if err := m.store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if got := store.IngestionAllowed(ctx, tx, binding); got != store.SecurityHold {
			t.Errorf("stale opened barrier=%v", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r.OperationID = "80000000-0000-4000-8000-000000000003"
	r.ExpectedBarrierVersion = 1
	receipt, err := service.Resume(ctx, p, r)
	if err != nil || receipt.Result.Code != "" {
		t.Fatalf("resume=%+v %v", receipt, err)
	}
	before := resolutions
	replay, err := service.Resume(ctx, p, r)
	if err != nil || !replay.Replayed || replay.AuditID != receipt.AuditID || resolutions != before {
		t.Fatalf("replay=%+v %v resolutions=%d", replay, err, resolutions)
	}
	allowed = false
	if _, err := service.Resume(ctx, p, r); err != store.Forbidden {
		t.Fatalf("private receipt disclosure=%v", err)
	}
}
