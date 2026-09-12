package connection

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ginsys/parley/internal/store"
)

func TestAdministrativeDisconnectIsExactAuditedAndReplaySafe(t *testing.T) {
	m, auth, _ := attachmentFixture(t)
	ctx := context.Background()
	socket := acceptSocket(t, m)
	session, err := m.Attach(ctx, socket, auth, 0)
	if err != nil {
		t.Fatal(err)
	}
	actor := store.CommandPrincipal{ID: adminID}
	authorize := func(context.Context, *sql.Tx, store.CommandPrincipal) error { return nil }
	request := DisconnectRequest{OperationID: targetID, BindingID: session.token.BindingID, ExpectedEpoch: session.token.Epoch, ExpectedGeneration: 1}
	if _, err := m.Disconnect(ctx, actor, request, nil); err != store.InvalidRequest {
		t.Fatalf("missing authority=%v", err)
	}
	denied := func(context.Context, *sql.Tx, store.CommandPrincipal) error { return store.Forbidden }
	if _, err := m.Disconnect(ctx, actor, request, denied); err != store.Forbidden {
		t.Fatalf("unauthorized=%v", err)
	}
	first, err := m.Disconnect(ctx, actor, request, authorize)
	if err != nil || first.Result.Code != "" {
		t.Fatalf("disconnect=%+v %v", first, err)
	}
	select {
	case <-socket.Context().Done():
	default:
		t.Fatal("disconnect retained socket")
	}
	successor, err := m.Attach(ctx, acceptSocket(t, m), auth, 1)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := m.Disconnect(ctx, actor, request, authorize)
	if err != nil || !replay.Replayed || replay.AuditID != first.AuditID {
		t.Fatalf("replay=%+v %v", replay, err)
	}
	if err := m.Heartbeat(ctx, successor); err != nil {
		t.Fatalf("replay disconnected successor: %v", err)
	}
	request.OperationID = "40000000-0000-4000-8000-000000000001"
	stale, err := m.Disconnect(ctx, actor, request, authorize)
	if err != nil || stale.Result.Code != store.GenerationConflict {
		t.Fatalf("stale target=%+v %v", stale, err)
	}
	if err := m.Heartbeat(ctx, successor); err != nil {
		t.Fatalf("stale target disconnected successor: %v", err)
	}
}
