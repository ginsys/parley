package connection

import (
	"context"
	"database/sql"

	"github.com/ginsys/parley/internal/store"
)

type DisconnectRequest struct {
	OperationID, BindingID, ExpectedEpoch string
	ExpectedGeneration                    int64
}

// Disconnect is internal administration, not an agent-facing endpoint. The
// administrator principal and mandatory authorizer come from the trusted control
// transport. Result replay cannot disconnect a later generation.
func (m *Manager) Disconnect(ctx context.Context, p store.CommandPrincipal, r DisconnectRequest, authorize func(context.Context, *sql.Tx, store.CommandPrincipal) error) (store.CommandReceipt, error) {
	if authorize == nil || !canonicalID(r.BindingID) || !canonicalID(r.ExpectedEpoch) || r.ExpectedGeneration < 1 {
		return store.CommandReceipt{}, store.InvalidRequest
	}
	request, err := store.NewCommandRequest("connection.disconnect", r.OperationID, store.Field{Name: "binding_id", Value: r.BindingID}, store.Field{Name: "expected_server_epoch", Value: r.ExpectedEpoch}, store.Field{Name: "expected_connection_generation", Value: r.ExpectedGeneration})
	if err != nil {
		return store.CommandReceipt{}, err
	}
	var target *Socket
	return m.store.Coordinator().Execute(ctx, p, request, func(ctx context.Context, tx *sql.Tx) error { return authorize(ctx, tx, p) }, func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
		if err := m.guard(ctx, tx, r.BindingID); err != nil {
			return domainRejection(err)
		}
		slot := m.slots[r.BindingID]
		if slot == nil || slot.token.Epoch != r.ExpectedEpoch || slot.token.Generation != r.ExpectedGeneration {
			return store.CommandResult{Code: store.GenerationConflict}, nil
		}
		target = slot.socket
		return store.CommandResult{Resources: []store.ResourceChange{{Kind: "connection", ID: r.BindingID, Before: r.ExpectedGeneration, After: r.ExpectedGeneration}}}, nil
	}, func(store.CommitView) {
		if target != nil {
			m.remove(target)
		}
	})
}
