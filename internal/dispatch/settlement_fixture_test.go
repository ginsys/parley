package dispatch

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/store"
	"github.com/ginsys/parley/internal/testfixture/identity"
	"github.com/google/uuid"
)

// The settlement fixture preserves direct exact-attempt access while acceptance
// and claim run the production authenticated capability path.
type settlementFixture struct {
	*AuthenticatedBridge
	db         *store.DB
	transport  Transport
	identities *identity.Fixture
}

func newSettlementFixture(t testing.TB, db *store.DB, transport Transport) *settlementFixture {
	t.Helper()
	ids := identity.For(t, db)
	if err := ids.GrantPeers(); err != nil {
		t.Fatal(err)
	}
	f := &settlementFixture{db: db, transport: transport, identities: ids}
	b, err := NewAuthenticated(db, ids.Manager, func(*connection.Session) Transport { return f.transport }, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	f.AuthenticatedBridge = b
	return f
}
func (f *settlementFixture) Send(ctx context.Context, conversation, from, to, text string, _ *string) (*store.Envelope, error) {
	s, err := f.identities.Session(from)
	if err != nil {
		return nil, err
	}
	if _, err := f.identities.Session(to); err != nil {
		return nil, err
	}
	receipt, err := f.AuthenticatedBridge.Send(ctx, s, connection.SendRequest{OperationID: uuid.NewString(), Conversation: conversation, Recipient: to, Text: text})
	if err != nil {
		return nil, err
	}
	if receipt.Result.Code != "" {
		return nil, receipt.Result.Code
	}
	var e *store.Envelope
	err = f.db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		e, err = store.GetByID(ctx, tx, receipt.Result.Resources[0].ID)
		return err
	})
	return e, err
}
func (f *settlementFixture) claim(ctx context.Context, id string) (*store.Envelope, bool, error) {
	e, _, domain, err := f.AuthenticatedBridge.claim(ctx, id)
	if err == store.SecurityHold || err == store.BindingUnavailable || err == store.NotReady || err == store.AuthenticationFailed {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return e, e != nil, domain
}
func (f *settlementFixture) settle(ctx context.Context, e *store.Envelope, err error) (Outcome, error) {
	return f.AuthenticatedBridge.bridge.settle(ctx, e, err)
}
