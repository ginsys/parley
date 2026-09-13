// Package bridge adapts pre-capability test scenarios to authenticated fixtures.
// It is not imported by production packages.
package bridge

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/dispatch"
	"github.com/ginsys/parley/internal/store"
	"github.com/ginsys/parley/internal/testfixture/identity"
	"github.com/google/uuid"
)

type Fixture struct {
	*dispatch.AuthenticatedBridge
	Identity *identity.Fixture
}

func New(t testing.TB, db *store.DB, transport dispatch.Transport) *Fixture {
	t.Helper()
	f := identity.For(t, db)
	if err := f.GrantPeers(); err != nil {
		t.Fatal(err)
	}
	b, err := dispatch.NewAuthenticated(db, f.Manager, func(*connection.Session) dispatch.Transport { return transport }, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	return &Fixture{b, f}
}

// Send derives a real private capability for the named synthetic fixture peer.
// correlation is historical test setup, separate from the production Send API.
func (f *Fixture) Send(ctx context.Context, conversation, from, to, text string, correlation *string) (*store.Envelope, error) {
	s, err := f.Identity.Session(from)
	if err != nil {
		return nil, err
	}
	if _, err := f.Identity.Session(to); err != nil {
		return nil, err
	}
	r, err := f.AuthenticatedBridge.Send(ctx, s, connection.SendRequest{OperationID: uuid.NewString(), Conversation: conversation, Recipient: to, Text: text})
	if err != nil {
		return nil, err
	}
	if r.Result.Code != "" {
		return nil, r.Result.Code
	}
	var e *store.Envelope
	if err := f.Identity.DB.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		e, err = store.GetByID(ctx, tx, r.Result.Resources[0].ID)
		return err
	}); err != nil {
		return nil, err
	}
	if correlation != nil {
		_, err = f.Identity.DB.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
			_, err := tx.ExecContext(ctx, "UPDATE envelopes SET in_reply_to=? WHERE id=?", *correlation, e.ID)
			return store.TransitionResult{Changed: true}, err
		}, nil)
		if err != nil {
			return nil, err
		}
		e.InReplyTo = correlation
	}
	return e, nil
}

// ErrNoMarker represents the retained no_marker classification in older tests.
var ErrNoMarker = errors.New("synthetic event contains no reply marker")

func IngestTurn(t testing.TB, ctx context.Context, db *store.DB, conversation, from, to, text string) (*store.Envelope, error) {
	t.Helper()
	f := identity.For(t, db)
	s, err := f.Session(from)
	if err != nil {
		return nil, err
	}
	if _, err := f.Session(to); err != nil {
		return nil, err
	}
	ingestor, err := connection.NewIngestor(connection.IngestorConfig{Manager: f.Manager, Verify: func(context.Context, connection.NativeTuple, connection.Token, connection.IngestRequest) error {
		return nil
	}, Origin: func(context.Context, connection.NativeTuple, connection.Token, string, string) error { return nil }})
	if err != nil {
		return nil, err
	}
	source, before := "", ""
	if err := db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, "SELECT c.source_id,c.cursor FROM ingestion_cursors c JOIN bindings b USING(binding_id) WHERE b.peer_id=?", from).Scan(&source, &before)
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}); err != nil {
		return nil, err
	}
	if source == "" {
		source = uuid.NewString()
		before = "start"
		if err := ingestor.Initialize(ctx, s, source, before); err != nil {
			return nil, err
		}
	}
	result, err := ingestor.Ingest(ctx, s, connection.IngestRequest{Event: connection.NativeEvent{ID: uuid.NewString(), SourceID: source, Revision: "1", Before: before, After: uuid.NewString()}, Conversation: conversation, Recipient: to, Text: text})
	if err != nil {
		return nil, err
	}
	if result.Classification == "no_marker" {
		return nil, ErrNoMarker
	}
	if result.Code != "" {
		return nil, result.Code
	}
	if result.Classification != "accepted" {
		return nil, store.TemporarilyUnavailable
	}
	var e *store.Envelope
	err = db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		e, err = store.GetByID(ctx, tx, result.EnvelopeID)
		return err
	})
	return e, err
}
