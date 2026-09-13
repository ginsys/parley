package dispatch

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/store"
)

type authenticatedFixture struct {
	db                *store.DB
	manager           *connection.Manager
	author, recipient *connection.Session
	recipientSocket   *connection.Socket
	auth              connection.Authentication
}

func authenticatedSetup(t *testing.T) *authenticatedFixture {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "auth.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.OpenReaders(ctx); err != nil {
		t.Fatal(err)
	}
	f := &authenticatedFixture{db: db}
	var auths []connection.Authentication
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for index, peer := range []string{"author", "recipient"} {
		b := store.BindingRecord{ID: fmt.Sprintf("30000000-0000-4000-8000-%012d", index+1), PeerID: peer, HostKind: "codex_cli", NamespaceID: "synthetic", SessionID: peer, ConnectorUID: uint32(os.Geteuid()), Status: "enabled", Version: 1}
		var secret [32]byte
		secret[0] = byte(index + 1)
		c := store.CredentialRecord{ID: fmt.Sprintf("40000000-0000-4000-8000-%012d", index+1), BindingID: b.ID, Version: 1, Status: "current", ExpiresAtNS: time.Unix(200, 0).UnixNano(), Verifier: sha256.Sum256(secret[:])}
		if err := store.InsertBindingCredential(ctx, tx, b, c); err != nil {
			t.Fatal(err)
		}
		auth, err := connection.NewAuthentication(c.ID, secret[:], connection.NativeTuple{Kind: b.HostKind, Namespace: b.NamespaceID, Session: b.SessionID})
		if err != nil {
			t.Fatal(err)
		}
		auths = append(auths, auth)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	m, err := connection.NewManager(connection.ManagerConfig{Store: db, MaxNonattached: 4, Now: func() time.Time { return time.Unix(110, 0) }, AfterFunc: func(time.Duration, func()) func() { return func() {} }, Guard: func(context.Context, *sql.Tx, string) error { return nil }, Verify: func(context.Context, connection.NativeTuple, connection.Token) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	f.manager = m
	f.author, _ = f.attach(t, auths[0], 0, true)
	f.recipient, f.recipientSocket = f.attach(t, auths[1], 0, false)
	f.auth = auths[1]
	if _, err := controller.New(db).Grant(ctx, controller.GrantParams{Conversation: "work", PeerAID: "author", PeerBID: "recipient", Direction: store.Bidirectional, MaxExchanges: 5}); err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *authenticatedFixture) attach(t *testing.T, auth connection.Authentication, generation int64, ready bool) (*connection.Session, *connection.Socket) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sock")
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	client, err := net.DialUnix("unix", nil, l.Addr().(*net.UnixAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	server, err := l.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	socket, err := f.manager.Accept(ctx, server)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { socket.Close() })
	session, err := f.manager.Attach(ctx, socket, auth, generation)
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		makeReady(t, f.manager, session)
	}
	return session, socket
}
func makeReady(t *testing.T, m *connection.Manager, s *connection.Session) {
	t.Helper()
	p, err := m.BeginReadiness(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Acknowledge(context.Background(), s, p); err != nil {
		t.Fatal(err)
	}
}

type authenticatedTransport func(context.Context, store.Envelope) error

func (f authenticatedTransport) Deliver(ctx context.Context, e store.Envelope) error {
	return f(ctx, e)
}
func (f *authenticatedFixture) send(t *testing.T) string {
	t.Helper()
	r, err := f.manager.Send(context.Background(), f.author, connection.SendRequest{OperationID: "80000000-0000-4000-8000-000000000001", Conversation: "work", Recipient: "recipient", Text: "synthetic"})
	if err != nil || r.Result.Code != "" {
		t.Fatalf("send=%+v %v", r, err)
	}
	return r.Result.Resources[0].ID
}
func (f *authenticatedFixture) assertBudget(t *testing.T, want int64) {
	t.Helper()
	if err := f.db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		g, err := store.CurrentGrant(ctx, tx, "work")
		if err == nil && g.ExchangesUsed != want {
			t.Errorf("budget=%d want%d", g.ExchangesUsed, want)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
func TestAuthenticatedClaimWaitsForReadinessThenUsesExactRecipient(t *testing.T) {
	f := authenticatedSetup(t)
	calls := 0
	b, err := NewAuthenticated(f.db, f.manager, func(s *connection.Session) Transport {
		if s != f.recipient {
			t.Fatal("wrong recipient capability")
		}
		return authenticatedTransport(func(context.Context, store.Envelope) error { calls++; return nil })
	}, func() time.Time { return time.Unix(110, 0) })
	if err != nil {
		t.Fatal(err)
	}
	id := f.send(t)
	out, err := b.DispatchOutcome(context.Background(), id)
	if err != nil || out.Attempted || calls != 0 {
		t.Fatalf("unready=%+v %v calls%d", out, err, calls)
	}
	f.assertBudget(t, 0)
	makeReady(t, f.manager, f.recipient)
	out, err = b.DispatchOutcome(context.Background(), id)
	if err != nil || !out.Attempted || out.State != store.HandedOff || calls != 1 {
		t.Fatalf("ready=%+v %v calls%d", out, err, calls)
	}
	f.assertBudget(t, 1)
}
func TestAuthenticatedHandoffCannotResolveReplacementSession(t *testing.T) {
	f := authenticatedSetup(t)
	makeReady(t, f.manager, f.recipient)
	old := f.recipient
	b, err := NewAuthenticated(f.db, f.manager, func(s *connection.Session) Transport {
		if s != old {
			t.Fatal("claim resolved replacement")
		}
		f.recipientSocket.Close()
		f.recipient, _ = f.attach(t, f.auth, 1, true)
		return authenticatedTransport(func(context.Context, store.Envelope) error {
			t.Fatal("closed claim delivered to replacement")
			return nil
		})
	}, func() time.Time { return time.Unix(110, 0) })
	if err != nil {
		t.Fatal(err)
	}
	id := f.send(t)
	out, err := b.DispatchOutcome(context.Background(), id)
	if err != nil || out.Attempted || out.State != store.Queued {
		t.Fatalf("closed before handoff=%+v %v", out, err)
	}
	f.assertBudget(t, 0)
}
func TestAuthenticatedClaimHonorsAuthoredRevocationHold(t *testing.T) {
	f := authenticatedSetup(t)
	makeReady(t, f.manager, f.recipient)
	id := f.send(t)
	_, err := f.db.Coordinator().Transition(context.Background(), func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		_, err := store.RevokeBinding(ctx, tx, store.RevocationRequest{BindingID: "30000000-0000-4000-8000-000000000001", IncidentID: "60000000-0000-4000-8000-000000000001", ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1, NowNS: time.Unix(110, 0).UnixNano()}, nil)
		return store.TransitionResult{Changed: true}, err
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewAuthenticated(f.db, f.manager, func(*connection.Session) Transport { t.Fatal("held work reached transport"); return nil }, func() time.Time { return time.Unix(110, 0) })
	if err != nil {
		t.Fatal(err)
	}
	out, err := b.DispatchOutcome(context.Background(), id)
	if err != nil || out.Attempted || out.State != store.Queued {
		t.Fatalf("held claim=%+v %v", out, err)
	}
	f.assertBudget(t, 0)
}

func TestAuthenticatedBudgetDiagnosticOverridesPriorAttemptWithoutRewritingIt(t *testing.T) {
	f := authenticatedSetup(t)
	makeReady(t, f.manager, f.recipient)
	calls := 0
	b, err := NewAuthenticated(f.db, f.manager, func(*connection.Session) Transport {
		return authenticatedTransport(func(context.Context, store.Envelope) error { calls++; return ErrNoAttempt })
	}, func() time.Time { return time.Unix(110, 0) })
	if err != nil {
		t.Fatal(err)
	}
	id := f.send(t)
	ctx := context.Background()
	first, err := b.DispatchOutcome(ctx, id)
	if err != nil || first.ErrorCode != "not_attempted" {
		t.Fatalf("first=%+v %v", first, err)
	}
	_, err = f.db.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		for i := 0; i < 5; i++ {
			ok, err := store.ClaimExchange(ctx, tx, "work", 1)
			if err != nil {
				return store.TransitionResult{}, err
			}
			if !ok {
				t.Fatal("synthetic competing budget claim")
			}
		}
		return store.TransitionResult{Changed: true}, nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := b.DispatchOutcome(ctx, id)
	if err != ErrBudgetExhausted || outcome.Attempted || outcome.State != store.Queued || outcome.ErrorCode != "budget_exhausted" || outcome.ErrorDetail == "" || calls != 1 {
		t.Fatalf("budget outcome=%+v %v calls%d", outcome, err, calls)
	}
	saved, err := f.db.Queries().Outcome(ctx, id)
	if err != nil || saved.ErrorCode != "not_attempted" {
		t.Fatalf("unattempted claim rewrote history=%+v %v", saved, err)
	}
}
