package dispatch

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/recovery"
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
	return authenticatedSetupBeforeManager(t, nil)
}
func authenticatedSetupBeforeManager(t *testing.T, setup func(*store.DB), configure ...func(*connection.ManagerConfig)) *authenticatedFixture {
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
	if _, err := controller.New(db).Grant(ctx, controller.GrantParams{Conversation: "work", PeerAID: "author", PeerBID: "recipient", Direction: store.Bidirectional, MaxExchanges: 5}); err != nil {
		t.Fatal(err)
	}
	if setup != nil {
		setup(db)
	}
	config := connection.ManagerConfig{Store: db, MaxNonattached: 4, Now: func() time.Time { return time.Unix(110, 0) }, AfterFunc: func(time.Duration, func()) func() { return func() {} }, Guard: func(context.Context, *sql.Tx, string) error { return nil }, Verify: func(context.Context, connection.NativeTuple, connection.Token) error { return nil }}
	for _, apply := range configure {
		apply(&config)
	}
	m, err := connection.NewManager(config)
	if err != nil {
		t.Fatal(err)
	}
	f.manager = m
	f.author, _ = f.attach(t, auths[0], 0, true)
	f.recipient, f.recipientSocket = f.attach(t, auths[1], 0, false)
	f.auth = auths[1]
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

func TestAuthenticatedCompatibilityDiagnosticPreservesHistory(t *testing.T) {
	for _, bad := range []string{"caf\u00e9", "a\xff", "a\x7f", strings.Repeat("x", store.MaxIdentityBytes+1)} {
		for _, field := range []string{"conversation", "from_peer", "to_peer"} {
			t.Run(fmt.Sprintf("%s/%x", field, bad), func(t *testing.T) {
				f := authenticatedSetup(t)
				id := f.send(t)
				ctx := context.Background()
				_, err := f.db.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
					if field == "conversation" {
						if err := store.EnsureConversation(ctx, tx, bad, bad, "1970-01-01T00:00:00Z"); err != nil {
							return store.TransitionResult{}, err
						}
						grant, err := store.CurrentGrant(ctx, tx, "work")
						if err != nil {
							return store.TransitionResult{}, err
						}
						grant.Conversation = bad
						if err := store.InsertGrant(ctx, tx, *grant); err != nil {
							return store.TransitionResult{}, err
						}
					}
					_, err := tx.ExecContext(ctx, "UPDATE envelopes SET "+field+"=? WHERE id=?", bad, id)
					return store.TransitionResult{Changed: true}, err
				}, nil)
				if err != nil {
					t.Fatal(err)
				}
				var before *store.Envelope
				if err := f.db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
					var err error
					before, err = store.GetByID(ctx, tx, id)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				b, err := NewAuthenticated(f.db, f.manager, func(*connection.Session) Transport { t.Fatal("incompatible work reached transport"); return nil }, nil)
				if err != nil {
					t.Fatal(err)
				}
				out, err := b.DispatchOutcome(ctx, id)
				if err != store.InvalidRequest || out.Attempted || out.State != store.Queued || out.ErrorCode != "incompatible_identifier" || out.ErrorDetail == "" {
					t.Fatalf("compatibility=%+v %v", out, err)
				}
				if err := f.db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
					after, err := store.GetByID(ctx, tx, id)
					if err == nil && !reflect.DeepEqual(before, after) {
						t.Error("compatibility rejection changed row")
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
				f.assertBudget(t, 0)
			})
		}
	}
}

func TestAuthenticatedBudgetExhaustionHasUnattemptedDiagnostic(t *testing.T) {
	f := authenticatedSetup(t)
	makeReady(t, f.manager, f.recipient)
	id := f.send(t)
	ctx := context.Background()
	var budget int64
	var before *store.Envelope
	_, err := f.db.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		if err := tx.QueryRowContext(ctx, "SELECT max_exchanges FROM grants WHERE conversation='work'").Scan(&budget); err != nil {
			return store.TransitionResult{}, err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE grants SET exchanges_used=max_exchanges WHERE conversation='work'"); err != nil {
			return store.TransitionResult{}, err
		}
		var err error
		before, err = store.GetByID(ctx, tx, id)
		return store.TransitionResult{Changed: true}, err
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewAuthenticated(f.db, f.manager, func(*connection.Session) Transport { t.Fatal("exhausted grant reached transport"); return nil }, func() time.Time { return time.Unix(110, 0) })
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		out, err := b.DispatchOutcome(ctx, id)
		if err != ErrBudgetExhausted || out.State != store.Queued || out.Attempted || out.ErrorCode != "budget_exhausted" || out.ErrorDetail == "" {
			t.Fatalf("budget wait=%+v %v", out, err)
		}
	}
	f.assertBudget(t, budget)
	if err := f.db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		after, err := store.GetByID(ctx, tx, id)
		if err == nil && !reflect.DeepEqual(before, after) {
			t.Error("budget wait changed queued work")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLateSettlementRecordsRollbackAndRefundsOnlyOnce(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(110, 0)
	var markers *recovery.Directory
	f := authenticatedSetupBeforeManager(t, func(db *store.DB) {
		path, tempErr := os.MkdirTemp("/tmp", "parley-settlement-")
		if tempErr != nil {
			t.Fatal(tempErr)
		}
		t.Cleanup(func() { os.RemoveAll(path) })
		var err error
		markers, err = recovery.NewDirectory(path, uint32(os.Geteuid()), 10)
		if err != nil {
			t.Fatal(err)
		}
		_, err = recovery.New(ctx, recovery.Config{Store: db, Markers: markers, Now: func() time.Time { return now }, FailStop: func() { t.Error("unexpected fail-stop") }})
		if err != nil {
			t.Fatal(err)
		}
	})
	makeReady(t, f.manager, f.recipient)
	id := f.send(t)
	var claim store.Envelope
	bridge, err := NewAuthenticated(f.db, f.manager, func(*connection.Session) Transport {
		return authenticatedTransport(func(_ context.Context, e store.Envelope) error {
			claim = e
			now = time.Unix(100, 0)
			return ErrNoAttempt
		})
	}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	out, err := bridge.DispatchOutcome(ctx, id)
	if err != nil || out.State != store.Queued || out.Attempted {
		t.Fatalf("late settlement=%+v %v", out, err)
	}
	saved, err := markers.List(ctx)
	if err != nil || len(saved) != 1 {
		t.Fatalf("settlement missed rollback marker=%+v %v", saved, err)
	}
	f.assertBudget(t, 0)
	if _, err := bridge.bridge.settle(ctx, &claim, ErrNoAttempt); err != nil {
		t.Fatal(err)
	}
	f.assertBudget(t, 0)
	if err := f.db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		e, err := store.GetByID(ctx, tx, id)
		if err == nil && (e.UpdatedAt != time.Unix(110, 0).UTC().Format(time.RFC3339Nano) || e.DispatchAttempt != 1) {
			t.Errorf("untrusted settlement=%+v", e)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticatedHandoffRechecksElapsedRecipientDeadline(t *testing.T) {
	for _, kind := range []string{"credential", "liveness"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(110, 0)
			f := authenticatedSetupBeforeManager(t, nil, func(c *connection.ManagerConfig) { c.Now = func() time.Time { return now } })
			makeReady(t, f.manager, f.recipient)
			id := f.send(t)
			deadline := time.Unix(141, 0)
			if kind == "credential" {
				// Keep liveness valid beyond the immutable credential expiry at 200.
				for sec := int64(130); sec <= 190; sec += 20 {
					now = time.Unix(sec, 0)
					if err := f.manager.Heartbeat(ctx, f.recipient); err != nil {
						t.Fatal(err)
					}
				}
				deadline = time.Unix(201, 0)
			}
			deliveries := 0
			bridge, err := NewAuthenticated(f.db, f.manager, func(*connection.Session) Transport {
				// Claim committed, but no timer callback has run before handoff.
				now = deadline
				return authenticatedTransport(func(context.Context, store.Envelope) error { deliveries++; return nil })
			}, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			out, err := bridge.DispatchOutcome(ctx, id)
			if err != nil || out.Attempted || out.State != store.Queued || deliveries != 0 {
				t.Fatalf("expired handoff=%+v %v deliveries=%d", out, err, deliveries)
			}
			f.assertBudget(t, 0)
			if f.recipient.Context().Err() == nil {
				t.Error("expired recipient remained live")
			}
			if kind == "credential" {
				if err := f.db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
					credential, err := store.ReadCredential(ctx, tx, "40000000-0000-4000-8000-000000000002")
					if err == nil && credential.Status != "expired" {
						t.Errorf("lost expiry evidence=%s", credential.Status)
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
