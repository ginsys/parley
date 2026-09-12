package connection

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/store"
)

func socketPair(t *testing.T, networks ...string) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	network := "unix"
	if len(networks) > 0 {
		network = networks[0]
	}
	listener, err := net.ListenUnix(network, &net.UnixAddr{Name: filepath.Join(t.TempDir(), "socket"), Net: network})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	client, err := net.DialUnix(network, nil, listener.Addr().(*net.UnixAddr))
	if err != nil {
		t.Fatal(err)
	}
	server, err := listener.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close(); client.Close() })
	return server, client
}
func attachmentFixture(t *testing.T, wrongUID ...bool) (*Manager, Authentication, *time.Time) {
	t.Helper()
	var file CredentialFile
	p, db := testProvisioner(t, PublisherFunc(func(_ context.Context, f CredentialFile) error { file = f; return nil }))
	registration := testRegistration()
	if len(wrongUID) > 0 && wrongUID[0] {
		registration.ConnectorUID = uint32(os.Geteuid()) + 1
	}
	if _, err := p.Register(context.Background(), store.CommandPrincipal{ID: adminID}, registration); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(110, 0)
	m, err := NewManager(ManagerConfig{Store: db, MaxNonattached: 4, Now: func() time.Time { return now }, AfterFunc: func(time.Duration, func()) func() { return func() {} },
		Guard:  func(context.Context, *sql.Tx, string) error { return nil },
		Verify: func(context.Context, NativeTuple, Token) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	auth, err := NewAuthentication(file.CredentialID, file.secret[:], testRegistration().Native)
	if err != nil {
		t.Fatal(err)
	}
	return m, auth, &now
}
func acceptSocket(t *testing.T, m *Manager) *Socket {
	t.Helper()
	server, _ := socketPair(t)
	s, err := m.Accept(context.Background(), server)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func TestInspectAttachExclusiveAndRepeat(t *testing.T) {
	m, auth, _ := attachmentFixture(t)
	ctx := context.Background()
	a, b := acceptSocket(t, m), acceptSocket(t, m)
	first, err := m.Inspect(ctx, a, auth)
	if err != nil || first.Generation != 0 || first.Active || first.Epoch == "" {
		t.Fatalf("inspect=%+v %v", first, err)
	}
	session, err := m.Attach(ctx, a, auth, 0)
	if err != nil {
		t.Fatal(err)
	}
	again, err := m.Attach(ctx, a, auth, 0)
	if err != nil || again != session {
		t.Fatalf("repeat=%p want=%p %v", again, session, err)
	}
	snapshot, err := m.Inspect(ctx, b, auth)
	if err != nil || snapshot.Generation != 1 || !snapshot.Active {
		t.Fatalf("inspect=%+v %v", snapshot, err)
	}
	if _, err := m.Attach(ctx, b, auth, 1); !errors.Is(err, store.AlreadyConnected) {
		t.Fatalf("second socket=%v", err)
	}
	a.Close()
	if _, err := m.Attach(ctx, b, auth, 0); !errors.Is(err, store.GenerationConflict) {
		t.Fatalf("stale generation=%v", err)
	}
	successor, err := m.Attach(ctx, b, auth, 1)
	if err != nil {
		t.Fatal(err)
	}
	a.Close() // stale callback cannot evict successor
	if err := m.Heartbeat(ctx, successor); err != nil {
		t.Fatalf("stale close evicted winner: %v", err)
	}
	if err := m.RequireReady(ctx, successor); !errors.Is(err, store.NotReady) {
		t.Fatalf("attached ready=%v", err)
	}
}
func TestAuthenticationFailureClosesWithoutDisclosure(t *testing.T) {
	for _, kind := range []string{"unknown", "secret", "tuple", "uid", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			m, auth, now := attachmentFixture(t, kind == "uid")
			s := acceptSocket(t, m)
			switch kind {
			case "unknown":
				auth.credentialID = targetID
			case "secret":
				auth.secret[0] ^= 1
			case "tuple":
				auth.native.Session = "forged"
			case "deadline":
				*now = now.Add(store.AuthenticationDeadline)
			}
			if _, err := m.Inspect(context.Background(), s, auth); !errors.Is(err, store.AuthenticationFailed) {
				t.Fatalf("result=%v", err)
			}
			select {
			case <-s.Context().Done():
			default:
				t.Fatal("failure left socket usable")
			}
			if _, err := m.Attach(context.Background(), s, auth, 0); !errors.Is(err, store.AuthenticationFailed) {
				t.Fatalf("second attempt=%v", err)
			}
		})
	}
}
func TestCredentialExpiryIsTerminalAndInspectDoesNotReserve(t *testing.T) {
	m, auth, now := attachmentFixture(t)
	*now = time.Unix(200, 0)
	s := acceptSocket(t, m)
	if _, err := m.Inspect(context.Background(), s, auth); !errors.Is(err, store.AuthenticationFailed) {
		t.Fatalf("expiry=%v", err)
	}
	*now = time.Unix(110, 0)
	other := acceptSocket(t, m)
	if _, err := m.Inspect(context.Background(), other, auth); !errors.Is(err, store.AuthenticationFailed) {
		t.Fatalf("clock rollback revived credential: %v", err)
	}
}

func TestManagerCannotEvictAnotherManagersWinner(t *testing.T) {
	m, auth, _ := attachmentFixture(t)
	s := acceptSocket(t, m)
	if _, err := m.Attach(context.Background(), s, auth, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := NewManager(ManagerConfig{Store: m.store, MaxNonattached: 4, Guard: m.guard, Verify: m.verify}); err != store.InvalidRequest {
		t.Fatalf("duplicate manager=%v", err)
	}
}
func TestDueSlotDoesNotBlockReplacementBeforeTimerRuns(t *testing.T) {
	m, auth, now := attachmentFixture(t)
	ctx := context.Background()
	old := acceptSocket(t, m)
	if _, err := m.Attach(ctx, old, auth, 0); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(store.LivenessDeadline)
	next := acceptSocket(t, m)
	snapshot, err := m.Inspect(ctx, next, auth)
	if err != nil || snapshot.Active {
		t.Fatalf("due slot active=%+v %v", snapshot, err)
	}
	session, err := m.Attach(ctx, next, auth, 1)
	if err != nil {
		t.Fatal(err)
	}
	m.expire(old)
	if err := m.Heartbeat(ctx, session); err != nil {
		t.Fatalf("stale timer evicted winner=%v", err)
	}
	select {
	case <-old.Context().Done():
	default:
		t.Fatal("overdue socket retained")
	}
}

func TestAdmissionBoundAndDeadlineIncludeCoordinatorWait(t *testing.T) {
	m, _, _ := attachmentFixture(t)
	scheduled := make(chan func(), 12)
	m.afterFunc = func(_ time.Duration, f func()) func() { scheduled <- f; return func() {} }
	held, release, writerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(writerDone)
		_, _ = m.store.Coordinator().Transition(context.Background(), func(context.Context, *sql.Tx, store.CommitView) (store.TransitionResult, error) {
			return store.TransitionResult{Changed: true}, nil
		}, func(store.CommitView) { close(held); <-release })
	}()
	<-held
	type outcome struct {
		s   *Socket
		err error
	}
	results := make(chan outcome, 4)
	var deadlines []func()
	for range 4 {
		server, _ := socketPair(t)
		go func() { s, err := m.Accept(context.Background(), server); results <- outcome{s, err} }()
		deadlines = append(deadlines, <-scheduled)
	}
	extra, _ := socketPair(t)
	if _, err := m.Accept(context.Background(), extra); err != store.CapacityExceeded {
		t.Errorf("waiting sockets bypass capacity: %v", err)
	}
	deadlines[0]() // Fire the controlled five-second admission timer while gate is held.
	select {
	case got := <-results:
		if got.s != nil || got.err == nil {
			t.Errorf("deadline accepted socket: %v", got.err)
		}
	case <-time.After(time.Second):
		t.Error("authentication deadline waits for coordinator")
	}
	close(release)
	<-writerDone
	for range 3 {
		got := <-results
		if got.err != nil {
			t.Error(got.err)
		} else {
			got.s.Close()
		}
	}
}
func TestCancelledSocketIsPrunedAfterWriterRecovers(t *testing.T) {
	m, _, _ := attachmentFixture(t)
	old := acceptSocket(t, m)
	// State left by Close when its writer transaction fails: cancellation is
	// authoritative, but removal could not publish. Next successful admission cleans it.
	old.cancel()
	old.releaseCapacity()
	next := acceptSocket(t, m)
	if len(m.sockets) != 1 {
		t.Fatalf("retained sockets=%d", len(m.sockets))
	}
	if _, ok := m.sockets[next]; !ok {
		t.Fatal("pruned live replacement")
	}
}

func TestConcurrentAttachmentHasOneWinner(t *testing.T) {
	m, auth, _ := attachmentFixture(t)
	ctx := context.Background()
	sockets := []*Socket{acceptSocket(t, m), acceptSocket(t, m), acceptSocket(t, m), acceptSocket(t, m)}
	start := make(chan struct{})
	results := make(chan error, len(sockets))
	for _, s := range sockets {
		go func() { <-start; _, err := m.Attach(ctx, s, auth, 0); results <- err }()
	}
	close(start)
	winners := 0
	for range sockets {
		err := <-results
		if err == nil {
			winners++
		} else if err != store.AlreadyConnected {
			t.Errorf("race loser=%v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("winners=%d", winners)
	}
}
func TestRestartPreservesGenerationButLosesReadiness(t *testing.T) {
	m, auth, now := attachmentFixture(t)
	ctx := context.Background()
	socket := acceptSocket(t, m)
	s, err := m.Attach(ctx, socket, auth, 0)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := m.BeginReadiness(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Acknowledge(ctx, s, probe); err != nil {
		t.Fatal(err)
	}
	var path string
	if err := m.store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path)
	}); err != nil {
		t.Fatal(err)
	}
	socket.Close()
	if err := m.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	next, err := NewManager(ManagerConfig{Store: reopened, MaxNonattached: 4, Now: func() time.Time { return *now }, AfterFunc: m.afterFunc, Guard: m.guard, Verify: m.verify})
	if err != nil {
		t.Fatal(err)
	}
	freshSocket := acceptSocket(t, next)
	defer freshSocket.Close()
	snapshot, err := next.Inspect(ctx, freshSocket, auth)
	if err != nil || snapshot.Generation != 1 || snapshot.Active || snapshot.Epoch == s.token.Epoch {
		t.Fatalf("restart snapshot=%+v %v", snapshot, err)
	}
	fresh, err := next.Attach(ctx, freshSocket, auth, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Acknowledge(ctx, fresh, probe); err != store.NotReady {
		t.Fatalf("old readiness survived restart: %v", err)
	}
	if fresh.token.Generation != 2 {
		t.Fatalf("generation=%d", fresh.token.Generation)
	}
}
func TestAttachmentGenerationOverflowDoesNotMutate(t *testing.T) {
	m, auth, _ := attachmentFixture(t)
	ctx := context.Background()
	if _, err := m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		_, err := tx.ExecContext(ctx, "UPDATE bindings SET connection_generation=9223372036854775807")
		return store.TransitionResult{Changed: true}, err
	}, nil); err != nil {
		t.Fatal(err)
	}
	socket := acceptSocket(t, m)
	if _, err := m.Attach(ctx, socket, auth, 9223372036854775807); err != store.InvalidRequest {
		t.Fatalf("overflow=%v", err)
	}
	snapshot, err := m.Inspect(ctx, socket, auth)
	if err != nil || snapshot.Active || snapshot.Generation != 9223372036854775807 {
		t.Fatalf("overflow mutated slot=%+v %v", snapshot, err)
	}
}

func TestDeadlineCrossingDuringGuardCannotPublishSuccess(t *testing.T) {
	for _, operation := range []string{"inspect", "attach"} {
		t.Run(operation, func(t *testing.T) {
			m, auth, now := attachmentFixture(t)
			socket := acceptSocket(t, m)
			m.guard = func(context.Context, *sql.Tx, string) error { *now = now.Add(store.AuthenticationDeadline); return nil }
			var err error
			if operation == "inspect" {
				_, err = m.Inspect(context.Background(), socket, auth)
			} else {
				_, err = m.Attach(context.Background(), socket, auth, 0)
			}
			if err != store.AuthenticationFailed {
				t.Fatalf("expired %s returned %v", operation, err)
			}
			if len(m.slots) != 0 {
				t.Fatal("published expired slot")
			}
		})
	}
}

func TestOnlyUnixStreamSocketsAreAccepted(t *testing.T) {
	m, _, _ := attachmentFixture(t)
	server, _ := socketPair(t, "unixpacket")
	if socket, err := m.Accept(context.Background(), server); err != store.AuthenticationFailed {
		if socket != nil {
			socket.Close()
		}
		t.Fatalf("non-stream socket accepted: %v", err)
	}
}

func TestInspectionFailurePreservesAuthenticatedSocket(t *testing.T) {
	for _, kind := range []string{"guard", "cancelled_wait"} {
		t.Run(kind, func(t *testing.T) {
			m, auth, _ := attachmentFixture(t)
			ctx := context.Background()
			socket := acceptSocket(t, m)
			session, err := m.Attach(ctx, socket, auth, 0)
			if err != nil {
				t.Fatal(err)
			}
			original := m.guard
			if kind == "guard" {
				m.guard = func(context.Context, *sql.Tx, string) error { return store.TemporarilyUnavailable }
			} else {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if _, err := m.Inspect(ctx, socket, auth); err == nil {
				t.Fatal("failed inspection succeeded")
			}
			m.guard = original
			if err := m.Heartbeat(context.Background(), session); err != nil {
				t.Fatalf("observation failure disconnected current slot: %v", err)
			}
		})
	}
}
