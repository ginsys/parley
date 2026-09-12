package connection

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/store"
)

func TestReadinessNonceGenerationAndHeartbeatFences(t *testing.T) {
	m, auth, now := attachmentFixture(t)
	ctx := context.Background()
	socket := acceptSocket(t, m)
	session, err := m.Attach(ctx, socket, auth, 0)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := m.BeginReadiness(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	wrong := probe
	wrong.Nonce = "forged"
	if err := m.Acknowledge(ctx, session, wrong); !errors.Is(err, store.NotReady) {
		t.Fatalf("forged ACK=%v", err)
	}
	if err := m.Heartbeat(ctx, session); err != nil {
		t.Fatal(err)
	}
	if err := m.RequireReady(ctx, session); !errors.Is(err, store.NotReady) {
		t.Fatalf("heartbeat readied: %v", err)
	}
	if err := m.Acknowledge(ctx, session, probe); err != nil {
		t.Fatal(err)
	}
	if err := m.RequireReady(ctx, session); err != nil {
		t.Fatal(err)
	}
	next, err := m.BeginReadiness(ctx, session)
	if err != nil || next.Nonce == probe.Nonce {
		t.Fatalf("fresh attempt=%+v %v", next, err)
	}
	if err := m.Acknowledge(ctx, session, probe); !errors.Is(err, store.NotReady) {
		t.Fatalf("old nonce=%v", err)
	}
	*now = now.Add(20 * time.Second)
	if err := m.Heartbeat(ctx, session); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(10 * time.Second)
	if err := m.Acknowledge(ctx, session, next); !errors.Is(err, store.NotReady) {
		t.Fatalf("deadline equality=%v", err)
	}
	third, err := m.BeginReadiness(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Acknowledge(ctx, session, third); err != nil {
		t.Fatal(err)
	}
	socket.Close()
	replacement := acceptSocket(t, m)
	successor, err := m.Attach(ctx, replacement, auth, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Acknowledge(ctx, successor, third); !errors.Is(err, store.NotReady) {
		t.Fatalf("stale generation ACK=%v", err)
	}
	if err := m.Heartbeat(ctx, session); !errors.Is(err, store.AuthenticationFailed) {
		t.Fatalf("stale heartbeat=%v", err)
	}
	*now = now.Add(store.LivenessDeadline)
	if err := m.Heartbeat(ctx, successor); !errors.Is(err, store.AuthenticationFailed) {
		t.Fatalf("late heartbeat revived slot=%v", err)
	}
	select {
	case <-successor.socket.Context().Done():
	default:
		t.Fatal("timeout retained transport")
	}
}
func TestHostVerifierFailClosedAndStaleCompletion(t *testing.T) {
	for _, kind := range []string{"mismatch", "unavailable", "disconnect"} {
		t.Run(kind, func(t *testing.T) {
			m, auth, _ := attachmentFixture(t)
			ctx := context.Background()
			socket := acceptSocket(t, m)
			s, err := m.Attach(ctx, socket, auth, 0)
			if err != nil {
				t.Fatal(err)
			}
			m.verify = func(ctx context.Context, n NativeTuple, token Token) error {
				if n != testRegistration().Native || token != s.token {
					t.Fatal("verifier lost trusted tuple/token")
				}
				if kind == "disconnect" {
					socket.Close()
					return nil
				}
				return errors.New("untrusted host diagnostic")
			}
			_, err = m.BeginReadiness(ctx, s)
			if err != store.HostUnverified && err != store.AuthenticationFailed {
				t.Fatalf("host result=%v", err)
			}
			if err := m.RequireReady(ctx, s); err == nil {
				t.Fatal("unverified host became ready")
			}
		})
	}
}

func TestDisconnectCancelsRunningHostVerification(t *testing.T) {
	m, auth, _ := attachmentFixture(t)
	ctx := context.Background()
	socket := acceptSocket(t, m)
	s, err := m.Attach(ctx, socket, auth, 0)
	if err != nil {
		t.Fatal(err)
	}
	entered, done := make(chan struct{}), make(chan error, 1)
	m.verify = func(ctx context.Context, _ NativeTuple, _ Token) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	go func() { _, err := m.BeginReadiness(ctx, s); done <- err }()
	<-entered
	socket.Close()
	select {
	case err := <-done:
		if err != store.HostUnverified {
			t.Fatalf("result=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("disconnect did not cancel verifier")
	}
}
