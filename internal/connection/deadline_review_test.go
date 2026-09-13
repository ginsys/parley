//go:build linux

package connection

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/store"
)

// Observe the real transaction boundary rather than assuming a count/order of
// clock calls: a completed sql.Tx returns ErrTxDone without executing SELECT.
func crossAuthorizationDeadline(t *testing.T, m *Manager, before, after time.Time, phase string) {
	t.Helper()
	var transaction *sql.Tx
	m.guard = func(_ context.Context, tx *sql.Tx, _ string) error { transaction = tx; return nil }
	m.now = func() time.Time {
		if transaction == nil {
			return before
		}
		if phase == "guard" {
			return after
		}
		_, err := transaction.ExecContext(context.Background(), "SELECT 1")
		if errors.Is(err, sql.ErrTxDone) {
			return after
		}
		if err != nil {
			t.Errorf("clock boundary probe: %v", err)
		}
		return before
	}
}

func TestCredentialDeadlineCrossingCannotPublishAttachment(t *testing.T) {
	for _, operation := range []string{"inspect", "attach"} {
		for _, phase := range []string{"guard", "commit"} {
			t.Run(operation+"/"+phase, func(t *testing.T) {
				m, auth, now := attachmentFixture(t)
				*now = time.Unix(199, 0)
				socket := acceptSocket(t, m)
				crossAuthorizationDeadline(t, m, *now, time.Unix(200, 0), phase)
				var err error
				if operation == "inspect" {
					_, err = m.Inspect(context.Background(), socket, auth)
				} else {
					_, err = m.Attach(context.Background(), socket, auth, 0)
				}
				if err != store.AuthenticationFailed {
					t.Errorf("expired %s=%v", operation, err)
				}
				if len(m.slots) != 0 || socket.Context().Err() == nil {
					t.Error("expired credential published/retained a socket")
				}
				if err := m.store.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
					c, err := store.ReadCredential(ctx, tx, auth.credentialID)
					if err == nil && c.Status != "expired" {
						t.Errorf("terminal expiry not persisted: %s", c.Status)
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
				m.guard = func(context.Context, *sql.Tx, string) error { return nil }
				m.now = func() time.Time { return *now }
				if _, err := m.Inspect(context.Background(), acceptSocket(t, m), auth); err != store.AuthenticationFailed {
					t.Errorf("rollback revived credential: %v", err)
				}
			})
		}
	}
}

func TestHeartbeatDeadlineCrossingCannotReviveSocket(t *testing.T) {
	for _, phase := range []string{"guard", "commit"} {
		t.Run(phase, func(t *testing.T) {
			m, auth, now := attachmentFixture(t)
			session, err := m.Attach(context.Background(), acceptSocket(t, m), auth, 0)
			if err != nil {
				t.Fatal(err)
			}
			previous := session.socket.lastHeartbeat
			*now = time.Unix(139, 0)
			crossAuthorizationDeadline(t, m, *now, time.Unix(140, 0), phase)
			if err := m.Heartbeat(context.Background(), session); err != store.AuthenticationFailed {
				t.Errorf("late heartbeat=%v", err)
			}
			if session.socket.lastHeartbeat != previous || session.socket.Context().Err() == nil || len(m.slots) != 0 {
				t.Error("late heartbeat renewed or retained expired socket")
			}
		})
	}
}

func TestReadinessACKDeadlineCrossingCannotReadySession(t *testing.T) {
	m, auth, now := attachmentFixture(t)
	ctx := context.Background()
	session, err := m.Attach(ctx, acceptSocket(t, m), auth, 0)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := m.BeginReadiness(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	*now = time.Unix(130, 0)
	if err := m.Heartbeat(ctx, session); err != nil {
		t.Fatal(err)
	}
	*now = time.Unix(139, 0)
	crossAuthorizationDeadline(t, m, *now, time.Unix(140, 0), "commit")
	if err := m.Acknowledge(ctx, session, probe); err != store.NotReady {
		t.Errorf("late ACK=%v", err)
	}
	if session.ready || session.socket.Context().Err() != nil {
		t.Error("late ACK readied session or discarded live socket")
	}
	if err := m.RequireReady(ctx, session); err != store.NotReady {
		t.Errorf("late ACK authorized ready work: %v", err)
	}
}

func TestReadinessVerifierPreservesUnavailableEvidence(t *testing.T) {
	for _, failure := range []error{store.TemporarilyUnavailable, fmt.Errorf("synthetic verifier: %w", store.TemporarilyUnavailable), context.DeadlineExceeded, errors.New("synthetic private source unavailable")} {
		t.Run(failure.Error(), func(t *testing.T) {
			m, auth, _ := attachmentFixture(t)
			ctx := context.Background()
			session, err := m.Attach(ctx, acceptSocket(t, m), auth, 0)
			if err != nil {
				t.Fatal(err)
			}
			m.verify = func(context.Context, NativeTuple, Token) error { return failure }
			if _, err := m.BeginReadiness(ctx, session); err != store.TemporarilyUnavailable {
				t.Errorf("unavailable verifier=%v", err)
			}
			if session.hostVerified || session.ready || session.socket.Context().Err() != nil {
				t.Error("unavailable verifier readied or discarded socket")
			}
			oldNonce := session.nonce
			m.verify = func(context.Context, NativeTuple, Token) error { return nil }
			probe, err := m.BeginReadiness(ctx, session)
			if err != nil || probe.Nonce == oldNonce {
				t.Fatalf("fresh verification=%+v err=%v", probe, err)
			}
			if err := m.Acknowledge(ctx, session, probe); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHostVerificationDeadlineCrossingCannotPublishEvidence(t *testing.T) {
	m, auth, now := attachmentFixture(t)
	ctx := context.Background()
	session, err := m.Attach(ctx, acceptSocket(t, m), auth, 0)
	if err != nil {
		t.Fatal(err)
	}
	m.verify = func(context.Context, NativeTuple, Token) error {
		*now = time.Unix(130, 0)
		if err := m.Heartbeat(ctx, session); err != nil {
			t.Fatal(err)
		}
		*now = time.Unix(139, 0)
		crossAuthorizationDeadline(t, m, *now, time.Unix(140, 0), "commit")
		return nil
	}
	if _, err := m.BeginReadiness(ctx, session); err != store.NotReady {
		t.Errorf("late host completion=%v", err)
	}
	if session.hostVerified || session.ready || session.socket.Context().Err() != nil {
		t.Error("late host evidence readied or discarded live socket")
	}
}

func TestFailedExpiryPersistenceRetainsDenialAcrossClockRollback(t *testing.T) {
	m, auth, now := attachmentFixture(t)
	ctx := context.Background()
	_, err := m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		_, err := tx.ExecContext(ctx, `CREATE TRIGGER reject_expiry BEFORE UPDATE OF status ON credentials WHEN NEW.status='expired' BEGIN SELECT RAISE(ABORT,'synthetic expiry failure'); END`)
		return store.TransitionResult{Changed: true}, err
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	*now = time.Unix(199, 0)
	socket := acceptSocket(t, m)
	crossAuthorizationDeadline(t, m, *now, time.Unix(200, 0), "commit")
	if _, err := m.Attach(ctx, socket, auth, 0); err != store.TemporarilyUnavailable {
		t.Errorf("failed expiry evidence=%v", err)
	}
	if !m.store.CredentialExpiryObserved(auth.credentialID) || len(m.slots) != 0 || socket.Context().Err() == nil {
		t.Error("failed expiry persistence lost denial or retained attachment")
	}
	m.now = func() time.Time { return *now }
	m.guard = func(context.Context, *sql.Tx, string) error { return nil }
	if _, err := m.Inspect(ctx, acceptSocket(t, m), auth); err != store.TemporarilyUnavailable {
		t.Errorf("rollback bypassed unpersisted expiry: %v", err)
	}
	_, err = m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		_, err := tx.ExecContext(ctx, "DROP TRIGGER reject_expiry")
		return store.TransitionResult{Changed: true}, err
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Inspect(ctx, acceptSocket(t, m), auth); err != store.AuthenticationFailed {
		t.Errorf("recovered persistence revived expired credential: %v", err)
	}
	if m.store.CredentialExpiryObserved(auth.credentialID) {
		t.Error("successful persistence retained temporary denial marker")
	}
	if err := m.store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		c, err := store.ReadCredential(ctx, tx, auth.credentialID)
		if err == nil && c.Status != "expired" {
			t.Errorf("persisted status=%s", c.Status)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
