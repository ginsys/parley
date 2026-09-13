package connection

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/store"
)

func TestFreshAuthenticationFailureConsumesSocket(t *testing.T) {
	for _, operation := range []string{"inspect", "attach"} {
		for _, failure := range []string{"guard", "read"} {
			t.Run(operation+"/"+failure, func(t *testing.T) {
				m, auth, _ := attachmentFixture(t)
				ctx := context.Background()
				socket := acceptSocket(t, m)
				if failure == "guard" {
					m.guard = func(context.Context, *sql.Tx, string) error { return store.TemporarilyUnavailable }
				} else {
					_, err := m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
						_, err := tx.ExecContext(ctx, "ALTER TABLE credentials RENAME TO hidden_credentials")
						return store.TransitionResult{Changed: true}, err
					}, nil)
					if err != nil {
						t.Fatal(err)
					}
				}
				var err error
				if operation == "inspect" {
					_, err = m.Inspect(ctx, socket, auth)
				} else {
					_, err = m.Attach(ctx, socket, auth, 0)
				}
				if err != store.AuthenticationFailed || socket.Context().Err() == nil || len(m.sockets) != 0 || len(m.pending) != 0 {
					t.Errorf("initial failure=%v closed=%v sockets=%d pending=%d", err, socket.Context().Err() != nil, len(m.sockets), len(m.pending))
				}
				if _, err := m.Attach(ctx, socket, Authentication{}, 0); err != store.AuthenticationFailed {
					t.Errorf("second credential attempt=%v", err)
				}
			})
		}
	}
}

func TestInspectionReportsSlotAfterPublicationPruning(t *testing.T) {
	m, auth, now := attachmentFixture(t)
	ctx := context.Background()
	old, err := m.Attach(ctx, acceptSocket(t, m), auth, 0)
	if err != nil {
		t.Fatal(err)
	}
	*now = time.Unix(139, 0)
	socket := acceptSocket(t, m)
	crossAuthorizationDeadline(t, m, *now, time.Unix(140, 0), "commit")
	snapshot, err := m.Inspect(ctx, socket, auth)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Active || snapshot.Generation != old.token.Generation || len(m.slots) != 0 {
		t.Fatalf("stale snapshot=%+v slots=%d", snapshot, len(m.slots))
	}
	if _, err := m.Attach(ctx, socket, auth, snapshot.Generation); err != nil {
		t.Fatalf("attach after pruning=%v", err)
	}
}

func TestFailedHostVerificationPersistsCredentialExpiry(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failure  error
		finished int64
	}{
		{"mismatch", store.HostUnverified, 200},
		{"unavailable", store.TemporarilyUnavailable, 200},
		{"cancelled", context.Canceled, 200},
		{"liveness_also_expired", store.HostUnverified, 230},
		{"socket_cancelled", context.Canceled, 230},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, auth, now := attachmentFixture(t)
			*now = time.Unix(199, 0)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, err := m.Attach(ctx, acceptSocket(t, m), auth, 0)
			if err != nil {
				t.Fatal(err)
			}
			m.verify = func(context.Context, NativeTuple, Token) error {
				*now = time.Unix(tc.finished, 0)
				if tc.failure == context.Canceled {
					cancel()
				}
				if tc.name == "socket_cancelled" {
					m.expire(s.socket)
				}
				return tc.failure
			}
			if _, err := m.BeginReadiness(ctx, s); err != store.AuthenticationFailed {
				t.Errorf("expired verification=%v", err)
			}
			if s.socket.Context().Err() == nil || len(m.slots) != 0 {
				t.Error("failed verifier retained expired session")
			}
			if err := m.store.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
				c, err := store.ReadCredential(ctx, tx, auth.credentialID)
				if err == nil && c.Status != "expired" {
					t.Errorf("credential status=%s", c.Status)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			*now = time.Unix(199, 0)
			if _, err := m.Inspect(context.Background(), acceptSocket(t, m), auth); err != store.AuthenticationFailed {
				t.Errorf("clock rollback revived expired credential: %v", err)
			}
		})
	}
}
