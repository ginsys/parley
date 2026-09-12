//go:build linux

package connection

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/store"
)

func testLifecycle(t *testing.T, m *Manager) *Lifecycle {
	t.Helper()
	service, err := NewLifecycle(LifecycleConfig{Store: m.store, Now: m.now,
		Authorize:          func(context.Context, *sql.Tx, store.CommandPrincipal) error { return nil },
		Guard:              func(context.Context, *sql.Tx, string) error { return nil },
		PendingWork:        func(context.Context, *sql.Tx, string) ([]store.WorkRef, error) { return nil, nil },
		PendingDisposition: func(context.Context, *sql.Tx, store.WorkRef, string) error { return store.Forbidden },
		LegacyEvidence:     func(context.Context, LegacyDispositionRequest) error { return nil },
		Invalidate:         m.Invalidate,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}
func TestBindingRevocationIsAuditedAndCancelsExactConnection(t *testing.T) {
	m, auth, _ := attachmentFixture(t)
	service := testLifecycle(t, m)
	ctx := context.Background()
	socket := acceptSocket(t, m)
	session, err := m.Attach(ctx, socket, auth, 0)
	if err != nil {
		t.Fatal(err)
	}
	request := BindingLifecycleRequest{OperationID: targetID, BindingID: session.token.BindingID, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1}
	actor := store.CommandPrincipal{ID: adminID}
	result, err := service.Revoke(ctx, actor, request)
	if err != nil || result.Result.Code != "" {
		t.Fatalf("revoke=%+v %v", result, err)
	}
	select {
	case <-socket.Context().Done():
	default:
		t.Fatal("revocation retained transport")
	}
	replay, err := service.Revoke(ctx, actor, request)
	if err != nil || !replay.Replayed || replay.AuditID != result.AuditID {
		t.Fatalf("revoke replay=%+v %v", replay, err)
	}
	fresh := acceptSocket(t, m)
	if _, err := m.Attach(ctx, fresh, auth, 1); err != store.AuthenticationFailed {
		t.Fatalf("revoked credential=%v", err)
	}
	request.OperationID = "80000000-0000-4000-8000-000000000001"
	request.ExpectedBindingVersion = 2
	retired, err := service.Retire(ctx, actor, request)
	if err != nil || retired.Result.Code != "" {
		t.Fatalf("retire=%+v %v", retired, err)
	}
	if err := m.store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		b, err := store.ReadBinding(ctx, tx, request.BindingID)
		if err != nil {
			return err
		}
		if b.Status != "retired" || b.Version != 3 {
			t.Errorf("retired=%+v", b)
		}
		var barriers int64
		err = tx.QueryRowContext(ctx, "SELECT barrier_version FROM ingestion_barriers WHERE binding_id=?", b.ID).Scan(&barriers)
		if barriers != 2 {
			t.Errorf("barrier version=%d", barriers)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReenrollmentPreservesRevocationBarrier(t *testing.T) {
	m, auth, now := attachmentFixture(t)
	ctx := context.Background()
	lifecycle := testLifecycle(t, m)
	socket := acceptSocket(t, m)
	session, err := m.Attach(ctx, socket, auth, 0)
	if err != nil {
		t.Fatal(err)
	}
	actor := store.CommandPrincipal{ID: adminID}
	revoked, err := lifecycle.Revoke(ctx, actor, BindingLifecycleRequest{OperationID: targetID, BindingID: session.token.BindingID, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1})
	if err != nil || revoked.Result.Code != "" {
		t.Fatalf("revoke=%+v %v", revoked, err)
	}
	var published CredentialFile
	p, err := NewProvisioner(ProvisioningConfig{Store: m.store, Now: func() time.Time { return *now },
		Authorize: func(context.Context, *sql.Tx, store.CommandPrincipal) error { return nil }, Guard: func(context.Context, *sql.Tx, string) error { return nil },
		Verify: func(context.Context, NativeTuple) error { return nil }, LegacyEligibility: func(context.Context, *sql.Tx, string) error { return nil },
		ReenrollEvidence: func(_ context.Context, ref string, native NativeTuple) error {
			if ref != adminID || native != testRegistration().Native {
				return store.HostUnverified
			}
			return nil
		},
		Target: func(string, uint32) (Publisher, error) {
			return PublisherFunc(func(_ context.Context, file CredentialFile) error { published = file; return nil }), nil
		}, Invalidate: m.Invalidate,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := ReenrollRequest{RotateRequest: RotateRequest{OperationID: "80000000-0000-4000-8000-000000000001", BindingID: session.token.BindingID, ExpectedBindingVersion: 2, ExpectedCredentialVersion: 1, ExpiresAt: time.Unix(250, 0), TargetRef: targetID}, HostEvidenceRef: adminID}
	result, err := p.Reenroll(ctx, actor, request)
	if err != nil || result.Receipt.Result.Code != "" || result.Publication != "published" {
		t.Fatalf("reenroll=%+v %v", result, err)
	}
	freshAuth, err := NewAuthentication(published.CredentialID, published.secret[:], testRegistration().Native)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := m.Attach(ctx, acceptSocket(t, m), freshAuth, 1)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.token.CredentialVersion != 2 {
		t.Fatalf("credential version=%d", fresh.token.CredentialVersion)
	}
	if err := m.store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var status string
		var version int64
		err := tx.QueryRowContext(ctx, "SELECT status,barrier_version FROM ingestion_barriers WHERE binding_id=?", session.token.BindingID).Scan(&status, &version)
		if status != "held" || version != 1 {
			t.Errorf("barrier cleared by reenroll: %s %d", status, version)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	p.config.ReenrollEvidence = func(context.Context, string, NativeTuple) error {
		t.Fatal("replay consulted host evidence")
		return nil
	}
	replay, err := p.Reenroll(ctx, actor, request)
	if err != nil || !replay.Receipt.Replayed {
		t.Fatalf("reenroll replay=%+v %v", replay, err)
	}
	if err := m.Heartbeat(ctx, fresh); err != nil {
		t.Fatalf("replay invalidated successor: %v", err)
	}
}

func TestHoldDispositionRetainsReasonAndReplayIdentity(t *testing.T) {
	empty, note := "", "Reviewed synthetic evidence"
	for _, value := range []*string{nil, &empty, &note} {
		name := "absent"
		if value != nil {
			name = "present_" + *value
		}
		t.Run(name, func(t *testing.T) {
			m, auth, _ := attachmentFixture(t)
			ctx := context.Background()
			service := testLifecycle(t, m)
			session, err := m.Attach(ctx, acceptSocket(t, m), auth, 0)
			if err != nil {
				t.Fatal(err)
			}
			work := "90000000-0000-4000-8000-000000000001"
			_, err = m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
				if err := store.EnsureConversation(ctx, tx, "reason", "reason", "2026-01-01T00:00:00Z"); err != nil {
					return store.TransitionResult{}, err
				}
				if err := store.InsertGrant(ctx, tx, store.Grant{Conversation: "reason", GrantVersion: 1, PeerAID: session.token.PeerID, PeerBID: "other", Direction: store.Bidirectional, MaxExchanges: 1, GrantedAt: "2026-01-01T00:00:00Z", Status: store.GrantActive}); err != nil {
					return store.TransitionResult{}, err
				}
				if err := store.InsertQueued(ctx, tx, store.Envelope{ID: work, Conversation: "reason", FromPeer: session.token.PeerID, ToPeer: "other", Text: "synthetic", GrantVersion: 1, CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z"}); err != nil {
					return store.TransitionResult{}, err
				}
				return store.TransitionResult{Changed: true}, store.RecordAuthenticatedEnvelope(ctx, tx, work, session.token.BindingID, 1)
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			actor := store.CommandPrincipal{ID: adminID}
			revoke, err := service.Revoke(ctx, actor, BindingLifecycleRequest{OperationID: targetID, BindingID: session.token.BindingID, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1})
			if err != nil || revoke.Result.Code != "" {
				t.Fatalf("revoke=%+v %v", revoke, err)
			}
			request := HoldDispositionRequest{OperationID: "80000000-0000-4000-8000-000000000001", Work: store.WorkRef{Kind: "envelope", ID: work}, IncidentID: revoke.Result.Resources[1].ID, ExpectedHoldVersion: 1, Action: "release", Reason: DispositionReason{Code: "owner_reviewed", Note: value}}
			receipt, err := service.HoldDisposition(ctx, actor, request)
			if err != nil || receipt.Result.Code != "" {
				t.Fatalf("disposition=%+v %v", receipt, err)
			}
			if err := m.store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
				var got sql.NullString
				err := tx.QueryRowContext(ctx, "SELECT reason_note FROM work_dispositions WHERE audit_principal_id=? AND audit_operation_id=?", adminID, request.OperationID).Scan(&got)
				if got.Valid != (value != nil) || (value != nil && got.String != *value) {
					t.Errorf("retained note=%+v", got)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			replay, err := service.HoldDisposition(ctx, actor, request)
			if err != nil || !replay.Replayed || replay.AuditID != receipt.AuditID {
				t.Fatalf("replay=%+v %v", replay, err)
			}
			if value == nil {
				request.Reason.Note = &empty
			} else {
				request.Reason.Note = nil
			}
			if _, err := service.HoldDisposition(ctx, actor, request); err != store.OperationConflict {
				t.Fatalf("changed note reused operation=%v", err)
			}
		})
	}
}
