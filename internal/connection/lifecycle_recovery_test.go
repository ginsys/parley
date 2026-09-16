//go:build linux

package connection

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/recovery"
	runtimeowner "github.com/ginsys/parley/internal/runtime"
	"github.com/ginsys/parley/internal/store"
)

// memoryMarkers is an in-memory recovery.Markers used only to construct a
// real recovery.Service for these tests. Production wiring uses the durable
// directory-backed implementation in internal/recovery/markers_linux.go.
type memoryMarkers struct {
	mu   sync.Mutex
	byID map[string]recovery.Marker
}

func newMemoryMarkers() *memoryMarkers { return &memoryMarkers{byID: map[string]recovery.Marker{}} }
func (m *memoryMarkers) List(context.Context) ([]recovery.Marker, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]recovery.Marker, 0, len(m.byID))
	for _, v := range m.byID {
		out = append(out, v)
	}
	return out, nil
}
func (m *memoryMarkers) Put(_ context.Context, marker recovery.Marker) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byID[marker.IncidentID] = marker
	return nil
}
func (m *memoryMarkers) Remove(_ context.Context, marker recovery.Marker) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.byID, marker.IncidentID)
	return nil
}

const retiredAdminID = "10000000-0000-4000-8000-000000000099"

// recoveryLifecycleFixture builds one store.DB with a real recovery.Service
// installed before any coordinator revision moves, then a Provisioner/
// Manager/Lifecycle sharing that same writer -- the actual configured
// connection.Lifecycle + recovery.Service + coordinator + store path a wire
// handler would use, not a stubbed Guard or a helper-predicate unit test.
func recoveryLifecycleFixture(t *testing.T) (svc *recovery.Service, m *Manager, lc *Lifecycle, auth Authentication, now *time.Time) {
	t.Helper()
	db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "recovery-lifecycle.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	wallClock := time.Unix(110, 0)
	svc, err = recovery.New(context.Background(), recovery.Config{
		Store: db, Markers: newMemoryMarkers(), Now: func() time.Time { return wallClock },
		FailStop: func() { t.Error("unexpected fail-stop") },
	})
	if err != nil {
		t.Fatal(err)
	}
	var file CredentialFile
	p, err := NewProvisioner(ProvisioningConfig{
		Store: db, Now: func() time.Time { return wallClock },
		Authorize:         func(context.Context, *sql.Tx, store.CommandPrincipal) error { return nil },
		Guard:             func(context.Context, *sql.Tx, string) error { return nil },
		Verify:            func(context.Context, NativeTuple) error { return nil },
		LegacyEligibility: func(context.Context, *sql.Tx, string) error { return nil },
		Target: func(string, uint32) (Publisher, error) {
			return PublisherFunc(func(_ context.Context, f CredentialFile) error { file = f; return nil }), nil
		},
		Invalidate: func(string) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Register(context.Background(), store.CommandPrincipal{ID: adminID}, testRegistration()); err != nil {
		t.Fatal(err)
	}
	m, err = NewManager(ManagerConfig{Store: db, MaxNonattached: 4, Now: func() time.Time { return wallClock },
		AfterFunc: func(time.Duration, func()) func() { return func() {} },
		Guard:     func(context.Context, *sql.Tx, string) error { return nil },
		Verify:    func(context.Context, NativeTuple, Token) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	auth, err = NewAuthentication(file.CredentialID, file.secret[:], testRegistration().Native)
	if err != nil {
		t.Fatal(err)
	}
	lc = testLifecycle(t, m)
	return svc, m, lc, auth, &wallClock
}

// retireNamespace performs an ordinary administrative namespace retirement
// using the real store.RetireRecoveryNamespace helper -- the same one
// recovery.RestoreAdministration.Complete uses -- so retirement in these
// tests is not a test-only stub that independently rejects every call first.
// retired_namespaces.incident_id requires an existing recovery_incidents row;
// this helper records one and immediately reconciles it, modelling an
// already-completed historical retirement rather than an active hold. Tests
// that need an active hold create one independently via clock rollback.
func retireNamespace(t *testing.T, db *store.DB, incident, principal string, now *time.Time) {
	t.Helper()
	if _, err := db.Coordinator().Transition(context.Background(), func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		var serverID string
		if err := tx.QueryRowContext(ctx, "SELECT server_id FROM installation WHERE singleton=1").Scan(&serverID); err != nil {
			return store.TransitionResult{}, err
		}
		if _, err := store.RecordRecovery(ctx, tx, store.RecoveryRecord{ID: incident, ServerID: serverID, Kind: "restore", Version: 1, Status: "held"}); err != nil {
			return store.TransitionResult{}, err
		}
		if err := store.RetireRecoveryNamespace(ctx, tx, incident, store.NamespaceRetirement{PrincipalID: principal}, now.UnixNano()); err != nil {
			return store.TransitionResult{}, err
		}
		if _, err := store.ReconcileRecovery(ctx, tx, incident, 1, incident); err != nil {
			return store.TransitionResult{}, err
		}
		// ReconcileRecovery only reaches "reconciled"; RecoveryHeld treats
		// anything but "cleared" as held, so finish clearing it here.
		_, err := store.ClearRecovery(ctx, tx, incident, 2, incident)
		return store.TransitionResult{Changed: true}, err
	}, nil); err != nil {
		t.Fatal(err)
	}
}

// heldEnvelopeFixture attaches a session for adminID's registered binding,
// queues one authenticated envelope and revokes that binding, producing a
// real security_holds row a disposition can target. Mirrors
// TestHoldDispositionRetainsReasonAndReplayIdentity.
func heldEnvelopeFixture(t *testing.T, m *Manager, lc *Lifecycle, auth Authentication, work string) (incidentID, bindingID string) {
	t.Helper()
	ctx := context.Background()
	session, err := m.Attach(ctx, acceptSocket(t, m), auth, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
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
	}, nil); err != nil {
		t.Fatal(err)
	}
	actor := store.CommandPrincipal{ID: adminID}
	revoke, err := lc.Revoke(ctx, actor, BindingLifecycleRequest{OperationID: targetID, BindingID: session.token.BindingID, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1})
	if err != nil || revoke.Result.Code != "" {
		t.Fatalf("revoke=%+v %v", revoke, err)
	}
	return revoke.Result.Resources[1].ID, session.token.BindingID
}

// TestRecoveryHoldAdmitsAuthorizedDispositionButNotRetiredOrUnrelatedWork is
// the D3/EP-01 acceptance test: it exercises hold.disposition and
// legacy.disposition through the actual configured connection.Lifecycle +
// recovery.Service + coordinator + store path, not a helper-predicate unit
// test or a Guard stub that rejects everything up front.
func TestRecoveryHoldAdmitsAuthorizedDispositionButNotRetiredOrUnrelatedWork(t *testing.T) {
	svc, m, lc, auth, now := recoveryLifecycleFixture(t)
	ctx := context.Background()
	actor := store.CommandPrincipal{ID: adminID}

	// A namespace retired well before any hold exists must still block a
	// later disposition attempt once the hold makes the operation reachable.
	retireNamespace(t, m.store, "60000000-0000-4000-8000-000000000060", retiredAdminID, now)

	incidentID, bindingID := heldEnvelopeFixture(t, m, lc, auth, "90000000-0000-4000-8000-000000000001")

	// Force a global recovery hold via clock-rollback detection, the same
	// mechanism internal/recovery/service_test.go uses.
	*now = time.Unix(100, 0)
	if _, err := m.store.Coordinator().Transition(ctx, func(context.Context, *sql.Tx, store.CommitView) (store.TransitionResult, error) {
		return store.TransitionResult{}, nil
	}, nil); err != store.RecoveryRequired {
		t.Fatalf("rollback did not latch a hold: %v", err)
	}
	if mode, err := svc.InspectRecovery(ctx, m.store); err != nil || mode != runtimeowner.Held {
		t.Fatalf("expected held runtime: mode=%v err=%v", mode, err)
	}
	*now = time.Unix(300, 0)

	// D3: an authorized, non-retired administrator can still dispose of the
	// exact reviewed item while the global hold remains.
	holdRequest := HoldDispositionRequest{OperationID: "80000000-0000-4000-8000-000000000001", Work: store.WorkRef{Kind: "envelope", ID: "90000000-0000-4000-8000-000000000001"}, IncidentID: incidentID, ExpectedHoldVersion: 1, Action: "release", Reason: DispositionReason{Code: "owner_reviewed"}}
	disposed, err := lc.HoldDisposition(ctx, actor, holdRequest)
	if err != nil || disposed.Result.Code != "" {
		t.Fatalf("hold.disposition blocked by hold: %+v %v", disposed, err)
	}

	// The hold does not broaden to unrelated operations: an otherwise valid
	// binding.revoke on the same session's own (now-version-2) binding
	// remains refused while held.
	if _, err := lc.Revoke(ctx, actor, BindingLifecycleRequest{OperationID: "80000000-0000-4000-8000-000000000002", BindingID: bindingID, ExpectedBindingVersion: 2, ExpectedCredentialVersion: 1}); err != store.RecoveryRequired {
		t.Fatalf("unrelated binding.revoke bypassed the hold: %v", err)
	}

	// legacy.disposition is likewise reachable: it reaches ApplyWorkDisposition
	// and gets a normal NotFound business rejection for a nonexistent
	// quarantine row, not a recovery block. Full quarantine-fixture coverage
	// for ApplyWorkDisposition itself already exists in
	// internal/store/dispositions_test.go and is unaffected by this change.
	legacyRequest := LegacyDispositionRequest{OperationID: "80000000-0000-4000-8000-000000000003", MigrationIncidentID: "60000000-0000-4000-8000-000000000099", WorkID: "legacy-work", ExpectedQuarantineVersion: 1, Action: "release", DispositionRef: "70000000-0000-4000-8000-000000000001"}
	legacy, err := lc.LegacyDisposition(ctx, actor, legacyRequest)
	if err != nil || legacy.Result.Code != store.NotFound {
		t.Fatalf("legacy.disposition unexpected result during hold: %+v %v", legacy, err)
	}

	// EP-01: a configured/authorized administrator whose namespace is
	// retired gains no fresh disposition authority merely because the
	// operation kind is reachable during a hold.
	retiredActor := store.CommandPrincipal{ID: retiredAdminID}
	retiredAttempt := holdRequest
	retiredAttempt.OperationID = "80000000-0000-4000-8000-000000000004"
	if _, err := lc.HoldDisposition(ctx, retiredActor, retiredAttempt); err != store.RecoveryRequired {
		t.Fatalf("retired administrator performed a fresh disposition: %v", err)
	}

	// A replay of the original authorized disposition remains idempotent
	// while still held: an independently authorized replay is distinguishable
	// from a new mutation, and this cannot accidentally break allowed
	// recovery-result replay.
	replay, err := lc.HoldDisposition(ctx, actor, holdRequest)
	if err != nil || !replay.Replayed || replay.AuditID != disposed.AuditID {
		t.Fatalf("replay during hold lost idempotency: %+v %v", replay, err)
	}
}

// TestRecoveryDispositionRefusesRetiredAdministratorWithoutAnyHold proves the
// retirement check is not itself conditioned on a hold being active: it is a
// distinct eligibility-for-a-new-mutation check, always enforced.
func TestRecoveryDispositionRefusesRetiredAdministratorWithoutAnyHold(t *testing.T) {
	_, m, lc, auth, now := recoveryLifecycleFixture(t)
	ctx := context.Background()
	retireNamespace(t, m.store, "60000000-0000-4000-8000-000000000061", retiredAdminID, now)
	incidentID, _ := heldEnvelopeFixture(t, m, lc, auth, "90000000-0000-4000-8000-000000000002")

	request := HoldDispositionRequest{OperationID: "80000000-0000-4000-8000-000000000010", Work: store.WorkRef{Kind: "envelope", ID: "90000000-0000-4000-8000-000000000002"}, IncidentID: incidentID, ExpectedHoldVersion: 1, Action: "release", Reason: DispositionReason{Code: "owner_reviewed"}}
	if _, err := lc.HoldDisposition(ctx, store.CommandPrincipal{ID: retiredAdminID}, request); err != store.RecoveryRequired {
		t.Fatalf("retired administrator disposed without any hold: %v", err)
	}
	// Sanity: the same request from the non-retired administrator succeeds,
	// proving the check above rejected on retirement, not on some unrelated
	// malformed request.
	ok, err := lc.HoldDisposition(ctx, store.CommandPrincipal{ID: adminID}, request)
	if err != nil || ok.Result.Code != "" {
		t.Fatalf("non-retired administrator unexpectedly refused: %+v %v", ok, err)
	}
}
