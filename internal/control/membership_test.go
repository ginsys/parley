package control

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/recovery"
	"github.com/ginsys/parley/internal/store"
)

// memoryMarkersForTest is a minimal in-memory recovery.Markers used only to
// construct a real recovery.Service, mirroring
// internal/connection/lifecycle_recovery_test.go's identical fixture (not
// shared across packages, per this codebase's convention).
type memoryMarkersForTestType struct {
	mu   sync.Mutex
	byID map[string]recovery.Marker
}

func memoryMarkersForTest() *memoryMarkersForTestType {
	return &memoryMarkersForTestType{byID: map[string]recovery.Marker{}}
}
func (m *memoryMarkersForTestType) List(context.Context) ([]recovery.Marker, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]recovery.Marker, 0, len(m.byID))
	for _, v := range m.byID {
		out = append(out, v)
	}
	return out, nil
}
func (m *memoryMarkersForTestType) Put(_ context.Context, marker recovery.Marker) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byID[marker.IncidentID] = marker
	return nil
}
func (m *memoryMarkersForTestType) Remove(_ context.Context, marker recovery.Marker) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.byID, marker.IncidentID)
	return nil
}

// membershipTestServer builds a real, negotiated session backed by a real
// on-disk store.DB -- membership.* mutations need an actual Coordinator, not
// the zero-value Queries testServer uses for the hello/operation.get tests.
func membershipTestServer(t *testing.T) (*Session, *store.DB) {
	t.Helper()
	db := controlTestDB(t)
	if err := db.OpenReaders(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg, err := NewConfig("/run/parley/admin.sock", 1000, map[string]uint32{testHelloAdmin: 1001})
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(cfg, db.Queries(), db, "server-id-fixture", "epoch-fixture", StateRunning)
	sess := srv.NewSession(Identity{PrincipalID: testHelloAdmin, UID: 1001})
	sess.negotiated = true
	return sess, db
}

// seedEnabledBinding inserts a synthetic enabled binding + current
// credential for peer, mirroring internal/dispatch/authenticated_linux_test.go's
// fixture pattern -- membership.enroll's binding-enabled precondition
// (store.EnabledPeer) needs a real bindings/credentials row, not a bare
// peer string.
func seedEnabledBinding(t *testing.T, db *store.DB, index int, peer string) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	b := store.BindingRecord{
		ID: fmt.Sprintf("60000000-0000-4000-8000-%012d", index), PeerID: peer,
		HostKind: "codex_cli", NamespaceID: "synthetic", SessionID: peer,
		ConnectorUID: 1001, Status: "enabled", Version: 1,
	}
	var secret [32]byte
	secret[0] = byte(index)
	c := store.CredentialRecord{
		ID: fmt.Sprintf("70000000-0000-4000-8000-%012d", index), BindingID: b.ID, Version: 1,
		Status: "current", ExpiresAtNS: time.Now().Add(24 * time.Hour).UnixNano(), Verifier: sha256.Sum256(secret[:]),
	}
	if err := store.InsertBindingCredential(ctx, tx, b, c); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func openMembers(a, b string) map[string]any {
	return map[string]any{
		"operation_id":           newOpID(),
		"conversation":           "conv-1",
		"expected_grant_version": "0",
		"members": []any{
			map[string]any{"peer_id": a, "role": "member"},
			map[string]any{"peer_id": b, "role": "member"},
		},
		"policy":        map[string]any{"kind": "open"},
		"max_exchanges": "5",
	}
}

var opCounter int

// newOpID mints a fresh canonical UUID string per call so successive test
// requests don't collide on the coordinator's operation_id+digest replay
// key unless a test deliberately reuses one.
func newOpID() string {
	opCounter++
	return fmt.Sprintf("80000000-0000-4000-8000-%012d", opCounter)
}

func TestMembershipEnrollSucceedsForSupportedOpenPolicy(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	resp, closeAfter := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if closeAfter || resp.Err != nil {
		t.Fatalf("unexpected rejection: %#v", resp.Err)
	}
	result := decodeResult[CommandReceiptResult](t, resp)
	if result.Result.Code != "" || len(result.Result.Resources) != 1 || result.Result.Resources[0].After != "1" {
		t.Fatalf("unexpected result: %#v", result)
	}
	if result.AuditID == "" || result.CommitView.Revision == "" {
		t.Fatalf("missing receipt evidence: %#v", result)
	}
}

func TestMembershipEnrollRejectsWithoutEnabledBinding(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	// peer-b has no binding at all.
	resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != DomainCode(store.BindingUnavailable) {
		t.Fatalf("expected binding_unavailable, got %#v", resp.Err)
	}
}

func TestMembershipEnrollRejectsUnsupportedShape(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	seedEnabledBinding(t, db, 3, "peer-c")
	params := map[string]any{
		"operation_id":           newOpID(),
		"conversation":           "conv-lead",
		"expected_grant_version": "0",
		"members": []any{
			map[string]any{"peer_id": "peer-a", "role": "member"},
			map[string]any{"peer_id": "peer-b", "role": "member"},
			map[string]any{"peer_id": "peer-c", "role": "member"},
		},
		"policy":        map[string]any{"kind": "open"},
		"max_exchanges": "5",
	}
	resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: params})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != DomainCode(store.UnsupportedMembership) {
		t.Fatalf("expected unsupported_membership, got %#v", resp.Err)
	}
}

func TestMembershipEnrollRejectsInvalidShape(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	params := map[string]any{
		"operation_id":           newOpID(),
		"conversation":           "conv-dup",
		"expected_grant_version": "0",
		"members": []any{
			map[string]any{"peer_id": "peer-a", "role": "member"},
			map[string]any{"peer_id": "peer-a", "role": "member"}, // duplicate
		},
		"policy":        map[string]any{"kind": "open"},
		"max_exchanges": "5",
	}
	resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: params})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != DomainCode(store.InvalidMembership) {
		t.Fatalf("expected invalid_membership, got %#v", resp.Err)
	}
}

func TestMembershipEnrollDirectedEdgeRoundTrip(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	params := map[string]any{
		"operation_id":           newOpID(),
		"conversation":           "conv-directed",
		"expected_grant_version": "0",
		"members": []any{
			map[string]any{"peer_id": "peer-a", "role": "member"},
			map[string]any{"peer_id": "peer-b", "role": "member"},
		},
		"policy":        map[string]any{"kind": "directed", "edges": []any{map[string]any{"from": "peer-b", "to": "peer-a"}}},
		"max_exchanges": "3",
	}
	resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: params})
	if resp.Err != nil {
		t.Fatalf("unexpected rejection: %#v", resp.Err)
	}
}

func TestMembershipEnrollReplayReturnsSameReceiptWithoutASecondGrant(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	params := openMembers("peer-a", "peer-b")
	first, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: params})
	if first.Err != nil {
		t.Fatalf("first attempt rejected: %#v", first.Err)
	}
	second, _ := sess.Handle(context.Background(), Request{ID: "2", Method: "membership.enroll", Params: params})
	if second.Err != nil {
		t.Fatalf("replay rejected: %#v", second.Err)
	}
	r1 := decodeResult[CommandReceiptResult](t, first)
	r2 := decodeResult[CommandReceiptResult](t, second)
	if r1.AuditID != r2.AuditID {
		t.Fatalf("replay produced a different audit record: %s vs %s", r1.AuditID, r2.AuditID)
	}
	var count int
	if err := db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM grants WHERE conversation='conv-1'").Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("replay must not create a second grant: found %d", count)
	}
}

func TestMembershipEnrollConflictingRetrySameOperationIDDifferentPayload(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	opID := newOpID()
	p1 := openMembers("peer-a", "peer-b")
	p1["operation_id"] = opID
	p2 := openMembers("peer-a", "peer-b")
	p2["operation_id"] = opID
	p2["conversation"] = "conv-different"
	first, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: p1})
	if first.Err != nil {
		t.Fatalf("first attempt rejected: %#v", first.Err)
	}
	second, _ := sess.Handle(context.Background(), Request{ID: "2", Method: "membership.enroll", Params: p2})
	if second.Err == nil || second.Err.Data == nil || second.Err.Data.Code != DomainCode(store.OperationConflict) {
		t.Fatalf("conflicting retry on the same operation_id must be rejected as operation_conflict, got %#v", second.Err)
	}
}

func TestMembershipRevokeThenRenewRejectsNoActiveGrant(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	enroll, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if enroll.Err != nil {
		t.Fatalf("enroll failed: %#v", enroll.Err)
	}
	revoke, _ := sess.Handle(context.Background(), Request{ID: "2", Method: "membership.revoke", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "conv-1", "expected_grant_version": "1",
	}})
	if revoke.Err != nil {
		t.Fatalf("revoke failed: %#v", revoke.Err)
	}
	renew, _ := sess.Handle(context.Background(), Request{ID: "3", Method: "membership.renew", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "conv-1", "expected_grant_version": "1",
	}})
	if renew.Err == nil || renew.Err.Data == nil || renew.Err.Data.Code != DomainCode(store.NoActiveGrant) {
		t.Fatalf("expected no_active_grant after revoke, got %#v", renew.Err)
	}
}

func TestMembershipRenewRejectsStaleVersion(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	enroll, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if enroll.Err != nil {
		t.Fatalf("enroll failed: %#v", enroll.Err)
	}
	renew, _ := sess.Handle(context.Background(), Request{ID: "2", Method: "membership.renew", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "conv-1", "expected_grant_version": "99",
	}})
	if renew.Err == nil || renew.Err.Data == nil || renew.Err.Data.Code != DomainCode(store.StaleGrantVersion) {
		t.Fatalf("expected stale_grant_version, got %#v", renew.Err)
	}
}

func TestMembershipReplaceChangesMembership(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	seedEnabledBinding(t, db, 3, "peer-c")
	enroll, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if enroll.Err != nil {
		t.Fatalf("enroll failed: %#v", enroll.Err)
	}
	replace, _ := sess.Handle(context.Background(), Request{ID: "2", Method: "membership.replace", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "conv-1", "expected_grant_version": "1",
		"members": []any{
			map[string]any{"peer_id": "peer-a", "role": "member"},
			map[string]any{"peer_id": "peer-c", "role": "member"},
		},
		"policy": map[string]any{"kind": "open"},
	}})
	if replace.Err != nil {
		t.Fatalf("replace failed: %#v", replace.Err)
	}
}

// TestMembershipEnrollRejectsUnderGlobalRecoveryHold proves the automatic
// recovery-hold gate (store.Coordinator.Execute's h.Before call) actually
// blocks membership.* mutations during a hold, with zero recovery-specific
// code in internal/control/membership.go itself: membership.* kinds are
// simply absent from recovery.humanRecovery's allowlist. Mirrors
// internal/connection/lifecycle_recovery_test.go's induced-hold pattern
// (a real recovery.Service installed on the coordinator, hold forced via
// clock-rollback detection), not a stubbed Guard.
func TestMembershipEnrollRejectsUnderGlobalRecoveryHold(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	ctx := context.Background()
	wallClock := time.Unix(110, 0)
	if _, err := recovery.New(ctx, recovery.Config{
		Store: db, Markers: memoryMarkersForTest(), Now: func() time.Time { return wallClock },
		FailStop: func() { t.Error("unexpected fail-stop") },
	}); err != nil {
		t.Fatal(err)
	}
	// Force a global recovery hold via clock-rollback detection, the same
	// mechanism internal/recovery/service_test.go and
	// internal/connection/lifecycle_recovery_test.go use.
	wallClock = time.Unix(100, 0)
	if _, err := db.Coordinator().Transition(ctx, func(context.Context, *sql.Tx, store.CommitView) (store.TransitionResult, error) {
		return store.TransitionResult{}, nil
	}, nil); err != store.RecoveryRequired {
		t.Fatalf("rollback did not latch a hold: %v", err)
	}
	resp, _ := sess.Handle(ctx, Request{ID: "1", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != DomainCode(store.RecoveryRequired) {
		t.Fatalf("expected recovery_required under a global hold, got %#v", resp.Err)
	}
}

func TestMembershipRejectsInvalidParamsShape(t *testing.T) {
	sess, _ := membershipTestServer(t)
	resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "conv-1",
		// missing expected_grant_version/members/policy/max_exchanges
	}})
	if resp.Err == nil || resp.Err.Code != InvalidParams {
		t.Fatalf("expected InvalidParams, got %#v", resp.Err)
	}
}
