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

// TestMembershipEnrollResultEchoesExactOperationID is MC-01.A's regression:
// store.CommandReceipt.OperationID must carry the exact operation_id this
// receipt was durably recorded under, distinct from the response's own
// JSON-RPC id (a separate, per-connection correlation number a wire client
// never even sees here -- Session.Handle takes a bare Request, not a
// Client.nextID-assigned envelope).
func TestMembershipEnrollResultEchoesExactOperationID(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	opID := newOpID()
	params := openMembers("peer-a", "peer-b")
	params["operation_id"] = opID
	resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: params})
	if resp.Err != nil {
		t.Fatalf("unexpected rejection: %#v", resp.Err)
	}
	result := decodeResult[CommandReceiptResult](t, resp)
	if result.OperationID != opID {
		t.Fatalf("operation_id=%q, want %q", result.OperationID, opID)
	}
	if !result.Usable() {
		t.Fatalf("a genuine receipt must be Usable: %#v", result)
	}
}

// TestMembershipRenewResultReportsCarriedAndCancelledCounts is MC-01.B's
// regression: RenewTx's SupersedeResult.Carried/Cancelled counts --
// previously discarded entirely -- must reach the wire result as
// queued_carried/queued_cancelled resource changes, so a caller can tell a
// renewal that silently cancelled pending replies from one that carried
// them forward, without re-deriving it from ListQueued itself.
func TestMembershipRenewResultReportsCarriedAndCancelledCounts(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	enroll, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if enroll.Err != nil {
		t.Fatalf("enroll failed: %#v", enroll.Err)
	}
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	original := "80000000-0000-4000-8000-000000000001"
	if err := store.InsertQueued(ctx, tx, store.Envelope{
		ID: original, Conversation: "conv-1", FromPeer: "peer-a", ToPeer: "peer-b",
		Text: "original", GrantVersion: 1, CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetState(ctx, tx, original, store.Queued, store.Acked, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	reply := "80000000-0000-4000-8000-000000000002"
	if err := store.InsertQueued(ctx, tx, store.Envelope{
		ID: reply, Conversation: "conv-1", FromPeer: "peer-b", ToPeer: "peer-a",
		Text: "reply", GrantVersion: 1, InReplyTo: &original, TrustedReply: true,
		CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	ordinary := "80000000-0000-4000-8000-000000000003"
	if err := store.InsertQueued(ctx, tx, store.Envelope{
		ID: ordinary, Conversation: "conv-1", FromPeer: "peer-a", ToPeer: "peer-b",
		Text: "ordinary", GrantVersion: 1, CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	renew, _ := sess.Handle(ctx, Request{ID: "2", Method: "membership.renew", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "conv-1", "expected_grant_version": "1",
	}})
	if renew.Err != nil {
		t.Fatalf("renew failed: %#v", renew.Err)
	}
	result := decodeResult[CommandReceiptResult](t, renew)
	var carried, cancelled string
	for _, r := range result.Result.Resources {
		switch r.Kind {
		case "queued_carried":
			carried = r.After
		case "queued_cancelled":
			cancelled = r.After
		}
	}
	if carried != "1" {
		t.Fatalf("queued_carried=%q, want 1: %#v", carried, result.Result.Resources)
	}
	if cancelled != "1" {
		t.Fatalf("queued_cancelled=%q, want 1: %#v", cancelled, result.Result.Resources)
	}
}

// TestMembershipReplaceResultReportsCarriedAndCancelledCounts is
// TestMembershipRenewResultReportsCarriedAndCancelledCounts's replace-side
// equivalent: replace shares supersede/SupersedeResult with renew, but is
// its own wire handler and must be checked independently rather than
// assumed identical from renew's coverage alone.
func TestMembershipReplaceResultReportsCarriedAndCancelledCounts(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	seedEnabledBinding(t, db, 3, "peer-c")
	enroll, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if enroll.Err != nil {
		t.Fatalf("enroll failed: %#v", enroll.Err)
	}
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := "80000000-0000-4000-8000-000000000004"
	if err := store.InsertQueued(ctx, tx, store.Envelope{
		ID: ordinary, Conversation: "conv-1", FromPeer: "peer-a", ToPeer: "peer-b",
		Text: "ordinary", GrantVersion: 1, CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	replace, _ := sess.Handle(ctx, Request{ID: "2", Method: "membership.replace", Params: map[string]any{
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
	result := decodeResult[CommandReceiptResult](t, replace)
	var cancelled string
	for _, r := range result.Result.Resources {
		if r.Kind == "queued_cancelled" {
			cancelled = r.After
		}
	}
	if cancelled != "1" {
		t.Fatalf("queued_cancelled=%q, want 1: %#v", cancelled, result.Result.Resources)
	}
}

// TestDecodePolicyEnforcesTaggedUnionEdgesPresence is MC-02's decode-side
// regression, complementing cmd/parleyctl's encode-side
// TestPolicyWireOmitsEdgesForOpenAndLeadOnlyButIncludesForDirected: open/
// lead_only must reject an edges key even when present as an empty array
// (decoded-length alone cannot distinguish "omitted" from "sent empty");
// directed must reject its absence.
func TestDecodePolicyEnforcesTaggedUnionEdgesPresence(t *testing.T) {
	cases := []struct {
		name string
		raw  map[string]any
		ok   bool
	}{
		{"open without edges", map[string]any{"kind": "open"}, true},
		{"open with empty edges", map[string]any{"kind": "open", "edges": []any{}}, false},
		{"lead_only with empty edges", map[string]any{"kind": "lead_only", "edges": []any{}}, false},
		{"directed without edges", map[string]any{"kind": "directed"}, false},
		{"directed with empty edges", map[string]any{"kind": "directed", "edges": []any{}}, true},
		{"directed with an edge", map[string]any{"kind": "directed", "edges": []any{map[string]any{"from": "a", "to": "b"}}}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, ok := decodePolicy(c.raw)
			if ok != c.ok {
				t.Fatalf("decodePolicy(%#v) ok=%v, want %v", c.raw, ok, c.ok)
			}
		})
	}
}

// TestParamOptionalExpiresAtValidatesFormAndRange is MC-03's regression:
// only a literal "Z" UTC suffix is accepted (a numeric offset is rejected
// outright, never normalized -- normalizing would change the wire byte
// string store.NewCommandRequest digests depending on which equivalent
// spelling was sent), sub-second precision is preserved through
// RFC3339Nano rather than truncated, and the range is bounded by
// store.InstantNanos, the same bound the coordinator's own authority
// instant must satisfy.
func TestParamOptionalExpiresAtValidatesFormAndRange(t *testing.T) {
	cases := []struct {
		name string
		s    string
		ok   bool
	}{
		{"omitted", "", true}, // handled separately below: no expires_at key at all
		{"UTC Z", "2030-06-15T12:00:00Z", true},
		{"UTC Z with fractional nanoseconds", "2030-06-15T12:00:00.123456789Z", true},
		{"numeric UTC offset rejected, not normalized", "2030-06-15T12:00:00+00:00", false},
		{"non-UTC numeric offset rejected", "2030-06-15T14:00:00+02:00", false},
		{"out of int64-nanosecond range", "3000-01-01T00:00:00Z", false},
		{"not RFC3339 at all", "not-a-timestamp", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			params := map[string]any{}
			if c.name != "omitted" {
				params["expires_at"] = c.s
			}
			parsed, text, ok := paramOptionalExpiresAt(params)
			if ok != c.ok {
				t.Fatalf("paramOptionalExpiresAt(%q) ok=%v, want %v", c.s, ok, c.ok)
			}
			if !ok {
				return
			}
			if c.name == "omitted" {
				if parsed != nil || text != "" {
					t.Fatalf("omitted expires_at must decode to nil/empty, got %v %q", parsed, text)
				}
				return
			}
			if text != c.s {
				t.Fatalf("text=%q, want the wire value preserved verbatim %q", text, c.s)
			}
			if parsed.UTC().Format(time.RFC3339Nano) != c.s {
				t.Fatalf("parsed=%v does not round-trip to %q (precision lost)", parsed, c.s)
			}
		})
	}
}

// TestMembershipEnrollRejectsIncompatibleConversationIdentifier and its
// renew/replace siblings are MC-02's other regression: a conversation
// identifier failing bridgetext.ValidateMetadata must get a deterministic
// IncompatibleIdentifier rejection directly from the wire handler, not
// reach controller.GrantTx/RenewTx/ReplaceTx (which reject it with a plain
// Go error, degrading via domainRejection's fallback to the generic
// TemporarilyUnavailable infrastructure code).
func TestMembershipEnrollRejectsIncompatibleConversationIdentifier(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	params := openMembers("peer-a", "peer-b")
	params["conversation"] = "bad\x7fconversation"
	resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: params})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != IncompatibleIdentifier {
		t.Fatalf("expected incompatible_identifier, got %#v", resp.Err)
	}
}

func TestMembershipRenewRejectsIncompatibleConversationIdentifier(t *testing.T) {
	sess, _ := membershipTestServer(t)
	resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.renew", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "bad\x7fconversation", "expected_grant_version": "1",
	}})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != IncompatibleIdentifier {
		t.Fatalf("expected incompatible_identifier, got %#v", resp.Err)
	}
}

func TestMembershipReplaceRejectsIncompatibleConversationIdentifier(t *testing.T) {
	sess, _ := membershipTestServer(t)
	resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.replace", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "bad\x7fconversation", "expected_grant_version": "1",
		"members": []any{
			map[string]any{"peer_id": "peer-a", "role": "member"},
			map[string]any{"peer_id": "peer-b", "role": "member"},
		},
		"policy": map[string]any{"kind": "open"},
	}})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != IncompatibleIdentifier {
		t.Fatalf("expected incompatible_identifier, got %#v", resp.Err)
	}
}

// TestMembershipRevokeBypassesIncompatibleIdentifierCheck confirms the
// deliberate asymmetry: revoke must still reach controller.RevokeTx for a
// byte-malformed historical conversation identifier (AGENTS.md's exact-key
// human revocation escape), not the new IncompatibleIdentifier check --
// it fails on no_active_grant (a domain rejection reached only past the
// check enroll/renew/replace apply), not incompatible_identifier.
func TestMembershipRevokeBypassesIncompatibleIdentifierCheck(t *testing.T) {
	sess, _ := membershipTestServer(t)
	resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.revoke", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "bad\x7fconversation", "expected_grant_version": "1",
	}})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code == IncompatibleIdentifier {
		t.Fatalf("revoke must not apply the new-enrollment identifier check, got %#v", resp.Err)
	}
}

// TestInvalidMembershipRejectionDoesNotReserveOperationID is the accepted
// audit-boundary consequence from the consolidated report's section 4: an
// invalid_membership rejection runs entirely before store.NewCommandRequest/
// Execute (membership.Validate's own precondition, not a store.Coordinator
// domain rejection), so it creates no operation_results/command_audit row
// and never reserves p.operationID -- a corrected retry reusing the same
// operation_id must therefore be free to execute normally, not fail with
// operation_conflict.
func TestInvalidMembershipRejectionDoesNotReserveOperationID(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	opID := newOpID()
	bad := map[string]any{
		"operation_id": opID, "conversation": "conv-1", "expected_grant_version": "0",
		"members": []any{
			map[string]any{"peer_id": "peer-a", "role": "member"},
			map[string]any{"peer_id": "peer-a", "role": "member"}, // duplicate -> invalid_membership
		},
		"policy": map[string]any{"kind": "open"}, "max_exchanges": "5",
	}
	first, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: bad})
	if first.Err == nil || first.Err.Data == nil || first.Err.Data.Code != DomainCode(store.InvalidMembership) {
		t.Fatalf("expected invalid_membership, got %#v", first.Err)
	}
	corrected := openMembers("peer-a", "peer-b")
	corrected["operation_id"] = opID
	second, _ := sess.Handle(context.Background(), Request{ID: "2", Method: "membership.enroll", Params: corrected})
	if second.Err != nil {
		t.Fatalf("a corrected retry reusing the same operation_id must execute, got %#v", second.Err)
	}
}

// TestUnsupportedMembershipRejectionReservesOperationIDAndConflictsOnRetry
// is the audit-boundary's other half, explicitly for unsupported_membership
// (the source reports' own named case, distinct from
// TestMembershipEnrollConflictingRetrySameOperationIDDifferentPayload's
// open-policy conflicting-conversation case): unsupported_membership is a
// terminal domain rejection recorded through operation_results/
// command_audit like any other, so a retry reusing the same operation_id --
// even with a now-corrected payload -- must durably conflict rather than
// silently succeed.
func TestUnsupportedMembershipRejectionReservesOperationIDAndConflictsOnRetry(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	seedEnabledBinding(t, db, 3, "peer-c")
	opID := newOpID()
	unsupported := map[string]any{
		"operation_id": opID, "conversation": "conv-1", "expected_grant_version": "0",
		"members": []any{
			map[string]any{"peer_id": "peer-a", "role": "member"},
			map[string]any{"peer_id": "peer-b", "role": "member"},
			map[string]any{"peer_id": "peer-c", "role": "member"}, // 3 members + open -> unsupported_membership
		},
		"policy": map[string]any{"kind": "open"}, "max_exchanges": "5",
	}
	first, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: unsupported})
	if first.Err == nil || first.Err.Data == nil || first.Err.Data.Code != DomainCode(store.UnsupportedMembership) {
		t.Fatalf("expected unsupported_membership, got %#v", first.Err)
	}
	corrected := openMembers("peer-a", "peer-b")
	corrected["operation_id"] = opID
	second, _ := sess.Handle(context.Background(), Request{ID: "2", Method: "membership.enroll", Params: corrected})
	if second.Err == nil || second.Err.Data == nil || second.Err.Data.Code != DomainCode(store.OperationConflict) {
		t.Fatalf("a corrected retry of a durably-rejected unsupported_membership operation_id must conflict, got %#v", second.Err)
	}
}
