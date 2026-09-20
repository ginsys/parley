package control

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
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
	// Registered after controlTestDB's own cleanup, so it runs first:
	// background expiry persistence drains before the store closes.
	t.Cleanup(srv.WaitBackground)
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

// seedExpiredBinding mirrors seedEnabledBinding but records a credential
// that already expired in the past (not merely unenabled or missing),
// exercising store.EnabledPeer's expired-but-still-"current" branch that
// calls recordExpiry -- distinct from seedEnabledBinding's always-valid
// fixture and from an absent/revoked binding.
func seedExpiredBinding(t *testing.T, db *store.DB, index int, peer string) {
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
		Status: "current", ExpiresAtNS: time.Now().Add(-time.Hour).UnixNano(), Verifier: sha256.Sum256(secret[:]),
	}
	if err := store.InsertBindingCredential(ctx, tx, b, c); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// revokeBindingDirectly transitions a previously-seeded binding straight to
// 'revoked', bypassing the normal connection.Lifecycle API -- membership
// tests only need the resulting store state, not the lifecycle machinery
// that would ordinarily produce it.
func revokeBindingDirectly(t *testing.T, db *store.DB, index int) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	id := fmt.Sprintf("60000000-0000-4000-8000-%012d", index)
	if _, err := tx.ExecContext(ctx, "UPDATE bindings SET status='revoked' WHERE binding_id=?", id); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// seedBindingExpiringSoon mirrors seedEnabledBinding but with a credential
// expiring shortly after insertion -- expires_at_ns is immutable once
// stored (registry_schema.sql's credential_identity_immutable trigger), so
// a test that needs a binding valid at enroll time and expired by renew
// time must seed this way and let real time elapse, rather than backdating
// an existing row.
func seedBindingExpiringSoon(t *testing.T, db *store.DB, index int, peer string, ttl time.Duration) {
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
		Status: "current", ExpiresAtNS: time.Now().Add(ttl).UnixNano(), Verifier: sha256.Sum256(secret[:]),
	}
	if err := store.InsertBindingCredential(ctx, tx, b, c); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// credentialStatus reads back a credential's durable status, used to prove
// store.ExpiryEvidence.Persist actually ran rather than merely that the
// mutation reported binding_unavailable.
func credentialStatus(t *testing.T, sess *Session, db *store.DB, index int) string {
	t.Helper()
	// Persistence runs off the request path; wait for it before reading.
	sess.server.WaitBackground()
	var status string
	if err := db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		c, err := store.ReadCredential(ctx, tx, fmt.Sprintf("70000000-0000-4000-8000-%012d", index))
		if err != nil {
			return err
		}
		status = c.Status
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return status
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

// Review 5256536448 (comment 4053873889): store.EnabledPeer's expired-
// credential branch calls recordExpiry, which is a no-op unless the
// handler wraps ctx with store.ObserveExpiries and calls Persist afterward.
// Before that wiring, this scenario correctly reported binding_unavailable
// but the credential's DB row stayed "current" forever -- no denial
// evidence was ever durably recorded (AGENTS.md: "Observed expiry is
// denial evidence... persist it independently").
func TestMembershipEnrollPersistsObservedCredentialExpiry(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedExpiredBinding(t, db, 2, "peer-b")
	resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != DomainCode(store.BindingUnavailable) {
		t.Fatalf("expected binding_unavailable, got %#v", resp.Err)
	}
	if status := credentialStatus(t, sess, db, 2); status != "expired" {
		t.Fatalf("expired credential observation was not persisted: status=%s", status)
	}
}

// TestMembershipEnrollPreservesRejectionWhenExpiryPersistFails is round-2
// finding 1 (review 5256660570, comment 4053958828): round-1's own fix
// above wrapped the expiry observation in a defer that overwrote resp with
// a generic domainCode(persistErr) whenever Persist failed -- reproducing,
// at the wire-response layer, the exact anti-pattern EC-02 fixed at the
// coordinator layer (a best-effort background write's failure must never
// overwrite an already-durable outcome). This forces a genuine Persist
// failure with a synthetic trigger (the same pattern
// internal/connection/deadline_review_test.go's
// TestFailedExpiryPersistenceRetainsDenialAcrossClockRollback uses) during
// an enroll whose underlying rejection (binding_unavailable) is already
// durable by the time the deferred Persist call runs, and proves the wire
// response still reports that exact rejection -- never a Persist-failure-
// derived code -- while the credential's stored status is untouched
// (Persist's own UPDATE genuinely failed, it did not silently no-op).
func TestMembershipEnrollPreservesRejectionWhenExpiryPersistFails(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedExpiredBinding(t, db, 2, "peer-b")
	ctx := context.Background()

	if _, err := db.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		_, err := tx.ExecContext(ctx, `CREATE TRIGGER reject_expiry BEFORE UPDATE OF status ON credentials WHEN NEW.status='expired' BEGIN SELECT RAISE(ABORT,'synthetic expiry failure'); END`)
		return store.TransitionResult{Changed: true}, err
	}, nil); err != nil {
		t.Fatal(err)
	}

	resp, _ := sess.Handle(ctx, Request{ID: "1", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != DomainCode(store.BindingUnavailable) {
		t.Fatalf("a Persist failure must not overwrite the genuine domain rejection: %#v", resp.Err)
	}
	if status := credentialStatus(t, sess, db, 2); status != "current" {
		t.Fatalf("Persist's UPDATE should have genuinely failed (trigger), not silently succeeded: status=%s", status)
	}
}

// TestExpiryPersistenceNeverHoldsTheRequestPath is review 5259563170
// (comment 4056153935): store.ExpiryEvidence.Persist waits on the
// coordinator under its own fresh deadline, so running it inline could hold
// a response past RequestDeadline. A first enroll leaves a retained
// observation (its Persist fails on the trigger); the coordinator is then
// held by a real blocked Transition, and persistExpiries must return at once
// while its write stays pending until the coordinator is free again.
func TestExpiryPersistenceNeverHoldsTheRequestPath(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedExpiredBinding(t, db, 2, "peer-b")
	ctx := context.Background()
	if _, err := db.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		_, err := tx.ExecContext(ctx, `CREATE TRIGGER reject_expiry BEFORE UPDATE OF status ON credentials WHEN NEW.status='expired' BEGIN SELECT RAISE(ABORT,'synthetic expiry failure'); END`)
		return store.TransitionResult{Changed: true}, err
	}, nil); err != nil {
		t.Fatal(err)
	}
	resp, _ := sess.Handle(ctx, Request{ID: "1", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != DomainCode(store.BindingUnavailable) {
		t.Fatalf("expected binding_unavailable, got %#v", resp.Err)
	}
	if status := credentialStatus(t, sess, db, 2); status != "current" {
		t.Fatalf("the trigger should have failed the first write: status=%s", status)
	}

	entered, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := db.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
			close(entered)
			<-release
			_, err := tx.ExecContext(ctx, `DROP TRIGGER reject_expiry`)
			return store.TransitionResult{Changed: true}, err
		}, nil)
		held <- err
	}()
	<-entered

	_, expiry := store.ObserveExpiries(ctx, db)
	started := time.Now()
	sess.server.persistExpiries(expiry)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("persistExpiries held its caller for %s while the coordinator was busy", elapsed)
	}
	drained := make(chan struct{})
	go func() {
		sess.server.WaitBackground()
		close(drained)
	}()
	select {
	case <-drained:
		t.Fatal("the expiry write completed while the coordinator was still held")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	<-drained
	if status := credentialStatus(t, sess, db, 2); status != "expired" {
		t.Fatalf("the retained observation was not persisted once the coordinator was free: status=%s", status)
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
	opID, _ := params["operation_id"].(string)
	// A replayed receipt must remain Usable exactly as it was when first
	// produced: Usable validates shape/domain only, never live server state
	// (the grant this replay describes has not changed version since, but
	// Usable must not depend on that -- see its own doc comment).
	if !r2.Usable(opID, "conv-1") {
		t.Fatalf("a replayed receipt must remain Usable: %#v", r2)
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

// TestMembershipEnrollRejectsMatchingVersionWhileActiveWithoutMutating,
// TestMembershipEnrollStaleVersionTakesPriorityOverActiveGrant and
// TestMembershipEnrollRejectedOperationRemainsDurableAcrossRevocationAndConflictsOnChangedRetry
// are the wire-level EC-01 regression matrix (thread PRRT_kwDOUT1JT86j_9VC,
// root 4053366763): GrantTx's restored active-grant precondition
// (internal/controller/controller.go), exercised through the actual
// membership.enroll wire handler so the matrix covers durability/replay
// behavior the unit-level controller tests can't (that layer never sees the
// coordinator's operation-result/audit ledger).
func TestMembershipEnrollRejectsMatchingVersionWhileActiveWithoutMutating(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	first := openMembers("peer-a", "peer-b")
	enroll, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: first})
	if enroll.Err != nil {
		t.Fatalf("fresh enrollment rejected: %#v", enroll.Err)
	}

	// A second enrollment attempt with a fresh operation ID but the same,
	// still-current expected_grant_version must be rejected as AlreadyActive
	// -- not silently accepted, and not the idx_grants_one_active storage
	// error the pre-fix regression degraded to.
	second := openMembers("peer-a", "peer-b")
	second["expected_grant_version"] = "1" // matches the latest historical version, which is active
	resp, _ := sess.Handle(context.Background(), Request{ID: "2", Method: "membership.enroll", Params: second})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != DomainCode(store.AlreadyActive) {
		t.Fatalf("expected already_active for a matching-version enrollment while active, got %#v", resp.Err)
	}

	var count int
	if err := db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM grants WHERE conversation='conv-1'").Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("a rejected enrollment must not create a new grant version: found %d rows", count)
	}

	// The original successful operation ID must still replay while its
	// grant remains active.
	replay, _ := sess.Handle(context.Background(), Request{ID: "3", Method: "membership.enroll", Params: first})
	if replay.Err != nil {
		t.Fatalf("replay of the original successful enrollment must still succeed while active: %#v", replay.Err)
	}
	r1 := decodeResult[CommandReceiptResult](t, enroll)
	r3 := decodeResult[CommandReceiptResult](t, replay)
	if r1.AuditID != r3.AuditID {
		t.Fatalf("replay of the original enrollment produced a different audit record: %s vs %s", r1.AuditID, r3.AuditID)
	}
}

func TestMembershipEnrollStaleVersionTakesPriorityOverActiveGrant(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	enroll, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if enroll.Err != nil {
		t.Fatalf("fresh enrollment rejected: %#v", enroll.Err)
	}

	// A stale (non-matching) expected_grant_version must report
	// StaleGrantVersion, never AlreadyActive -- GrantTx checks
	// ExpectedVersion before CurrentGrant regardless of whether a grant
	// happens to be active (see the EC-01 doc comment in controller.go).
	stale := openMembers("peer-a", "peer-b")
	stale["expected_grant_version"] = "99"
	resp, _ := sess.Handle(context.Background(), Request{ID: "2", Method: "membership.enroll", Params: stale})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != DomainCode(store.StaleGrantVersion) {
		t.Fatalf("expected stale_grant_version to take priority over already_active, got %#v", resp.Err)
	}
}

func TestMembershipEnrollRejectedOperationRemainsDurableAcrossRevocationAndConflictsOnChangedRetry(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	enroll, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if enroll.Err != nil {
		t.Fatalf("fresh enrollment rejected: %#v", enroll.Err)
	}

	rejected := openMembers("peer-a", "peer-b")
	rejected["expected_grant_version"] = "1" // matches the latest historical version, which is active
	rejectedOpID, _ := rejected["operation_id"].(string)
	first, _ := sess.Handle(context.Background(), Request{ID: "2", Method: "membership.enroll", Params: rejected})
	if first.Err == nil || first.Err.Data == nil || first.Err.Data.Code != DomainCode(store.AlreadyActive) {
		t.Fatalf("expected already_active, got %#v", first.Err)
	}
	rec1, err := db.Queries().OperationRecord(context.Background(), testHelloAdmin, rejectedOpID)
	if err != nil {
		t.Fatalf("rejected enrollment must still be durably recorded: %v", err)
	}

	// Changed-input reuse of the same rejected operation ID must conflict,
	// not silently re-execute against the (unchanged) current state.
	changed := openMembers("peer-a", "peer-b")
	changed["operation_id"] = rejectedOpID
	changed["max_exchanges"] = "9"
	conflict, _ := sess.Handle(context.Background(), Request{ID: "3", Method: "membership.enroll", Params: changed})
	if conflict.Err == nil || conflict.Err.Data == nil || conflict.Err.Data.Code != DomainCode(store.OperationConflict) {
		t.Fatalf("changed-input retry of a rejected operation id must conflict, got %#v", conflict.Err)
	}

	// Revoke the active grant, then replay the exact same rejected payload
	// under the same operation ID: this must still return the original
	// AlreadyActive rejection (durable, digest-keyed), never re-execute now
	// that an active grant no longer exists -- the entire point of the
	// original defect being that this same operation ID could silently
	// execute as new work after an unrelated revocation.
	revoke, _ := sess.Handle(context.Background(), Request{ID: "4", Method: "membership.revoke", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "conv-1", "expected_grant_version": "1",
	}})
	if revoke.Err != nil {
		t.Fatalf("revoke failed: %#v", revoke.Err)
	}
	replay, _ := sess.Handle(context.Background(), Request{ID: "5", Method: "membership.enroll", Params: rejected})
	if replay.Err == nil || replay.Err.Data == nil || replay.Err.Data.Code != DomainCode(store.AlreadyActive) {
		t.Fatalf("replay of a rejected operation after an unrelated revocation must return its original rejection, got %#v", replay.Err)
	}
	rec2, err := db.Queries().OperationRecord(context.Background(), testHelloAdmin, rejectedOpID)
	if err != nil {
		t.Fatal(err)
	}
	if rec1.AuditSequence != rec2.AuditSequence || rec1.AuditID != rec2.AuditID {
		t.Fatalf("replay after revocation must not create a second audit record: %+v vs %+v", rec1, rec2)
	}
	var count int
	if err := db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM grants WHERE conversation='conv-1'").Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("the durable replay must not create a new grant version: found %d rows", count)
	}

	// A genuinely new operation ID may re-enroll after the revocation.
	fresh := openMembers("peer-a", "peer-b")
	fresh["expected_grant_version"] = "1" // latest historical version is still 1 after revoke
	reEnroll, _ := sess.Handle(context.Background(), Request{ID: "6", Method: "membership.enroll", Params: fresh})
	if reEnroll.Err != nil {
		t.Fatalf("a genuinely new operation id must be able to re-enroll after revocation: %#v", reEnroll.Err)
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

// Review 5256536448 (comment 4053873898): RenewTx previously superseded a
// grant without rechecking either current peer's binding, so a peer
// revoked after enrollment could still have its grant renewed -- the
// rejection only surfaced later, at dispatch time, after renewal had
// already been durably reported successful.
func TestMembershipRenewRejectsWhenCurrentPeerBindingRevokedSinceEnrollment(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	enroll, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if enroll.Err != nil {
		t.Fatalf("enroll failed: %#v", enroll.Err)
	}
	revokeBindingDirectly(t, db, 2)
	renew, _ := sess.Handle(context.Background(), Request{ID: "2", Method: "membership.renew", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "conv-1", "expected_grant_version": "1",
	}})
	if renew.Err == nil || renew.Err.Data == nil || renew.Err.Data.Code != DomainCode(store.BindingUnavailable) {
		t.Fatalf("expected binding_unavailable, got %#v", renew.Err)
	}
}

// Companion to TestMembershipEnrollPersistsObservedCredentialExpiry: proves
// the same observe-and-persist wiring on the renew path, where the expired
// peer is resolved from the current grant's stored peers, not from the
// caller's request.
func TestMembershipRenewPersistsObservedCredentialExpiry(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedBindingExpiringSoon(t, db, 2, "peer-b", 50*time.Millisecond)
	enroll, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if enroll.Err != nil {
		t.Fatalf("enroll failed: %#v", enroll.Err)
	}
	time.Sleep(150 * time.Millisecond)
	renew, _ := sess.Handle(context.Background(), Request{ID: "2", Method: "membership.renew", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "conv-1", "expected_grant_version": "1",
	}})
	if renew.Err == nil || renew.Err.Data == nil || renew.Err.Data.Code != DomainCode(store.BindingUnavailable) {
		t.Fatalf("expected binding_unavailable, got %#v", renew.Err)
	}
	if status := credentialStatus(t, sess, db, 2); status != "expired" {
		t.Fatalf("expired credential observation was not persisted: status=%s", status)
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

// Companion to TestMembershipEnrollPersistsObservedCredentialExpiry on the
// replace path.
func TestMembershipReplacePersistsObservedCredentialExpiry(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	seedExpiredBinding(t, db, 3, "peer-c")
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
	if replace.Err == nil || replace.Err.Data == nil || replace.Err.Data.Code != DomainCode(store.BindingUnavailable) {
		t.Fatalf("expected binding_unavailable, got %#v", replace.Err)
	}
	if status := credentialStatus(t, sess, db, 3); status != "expired" {
		t.Fatalf("expired credential observation was not persisted: status=%s", status)
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
	if !result.Usable(opID, "conv-1") {
		t.Fatalf("a genuine receipt must be Usable: %#v", result)
	}
	if result.Usable("some-other-operation-id", "conv-1") {
		t.Fatalf("a receipt for a different operation_id must not be Usable: %#v", result)
	}
}

// TestCommandReceiptResultUsableValidatesActualReceiptContents is MC-01's
// second residual: checking only that AuditID/OperationID/CommitView's
// fields were nonempty strings and Result.Code was "" (the version
// TestMembershipEnrollResultEchoesExactOperationID's fix left behind) let a
// missing or `null` "result" member -- which decodes into a zero-valued
// wireCommandResult, not an error, since an absent/null field is a no-op
// for encoding/json's struct decode -- and placeholder metadata like
// "audit-1"/"epoch-1"/"not-a-number" pass despite describing no actual
// mutation. Each case here starts from a genuinely valid receipt (the same
// shape a real successful membership.enroll produces) and corrupts exactly
// one documented field/domain rule, proving Usable rejects that specific
// defect and nothing else -- with the unmodified receipt itself as the
// positive control.
func TestCommandReceiptResultUsableValidatesActualReceiptContents(t *testing.T) {
	const opID = "80000000-0000-4000-8000-000000000099"
	valid := func() CommandReceiptResult {
		return CommandReceiptResult{
			AuditID:     "60000000-0000-4000-8000-000000000001",
			OperationID: opID,
			CommitView:  CommitView{Epoch: "70000000-0000-4000-8000-000000000001", Revision: "1"},
			Result: wireCommandResult{
				Resources: []wireResourceChange{{Kind: "grant", ID: "conv-1", Before: "0", After: "1"}},
			},
		}
	}
	tests := []struct {
		name string
		want bool
		make func() CommandReceiptResult
	}{
		{"valid receipt is the positive control", true, valid},
		{"missing result (zero-valued wireCommandResult)", false, func() CommandReceiptResult {
			r := valid()
			r.Result = wireCommandResult{}
			return r
		}},
		{"result present but resources empty", false, func() CommandReceiptResult {
			r := valid()
			r.Result.Resources = nil
			return r
		}},
		{"non-UUID audit_id placeholder", false, func() CommandReceiptResult {
			r := valid()
			r.AuditID = "audit-1"
			return r
		}},
		{"non-UUID epoch placeholder", false, func() CommandReceiptResult {
			r := valid()
			r.CommitView.Epoch = "epoch-1"
			return r
		}},
		{"non-decimal revision", false, func() CommandReceiptResult {
			r := valid()
			r.CommitView.Revision = "not-a-number"
			return r
		}},
		{"empty revision", false, func() CommandReceiptResult {
			r := valid()
			r.CommitView.Revision = ""
			return r
		}},
		{"resource kind empty", false, func() CommandReceiptResult {
			r := valid()
			r.Result.Resources[0].Kind = ""
			return r
		}},
		{"resource id empty", false, func() CommandReceiptResult {
			r := valid()
			r.Result.Resources[0].ID = ""
			return r
		}},
		{"resource before not decimal", false, func() CommandReceiptResult {
			r := valid()
			r.Result.Resources[0].Before = "one"
			return r
		}},
		{"resource after not decimal", false, func() CommandReceiptResult {
			r := valid()
			r.Result.Resources[0].After = "01"
			return r
		}},
		{"nonempty result.code", false, func() CommandReceiptResult {
			r := valid()
			r.Result.Code = store.NoActiveGrant
			return r
		}},
		{"mismatched operation_id", false, func() CommandReceiptResult {
			r := valid()
			r.OperationID = "80000000-0000-4000-8000-000000000001"
			return r
		}},
		{"non-UUID operation_id even if it matches the request string", false, func() CommandReceiptResult {
			r := valid()
			r.OperationID = "not-a-uuid"
			return r
		}},
		{"resource id empty", false, func() CommandReceiptResult {
			r := valid()
			r.Result.Resources[0].ID = ""
			return r
		}},
		{"resource id present but does not match the requested conversation", false, func() CommandReceiptResult {
			r := valid()
			r.Result.Resources[0].ID = "conv-2"
			return r
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.make()
			requested := opID
			if tt.name == "non-UUID operation_id even if it matches the request string" {
				requested = "not-a-uuid"
			}
			if got := r.Usable(requested, "conv-1"); got != tt.want {
				t.Fatalf("Usable()=%v, want %v: %#v", got, tt.want, r)
			}
		})
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

// TestMembershipRenewResultReportsDispatchingAndHandedOffCounts is MC-04's
// residual on top of the carried/cancelled coverage above: the wire
// response's queued_already_dispatching/queued_already_handed_off resource
// kinds had no dedicated nonzero-count test -- a regression that stopped
// reporting them, or reported the wrong value, would not have been caught by
// carried/cancelled-only coverage. It also exercises the property documented
// on controller.supersede: an in-flight envelope's dispatching/handed-off
// state is counted conversation-wide, not scoped to the grant version it was
// claimed under -- work retained from grant version 1 must still be counted
// after a SECOND renewal (to version 3), even though that envelope's own
// grant_version (1) is by then two versions behind current.
func TestMembershipRenewResultReportsDispatchingAndHandedOffCounts(t *testing.T) {
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
	dispatching := "80000000-0000-4000-8000-000000000011"
	if err := store.InsertQueued(ctx, tx, store.Envelope{
		ID: dispatching, Conversation: "conv-1", FromPeer: "peer-a", ToPeer: "peer-b",
		Text: "dispatching", GrantVersion: 1, CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetState(ctx, tx, dispatching, store.Queued, store.Dispatching, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	handedOff := "80000000-0000-4000-8000-000000000012"
	if err := store.InsertQueued(ctx, tx, store.Envelope{
		ID: handedOff, Conversation: "conv-1", FromPeer: "peer-a", ToPeer: "peer-b",
		Text: "handed-off", GrantVersion: 1, CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetState(ctx, tx, handedOff, store.Queued, store.Dispatching, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetState(ctx, tx, handedOff, store.Dispatching, store.HandedOff, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	counts := func(resp Response) (dispatchingCount, handedOffCount string) {
		result := decodeResult[CommandReceiptResult](t, resp)
		for _, r := range result.Result.Resources {
			switch r.Kind {
			case "queued_already_dispatching":
				dispatchingCount = r.After
			case "queued_already_handed_off":
				handedOffCount = r.After
			}
		}
		return
	}

	renew1, _ := sess.Handle(ctx, Request{ID: "2", Method: "membership.renew", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "conv-1", "expected_grant_version": "1",
	}})
	if renew1.Err != nil {
		t.Fatalf("first renew failed: %#v", renew1.Err)
	}
	if d, h := counts(renew1); d != "1" || h != "1" {
		t.Fatalf("after first renew: queued_already_dispatching=%q queued_already_handed_off=%q, want 1/1", d, h)
	}

	renew2, _ := sess.Handle(ctx, Request{ID: "3", Method: "membership.renew", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "conv-1", "expected_grant_version": "2",
	}})
	if renew2.Err != nil {
		t.Fatalf("second renew failed: %#v", renew2.Err)
	}
	if d, h := counts(renew2); d != "1" || h != "1" {
		t.Fatalf("after second renew: work claimed under grant version 1 must still be counted (conversation-wide, not per-version): queued_already_dispatching=%q queued_already_handed_off=%q, want 1/1", d, h)
	}
}

// TestMembershipReplaceResultReportsCarriedAndCancelledCounts is
// TestMembershipRenewResultReportsCarriedAndCancelledCounts's replace-side
// equivalent: replace shares supersede/SupersedeResult with renew, but is
// its own wire handler and must be checked independently rather than
// assumed identical from renew's coverage alone. Unlike an earlier version
// of this test (a hosted review finding), it seeds an actual carryable
// trusted reply -- not just an ordinary queued message -- and asserts
// queued_carried explicitly: removing carried-count reporting from
// replace's response entirely would not have failed the earlier version.
// peer-a/peer-b are kept as members under an "open" policy (bidirectional
// direction) in the replacement, matching the sibling renew test: the reply
// flows peer-b -> peer-a, which CarryForwardQueuedReplies' own direction
// re-check only carries under 'bidirectional' or 'b_to_a' -- a directed
// a->b-only policy would make the reply itself ineligible and turn this into
// a test of rejection, not of carry-forward reporting.
func TestMembershipReplaceResultReportsCarriedAndCancelledCounts(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	enroll, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "conv-1", "expected_grant_version": "0",
		"members": []any{
			map[string]any{"peer_id": "peer-a", "role": "member"},
			map[string]any{"peer_id": "peer-b", "role": "member"},
		},
		"policy":        map[string]any{"kind": "open"},
		"max_exchanges": "5",
	}})
	if enroll.Err != nil {
		t.Fatalf("enroll failed: %#v", enroll.Err)
	}
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	original := "80000000-0000-4000-8000-000000000005"
	if err := store.InsertQueued(ctx, tx, store.Envelope{
		ID: original, Conversation: "conv-1", FromPeer: "peer-a", ToPeer: "peer-b",
		Text: "original", GrantVersion: 1, CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetState(ctx, tx, original, store.Queued, store.Acked, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	reply := "80000000-0000-4000-8000-000000000006"
	if err := store.InsertQueued(ctx, tx, store.Envelope{
		ID: reply, Conversation: "conv-1", FromPeer: "peer-b", ToPeer: "peer-a",
		Text: "reply", GrantVersion: 1, InReplyTo: &original, TrustedReply: true,
		CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z",
	}); err != nil {
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
		// Kept at exactly the two members enroll used (membership.Supported
		// rejects a >2-member shape outright, e.g. as unsupported_membership
		// -- irrelevant to what this test checks, but a >2-member
		// replacement here would fail the call entirely for that unrelated
		// reason before ever reaching supersede). peer-c stays enrolled as
		// an unused binding.
		"members": []any{
			map[string]any{"peer_id": "peer-a", "role": "member"},
			map[string]any{"peer_id": "peer-b", "role": "member"},
		},
		"policy": map[string]any{"kind": "open"},
	}})
	if replace.Err != nil {
		t.Fatalf("replace failed: %#v", replace.Err)
	}
	result := decodeResult[CommandReceiptResult](t, replace)
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
// instant must satisfy. The comma-fraction/overlong-fraction/single-digit-
// hour cases are expiresAtGrammar's own regressions (F3): a bare
// strings.HasSuffix(s, "Z") check let all three slip through to
// time.Parse(time.RFC3339Nano, ...), which either truncated a >9-digit
// fraction silently instead of rejecting it, or (for the comma/single-digit
// cases) simply failed to parse -- but as a parse failure indistinguishable
// from "not a timestamp at all", not a named lexical-grammar rejection.
// ".750Z" is the named case that must keep working: a valid trailing-zero
// fractional-second spelling, not a malformed one.
func TestParamOptionalExpiresAtValidatesFormAndRange(t *testing.T) {
	cases := []struct {
		name string
		s    string
		ok   bool
	}{
		{"omitted", "", true}, // handled separately below: no expires_at key at all
		{"UTC Z", "2030-06-15T12:00:00Z", true},
		{"UTC Z with fractional nanoseconds", "2030-06-15T12:00:00.123456789Z", true},
		{"UTC Z with valid trailing-zero fraction", "2030-06-15T12:00:00.750Z", true},
		{"numeric UTC offset rejected, not normalized", "2030-06-15T12:00:00+00:00", false},
		{"non-UTC numeric offset rejected", "2030-06-15T14:00:00+02:00", false},
		{"out of int64-nanosecond range", "3000-01-01T00:00:00Z", false},
		{"not RFC3339 at all", "not-a-timestamp", false},
		{"comma fraction separator rejected", "2030-06-15T12:00:00,750Z", false},
		{"more than 9 fraction digits rejected", "2030-06-15T12:00:00.1234567890Z", false},
		{"single-digit hour rejected", "2030-06-15T2:00:00Z", false},
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
			// Compare against a fresh reference parse rather than
			// re-Format()ing parsed and string-matching c.s: Go's
			// time.Format trims trailing zero fraction digits (".750"
			// formats back as ".75"), which would wrongly fail a valid
			// trailing-zero spelling like ".750Z" even though no precision
			// was actually lost. text's separate verbatim check above is
			// what proves the wire byte string itself survives unchanged.
			want, err := time.Parse(time.RFC3339Nano, c.s)
			if err != nil {
				t.Fatalf("test fixture %q must itself be valid RFC3339Nano: %v", c.s, err)
			}
			if !parsed.Equal(want) {
				t.Fatalf("parsed=%v, want %v (precision lost)", parsed, want)
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

// TestIncompatibleIdentifierRejectionDoesNotReserveOperationID is the
// audit-boundary's third case, for MC-02/C1's own control-layer
// pre-check (handleMembershipEnroll's bridgetext.ValidateMetadata(p.
// conversation), returning IncompatibleIdentifier directly): like
// invalid_membership and unlike unsupported_membership, this check runs
// entirely before store.NewCommandRequest/Execute, so it reserves no
// operation_id and creates no operation_results/command_audit row. A
// corrected retry reusing the same operation_id must therefore execute
// normally rather than durably conflict -- proving this pre-check is
// genuinely safe to retry, not silently masking a different failure mode
// that would need its own audit trail.
func TestIncompatibleIdentifierRejectionDoesNotReserveOperationID(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	opID := newOpID()
	bad := openMembers("peer-a", "peer-b")
	bad["operation_id"] = opID
	bad["conversation"] = "bad\x7fconversation"
	first, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: bad})
	if first.Err == nil || first.Err.Data == nil || first.Err.Data.Code != IncompatibleIdentifier {
		t.Fatalf("expected incompatible_identifier, got %#v", first.Err)
	}
	corrected := openMembers("peer-a", "peer-b")
	corrected["operation_id"] = opID
	second, _ := sess.Handle(context.Background(), Request{ID: "2", Method: "membership.enroll", Params: corrected})
	if second.Err != nil {
		t.Fatalf("a corrected retry reusing the same operation_id must execute, got %#v", second.Err)
	}
}

// TestMembershipEnrollRejectsOversizedConversationDeterministically and
// TestMembershipEnrollAcceptsConversationAtTheMaximumLength are review
// 5255666571's issuecomment-5741742001 length finding: a conversation
// identifier's byte SHAPE was checked before Execute, but its LENGTH was
// not, letting a valid-ASCII, oversized identifier reach
// controller.GrantTx/store.EnabledPeer and only then be rejected by
// store.Coordinator.Execute's own generic per-resource length check
// (store.MaxIdentityBytes) -- correct in outcome, but as a wasted mutation
// attempt reported as a bare invalid_request rather than this package's
// own explicit, retryable incompatible_identifier. incompatibleConversation
// now checks length too, at the exact store.MaxIdentityBytes boundary.
func TestMembershipEnrollRejectsOversizedConversationDeterministically(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	params := openMembers("peer-a", "peer-b")
	params["conversation"] = strings.Repeat("x", store.MaxIdentityBytes+1)
	resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: params})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != IncompatibleIdentifier {
		t.Fatalf("expected incompatible_identifier for a 257-byte conversation, got %#v", resp.Err)
	}
	// No business mutation: no conversation or grant row exists.
	tx, err := db.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, table := range []string{"conversations", "grants"} {
		var count int
		if err := tx.QueryRowContext(context.Background(), "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 0 {
			t.Errorf("%s rows=%d: %v", table, count, err)
		}
	}
}

func TestMembershipEnrollAcceptsConversationAtTheMaximumLength(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-b")
	params := openMembers("peer-a", "peer-b")
	params["conversation"] = strings.Repeat("x", store.MaxIdentityBytes)
	resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.enroll", Params: params})
	if resp.Err != nil {
		t.Fatalf("expected a 256-byte conversation to be accepted, got %#v", resp.Err)
	}
}

// TestMembershipRenewAndReplaceRejectOversizedConversationDeterministically
// is the same length boundary for renew/replace's own top-level
// conversation pre-check, distinct from the enroll coverage above.
func TestMembershipRenewAndReplaceRejectOversizedConversationDeterministically(t *testing.T) {
	oversized := strings.Repeat("x", store.MaxIdentityBytes+1)
	t.Run("renew", func(t *testing.T) {
		sess, _ := membershipTestServer(t)
		resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.renew", Params: map[string]any{
			"operation_id": newOpID(), "conversation": oversized, "expected_grant_version": "1",
		}})
		if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != IncompatibleIdentifier {
			t.Fatalf("expected incompatible_identifier, got %#v", resp.Err)
		}
	})
	t.Run("replace", func(t *testing.T) {
		sess, _ := membershipTestServer(t)
		resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.replace", Params: map[string]any{
			"operation_id": newOpID(), "conversation": oversized, "expected_grant_version": "1",
			"members": []any{
				map[string]any{"peer_id": "peer-a", "role": "member"},
				map[string]any{"peer_id": "peer-b", "role": "member"},
			},
			"policy": map[string]any{"kind": "open"},
		}})
		if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != IncompatibleIdentifier {
			t.Fatalf("expected incompatible_identifier, got %#v", resp.Err)
		}
	})
}

// TestMembershipReplaceRejectsIncompatibleCurrentPeersWithoutMutating is
// review 5255666571's finding (root 4053127303, thread
// PRRT_kwDOUT1JT86j_Wm8): RenewTx already validates a conversation's
// current, already-stored peer IDs before superseding it
// (internal/controller/controller.go's RenewTx); ReplaceTx validated only
// the caller's NEW replacement peers, never the grant it was about to
// supersede's own stored peers -- letting a byte-malformed legacy pair
// (recorded before today's ASCII-compatibility rule existed) be silently
// superseded by an unrelated, otherwise-valid replacement rather than
// durably rejected the way a renewal of the same legacy grant already is.
// Historical bytes must never silently become new authorization
// (docs/specifications/membership.md, AGENTS.md's exact-key boundary).
// Seeding mirrors TestRenewRejectsLegacyUnsafePeersWithoutChangingHistory
// (internal/controller/controller_test.go), at the wire layer so the
// durable-audit consequence (unlike the top-level pre-Execute conversation
// check covered above) is directly observable.
func TestMembershipReplaceRejectsIncompatibleCurrentPeersWithoutMutating(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-c")
	seedEnabledBinding(t, db, 3, "peer-b")
	ctx := context.Background()

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureConversation(ctx, tx, "conv-legacy", "conv-legacy", "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	// A pre-existing grant recorded with a legacy, byte-malformed peer --
	// never reachable through GrantTx today, but a real historical
	// condition RenewTx/ReplaceTx must both still cope with.
	if err := store.InsertGrant(ctx, tx, store.Grant{
		Conversation: "conv-legacy", GrantVersion: 1, PeerAID: "legacy\x7fpeer", PeerBID: "peer-a",
		Direction: store.Bidirectional, MaxExchanges: 2, GrantedAt: "2026-01-01T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	replaceLegacy := func(operationID, otherPeer string) Response {
		resp, _ := sess.Handle(ctx, Request{ID: "1", Method: "membership.replace", Params: map[string]any{
			"operation_id": operationID, "conversation": "conv-legacy", "expected_grant_version": "1",
			"members": []any{
				map[string]any{"peer_id": "peer-a", "role": "member"},
				map[string]any{"peer_id": otherPeer, "role": "member"},
			},
			"policy": map[string]any{"kind": "open"},
		}})
		return resp
	}

	opID := newOpID()
	first := replaceLegacy(opID, "peer-c")
	if first.Err == nil || first.Err.Data == nil || first.Err.Data.Code != IncompatibleIdentifier {
		t.Fatalf("expected incompatible_identifier for a legacy current peer, got %#v", first.Err)
	}

	// No business mutation: the legacy grant is completely unchanged.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	g, err := store.CurrentGrant(ctx, tx, "conv-legacy")
	if err != nil || g.GrantVersion != 1 || g.PeerAID != "legacy\x7fpeer" || g.PeerBID != "peer-a" {
		t.Errorf("legacy grant changed by a rejected replace: %+v: %v", g, err)
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM grants WHERE conversation='conv-legacy'").Scan(&count); err != nil || count != 1 {
		t.Errorf("history rows=%d: %v", count, err)
	}
	tx.Rollback()

	// Durable incompatibility result + same-operation-id replay: unlike
	// the top-level pre-Execute conversation check, this rejection is
	// reached inside ReplaceTx's own mutate callback and durably recorded
	// (validatePeerIDs returns store.IncompatibleIdentifier, a
	// terminalResult code, through domainRejection -- exactly RenewTx's
	// existing behavior for the same check against stored peers). A retry
	// with the identical operation_id and payload must replay the same
	// recorded rejection, not re-run ReplaceTx.
	replay := replaceLegacy(opID, "peer-c")
	if replay.Err == nil || replay.Err.Data == nil || replay.Err.Data.Code != IncompatibleIdentifier {
		t.Fatalf("expected a replayed incompatible_identifier, got %#v", replay.Err)
	}

	// Changed-input conflict: the same operation_id with a different
	// payload must conflict against the durably-recorded rejection, like
	// any other terminal result -- not silently re-evaluate.
	conflict := replaceLegacy(opID, "peer-different")
	if conflict.Err == nil || conflict.Err.Data == nil || conflict.Err.Data.Code != DomainCode(store.OperationConflict) {
		t.Fatalf("expected operation_conflict for a changed retry, got %#v", conflict.Err)
	}

	// Healthy replacement control: an unaffected conversation's replace
	// still succeeds with the new current-peer check active.
	enroll, _ := sess.Handle(ctx, Request{ID: "2", Method: "membership.enroll", Params: openMembers("peer-a", "peer-b")})
	if enroll.Err != nil {
		t.Fatalf("control enroll failed: %#v", enroll.Err)
	}
	control, _ := sess.Handle(ctx, Request{ID: "3", Method: "membership.replace", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "conv-1", "expected_grant_version": "1",
		"members": []any{
			map[string]any{"peer_id": "peer-a", "role": "member"},
			map[string]any{"peer_id": "peer-c", "role": "member"},
		},
		"policy": map[string]any{"kind": "open"},
	}})
	if control.Err != nil {
		t.Fatalf("healthy replacement control failed: %#v", control.Err)
	}

	// Revoke still reaches a grant whose stored peers are incompatible:
	// RevokeTx never validates peers, only the conversation is checked.
	revoke, _ := sess.Handle(ctx, Request{ID: "4", Method: "membership.revoke", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "conv-legacy", "expected_grant_version": "1",
	}})
	if revoke.Err != nil {
		t.Fatalf("legacy grant must remain revocable: %#v", revoke.Err)
	}
}

// TestMembershipReplaceRejectsOversizedButByteValidCurrentPeer is round-2
// finding 2 (review 5256660570, comment 4053958833):
// TestMembershipReplaceRejectsIncompatibleCurrentPeersWithoutMutating above
// uses a control-character peer, which bridgetext.ValidateMetadata alone
// already rejected before this session's fix -- it never exercised the new
// len(id) > store.MaxIdentityBytes bound controller.validatePeerIDs gained.
// This uses a peer that is valid printable ASCII (passes
// bridgetext.ValidateMetadata) but longer than MaxIdentityBytes, so it can
// never have a real registry binding, and proves Replace's supersede of the
// OLD peers -- which never calls store.EnabledPeer, only the NEW peers do,
// in this handler -- still rejects it via validatePeerIDs's own bound
// rather than superseding the historical grant outright.
func TestMembershipReplaceRejectsOversizedButByteValidCurrentPeer(t *testing.T) {
	sess, db := membershipTestServer(t)
	seedEnabledBinding(t, db, 1, "peer-a")
	seedEnabledBinding(t, db, 2, "peer-c")
	ctx := context.Background()
	oversizedPeer := strings.Repeat("x", store.MaxIdentityBytes+1)

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureConversation(ctx, tx, "conv-oversized", "conv-oversized", "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertGrant(ctx, tx, store.Grant{
		Conversation: "conv-oversized", GrantVersion: 1, PeerAID: oversizedPeer, PeerBID: "peer-a",
		Direction: store.Bidirectional, MaxExchanges: 2, GrantedAt: "2026-01-01T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	resp, _ := sess.Handle(ctx, Request{ID: "1", Method: "membership.replace", Params: map[string]any{
		"operation_id": newOpID(), "conversation": "conv-oversized", "expected_grant_version": "1",
		"members": []any{
			map[string]any{"peer_id": "peer-a", "role": "member"},
			map[string]any{"peer_id": "peer-c", "role": "member"},
		},
		"policy": map[string]any{"kind": "open"},
	}})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != IncompatibleIdentifier {
		t.Fatalf("expected incompatible_identifier for an oversized-but-byte-valid current peer, got %#v", resp.Err)
	}

	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if g, err := store.CurrentGrant(ctx, tx, "conv-oversized"); err != nil || g.GrantVersion != 1 || g.PeerAID != oversizedPeer {
		t.Errorf("the oversized historical grant must remain unsuperseded: %+v: %v", g, err)
	}
}

// seedGrantDirectly inserts a conversation and an active grant through the
// store, never through GrantTx, so a test can stage a row no wire call can
// produce.
func seedGrantDirectly(t *testing.T, db *store.DB, conversation string) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureConversation(ctx, tx, conversation, conversation, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertGrant(ctx, tx, store.Grant{
		Conversation: conversation, GrantVersion: 1, PeerAID: "peer-a", PeerBID: "peer-b",
		Direction: store.Bidirectional, MaxExchanges: 2, GrantedAt: "2026-01-01T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// TestMembershipRevokeRejectsIncompatibleConversationIdentifierWithoutMutating
// is the owner decision of 2026-09-20: Parley is unreleased, no database
// predating the identifier rule exists, so membership.revoke applies the
// same conversation check as enroll/renew/replace and carries no exact-key
// escape. Each case stages a grant directly under an identifier no wire
// call can produce and proves revoke refuses it before Execute: the grant
// is untouched and the operation_id is not reserved.
func TestMembershipRevokeRejectsIncompatibleConversationIdentifierWithoutMutating(t *testing.T) {
	cases := map[string]string{
		"empty":                  "",
		"control character":      "bad\x7fconversation",
		"valid non-ASCII UTF-8":  "café-conversation",
		"past MaxIdentityBytes":  strings.Repeat("x", store.MaxIdentityBytes+1),
		"space only":             "   ",
		"raw NUL inside the key": "conv\x00ersation",
	}
	for name, conversation := range cases {
		t.Run(name, func(t *testing.T) {
			sess, db := membershipTestServer(t)
			seedGrantDirectly(t, db, conversation)
			opID := newOpID()
			first, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.revoke", Params: map[string]any{
				"operation_id": opID, "conversation": conversation, "expected_grant_version": "1",
			}})
			if first.Err == nil || first.Err.Data == nil || first.Err.Data.Code != IncompatibleIdentifier {
				t.Fatalf("expected incompatible_identifier, got %#v", first.Err)
			}
			// Rolled back immediately, not deferred: db.Begin is an immediate
			// writer transaction, so leaving it open would deadlock the
			// sess.Handle call below against Coordinator.Execute's own Begin.
			tx, err := db.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			g, err := store.CurrentGrant(context.Background(), tx, conversation)
			tx.Rollback()
			if err != nil || g.GrantVersion != 1 {
				t.Errorf("a pre-Execute refusal must leave the grant untouched: %+v: %v", g, err)
			}
			// Not durably audited: a corrected retry reusing the same
			// operation_id executes normally instead of conflicting (see
			// TestInvalidMembershipRejectionDoesNotReserveOperationID).
			second, _ := sess.Handle(context.Background(), Request{ID: "2", Method: "membership.revoke", Params: map[string]any{
				"operation_id": opID, "conversation": "conv-does-not-exist", "expected_grant_version": "1",
			}})
			if second.Err == nil || second.Err.Data == nil || second.Err.Data.Code != DomainCode(store.NoActiveGrant) {
				t.Fatalf("a corrected retry reusing the same operation_id must execute normally, got %#v", second.Err)
			}
		})
	}
}

// TestMembershipRevokeAtIdentityBoundPreservesExactIdentifierThroughAuditAndReplay
// revokes a conversation identifier exactly store.MaxIdentityBytes long,
// built from '"'/'\' bytes JSON must escape, through the real wire handler,
// then reads it back through operation.get and replays it -- proving the
// exact identifier (not a re-escaped or truncated copy) survives every
// stage of durable storage and retrieval.
func TestMembershipRevokeAtIdentityBoundPreservesExactIdentifierThroughAuditAndReplay(t *testing.T) {
	sess, db := membershipTestServer(t)
	conversation := strings.Repeat(`x"\y`, store.MaxIdentityBytes/4)
	if len(conversation) != store.MaxIdentityBytes {
		t.Fatalf("test fixture must be exactly at the boundary, got %d bytes", len(conversation))
	}
	seedGrantDirectly(t, db, conversation)

	opID := newOpID()
	revoke, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "membership.revoke", Params: map[string]any{
		"operation_id": opID, "conversation": conversation, "expected_grant_version": "1",
	}})
	if revoke.Err != nil {
		t.Fatalf("revoke at the exact boundary must succeed: %#v", revoke.Err)
	}
	result := decodeResult[CommandReceiptResult](t, revoke)
	if !result.Usable(opID, conversation) {
		t.Fatalf("boundary receipt must be Usable against the exact identifier: %#v", result)
	}
	if len(result.Result.Resources) == 0 || result.Result.Resources[0].ID != conversation {
		t.Fatalf("resource id mismatch: %#v", result.Result.Resources)
	}

	getResp, _ := sess.Handle(context.Background(), Request{ID: "2", Method: "operation.get", Params: map[string]any{"operation_id": opID}})
	if getResp.Err != nil {
		t.Fatalf("operation.get must retrieve the revoke record: %#v", getResp.Err)
	}
	getResult := decodeResult[OperationGetResult](t, getResp)
	if getResult.AuditID != result.AuditID {
		t.Fatalf("operation.get audit id mismatch: %s vs %s", getResult.AuditID, result.AuditID)
	}
	var raw struct {
		Resources []struct {
			ID string `json:"id"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(getResult.Result, &raw); err != nil {
		t.Fatal(err)
	}
	if len(raw.Resources) == 0 || raw.Resources[0].ID != conversation {
		t.Fatalf("operation.get must republish the exact identifier verbatim, got %#v", raw.Resources)
	}

	replay, _ := sess.Handle(context.Background(), Request{ID: "3", Method: "membership.revoke", Params: map[string]any{
		"operation_id": opID, "conversation": conversation, "expected_grant_version": "1",
	}})
	if replay.Err != nil {
		t.Fatalf("replay must succeed: %#v", replay.Err)
	}
	r2 := decodeResult[CommandReceiptResult](t, replay)
	if r2.AuditID != result.AuditID || r2.Result.Resources[0].ID != conversation {
		t.Fatalf("replay must reproduce the identical durable receipt: %#v", r2)
	}
}
