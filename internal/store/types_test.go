package store_test

import (
	"testing"
	"time"

	"github.com/ginsys/parley/internal/store"
)

// Regression for a finding on PR #4: Permits is the named authorization gate
// on Grant, but it used to consult only PeerAID/PeerBID/Direction, never
// RevokedAt — a grant resolved some way other than store.CurrentGrant (whose
// query already filters to status='active') could otherwise be reported as
// permitting a message despite carrying a revocation timestamp.
func TestGrantPermitsRejectsRevokedGrant(t *testing.T) {
	revokedAt := "2026-01-01T00:00:00Z"
	g := store.Grant{
		PeerAID:   "codex-thread-b",
		PeerBID:   "claude-session-a",
		Direction: store.Bidirectional,
		RevokedAt: &revokedAt,
	}
	now := time.Now()
	if g.Permits("codex-thread-b", "claude-session-a", now) {
		t.Fatalf("want a revoked grant to permit nothing")
	}
	if g.Permits("claude-session-a", "codex-thread-b", now) {
		t.Fatalf("want a revoked grant to permit nothing in either direction")
	}
}

func TestGrantPermitsAllowsActiveGrant(t *testing.T) {
	g := store.Grant{
		PeerAID:   "codex-thread-b",
		PeerBID:   "claude-session-a",
		Direction: store.Bidirectional,
	}
	if !g.Permits("codex-thread-b", "claude-session-a", time.Now()) {
		t.Fatalf("want an active bidirectional grant to permit this direction")
	}
}

// Regression for a finding on the merge-triggered review: Permits never
// checked ExpiresAt, so an expired grant (status still 'active' until an
// operator revokes it — nothing flips status on the clock alone) would
// permit messages indefinitely.
func TestGrantPermitsRejectsExpiredGrant(t *testing.T) {
	expiresAt := "2026-01-01T00:00:00Z"
	g := store.Grant{
		PeerAID:   "codex-thread-b",
		PeerBID:   "claude-session-a",
		Direction: store.Bidirectional,
		ExpiresAt: &expiresAt,
	}
	after, err := time.Parse(time.RFC3339, "2026-01-01T00:00:01Z")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if g.Permits("codex-thread-b", "claude-session-a", after) {
		t.Fatalf("want an expired grant to permit nothing")
	}
}

func TestGrantPermitsAllowsUnexpiredGrant(t *testing.T) {
	expiresAt := "2026-01-01T00:00:00Z"
	g := store.Grant{
		PeerAID:   "codex-thread-b",
		PeerBID:   "claude-session-a",
		Direction: store.Bidirectional,
		ExpiresAt: &expiresAt,
	}
	before, err := time.Parse(time.RFC3339, "2025-12-31T23:59:59Z")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !g.Permits("codex-thread-b", "claude-session-a", before) {
		t.Fatalf("want a not-yet-expired grant to permit this direction")
	}
}
