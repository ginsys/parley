package store_test

import (
	"testing"

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
	if g.Permits("codex-thread-b", "claude-session-a") {
		t.Fatalf("want a revoked grant to permit nothing")
	}
	if g.Permits("claude-session-a", "codex-thread-b") {
		t.Fatalf("want a revoked grant to permit nothing in either direction")
	}
}

func TestGrantPermitsAllowsActiveGrant(t *testing.T) {
	g := store.Grant{
		PeerAID:   "codex-thread-b",
		PeerBID:   "claude-session-a",
		Direction: store.Bidirectional,
	}
	if !g.Permits("codex-thread-b", "claude-session-a") {
		t.Fatalf("want an active bidirectional grant to permit this direction")
	}
}
