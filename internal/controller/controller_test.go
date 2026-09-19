package controller

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/ginsys/parley/internal/store"
	"path/filepath"
	"testing"
	"time"
)

func TestUnsafePeerIdentifiersNeverCreateGrants(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "peers.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctrl := New(db)
	for _, id := range []string{"a\xff", "a\xfe", "café", "a\ufffd", "peer\x7f", "peer\n", "peer\r", "peer\t", "peer\x00", "peer\u0085", "peer\u2028", "peer\u2029", "peer\u200b", "peer\u202e"} {
		for _, side := range []string{"conversation", "a", "b"} {
			t.Run(fmt.Sprintf("%s/%q", side, id), func(t *testing.T) {
				p := GrantParams{Conversation: "fresh", PeerAID: "a", PeerBID: "b", Direction: store.Bidirectional, MaxExchanges: 2}
				if side == "conversation" {
					p.Conversation = id
				} else if side == "a" {
					p.PeerAID = id
				} else {
					p.PeerBID = id
				}
				if _, err := ctrl.Grant(ctx, p); err == nil {
					t.Error("accepted an undeliverable peer identifier")
				}
			})
		}
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, table := range []string{"conversations", "grants"} {
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 0 {
			t.Errorf("%s rows=%d: %v", table, count, err)
		}
	}
}

func TestRenewRejectsLegacyUnsafePeersWithoutChangingHistory(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "legacy-peers.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureConversation(ctx, tx, "c", "c", "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertGrant(ctx, tx, store.Grant{Conversation: "c", GrantVersion: 1, PeerAID: "a\n", PeerBID: "b", Direction: store.Bidirectional, MaxExchanges: 2, GrantedAt: "2026-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	ctrl := New(db)
	if _, err := ctrl.Renew(ctx, RenewParams{Conversation: "c", MaxExchanges: 3}); err == nil {
		t.Error("renewed unusable legacy peers")
	}
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	g, err := store.CurrentGrant(ctx, tx, "c")
	if err != nil || g.GrantVersion != 1 || g.MaxExchanges != 2 || g.PeerAID != "a\n" {
		t.Errorf("legacy grant changed: %+v: %v", g, err)
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM grants").Scan(&count); err != nil || count != 1 {
		t.Errorf("history rows=%d: %v", count, err)
	}
	tx.Rollback()
	if _, err := ctrl.Revoke(ctx, "c"); err != nil {
		t.Fatalf("legacy grant cannot be revoked: %v", err)
	}
}

func TestInvalidGrantAndRenewLeaveStateUnchanged(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "validation.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctrl := New(db)
	valid := GrantParams{Conversation: "c", PeerAID: "a", PeerBID: "b", Direction: store.Bidirectional, MaxExchanges: 2}
	for _, mutate := range []func(*GrantParams){
		func(p *GrantParams) { p.Conversation = " " }, func(p *GrantParams) { p.PeerAID = "" }, func(p *GrantParams) { p.PeerBID = p.PeerAID },
		func(p *GrantParams) { p.Direction = "invalid" }, func(p *GrantParams) { p.MaxExchanges = -1 }, func(p *GrantParams) { p.MaxExchanges = 0 },
		func(p *GrantParams) { past := time.Now().Add(-time.Hour); p.ExpiresAt = &past },
	} {
		p := valid
		mutate(&p)
		if _, err := ctrl.Grant(ctx, p); err == nil {
			t.Fatalf("accepted %+v", p)
		}
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM conversations").Scan(&count); err != nil || count != 0 {
		t.Fatalf("conversations=%d: %v", count, err)
	}
	tx.Rollback()
	if _, err := ctrl.Grant(ctx, valid); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Hour)
	for _, p := range []RenewParams{{Conversation: "c", MaxExchanges: -1}, {Conversation: "c", ExpiresAt: &past}, {Conversation: " "}} {
		if _, err := ctrl.Renew(ctx, p); err == nil {
			t.Fatalf("accepted %+v", p)
		}
	}
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	g, err := store.CurrentGrant(ctx, tx, "c")
	if err != nil || g.GrantVersion != 1 || g.MaxExchanges != 2 {
		t.Fatalf("grant=%+v: %v", g, err)
	}
}

func TestReenrollmentPreservesHistoricalVersions(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "regrant.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctrl := New(db)
	p := GrantParams{Conversation: "c", PeerAID: "a", PeerBID: "b", Direction: store.Bidirectional, MaxExchanges: 2}
	for version := int64(1); version <= 3; version++ {
		g, err := ctrl.Grant(ctx, p)
		if err != nil || g.GrantVersion != version {
			t.Fatalf("grant=%+v: %v", g, err)
		}
		if _, err := ctrl.Revoke(ctx, "c"); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM grants WHERE status='revoked'").Scan(&count); err != nil || count != 3 {
		t.Fatalf("history=%d: %v", count, err)
	}
}

func TestConversationAndPeerIdentifiersRemainExact(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "exact.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctrl := New(db)
	for _, name := range []string{"x", " x"} {
		g, err := ctrl.Grant(ctx, GrantParams{Conversation: name, PeerAID: "a", PeerBID: "a ", Direction: store.Bidirectional, MaxExchanges: 2})
		if err != nil || g.Conversation != name || g.PeerBID != "a " {
			t.Fatalf("grant: %+v %v", g, err)
		}
	}
	if _, err := ctrl.Renew(ctx, RenewParams{Conversation: " x", MaxExchanges: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := ctrl.Revoke(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	remaining, err := store.CurrentGrant(ctx, tx, " x")
	if err != nil || remaining.GrantVersion != 2 || remaining.MaxExchanges != 3 {
		t.Fatalf("retargeted operation: %+v %v", remaining, err)
	}
	if remaining.Permits("a", "a", time.Now()) || !remaining.Permits("a", "a ", time.Now()) {
		t.Fatal("peer identities were conflated")
	}
	tx.Rollback()
	if _, err := ctrl.Revoke(ctx, " x"); err != nil {
		t.Fatalf("exact historical name inaccessible: %v", err)
	}
}

// TestGrantAndRenewRejectExpiryAtOrBeforeAuthorityInstant is MC-03/F2's
// regression at the controller boundary: validateGrant/validateRenewalInput
// must reject on the *authority* instant, not an independently-sampled
// time.Now(), and must return the durable store.RequestExpired code (not a
// plain wrapped error that degrades to TemporarilyUnavailable through
// domainRejection's fallback).
//
// The original version of this test called time.Now() twice a moment apart
// and relied on a real clock only ever advancing to exercise the boundary --
// it could not actually distinguish authorityInstant(ctx) from time.Now(),
// since both samples always agreed in practice. This version installs a
// store.RecoveryHooks fixture with a fixed clock, wholly independent of wall
// time, on a real store.Coordinator, and drives GrantTx/RenewTx through
// Coordinator.Execute exactly as internal/control's membership.enroll/renew
// handlers do -- so a regression that reintroduced an independently-sampled
// time.Now() inside GrantTx/RenewTx/supersede would show up as a wrong
// accept/reject decision or a stored granted_at that disagrees with the
// fixed authority instant, not merely as a flaky timing race.
func TestGrantAndRenewRejectExpiryAtOrBeforeAuthorityInstant(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "expiry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// A fixed authority instant far from time.Now(): if GrantTx/RenewTx ever
	// regressed to comparing against time.Now() instead of this ctx-carried
	// value, every case below would flip (the "future" cases would appear
	// expired against the real wall clock, or the boundary case would
	// wrongly pass).
	authority := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := db.Coordinator().InstallRecovery(store.RecoveryHooks{
		Before: func(context.Context, string) error { return nil },
		Time:   func(context.Context, *sql.Tx, string) (time.Time, error) { return authority, nil },
		After:  func() error { return nil },
	}); err != nil {
		t.Fatal(err)
	}

	principal := store.CommandPrincipal{ID: "10000000-0000-4000-8000-000000000001"}
	allowNoAuth := func(context.Context, *sql.Tx) error { return nil }
	grantRequest := func(id string, expiresAt time.Time, maxExchanges int64) store.CommandRequest {
		r, err := store.NewCommandRequest("membership.enroll", id,
			store.Field{Name: "conversation", Value: "c"},
			store.Field{Name: "expires_at", Value: expiresAt.UTC().Format(time.RFC3339Nano)},
			store.Field{Name: "max_exchanges", Value: maxExchanges},
		)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	grantMutate := func(expiresAt time.Time, maxExchanges int64, capture **store.Grant) func(context.Context, *sql.Tx) (store.CommandResult, error) {
		return func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
			g, err := GrantTx(ctx, tx, GrantParams{
				Conversation: "c", PeerAID: "a", PeerBID: "b", Direction: store.Bidirectional,
				MaxExchanges: maxExchanges, ExpiresAt: &expiresAt,
			})
			if err != nil {
				return store.CommandResult{}, err
			}
			if capture != nil {
				*capture = g
			}
			return store.CommandResult{Resources: []store.ResourceChange{{Kind: "grant", ID: g.Conversation, After: g.GrantVersion}}}, nil
		}
	}
	neverInvoked := func(context.Context, *sql.Tx) (store.CommandResult, error) {
		t.Fatal("mutate re-invoked on a path that must not run business logic again")
		return store.CommandResult{}, nil
	}

	// Equality-boundary rejection: expires_at == authority instant exactly,
	// not merely before it.
	boundaryID := "20000000-0000-4000-8000-000000000001"
	if _, err := db.Coordinator().Execute(ctx, principal, grantRequest(boundaryID, authority, 2), allowNoAuth, grantMutate(authority, 2, nil), nil); !errors.Is(err, store.RequestExpired) {
		t.Fatalf("grant with expires_at == authority instant: got %v, want store.RequestExpired", err)
	}

	// Future acceptance, plus stored-timestamp consistency: the accepted
	// grant's GrantedAt must equal the fixed authority instant exactly, not
	// whatever time.Now() happened to be when this test ran.
	future := authority.Add(time.Hour)
	grantID := "20000000-0000-4000-8000-000000000002"
	var granted *store.Grant
	receipt1, err := db.Coordinator().Execute(ctx, principal, grantRequest(grantID, future, 2), allowNoAuth, grantMutate(future, 2, &granted), nil)
	if err != nil || receipt1.Result.Code != "" {
		t.Fatalf("grant with future expires_at rejected: %+v %v", receipt1, err)
	}
	if granted == nil || granted.GrantedAt != authority.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("granted_at=%v, want the fixed authority instant %v", granted, authority)
	}

	// Terminal-result replay: the identical operation_id and identical
	// fields must return the same audit record without re-running GrantTx
	// at all -- neverInvoked fails the test if mutate is called again.
	receipt2, err := db.Coordinator().Execute(ctx, principal, grantRequest(grantID, future, 2), allowNoAuth, neverInvoked, nil)
	if err != nil || !receipt2.Replayed || receipt2.AuditID != receipt1.AuditID {
		t.Fatalf("replay=%+v %v, want Replayed with AuditID=%q", receipt2, err, receipt1.AuditID)
	}

	// Terminal-result conflict: the same operation_id with different fields
	// (a different budget) must be rejected as a conflicting retry, again
	// without invoking mutate.
	if _, err := db.Coordinator().Execute(ctx, principal, grantRequest(grantID, future, 3), allowNoAuth, neverInvoked, nil); !errors.Is(err, store.OperationConflict) {
		t.Fatalf("conflicting retry under the same operation_id: got %v, want store.OperationConflict", err)
	}

	renewRequest := func(id string, expiresAt time.Time, maxExchanges int64) store.CommandRequest {
		r, err := store.NewCommandRequest("membership.renew", id,
			store.Field{Name: "conversation", Value: "c"},
			store.Field{Name: "expires_at", Value: expiresAt.UTC().Format(time.RFC3339Nano)},
			store.Field{Name: "max_exchanges", Value: maxExchanges},
		)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	renewMutate := func(expiresAt time.Time, maxExchanges int64, capture **SupersedeResult) func(context.Context, *sql.Tx) (store.CommandResult, error) {
		return func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
			result, err := RenewTx(ctx, tx, RenewParams{Conversation: "c", MaxExchanges: maxExchanges, ExpiresAt: &expiresAt})
			if err != nil {
				return store.CommandResult{}, err
			}
			if capture != nil {
				*capture = result
			}
			return store.CommandResult{Resources: []store.ResourceChange{{Kind: "grant", ID: result.Grant.Conversation, After: result.Grant.GrantVersion}}}, nil
		}
	}

	// Renew's own equality-boundary rejection, against the same fixed
	// authority instant (still unmoved -- the recovery fixture's clock
	// never advances on its own).
	renewBoundaryID := "20000000-0000-4000-8000-000000000003"
	if _, err := db.Coordinator().Execute(ctx, principal, renewRequest(renewBoundaryID, authority, 3), allowNoAuth, renewMutate(authority, 3, nil), nil); !errors.Is(err, store.RequestExpired) {
		t.Fatalf("renew with expires_at == authority instant: got %v, want store.RequestExpired", err)
	}

	// Renew's future acceptance and stored-timestamp consistency.
	renewFuture := authority.Add(2 * time.Hour)
	renewID := "20000000-0000-4000-8000-000000000004"
	var superseded *SupersedeResult
	if _, err := db.Coordinator().Execute(ctx, principal, renewRequest(renewID, renewFuture, 3), allowNoAuth, renewMutate(renewFuture, 3, &superseded), nil); err != nil {
		t.Fatalf("renew with future expires_at rejected: %v", err)
	}
	if superseded == nil || superseded.Grant.GrantedAt != authority.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("renewed grant's granted_at=%v, want the fixed authority instant %v", superseded, authority)
	}
}
