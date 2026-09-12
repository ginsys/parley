package controller

import (
	"context"
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
