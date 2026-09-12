package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func seedVersionFour(t *testing.T) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "registry.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, migration := range migrations[:4] {
		if err := migration(context.Background(), tx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(`PRAGMA user_version=4; INSERT INTO conversations VALUES('old','old','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return db, path
}

func TestRegistryMigrationPreservesIdentityAndLegacy(t *testing.T) {
	seed, path := seedVersionFour(t)
	var server string
	for attempt := 0; attempt < 2; attempt++ {
		db, err := Open(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		var got string
		if err := seed.QueryRow("SELECT server_id FROM installation WHERE singleton=1").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if attempt == 0 {
			server = got
		} else if got != server {
			t.Fatal("migration replaced installation identity")
		}
		var version, old, bindings, credentials int
		if err := seed.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 5 {
			t.Fatalf("version=%d err=%v", version, err)
		}
		for query, dest := range map[string]*int{"SELECT count(*) FROM conversations WHERE id='old'": &old, "SELECT count(*) FROM bindings": &bindings, "SELECT count(*) FROM credentials": &credentials} {
			if err := seed.QueryRow(query).Scan(dest); err != nil {
				t.Fatal(err)
			}
		}
		if old != 1 || bindings != 0 || credentials != 0 {
			t.Fatalf("legacy=%d bindings=%d credentials=%d", old, bindings, credentials)
		}
	}
}

func TestRegistryMigrationFailureRollsBackEverything(t *testing.T) {
	seed, path := seedVersionFour(t)
	if _, err := seed.Exec("CREATE TABLE credentials (collision TEXT)"); err != nil {
		t.Fatal(err)
	}
	before := schemaSnapshot(t, seed)
	db, err := Open(context.Background(), path)
	if err == nil {
		db.Close()
		t.Fatal("accepted migration collision")
	}
	if !reflect.DeepEqual(before, schemaSnapshot(t, seed)) {
		t.Fatal("partial schema after failure")
	}
	var version int
	if err := seed.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 4 {
		t.Fatalf("version=%d err=%v", version, err)
	}
}

func TestRegistryIdentityConstraintsAndBounds(t *testing.T) {
	db := commandDB(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	b := BindingRecord{ID: "30000000-0000-4000-8000-000000000001", PeerID: " peer ", HostKind: "codex_cli", NamespaceID: "test", SessionID: "synthetic", ConnectorUID: 1000, Status: "enabled", Version: 1}
	c := CredentialRecord{BindingID: b.ID, ID: "40000000-0000-4000-8000-000000000001", Version: 1, Status: "current", ExpiresAtNS: 1}
	if err := InsertBindingCredential(ctx, tx, b, c); err != nil {
		t.Fatal(err)
	}
	got, err := ReadBinding(ctx, tx, b.ID)
	if err != nil || got != b {
		t.Fatalf("binding=%+v err=%v", got, err)
	}
	// Tuple, peer and credential collisions cannot leave an extra binding behind.
	for _, collision := range []string{"tuple", "peer", "credential"} {
		candidate, credential := b, c
		candidate.ID = "30000000-0000-4000-8000-000000000002"
		credential.BindingID = candidate.ID
		if collision != "tuple" {
			candidate.SessionID = "second"
		}
		if collision != "peer" {
			candidate.PeerID = "second"
		}
		if collision != "credential" {
			credential.ID = "40000000-0000-4000-8000-000000000002"
		}
		if err := InsertBindingCredential(ctx, tx, candidate, credential); err != IdentityConflict {
			t.Fatalf("%s: %v", collision, err)
		}
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM bindings").Scan(&count); err != nil || count != 1 {
		t.Fatalf("partial binding count=%d err=%v", count, err)
	}
	for _, query := range []string{
		"UPDATE bindings SET peer_id='replacement'", "UPDATE bindings SET connector_uid=1001", "DELETE FROM bindings",
		"UPDATE bindings SET binding_version=9223372036854775807+1", "UPDATE bindings SET connection_generation=-1",
		"UPDATE credentials SET verifier=zeroblob(32)", "UPDATE credentials SET expires_at_ns=2", "DELETE FROM credentials",
		"UPDATE installation SET server_id='30000000-0000-4000-8000-000000000003'", "DELETE FROM installation",
	} {
		if _, err := tx.ExecContext(ctx, query); err == nil {
			t.Fatalf("accepted invariant violation: %s", query)
		}
	}
	for _, bad := range []BindingRecord{
		{ID: b.ID, PeerID: "not enough fields"},
		func() BindingRecord { x := b; x.PeerID = strings.Repeat("a", 257); return x }(),
		func() BindingRecord { x := b; x.NamespaceID = strings.Repeat("a", 4097); return x }(),
		func() BindingRecord { x := b; x.SessionID = "\xff"; return x }(),
	} {
		if err := InsertBindingCredential(ctx, tx, bad, c); err != InvalidRequest {
			t.Fatalf("invalid binding: %v", err)
		}
	}
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM grants").Scan(&count); err != nil || count != 0 {
		t.Fatalf("unexpected grants=%d err=%v", count, err)
	}
}
