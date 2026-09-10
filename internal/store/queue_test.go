package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
)

func seedQueue(t *testing.T, tx *sql.Tx) {
	t.Helper()
	if _, err := tx.Exec(`INSERT INTO conversations VALUES('c','c','2026-01-01T00:00:00Z');
 INSERT INTO grants(conversation,grant_version,peer_a_id,peer_b_id,direction,max_exchanges,granted_at,status) VALUES('c',1,'a','b','bidirectional',200,'2026-01-01T00:00:00Z','active')`); err != nil {
		t.Fatal(err)
	}
}

func TestQueueOrdersMixedTimestampPrecisionAndTies(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	seedQueue(t, tx)
	for _, e := range []Envelope{
		{ID: "later", CreatedAt: "2026-01-01T00:00:00.12Z"},
		{ID: "tie-b", CreatedAt: "2026-01-01T00:00:00.100Z"},
		{ID: "tie-a", CreatedAt: "2026-01-01T00:00:00.1Z"},
		{ID: "first", CreatedAt: "2026-01-01T00:00:00Z"},
	} {
		e.Conversation = "c"
		e.FromPeer = "a"
		e.ToPeer = "b"
		e.GrantVersion = 1
		if err := InsertQueued(ctx, tx, e); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := ListQueued(ctx, tx, "c")
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, e := range rows {
		ids = append(ids, e.ID)
	}
	if !reflect.DeepEqual(ids, []string{"first", "tie-a", "tie-b", "later"}) {
		t.Fatalf("order=%v", ids)
	}
}

func TestQueueTimestampMigrationIsAtomic(t *testing.T) {
	for _, stamp := range []string{"2026-01-01T00:00:00.1Z", "malformed", "9999-01-01T00:00:00Z"} {
		t.Run(stamp, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "legacy.db")
			raw, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			if _, err := raw.Exec(schema); err != nil {
				t.Fatal(err)
			}
			tx, err := raw.Begin()
			if err != nil {
				t.Fatal(err)
			}
			seedQueue(t, tx)
			if _, err := tx.Exec(`INSERT INTO envelopes(id,conversation,from_peer,to_peer,text,grant_version,state,created_at,updated_at) VALUES('e','c','a','b','body',1,'queued',?,?)`, stamp, stamp); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			db, openErr := Open(context.Background(), path)
			if stamp == "2026-01-01T00:00:00.1Z" {
				if openErr != nil {
					t.Fatal(openErr)
				}
				db.Close()
				var original string
				var numeric int64
				if err := raw.QueryRow("SELECT created_at,created_at_ns FROM envelopes").Scan(&original, &numeric); err != nil || original != stamp || numeric != 1767225600100000000 {
					t.Fatalf("migration: %q %d %v", original, numeric, err)
				}
			} else {
				if openErr == nil {
					db.Close()
					t.Fatal("accepted invalid timestamp")
				}
				var version, columns int
				if err := raw.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 0 {
					t.Fatalf("version=%d %v", version, err)
				}
				if err := raw.QueryRow("SELECT count(*) FROM pragma_table_info('envelopes') WHERE name IN ('dispatch_attempt','created_at_ns')").Scan(&columns); err != nil || columns != 0 {
					t.Fatalf("partial columns=%d %v", columns, err)
				}
			}
		})
	}
}

func TestQueueIDsAreRecipientFilteredAndBounded(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "bounded.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	seedQueue(t, tx)
	for i := 0; i < 105; i++ {
		for _, peer := range []string{"a", "b"} {
			e := Envelope{ID: fmt.Sprintf("%s-%03d", peer, i), Conversation: "c", FromPeer: "a", ToPeer: peer, GrantVersion: 1, CreatedAt: "2026-01-01T00:00:00Z"}
			if err := InsertQueued(ctx, tx, e); err != nil {
				t.Fatal(err)
			}
		}
	}
	ids, err := ListQueuedIDs(ctx, tx, "c", "b", 100, nil)
	if err != nil || len(ids) != 100 || ids[0].ID != "b-000" || ids[99].ID != "b-099" {
		t.Fatalf("IDs=%v %v", ids, err)
	}
	for _, limit := range []int{0, -1, 101} {
		if _, err := ListQueuedIDs(ctx, tx, "c", "b", limit, nil); err == nil {
			t.Fatalf("accepted limit %d", limit)
		}
	}
}
