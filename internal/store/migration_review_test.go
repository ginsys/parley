package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func historicalSchema(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("legacy_schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestLegacyAdoptionRejectsUnknownSemantics(t *testing.T) {
	for name, ddl := range map[string]string{
		"extra table unique": strings.Replace(schema, "text          TEXT    NOT NULL,", "text          TEXT    NOT NULL UNIQUE,", 1),
		"missing unique":     strings.Replace(schema, "name       TEXT NOT NULL UNIQUE", "name       TEXT NOT NULL", 1),
		"missing check":      strings.Replace(schema, " CHECK (direction IN ('bidirectional', 'a_to_b', 'b_to_a'))", "", 1),
		"changed check":      strings.Replace(schema, "'bidirectional', 'a_to_b', 'b_to_a'", "'bidirectional', 'a_to_b', 'invalid'", 1),
		"collation":          strings.Replace(schema, "name       TEXT NOT NULL UNIQUE", "name       TEXT COLLATE NOCASE NOT NULL UNIQUE", 1),
		"extra index":        schema + "; CREATE UNIQUE INDEX extra ON envelopes(text)",
		"extra trigger":      schema + "; CREATE TRIGGER reject_message BEFORE INSERT ON envelopes BEGIN SELECT RAISE(ABORT, 'blocked'); END",
		"extra view":         schema + "; CREATE VIEW messages AS SELECT * FROM envelopes",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "unknown.db")
			seed, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer seed.Close()
			if ddl == schema {
				t.Fatal("fixture did not alter schema")
			}
			if _, err := seed.Exec(ddl + "; INSERT INTO conversations VALUES('c','c','2026-01-01T00:00:00Z')"); err != nil {
				t.Fatal(err)
			}
			before := schemaSnapshot(t, seed)
			opened, err := Open(context.Background(), path)
			if err == nil {
				opened.Close()
				t.Error("accepted unsupported legacy semantics")
			}
			if after := schemaSnapshot(t, seed); !reflect.DeepEqual(before, after) {
				t.Error("schema changed after rejected adoption")
			}
			var version, rows int
			if err := seed.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 0 {
				t.Errorf("version=%d: %v", version, err)
			}
			if err := seed.QueryRow("SELECT count(*) FROM conversations WHERE id='c'").Scan(&rows); err != nil || rows != 1 {
				t.Errorf("preserved rows=%d: %v", rows, err)
			}
		})
	}
}

// Snapshot independently of the production catalog comparison, including all
// automatic indexes and preserving NULL SQL as a separate value.
func schemaSnapshot(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var kind, name, table string
		var ddl sql.NullString
		if err := rows.Scan(&kind, &name, &table, &ddl); err != nil {
			t.Fatal(err)
		}
		result = append(result, fmt.Sprintf("%q/%q/%q/%v", kind, name, table, ddl))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestHistoricalSchemasRemainUsable(t *testing.T) {
	for name, ddl := range map[string]string{
		"initial a024019": historicalSchema(t),
		"fresh 8af08cb":   schema,
		"altered 0edf451": historicalSchema(t) + "; ALTER TABLE envelopes ADD COLUMN is_trusted_reply INTEGER NOT NULL DEFAULT 0 CHECK (is_trusted_reply IN (0, 1))",
		"missing indexes": historicalSchema(t) + "; DROP INDEX idx_grants_one_active; DROP INDEX idx_envelopes_conversation_state",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "historical.db")
			seed, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer seed.Close()
			if _, err := seed.Exec(ddl + `;
INSERT INTO conversations VALUES('c','c','2026-01-01T00:00:00Z');
INSERT INTO grants VALUES('c',1,'a','b','bidirectional',2,0,'2026-01-01T00:00:00Z',NULL,'active',NULL);
INSERT INTO envelopes(id,conversation,from_peer,to_peer,text,grant_version,state,created_at,updated_at) VALUES('e1','c','a','b','repeat',1,'queued','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');`); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				db, err := Open(context.Background(), path)
				if err != nil {
					t.Fatal(err)
				}
				db.Close()
			}
			var trusted, version int
			if err := seed.QueryRow("SELECT is_trusted_reply FROM envelopes WHERE id='e1'").Scan(&trusted); err != nil || trusted != 0 {
				t.Fatalf("trusted=%d: %v", trusted, err)
			}
			if err := seed.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != len(migrations) {
				t.Fatalf("version=%d: %v", version, err)
			}
			if _, err := seed.Exec(`INSERT INTO envelopes(id,conversation,from_peer,to_peer,text,grant_version,state,created_at,updated_at) VALUES('e2','c','a','b','repeat',1,'queued','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`); err != nil {
				t.Fatalf("repeated message after adoption: %v", err)
			}
		})
	}
}

func TestSharedCacheMigrationWaitsForWriter(t *testing.T) {
	for _, cancelWait := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", cancelWait), func(t *testing.T) {
			uri := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
			keeper, err := sql.Open("sqlite", uri)
			if err != nil {
				t.Fatal(err)
			}
			defer keeper.Close()
			keeper.SetMaxOpenConns(1)
			if _, err := keeper.Exec(historicalSchema(t)); err != nil {
				t.Fatal(err)
			}
			if _, err := keeper.Exec("BEGIN IMMEDIATE"); err != nil {
				t.Fatal(err)
			}
			defer keeper.Exec("ROLLBACK")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			finished := make(chan error, 1)
			go func() {
				db, err := Open(ctx, uri)
				if db != nil {
					db.Close()
				}
				finished <- err
			}()
			select {
			case err := <-finished:
				t.Fatalf("Open returned while writer held lock: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			if cancelWait {
				cancel()
			} else if _, err := keeper.Exec("COMMIT"); err != nil {
				t.Fatal(err)
			}
			err = <-finished
			if cancelWait {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation: %v", err)
				}
			} else if err != nil {
				t.Fatalf("Open after writer released lock: %v", err)
			}
		})
	}
}
