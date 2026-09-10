package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestCancelledTransactionReleasesConnection(t *testing.T) {
	for _, operation := range []string{"commit", "rollback"} {
		t.Run(operation, func(t *testing.T) {
			db, err := Open(context.Background(), filepath.Join(t.TempDir(), "cancel.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx, cancel := context.WithCancel(context.Background())
			tx, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			cancel()
			if operation == "commit" {
				err = tx.Commit()
			} else {
				err = tx.Rollback()
			}
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, sql.ErrTxDone) {
				t.Fatal(err)
			}
			fresh, err := db.Begin(context.Background())
			if err != nil {
				t.Fatalf("fresh transaction: %v", err)
			}
			defer fresh.Rollback()
			var fk int
			if err := fresh.QueryRowContext(context.Background(), "PRAGMA foreign_keys").Scan(&fk); err != nil || fk != 1 {
				t.Fatalf("foreign_keys=%d: %v", fk, err)
			}
		})
	}
}

func TestFailedCommitRollsBack(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "commit.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "PRAGMA defer_foreign_keys=ON"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO grants(conversation,grant_version,peer_a_id,peer_b_id,direction,max_exchanges,granted_at,status) VALUES('missing',1,'a','b','bidirectional',1,'now','active')`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err == nil {
		t.Fatal("expected deferred foreign key failure")
	}
	fresh, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("fresh transaction: %v", err)
	}
	defer fresh.Rollback()
	var count int
	if err := fresh.QueryRowContext(ctx, "SELECT count(*) FROM grants").Scan(&count); err != nil || count != 0 {
		t.Fatalf("invalid rows=%d: %v", count, err)
	}
}

func TestIndependentConnectionsAcquireImmediateLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locks.db")
	ctx := context.Background()
	a, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	tx, err := a.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	blocked, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	other, err := b.Begin(blocked)
	if err == nil {
		other.Rollback()
		t.Fatal("second writer acquired lock before first completed")
	}
}

func TestConcurrentMigrationAndRepeatedOpen(t *testing.T) {
	for name, path := range map[string]string{
		"disk":          filepath.Join(t.TempDir(), "migrate.db"),
		"shared memory": "file:concurrent-migration?mode=memory&cache=shared",
	} {
		t.Run(name, func(t *testing.T) {
			seed, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := seed.Exec(historicalSchema(t)); err != nil {
				t.Fatal(err)
			}
			defer seed.Close()
			var wg sync.WaitGroup
			for range 4 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					db, err := Open(context.Background(), path)
					if err != nil {
						t.Error(err)
						return
					}
					db.Close()
				}()
			}
			wg.Wait()
			db, err := Open(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var version int
			if err := db.sql.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != len(migrations) {
				t.Fatalf("version=%d: %v", version, err)
			}
		})
	}
}

func TestOpenRejectsUnknownSchemaWithoutMutation(t *testing.T) {
	for _, statement := range []string{"PRAGMA user_version=999", "CREATE TABLE unexpected(x TEXT)"} {
		t.Run(statement, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "unknown.db")
			seed, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer seed.Close()
			if _, err := seed.Exec(statement); err != nil {
				t.Fatal(err)
			}
			db, err := Open(context.Background(), path)
			if err == nil {
				db.Close()
				t.Fatal("accepted unsupported schema")
			}
			var count int
			if err := seed.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='envelopes'").Scan(&count); err != nil || count != 0 {
				t.Fatalf("schema mutated: %d, %v", count, err)
			}
		})
	}
}

func TestDatabasePathsAndURISettings(t *testing.T) {
	for _, path := range []string{":memory:", "file:parley-test?mode=memory&cache=shared", filepath.Join(t.TempDir(), "literal?#%.db")} {
		t.Run(path, func(t *testing.T) {
			db, err := Open(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			tx, err := db.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			var count int
			if err := tx.QueryRowContext(context.Background(), "SELECT count(*) FROM envelopes").Scan(&count); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, path := range []string{"file:test?_fk=off", "file:test?_txlock=deferred", "file:test?mode=memory&mode=rwc"} {
		if _, err := databaseDSN(path); err == nil {
			t.Fatalf("accepted override %s", path)
		}
	}
}

func TestMigrationFailurePreservesSchemaVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failure.db")
	seed, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Close()
	if _, err := seed.Exec(historicalSchema(t)); err != nil {
		t.Fatal(err)
	}
	// Missing index with conflicting history must fail its recreation atomically.
	if _, err := seed.Exec(`DROP INDEX idx_grants_one_active;
 INSERT INTO conversations VALUES('c','c','now');
 INSERT INTO grants VALUES('c',1,'a','b','bidirectional',1,0,'now',NULL,'active',NULL);
 INSERT INTO grants VALUES('c',2,'a','b','bidirectional',1,0,'now',NULL,'active',NULL);`); err != nil {
		t.Fatal(err)
	}
	if db, err := Open(context.Background(), path); err == nil {
		db.Close()
		t.Fatal("accepted conflicting active history")
	}
	var columnCount int
	if err := seed.QueryRow("SELECT count(*) FROM pragma_table_info('envelopes') WHERE name='is_trusted_reply'").Scan(&columnCount); err != nil || columnCount != 0 {
		t.Fatalf("partial column migration: %d: %v", columnCount, err)
	}
	var version, count int
	if err := seed.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 0 {
		t.Fatalf("version=%d: %v", version, err)
	}
	if err := seed.QueryRow("SELECT count(*) FROM grants").Scan(&count); err != nil || count != 2 {
		t.Fatalf("count=%d: %v", count, err)
	}
}

func TestUnknownIndexRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad-index.db")
	seed, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Close()
	if _, err := seed.Exec(schema + "; DROP INDEX idx_grants_one_active; CREATE INDEX idx_grants_one_active ON grants(conversation);"); err != nil {
		t.Fatal(err)
	}
	if db, err := Open(context.Background(), path); err == nil {
		db.Close()
		t.Fatal("accepted nonunique grant index")
	}
	var v int
	if err := seed.QueryRow("PRAGMA user_version").Scan(&v); err != nil || v != 0 {
		t.Fatalf("version=%d: %v", v, err)
	}
}
func TestReplacementConnectionKeepsPragmas(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "replace.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.sql.SetMaxIdleConns(0)
	for range 2 {
		tx, err := db.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var fk int
		if err := tx.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&fk); err != nil || fk != 1 {
			t.Fatalf("fk=%d: %v", fk, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO grants(conversation,grant_version,peer_a_id,peer_b_id,direction,max_exchanges,granted_at,status) VALUES('missing',1,'a','b','bidirectional',1,'now','active')`); err == nil {
			t.Fatal("foreign key not enforced")
		}
		tx.Rollback()
	}
}

func TestIndexPredicateIsCaseSensitive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "predicate.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(schema + "; DROP INDEX idx_grants_one_active; CREATE UNIQUE INDEX idx_grants_one_active ON grants(conversation) WHERE status='ACTIVE'"); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(context.Background(), path); err == nil {
		opened.Close()
		t.Fatal("accepted wrong predicate")
	}
}
