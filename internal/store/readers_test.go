package store

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func readerTestDB(t *testing.T) *DB {
	t.Helper()
	d, err := Open(context.Background(), filepath.Join(t.TempDir(), "readers.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	if err := d.OpenReaders(context.Background()); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestReaderDSNAndInitialization(t *testing.T) {
	for _, path := range []string{":memory:", "file::memory:?cache=shared", "file:test?mode=memory", "file:test?_pragma=query_only(0)", "file:test?_query_only=off", "file:test?_txlock=immediate", "file:test?mode=ro&mode=rw"} {
		if _, err := readerDSN(path); err == nil {
			t.Fatalf("accepted %s", path)
		}
	}
	path := filepath.Join(t.TempDir(), "literal?#%.db")
	d, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := d.Queries().Outcome(context.Background(), "missing"); !errors.Is(err, ErrReadersNotReady) {
		t.Fatal(err)
	}
	if err := d.OpenReaders(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Queries().Outcome(context.Background(), "missing"); !errors.Is(err, ErrEnvelopeNotFound) {
		t.Fatal(err)
	}
	dsn, err := readerDSN(path)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if u.Query().Get("_journal_mode") != "" {
		t.Fatal("reader sets WAL")
	}
	missing := filepath.Join(t.TempDir(), "missing.db")
	dsn, err = readerDSN(missing)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.PingContext(context.Background()); err == nil {
		t.Fatal("reader created database")
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if d, err := OpenExisting(context.Background(), missing); err == nil {
		d.Close()
		t.Fatal("runtime writer created database")
	}
}

func TestFourReadersAndWriter(t *testing.T) {
	d := readerTestDB(t)
	ctx := context.Background()
	var txs []*sql.Tx
	defer func() {
		for _, tx := range txs {
			tx.Rollback()
		}
	}()
	writer, err := d.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, `INSERT INTO conversations(id,name,created_at) VALUES('new','new','now')`); err != nil {
		t.Fatal(err)
	}
	for range MaxReaders {
		tx, err := d.readers.Load().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		txs = append(txs, tx)
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM conversations").Scan(&count); err != nil || count != 0 {
			t.Fatalf("snapshot=%d: %v", count, err)
		}
	}
	// Deferred readers neither block an immediate writer nor see its uncommitted data.
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := d.Queries().Outcome(short, "missing"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fifth acquisition: %v", err)
	}
	for _, tx := range txs {
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM conversations").Scan(&count); err != nil || count != 0 {
			t.Fatalf("changed snapshot=%d: %v", count, err)
		}
		tx.Rollback()
	}
	if err := d.Queries().snapshot(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM conversations").Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			t.Fatalf("new snapshot=%d", count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReaderIsolationAndReplacement(t *testing.T) {
	d := readerTestDB(t)
	pool := d.readers.Load()
	for range 2 {
		var conns []*sql.Conn
		for range MaxReaders {
			conn, err := pool.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			conns = append(conns, conn)
			for pragma, want := range map[string]int{"query_only": 1, "foreign_keys": 1, "busy_timeout": 5000} {
				var got int
				if err := conn.QueryRowContext(context.Background(), "PRAGMA "+pragma).Scan(&got); err != nil || got != want {
					t.Fatalf("%s=%d: %v", pragma, got, err)
				}
			}
			for _, statement := range []string{"CREATE TABLE forbidden(id)", "INSERT INTO conversations(id,name,created_at) VALUES('x','x','x')", "PRAGMA user_version=999"} {
				if _, err := conn.ExecContext(context.Background(), statement); err == nil {
					t.Fatalf("reader mutated: %s", statement)
				}
			}
			// Even disabling the connection pragma cannot bypass the read-only VFS.
			if _, err := conn.ExecContext(context.Background(), "PRAGMA query_only=off"); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.ExecContext(context.Background(), "CREATE TABLE forbidden(id)"); err == nil {
				t.Fatal("VFS writable")
			}
		}
		pool.SetMaxIdleConns(0)
		for _, c := range conns {
			c.Close()
		}
		pool.SetMaxIdleConns(MaxReaders)
	}
}

func TestReaderQueryAndMaterializationCancellation(t *testing.T) {
	d := readerTestDB(t)
	for _, phase := range []string{"query", "materialization"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err := d.Queries().snapshot(ctx, func(ctx context.Context, tx *sql.Tx) error {
				if phase == "query" {
					short, stop := context.WithTimeout(ctx, 20*time.Millisecond)
					defer stop()
					var n int64
					return tx.QueryRowContext(short, `WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n`).Scan(&n)
				}
				rows, err := tx.QueryContext(ctx, `WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT x FROM n`)
				if err != nil {
					return err
				}
				defer rows.Close()
				if !rows.Next() {
					t.Fatal("no first row")
				}
				cancel()
				for rows.Next() {
					var n int
					if err := rows.Scan(&n); err != nil {
						return err
					}
				}
				return rows.Err()
			})
			if err == nil {
				t.Fatal("cancelled read succeeded")
			}
			// Reacquire the entire pool; cleanup may complete asynchronously after cancellation.
			ctx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			var held []*sql.Conn
			defer func() {
				for _, c := range held {
					c.Close()
				}
			}()
			for range MaxReaders {
				c, err := d.readers.Load().Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				held = append(held, c)
			}
		})
	}
}

func TestReadersPinWriterPath(t *testing.T) {
	first, second := t.TempDir(), t.TempDir()
	t.Chdir(first)
	d, err := Open(context.Background(), "relative.db")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	t.Chdir(second)
	if err := d.OpenReaders(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(second, "relative.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("reader followed changed cwd:", err)
	}
}

func TestReaderDefaultDeadlineAndDetachedResults(t *testing.T) {
	d := readerTestDB(t)
	pool := d.readers.Load()
	for _, operation := range []func() error{
		func() error {
			_, _, err := d.Queries().QueueBatch(context.Background(), "c", "b", 100, &QueueCursor{ID: "tail", CreatedAtNS: 1})
			return err
		},
		func() error {
			_, err := d.Queries().Outcome(context.Background(), "missing")
			if errors.Is(err, ErrEnvelopeNotFound) {
				return nil
			}
			return err
		},
	} {
		if err := operation(); err != nil {
			t.Fatal(err)
		}
		if pool.Stats().InUse != 0 {
			t.Fatal("query retained SQLite resources after returning")
		}
	}
	var held []*sql.Conn
	defer func() {
		for _, c := range held {
			c.Close()
		}
	}()
	for range MaxReaders {
		c, err := pool.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
	}
	started := time.Now()
	_, err := d.Queries().Outcome(context.Background(), "missing")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unbounded read:", err)
	}
	if elapsed := time.Since(started); elapsed < ReadTimeout-100*time.Millisecond {
		t.Fatalf("unexpected early timeout: %s", elapsed)
	}
}

func TestRuntimeWriterRequiresExplicitInitialization(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"empty", "empty-catalog"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "uninitialized.db")
			if err := os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
			if kind == "empty-catalog" {
				raw, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := raw.Exec("VACUUM"); err != nil {
					t.Fatal(err)
				}
				if err := raw.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if db, err := OpenExisting(ctx, path); err == nil {
				db.Close()
				t.Fatal("runtime initialized database")
			}
			if kind == "empty" {
				info, err := os.Stat(path)
				if err != nil || info.Size() != 0 {
					t.Fatal("placeholder modified", err)
				}
			}
			// Explicit library initialization remains available for the future human action.
			db, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			db.Close()
			db, err = OpenExisting(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			db.Close()
		})
	}
}
