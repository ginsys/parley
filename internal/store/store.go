// Package store owns Parley's SQLite schema and immediate write transactions.
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

// DB owns one immediate writer and, after explicit initialization, deferred readers.
type DB struct {
	sql       *sql.DB
	path      string
	readers   atomic.Pointer[sql.DB]
	lifecycle sync.Mutex
	closed    bool
	closeErr  error
}

func Open(ctx context.Context, path string) (*DB, error) {
	return open(ctx, path, false)
}

// OpenExisting is the runtime writer path: SQLite may not create a replacement DB.
func OpenExisting(ctx context.Context, path string) (*DB, error) {
	// Check before SQLite can set journal mode on an empty placeholder.
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var header [16]byte
	_, readErr := f.ReadAt(header[:], 0)
	closeErr := f.Close()
	if readErr != nil || string(header[:]) != "SQLite format 3\x00" {
		return nil, fmt.Errorf("database initialization required")
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return open(ctx, path, true)
}

func open(ctx context.Context, path string, existing bool) (*DB, error) {
	normalized, err := normalizedDatabaseURL(path)
	if err != nil {
		return nil, err
	}
	// Pin relative paths at writer initialization; reader initialization must not
	// follow a later process working-directory change to a different database.
	path = normalized.String()
	dsn, err := databaseDSN(path)
	if err != nil {
		return nil, err
	}
	if existing {
		u, e := url.Parse(dsn)
		if e != nil {
			return nil, e
		}
		q := u.Query()
		q.Set("mode", "rw")
		u.RawQuery = q.Encode()
		dsn = u.String()
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := migrate(ctx, db, !existing); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate schema: %w", err)
	}
	return &DB{sql: db, path: path}, nil
}

// Only memory/cache URI options are caller-selectable. Locking and foreign-key
// settings belong to the store and cannot be overridden through DSN aliases.
func normalizedDatabaseURL(path string) (*url.URL, error) {
	var u *url.URL
	var err error
	switch {
	case path == "":
		return nil, fmt.Errorf("database path is required")
	case path == ":memory:":
		u = &url.URL{Scheme: "file", Opaque: ":memory:"}
	case strings.HasPrefix(path, "file:"):
		u, err = url.Parse(path)
		if err != nil {
			return nil, fmt.Errorf("database URI: %w", err)
		}
	default:
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		u = &url.URL{Scheme: "file", Path: absolute}
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, err
	}
	for key, values := range q {
		if (key != "mode" && key != "cache") || len(values) != 1 {
			return nil, fmt.Errorf("unsupported database URI option %q", key)
		}
	}
	u.RawQuery = q.Encode()
	return u, nil
}

func databaseDSN(path string) (string, error) {
	u, err := normalizedDatabaseURL(path)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("_txlock", "immediate")
	q.Set("_busy_timeout", "5000")
	q.Set("_foreign_keys", "on")
	q.Set("_journal_mode", "WAL")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (d *DB) Close() error {
	d.lifecycle.Lock()
	defer d.lifecycle.Unlock()
	if d.closed {
		return d.closeErr
	}
	d.closed = true
	if readers := d.readers.Swap(nil); readers != nil {
		d.closeErr = readers.Close()
	}
	d.closeErr = errors.Join(d.closeErr, d.sql.Close())
	return d.closeErr
}

// Begin acquires SQLite's write lock before reading authorization or claiming
// budget. database/sql and the driver own transaction termination and pooling.
func (d *DB) Begin(ctx context.Context) (*sql.Tx, error) { return d.sql.BeginTx(ctx, nil) }

// RecoverUncertain marks every envelope still in 'dispatching' as
// 'uncertain', in one transaction. Call this once at bridge startup: any row
// found here means a prior process died between the host call returning and
// its outcome being recorded (fixture: crash-after-handoff). It is never
// auto-retried or auto-resolved past this point — an operator resolves each
// uncertain row by hand, as documented in docs/architecture.md.
func (d *DB) RecoverUncertain(ctx context.Context) (int64, error) {
	tx, err := d.Begin(ctx)
	if err != nil {
		return 0, err
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()
	res, err := tx.ExecContext(ctx, `
		UPDATE envelopes SET state = 'uncertain', error_code='interrupted', error_detail='Dispatch was interrupted; host acceptance is unknown and automatic retry is disabled.', updated_at = ?
		WHERE state = 'dispatching'`, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("recover uncertain: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("recover uncertain: rows affected: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	committed = true
	return n, nil
}
