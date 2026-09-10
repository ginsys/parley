// Package store owns Parley's sqlite schema and the one safe way to run a
// serialized write transaction against it: BEGIN IMMEDIATE on a pinned
// connection, so a concurrent send and revoke/renew against the same
// conversation cannot interleave (see the design plan's Revocation section).
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

// DB wraps the underlying sql.DB. Callers get a serialized write transaction
// via Begin, never a bare db.Exec for anything that touches grants or
// envelopes.
type DB struct {
	sql *sql.DB
}

// Open opens (creating if needed) the sqlite database at path and applies the
// schema. WAL mode and foreign keys are turned on for every connection.
func Open(ctx context.Context, path string) (*DB, error) {
	sqlDB, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	// A single writer connection avoids sqlite's "database is locked" churn
	// under WAL; readers still get their own connections.
	sqlDB.SetMaxOpenConns(1)

	for _, pragma := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
	} {
		if _, err := sqlDB.ExecContext(ctx, pragma); err != nil {
			sqlDB.Close()
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}
	if _, err := sqlDB.ExecContext(ctx, schema); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	if err := migrate(ctx, sqlDB); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("migrate schema: %w", err)
	}
	return &DB{sql: sqlDB}, nil
}

// migrate applies schema changes that CREATE TABLE IF NOT EXISTS cannot: it
// is a no-op against an existing table, so a column added to schema.sql
// after a database was first created (e.g. envelopes.is_trusted_reply) would
// otherwise silently never appear on that database, and every query naming
// the new column would fail with "no such column" against it. Each step
// checks column presence via PRAGMA table_info before adding it, so this
// stays a no-op both on a freshly created database (schema already has the
// column) and on one that's already been migrated once.
func migrate(ctx context.Context, sqlDB *sql.DB) error {
	steps := []struct {
		table  string
		column string
		ddl    string
	}{
		{"envelopes", "is_trusted_reply",
			"ALTER TABLE envelopes ADD COLUMN is_trusted_reply INTEGER NOT NULL DEFAULT 0 CHECK (is_trusted_reply IN (0, 1))"},
	}
	for _, s := range steps {
		has, err := hasColumn(ctx, sqlDB, s.table, s.column)
		if err != nil {
			return fmt.Errorf("check %s.%s: %w", s.table, s.column, err)
		}
		if has {
			continue
		}
		if _, err := sqlDB.ExecContext(ctx, s.ddl); err != nil {
			return fmt.Errorf("add %s.%s: %w", s.table, s.column, err)
		}
	}
	return nil
}

func hasColumn(ctx context.Context, sqlDB *sql.DB, table, column string) (bool, error) {
	rows, err := sqlDB.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid        int
			name       string
			ctype      string
			notNull    int
			dfltValue  sql.NullString
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dfltValue, &primaryKey); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (d *DB) Close() error { return d.sql.Close() }

// Tx is a single BEGIN IMMEDIATE transaction pinned to one connection, so
// sqlite's write lock is acquired at Begin, not at the first write.
type Tx struct {
	conn *sql.Conn
}

// Begin acquires a pinned connection and starts a BEGIN IMMEDIATE
// transaction. The caller must call Commit or Rollback exactly once.
func (d *DB) Begin(ctx context.Context) (*Tx, error) {
	conn, err := d.sql.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		conn.Close()
		return nil, fmt.Errorf("begin immediate: %w", err)
	}
	return &Tx{conn: conn}, nil
}

func (t *Tx) Commit(ctx context.Context) error {
	defer t.conn.Close()
	_, err := t.conn.ExecContext(ctx, "COMMIT")
	return err
}

func (t *Tx) Rollback(ctx context.Context) error {
	defer t.conn.Close()
	_, err := t.conn.ExecContext(ctx, "ROLLBACK")
	return err
}

func (t *Tx) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.conn.ExecContext(ctx, query, args...)
}

func (t *Tx) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return t.conn.QueryRowContext(ctx, query, args...)
}

func (t *Tx) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.conn.QueryContext(ctx, query, args...)
}

// RecoverUncertain marks every envelope still in 'dispatching' as
// 'uncertain', in one transaction. Call this once at bridge startup: any row
// found here means a prior process died between the host call returning and
// its outcome being recorded (fixture: crash-after-handoff). It is never
// auto-retried or auto-resolved past this point — an operator resolves each
// uncertain row by hand, per the design plan.
func (d *DB) RecoverUncertain(ctx context.Context) (int64, error) {
	tx, err := d.Begin(ctx)
	if err != nil {
		return 0, err
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback(ctx)
		}
	}()
	res, err := tx.Exec(ctx, `
		UPDATE envelopes SET state = 'uncertain', updated_at = ?
		WHERE state = 'dispatching'`, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("recover uncertain: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("recover uncertain: rows affected: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	committed = true
	return n, nil
}
