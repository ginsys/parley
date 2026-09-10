package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"modernc.org/sqlite"
	"reflect"
	"slices"
	"strings"
	"time"
)

// Each numbered step and user_version update commits in the same immediate
// transaction. Only version-zero adoption examines historical column layouts.
var migrations = []func(context.Context, *sql.Tx) error{adoptLegacySchema}

func migrate(ctx context.Context, db *sql.DB) error {
	tx, err := beginMigration(ctx, db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version < 0 || version > len(migrations) {
		return fmt.Errorf("unsupported schema version %d", version)
	}
	for version < len(migrations) {
		if err := migrations[version](ctx, tx); err != nil {
			return fmt.Errorf("migration %d: %w", version+1, err)
		}
		version++
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version=%d", version)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func adoptLegacySchema(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return err
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return err
		}
		tables = append(tables, name)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		_, err := tx.ExecContext(ctx, schema)
		return err
	}
	if !slices.Equal(tables, []string{"conversations", "envelopes", "grants"}) {
		return fmt.Errorf("unrecognized unversioned tables: %v", tables)
	}
	// Compare structural metadata to the frozen version-one schema, allowing
	// only the historically absent trusted-reply column. CHECK text is not
	// compared because the supported pre-upgrade fixture predates those checks.
	reference, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return err
	}
	defer reference.Close()
	reference.SetMaxOpenConns(1)
	if _, err := reference.ExecContext(ctx, schema); err != nil {
		return err
	}
	trusted := false
	for _, table := range tables {
		actual, err := tableColumns(ctx, tx, table)
		if err != nil {
			return err
		}
		wanted, err := tableColumns(ctx, reference, table)
		if err != nil {
			return err
		}
		if table == "envelopes" {
			_, trusted = actual["is_trusted_reply"]
			if !trusted {
				delete(wanted, "is_trusted_reply")
			}
		}
		if !reflect.DeepEqual(actual, wanted) {
			return fmt.Errorf("unrecognized unversioned columns in %s", table)
		}
		actualFK, err := tableForeignKeys(ctx, tx, table)
		if err != nil {
			return err
		}
		wantedFK, err := tableForeignKeys(ctx, reference, table)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(actualFK, wantedFK) {
			return fmt.Errorf("unrecognized foreign keys in %s", table)
		}
	}
	for _, name := range []string{"idx_grants_one_active", "idx_envelopes_conversation_state"} {
		var actual, wanted string
		err := tx.QueryRowContext(ctx, "SELECT sql FROM sqlite_master WHERE type='index' AND name=?", name).Scan(&actual)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return err
		}
		if err := reference.QueryRowContext(ctx, "SELECT sql FROM sqlite_master WHERE type='index' AND name=?", name).Scan(&wanted); err != nil {
			return err
		}
		if normalizeDDL(actual) != normalizeDDL(wanted) {
			return fmt.Errorf("unrecognized index %s", name)
		}
	}
	if !trusted {
		if _, err := tx.ExecContext(ctx, "ALTER TABLE envelopes ADD COLUMN is_trusted_reply INTEGER NOT NULL DEFAULT 0 CHECK (is_trusted_reply IN (0,1))"); err != nil {
			return err
		}
	}
	// Install the known indexes after validating the legacy tables. This also
	// rejects histories with multiple active grants without partially upgrading.
	_, err = tx.ExecContext(ctx, schema)
	return err
}

// sql.Tx and sql.DB both implement the inspection operations.
type schemaReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func tableColumns(ctx context.Context, db schemaReader, table string) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]string{}
	for rows.Next() {
		var cid, nn, pk int
		var name, kind string
		var def sql.NullString
		if err := rows.Scan(&cid, &name, &kind, &nn, &def, &pk); err != nil {
			return nil, err
		}
		result[name] = fmt.Sprintf("%s/%d/%d/%v", strings.ToUpper(kind), nn, pk, def)
	}
	return result, rows.Err()
}
func tableForeignKeys(ctx context.Context, db schemaReader, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_list("+table+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	groups := map[int][]string{}
	for rows.Next() {
		var id, seq int
		var target, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &seq, &target, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return nil, err
		}
		groups[id] = append(groups[id], fmt.Sprintf("%d:%s:%s:%s:%s:%s:%s", seq, target, from, to, onUpdate, onDelete, match))
	}
	var result []string
	for _, group := range groups {
		slices.Sort(group)
		result = append(result, strings.Join(group, ","))
	}
	slices.Sort(result)
	return result, rows.Err()
}
func normalizeDDL(s string) string {
	// Normalize syntax only. SQL string literals are case/space sensitive.
	var out strings.Builder
	quoted := false
	for _, r := range strings.ReplaceAll(s, "IF NOT EXISTS", "") {
		if r == '\'' {
			quoted = !quoted
			out.WriteRune(r)
			continue
		}
		if quoted {
			out.WriteRune(r)
			continue
		}
		if r == ' ' || r == '\n' || r == '\r' || r == '\t' {
			continue
		}
		out.WriteString(strings.ToLower(string(r)))
	}
	return out.String()
}

// SQLite can reject concurrent journal-mode initialization before its busy
// handler waits. Retrying a failed Begin is safe: no migration SQL has run.
func beginMigration(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		tx, err := db.BeginTx(ctx, nil)
		var sqliteErr *sqlite.Error
		if err == nil || !errors.As(err, &sqliteErr) || sqliteErr.Code()&255 != 5 || !time.Now().Before(deadline) {
			return tx, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
