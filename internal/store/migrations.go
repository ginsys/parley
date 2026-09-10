package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Each numbered step and user_version update commits in the same immediate
// transaction. Only version-zero adoption examines historical column layouts.
var migrations = []func(context.Context, *sql.Tx) error{adoptLegacySchema, addRenewalPolicy, addDeliveryOutcomes, addQueueOrdering}

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

// Frozen schema from a024019, before trusted reply provenance was introduced.
// Together with schema.sql (8af08cb) and the original 0edf451 ALTER, this
// describes every shipped version-zero representation. Preserve stored DDL,
// including comments inside CREATE statements: adoption intentionally accepts
// known catalogs, not arbitrary semantically equivalent SQL rewrites.
//
//go:embed legacy_schema.sql
var legacySchema string

const addTrustedReply = "ALTER TABLE envelopes ADD COLUMN is_trusted_reply INTEGER NOT NULL DEFAULT 0 CHECK (is_trusted_reply IN (0, 1))"

func adoptLegacySchema(ctx context.Context, tx *sql.Tx) error {
	actual, err := readSchemaCatalog(ctx, tx)
	if err != nil {
		return err
	}
	if len(actual) == 0 {
		_, err := tx.ExecContext(ctx, schema)
		return err
	}
	for _, variant := range []struct {
		ddl               string
		needsTrustedReply bool
	}{
		{schema, false},
		{legacySchema, true},
		{legacySchema + ";" + addTrustedReply, false},
	} {
		wanted, err := referenceCatalog(ctx, variant.ddl)
		if err != nil {
			return err
		}
		if !matchesLegacyCatalog(actual, wanted) {
			continue
		}
		if variant.needsTrustedReply {
			if _, err := tx.ExecContext(ctx, addTrustedReply); err != nil {
				return err
			}
		}
		// Recreate the two known indexes if missing. Conflicting active grant
		// history fails here and rolls back the column and version as well.
		_, err = tx.ExecContext(ctx, schema)
		return err
	}
	return fmt.Errorf("unrecognized unversioned schema")
}

type schemaObject struct {
	kind, name, table string
	ddl               sql.NullString
}

// sql.Tx and sql.DB both implement the inspection operations.
type schemaReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func readSchemaCatalog(ctx context.Context, db schemaReader) ([]schemaObject, error) {
	// Filter by owning table, not object name: automatic indexes belonging to
	// application tables are part of the contract even though their names start
	// with sqlite_. SQLite-owned statistics tables are not application schema.
	rows, err := db.QueryContext(ctx, "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name NOT GLOB 'sqlite_*'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var objects []schemaObject
	for rows.Next() {
		var object schemaObject
		if err := rows.Scan(&object.kind, &object.name, &object.table, &object.ddl); err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	return objects, rows.Err()
}

func referenceCatalog(ctx context.Context, ddl string) ([]schemaObject, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return nil, err
	}
	return readSchemaCatalog(ctx, db)
}

func matchesLegacyCatalog(actual, wanted []schemaObject) bool {
	remaining := make(map[schemaObject]bool, len(actual))
	for _, object := range actual {
		remaining[object] = true
	}
	for _, object := range wanted {
		if remaining[object] {
			delete(remaining, object)
			continue
		}
		if object.kind == "index" && (object.name == "idx_grants_one_active" || object.name == "idx_envelopes_conversation_state") {
			continue
		}
		return false
	}
	// A changed named index remains here, as do extra indexes, triggers/views,
	// or tables. Table SQL covers CHECKs, UNIQUEs, collations, and table options.
	return len(remaining) == 0
}

// SQLite can reject concurrent journal-mode initialization before its busy
// handler waits; shared-cache writers instead return LOCKED_SHAREDCACHE.
// Retrying either failed Begin is safe: no migration SQL has run.
func beginMigration(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		tx, err := db.BeginTx(ctx, nil)
		var sqliteErr *sqlite.Error
		if err == nil || !errors.As(err, &sqliteErr) || (sqliteErr.Code()&255 != sqlite3.SQLITE_BUSY && sqliteErr.Code() != sqlite3.SQLITE_LOCKED_SHAREDCACHE) || !time.Now().Before(deadline) {
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

func addRenewalPolicy(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "ALTER TABLE grants ADD COLUMN cancel_pending_replies INTEGER NOT NULL DEFAULT 0 CHECK(cancel_pending_replies IN (0,1))")
	return err
}

func addDeliveryOutcomes(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE envelopes ADD COLUMN dispatch_attempt INTEGER NOT NULL DEFAULT 0;
 ALTER TABLE envelopes ADD COLUMN error_code TEXT NOT NULL DEFAULT '';
 ALTER TABLE envelopes ADD COLUMN error_detail TEXT NOT NULL DEFAULT '';`)
	return err
}
