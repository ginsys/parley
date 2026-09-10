package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func timestampNanos(value string) (int64, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return 0, fmt.Errorf("invalid envelope timestamp: %w", err)
	}
	n := parsed.UnixNano()
	if !time.Unix(0, n).Equal(parsed) {
		return 0, fmt.Errorf("envelope timestamp is outside nanosecond range")
	}
	return n, nil
}

func addQueueOrdering(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, "ALTER TABLE envelopes ADD COLUMN created_at_ns INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	// Read only IDs and timestamps in bounded pages; close each cursor before writes.
	var after string
	first := true
	for {
		rows, err := tx.QueryContext(ctx, "SELECT id,created_at FROM envelopes WHERE (? OR id>?) ORDER BY id LIMIT 100", first, after)
		if err != nil {
			return err
		}
		type entry struct {
			id string
			ns int64
		}
		var entries []entry
		for rows.Next() {
			var id, stamp string
			if err := rows.Scan(&id, &stamp); err != nil {
				rows.Close()
				return err
			}
			ns, err := timestampNanos(stamp)
			if err != nil {
				rows.Close()
				return fmt.Errorf("migrate envelope %q: %w", id, err)
			}
			entries = append(entries, entry{id, ns})
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			break
		}
		for _, e := range entries {
			if _, err := tx.ExecContext(ctx, "UPDATE envelopes SET created_at_ns=? WHERE id=?", e.ns, e.id); err != nil {
				return err
			}
		}
		after = entries[len(entries)-1].id
		first = false
	}
	_, err := tx.ExecContext(ctx, "CREATE INDEX idx_envelopes_recipient_queue ON envelopes(conversation,state,to_peer,created_at_ns,id)")
	return err
}
