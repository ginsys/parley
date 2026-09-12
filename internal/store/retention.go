package store

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	"github.com/google/uuid"
)

//go:embed retention_schema.sql
var retentionSchema string

//go:embed retention_guards.sql
var retentionGuards string

func addWorkRetention(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, retentionSchema); err != nil {
		return err
	}
	incident, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO migration_incidents(incident_id,from_schema_version,migration_version) VALUES(?,5,6)", incident.String()); err != nil {
		return err
	}
	// One atomic migration, bounded memory. Original identifiers may be incompatible
	// with current wire rules; preserve their exact bytes without invented bindings.
	cursor := ""
	first := true
	for {
		rows, err := tx.QueryContext(ctx, `SELECT id,conversation,from_peer,to_peer,grant_version,state FROM envelopes WHERE ? OR id>? ORDER BY id LIMIT 100`, first, cursor)
		if err != nil {
			return err
		}
		type legacyRow struct {
			id, conversation, from, to, state string
			version                           int64
		}
		var batch []legacyRow
		for rows.Next() {
			var row legacyRow
			if err := rows.Scan(&row.id, &row.conversation, &row.from, &row.to, &row.version, &row.state); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, row)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}
		for _, row := range batch {
			if _, err := tx.ExecContext(ctx, `INSERT INTO work_provenance(work_kind,work_id,envelope_id,provenance,migration_incident_id,original_conversation,original_from_peer,original_to_peer,original_grant_version) VALUES('envelope',?,?,'legacy',?,?,?,?,?)`, row.id, row.id, incident.String(), row.conversation, row.from, row.to, row.version); err != nil {
				return err
			}
			switch EnvelopeState(row.state) {
			case Queued, Dispatching, HandedOff, Uncertain:
				id, err := uuid.NewRandom()
				if err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO migration_quarantine(quarantine_id,incident_id,work_id) VALUES(?,?,?)`, id.String(), incident.String(), row.id); err != nil {
					return err
				}
			case Acked, Failed, Cancelled:
			default:
				return InvalidRequest
			}
		}
		cursor = batch[len(batch)-1].id
		first = false
	}
	if _, err := tx.ExecContext(ctx, `CREATE TRIGGER provenance_no_new_legacy BEFORE INSERT ON work_provenance WHEN NEW.provenance='legacy' BEGIN SELECT RAISE(ABORT,'legacy provenance is migration-only'); END`); err != nil {
		return err
	}
	return protectRetention(ctx, tx)
}
func protectRetention(ctx context.Context, tx *sql.Tx) error {
	// Names are closed implementation constants, never identifiers from a request.
	immutable := []string{"migration_incidents", "work_provenance", "revocation_incidents", "work_dispositions", "ingestion_barrier_incidents", "ingestion_dispositions", "retired_namespaces", "recovery_clock_dispositions"}
	retained := append(append([]string{}, immutable...), "migration_quarantine", "security_holds", "ingestion_cursors", "ingestion_evidence", "ingestion_barriers", "clock_checkpoint", "recovery_incidents")
	for _, table := range immutable {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("CREATE TRIGGER %s_immutable BEFORE UPDATE ON %s BEGIN SELECT RAISE(ABORT,'immutable retained evidence'); END", table, table)); err != nil {
			return err
		}
	}
	for _, table := range retained {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("CREATE TRIGGER %s_retained BEFORE DELETE ON %s BEGIN SELECT RAISE(ABORT,'evidence retained'); END", table, table)); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, retentionGuards)
	return err
}
