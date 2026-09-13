package store

import (
	"context"
	"database/sql"
	"errors"
)

type ClockCheckpoint struct {
	Version int64
	Instant sql.NullInt64
}

func ReadClockCheckpoint(ctx context.Context, tx *sql.Tx) (ClockCheckpoint, error) {
	var checkpoint ClockCheckpoint
	err := tx.QueryRowContext(ctx, "SELECT checkpoint_version,last_trusted_ns FROM clock_checkpoint WHERE singleton=1").Scan(&checkpoint.Version, &checkpoint.Instant)
	if err != nil {
		return checkpoint, storageCode(err)
	}
	return checkpoint, nil
}
func AdvanceClockCheckpoint(ctx context.Context, tx *sql.Tx, instant int64) (bool, error) {
	checkpoint, err := ReadClockCheckpoint(ctx, tx)
	if err != nil {
		return false, err
	}
	if checkpoint.Instant.Valid && instant < checkpoint.Instant.Int64 {
		return false, RecoveryRequired
	}
	if checkpoint.Instant.Valid && instant == checkpoint.Instant.Int64 {
		return false, nil
	}
	next, err := NextVersion(checkpoint.Version)
	if err != nil {
		return false, err
	}
	_, err = tx.ExecContext(ctx, "UPDATE clock_checkpoint SET last_trusted_ns=?,checkpoint_version=? WHERE singleton=1", instant, next)
	if err != nil {
		return false, storageCode(err)
	}
	return true, nil
}

type RecoveryRecord struct {
	ID, ServerID, Kind, Status string
	Version                    int64
	Floor, Observed            sql.NullInt64
	Evidence                   sql.NullString
}

func ReadRecovery(ctx context.Context, tx *sql.Tx, id string) (RecoveryRecord, error) {
	var r RecoveryRecord
	err := tx.QueryRowContext(ctx, "SELECT incident_id,server_id,kind,status,recovery_version,last_trusted_ns,observed_ns,evidence_ref FROM recovery_incidents WHERE incident_id=?", id).Scan(&r.ID, &r.ServerID, &r.Kind, &r.Status, &r.Version, &r.Floor, &r.Observed, &r.Evidence)
	if errors.Is(err, sql.ErrNoRows) {
		return r, NotFound
	}
	if err != nil {
		return r, storageCode(err)
	}
	return r, nil
}
func RecordRecovery(ctx context.Context, tx *sql.Tx, r RecoveryRecord) (bool, error) {
	if !validUUID(r.ID) || !validUUID(r.ServerID) || (r.Kind != "clock" && r.Kind != "restore") || r.Version != 1 || r.Status != "held" || r.Evidence.Valid {
		return false, InvalidRequest
	}
	if (r.Kind == "clock") != r.Floor.Valid || (r.Kind == "clock") != r.Observed.Valid {
		return false, InvalidRequest
	}
	var server string
	if err := tx.QueryRowContext(ctx, "SELECT server_id FROM installation WHERE singleton=1").Scan(&server); err != nil {
		return false, storageCode(err)
	}
	if server != r.ServerID {
		return false, RecoveryRequired
	}
	old, err := ReadRecovery(ctx, tx, r.ID)
	if err == nil {
		if old.ServerID != r.ServerID || old.Kind != r.Kind || old.Floor != r.Floor || old.Observed != r.Observed {
			return false, RecoveryRequired
		}
		// Reconciled records still hold work during marker-removal retries.
		// Cleared identities are terminal and cannot record a new incident.
		if old.Status == "cleared" {
			return false, RecoveryRequired
		}
		return false, nil
	}
	if err != NotFound {
		return false, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO recovery_incidents(incident_id,server_id,kind,last_trusted_ns,observed_ns) VALUES(?,?,?,?,?)", r.ID, r.ServerID, r.Kind, r.Floor, r.Observed)
	if err != nil {
		return false, storageCode(err)
	}
	return true, nil
}
func RecoveryHeld(ctx context.Context, tx *sql.Tx) (bool, error) {
	var held bool
	err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM recovery_incidents WHERE status!='cleared')").Scan(&held)
	if err != nil {
		return false, storageCode(err)
	}
	return held, nil
}
func ReconcileRecovery(ctx context.Context, tx *sql.Tx, id string, expected int64, evidence string) (ResourceChange, error) {
	if !validUUID(id) || !validUUID(evidence) || expected < 1 {
		return ResourceChange{}, InvalidRequest
	}
	r, err := ReadRecovery(ctx, tx, id)
	if err != nil {
		return ResourceChange{}, err
	}
	if r.Version != expected {
		return ResourceChange{}, VersionConflict
	}
	if r.Status != "held" {
		return ResourceChange{}, RequestTerminal
	}
	next, err := NextVersion(r.Version)
	if err != nil {
		return ResourceChange{}, err
	}
	if _, err := NextVersion(next); err != nil {
		return ResourceChange{}, err
	}
	_, err = tx.ExecContext(ctx, "UPDATE recovery_incidents SET status='reconciled',recovery_version=?,evidence_ref=? WHERE incident_id=?", next, evidence, id)
	if err != nil {
		return ResourceChange{}, storageCode(err)
	}
	return ResourceChange{Kind: "recovery_incident", ID: id, Before: r.Version, After: next}, nil
}

// ClearRecovery is trusted post-marker-removal maintenance. Only the exact
// audited reconciled record may be cleared; it creates no new authority itself.
func ClearRecovery(ctx context.Context, tx *sql.Tx, id string, expected int64, evidence string) (bool, error) {
	if !validUUID(id) || !validUUID(evidence) || expected < 1 {
		return false, InvalidRequest
	}
	expectedCleared, err := NextVersion(expected)
	if err != nil {
		return false, err
	}
	r, err := ReadRecovery(ctx, tx, id)
	if err != nil {
		return false, err
	}
	if r.Evidence.String != evidence || !r.Evidence.Valid {
		return false, RecoveryRequired
	}
	if r.Status == "cleared" && r.Version == expectedCleared {
		return false, nil
	}
	if r.Status != "reconciled" || r.Version != expected {
		return false, VersionConflict
	}
	next, err := NextVersion(r.Version)
	if err != nil {
		return false, err
	}
	_, err = tx.ExecContext(ctx, "UPDATE recovery_incidents SET status='cleared',recovery_version=? WHERE incident_id=?", next, id)
	if err != nil {
		return false, storageCode(err)
	}
	return true, nil
}
