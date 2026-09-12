package store

import (
	"context"
	"database/sql"
	"errors"
	"github.com/google/uuid"
)

type NamespaceRetirement struct {
	PrincipalID                       string
	BindingVersion, CredentialVersion int64
}

// RetireRecoveryNamespace permanently closes mutation/event identity reuse. For
// bindings, ordinary retirement also holds authored work and creates a barrier.
func RetireRecoveryNamespace(ctx context.Context, tx *sql.Tx, incident string, r NamespaceRetirement, now int64) error {
	if !validUUID(incident) || !validUUID(r.PrincipalID) {
		return InvalidRequest
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM retired_namespaces WHERE principal_id=?)", r.PrincipalID).Scan(&exists); err != nil {
		return storageCode(err)
	}
	if exists {
		return nil
	}
	binding, err := ReadBinding(ctx, tx, r.PrincipalID)
	var bindingID any
	if err == nil {
		if r.BindingVersion != binding.Version {
			return VersionConflict
		}
		credential, err := LatestCredential(ctx, tx, binding.ID)
		if err != nil {
			return err
		}
		if r.CredentialVersion != credential.Version {
			return VersionConflict
		}
		if binding.Status != "retired" {
			id, err := uuid.NewRandom()
			if err != nil {
				return TemporarilyUnavailable
			}
			if _, err := RevokeBinding(ctx, tx, RevocationRequest{BindingID: binding.ID, IncidentID: id.String(), ExpectedBindingVersion: r.BindingVersion, ExpectedCredentialVersion: r.CredentialVersion, Retire: true, NowNS: now}, nil); err != nil {
				return err
			}
		}
		bindingID = binding.ID
	} else if err != BindingUnavailable {
		return err
	} else if r.BindingVersion != 0 || r.CredentialVersion != 0 {
		return VersionConflict
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO retired_namespaces(principal_id,binding_id,incident_id,retired_at_ns) VALUES(?,?,?,?)", r.PrincipalID, bindingID, incident, now)
	if err != nil {
		return storageCode(err)
	}
	return nil
}

// HoldRestoredWork conservatively retains every outstanding envelope independently
// of snapshot delivery state. No ACK, budget or attempt evidence is rewritten.
func HoldRestoredWork(ctx context.Context, tx *sql.Tx, incident string, pending []WorkRef) error {
	if !validUUID(incident) {
		return InvalidRequest
	}
	cursor := ""
	first := true
	for {
		rows, err := tx.QueryContext(ctx, "SELECT id FROM envelopes WHERE state IN ('queued','dispatching','handed_off','uncertain') AND (? OR id>?) ORDER BY id LIMIT 100", first, cursor)
		if err != nil {
			return storageCode(err)
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return storageCode(err)
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return storageCode(err)
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			if err := insertRestoreHold(ctx, tx, incident, WorkRef{Kind: "envelope", ID: id}); err != nil {
				return err
			}
		}
		cursor = ids[len(ids)-1]
		first = false
	}
	for _, work := range pending {
		if work.Kind == "envelope" {
			return InvalidRequest
		}
		if err := insertRestoreHold(ctx, tx, incident, work); err != nil {
			return err
		}
	}
	return nil
}
func insertRestoreHold(ctx context.Context, tx *sql.Tx, incident string, work WorkRef) error {
	if !work.valid() {
		return InvalidRequest
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM security_holds WHERE work_kind=? AND work_id=? AND incident_id=?)", work.Kind, work.ID, incident).Scan(&exists); err != nil {
		return storageCode(err)
	}
	if exists {
		return nil
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return TemporarilyUnavailable
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO security_holds(hold_id,work_kind,work_id,incident_id,recovery_incident_id) VALUES(?,?,?,?,?)", id.String(), work.Kind, work.ID, incident, incident)
	if err != nil {
		return storageCode(err)
	}
	return nil
}

type ReviewedClockFloor struct{ CheckpointVersion, Previous, Reviewed int64 }

// ResetReviewedClockFloor is only part of broader restore/retirement disposition.
// Its immutable audit-linked record is matched by migration6's exact-version guard.
func ResetReviewedClockFloor(ctx context.Context, tx *sql.Tx, incident string, f ReviewedClockFloor, principal, operation string) error {
	if !validUUID(incident) || !validUUID(principal) || !validUUID(operation) || f.CheckpointVersion < 1 || f.Reviewed >= f.Previous {
		return InvalidRequest
	}
	checkpoint, err := ReadClockCheckpoint(ctx, tx)
	if err != nil {
		return err
	}
	if checkpoint.Version != f.CheckpointVersion || !checkpoint.Instant.Valid || checkpoint.Instant.Int64 != f.Previous {
		return VersionConflict
	}
	next, err := NextVersion(checkpoint.Version)
	if err != nil {
		return err
	}
	// At least one affected authority namespace must be permanently retired by
	// this broader incident; a standalone clock reconciliation cannot use this path.
	var retired bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM retired_namespaces WHERE incident_id=?)", incident).Scan(&retired); err != nil {
		return storageCode(err)
	}
	if !retired {
		return Forbidden
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO recovery_clock_dispositions(incident_id,checkpoint_version,previous_floor_ns,reviewed_floor_ns,audit_principal_id,audit_operation_id) VALUES(?,?,?,?,?,?)", incident, f.CheckpointVersion, f.Previous, f.Reviewed, principal, operation)
	if err != nil {
		return storageCode(err)
	}
	_, err = tx.ExecContext(ctx, "UPDATE clock_checkpoint SET last_trusted_ns=?,checkpoint_version=? WHERE singleton=1", f.Reviewed, next)
	if err != nil {
		return storageCode(err)
	}
	return nil
}

// CheckRetiredMutation is also available to trusted administrative consumers.
func CheckRetiredMutation(ctx context.Context, tx *sql.Tx, principal string) error {
	var id string
	err := tx.QueryRowContext(ctx, "SELECT principal_id FROM retired_namespaces WHERE principal_id=?", principal).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return storageCode(err)
	}
	return RecoveryRequired
}
