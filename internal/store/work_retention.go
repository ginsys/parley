package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"
)

type WorkRef struct{ Kind, ID string }

func (w WorkRef) valid() bool {
	return (w.Kind == "envelope" || w.Kind == "pending" || w.Kind == "join") && w.ID != ""
}

// RecordAuthenticatedEnvelope is a transaction primitive called after exact
// session/credential/membership authorization. Its trigger compares immutable
// accepting identity against the actual envelope; callers cannot choose legacy.
func RecordAuthenticatedEnvelope(ctx context.Context, tx *sql.Tx, id, binding string, credentialVersion int64) error {
	if !validUUID(id) || !validUUID(binding) || credentialVersion < 1 {
		return InvalidRequest
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO work_provenance(work_kind,work_id,envelope_id,provenance,binding_id,credential_version,original_conversation,original_from_peer,original_to_peer,original_grant_version)
 SELECT 'envelope',id,id,'authenticated',?,?,conversation,from_peer,to_peer,grant_version FROM envelopes WHERE id=?`, binding, credentialVersion, id)
	if err != nil {
		return storageCode(err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return storageCode(err)
	}
	if count != 1 {
		return NotFound
	}
	return nil
}
func WorkHeld(ctx context.Context, tx *sql.Tx, w WorkRef) (bool, error) {
	return workRestriction(ctx, tx, w, false)
}
func WorkCancelled(ctx context.Context, tx *sql.Tx, w WorkRef) (bool, error) {
	return workRestriction(ctx, tx, w, true)
}
func workRestriction(ctx context.Context, tx *sql.Tx, w WorkRef, cancelOnly bool) (bool, error) {
	if !w.valid() {
		return false, InvalidRequest
	}
	var blocked bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM security_holds WHERE work_kind=? AND work_id=? AND (status='cancelled' OR (?=0 AND status='held')))
 OR EXISTS(SELECT 1 FROM migration_quarantine WHERE work_kind=? AND work_id=? AND (status='cancelled' OR (?=0 AND status='held')))`, w.Kind, w.ID, cancelOnly, w.Kind, w.ID, cancelOnly).Scan(&blocked)
	if err != nil {
		return false, storageCode(err)
	}
	return blocked, nil
}

type RevocationRequest struct {
	BindingID, IncidentID                             string
	ExpectedBindingVersion, ExpectedCredentialVersion int64
	Retire                                            bool
	NowNS                                             int64
}
type RevocationChange struct {
	BindingVersion, CredentialVersion, BarrierVersion int64
	IncidentID                                        string
}

// RevokeBinding is used inside an audited human command. pending contains only
// outstanding authored work supplied by #28's trusted admission implementation.
// Every insert is additionally checked against immutable accepting provenance.
func RevokeBinding(ctx context.Context, tx *sql.Tx, r RevocationRequest, pending []WorkRef) (RevocationChange, error) {
	if !validUUID(r.BindingID) || !validUUID(r.IncidentID) || r.ExpectedBindingVersion < 1 || r.ExpectedCredentialVersion < 1 {
		return RevocationChange{}, InvalidRequest
	}
	b, err := ReadBinding(ctx, tx, r.BindingID)
	if err != nil {
		return RevocationChange{}, err
	}
	credential, err := LatestCredential(ctx, tx, b.ID)
	if err != nil {
		return RevocationChange{}, err
	}
	if b.Version != r.ExpectedBindingVersion || credential.Version != r.ExpectedCredentialVersion {
		return RevocationChange{}, VersionConflict
	}
	if b.Status == "retired" || (!r.Retire && b.Status != "enabled") {
		return RevocationChange{}, BindingUnavailable
	}
	version, err := NextVersion(b.Version)
	if err != nil {
		return RevocationChange{}, err
	}
	var barrierVersion int64
	var barrierStatus string
	var paused sql.NullString
	err = tx.QueryRowContext(ctx, "SELECT barrier_version,status,paused_cursor FROM ingestion_barriers WHERE binding_id=?", b.ID).Scan(&barrierVersion, &barrierStatus, &paused)
	exists := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return RevocationChange{}, storageCode(err)
	}
	nextBarrier, err := NextVersion(barrierVersion)
	if err != nil {
		return RevocationChange{}, err
	}
	if barrierStatus != "held" {
		err = tx.QueryRowContext(ctx, "SELECT cursor FROM ingestion_cursors WHERE binding_id=?", b.ID).Scan(&paused)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return RevocationChange{}, storageCode(err)
		}
	}
	status := "revoked"
	if r.Retire {
		status = "retired"
	}
	if _, err := tx.ExecContext(ctx, "UPDATE bindings SET status=?,binding_version=? WHERE binding_id=?", status, version, b.ID); err != nil {
		return RevocationChange{}, storageCode(err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE credentials SET status='revoked' WHERE binding_id=? AND status='current'", b.ID); err != nil {
		return RevocationChange{}, storageCode(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO revocation_incidents(incident_id,binding_id,credential_version,binding_version,kind,created_at_ns) VALUES(?,?,?,?,?,?)`, r.IncidentID, b.ID, credential.Version, version, status, r.NowNS); err != nil {
		return RevocationChange{}, storageCode(err)
	}
	if exists {
		_, err = tx.ExecContext(ctx, "UPDATE ingestion_barriers SET barrier_version=?,status='held',paused_cursor=?,disposition_ref=NULL WHERE binding_id=?", nextBarrier, paused, b.ID)
	} else {
		_, err = tx.ExecContext(ctx, "INSERT INTO ingestion_barriers(binding_id,barrier_version,status,paused_cursor) VALUES(?,?,'held',?)", b.ID, nextBarrier, paused)
	}
	if err != nil {
		return RevocationChange{}, storageCode(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO ingestion_barrier_incidents(binding_id,incident_id,barrier_version) VALUES(?,?,?)", b.ID, r.IncidentID, nextBarrier); err != nil {
		return RevocationChange{}, storageCode(err)
	}
	cursor := ""
	first := true
	for {
		rows, err := tx.QueryContext(ctx, `SELECT p.work_id FROM work_provenance p JOIN envelopes e ON e.id=p.envelope_id
 WHERE p.work_kind='envelope' AND (p.binding_id=? OR (p.provenance='legacy' AND p.original_from_peer=?))
 AND e.state IN ('queued','dispatching','handed_off','uncertain') AND (? OR p.work_id>?) ORDER BY p.work_id LIMIT 100`, b.ID, b.PeerID, first, cursor)
		if err != nil {
			return RevocationChange{}, storageCode(err)
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return RevocationChange{}, storageCode(err)
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return RevocationChange{}, storageCode(err)
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			if err := insertRevocationHold(ctx, tx, WorkRef{"envelope", id}, r.IncidentID); err != nil {
				return RevocationChange{}, err
			}
		}
		cursor = ids[len(ids)-1]
		first = false
	}
	for _, work := range pending {
		if !work.valid() || work.Kind == "envelope" {
			return RevocationChange{}, InvalidRequest
		}
		if err := insertRevocationHold(ctx, tx, work, r.IncidentID); err != nil {
			return RevocationChange{}, err
		}
	}
	return RevocationChange{version, credential.Version, nextBarrier, r.IncidentID}, nil
}
func insertRevocationHold(ctx context.Context, tx *sql.Tx, w WorkRef, incident string) error {
	id, err := uuid.NewRandom()
	if err != nil {
		return TemporarilyUnavailable
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO security_holds(hold_id,work_kind,work_id,incident_id,revocation_incident_id) VALUES(?,?,?,?,?)", id.String(), w.Kind, w.ID, incident, incident)
	if err != nil {
		return storageCode(err)
	}
	return nil
}
