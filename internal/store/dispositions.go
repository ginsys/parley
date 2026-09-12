package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
)

type DispositionRequest struct {
	ReasonNote                      *string
	Legacy                          bool
	Work                            WorkRef
	IncidentID                      string
	ExpectedVersion                 int64
	Action, ReasonCode, EvidenceRef string
	PrincipalID, OperationID        string
	NowNS                           int64
}

// ApplyWorkDisposition runs inside Execute so its deferred audit FK and effect
// commit together. Trusted evidence resolution and reason-note validation belong
// to the human service. A pending-work callback is supplied by #28.
func ApplyWorkDisposition(ctx context.Context, tx *sql.Tx, r DispositionRequest, pending func(context.Context, *sql.Tx, WorkRef, string) error) (ResourceChange, error) {
	if !r.Work.valid() || !validUUID(r.IncidentID) || !validUUID(r.PrincipalID) || !validUUID(r.OperationID) || r.ExpectedVersion < 1 || (r.Action != "cancel" && r.Action != "release") {
		return ResourceChange{}, InvalidRequest
	}
	if r.Legacy && (r.Work.Kind != "envelope" || !validUUID(r.EvidenceRef)) {
		return ResourceChange{}, InvalidRequest
	}
	switch r.ReasonCode {
	case "owner_reviewed", "compromise", "retirement", "restore_reconciled", "cancelled":
	default:
		return ResourceChange{}, InvalidRequest
	}
	if r.Action == "release" && r.ReasonCode != "owner_reviewed" && r.ReasonCode != "restore_reconciled" {
		return ResourceChange{}, InvalidRequest
	}
	var id, status string
	var version int64
	var err error
	if r.Legacy {
		err = tx.QueryRowContext(ctx, "SELECT quarantine_id,status,quarantine_version FROM migration_quarantine WHERE incident_id=? AND work_kind=? AND work_id=?", r.IncidentID, r.Work.Kind, r.Work.ID).Scan(&id, &status, &version)
	} else {
		err = tx.QueryRowContext(ctx, "SELECT hold_id,status,hold_version FROM security_holds WHERE incident_id=? AND work_kind=? AND work_id=?", r.IncidentID, r.Work.Kind, r.Work.ID).Scan(&id, &status, &version)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return ResourceChange{}, NotFound
	}
	if err != nil {
		return ResourceChange{}, storageCode(err)
	}
	if version != r.ExpectedVersion {
		return ResourceChange{}, VersionConflict
	}
	if status == "cancelled" || (status == "released" && r.Action == "release") {
		return ResourceChange{}, RequestTerminal
	}
	next, err := NextVersion(version)
	if err != nil {
		return ResourceChange{}, err
	}
	if r.Work.Kind == "envelope" {
		e, err := GetByID(ctx, tx, r.Work.ID)
		if err != nil {
			return ResourceChange{}, storageCode(err)
		}
		if r.Legacy && r.Action == "release" && e.State != Queued && e.State != HandedOff {
			return ResourceChange{}, RequestTerminal
		}
		if r.Action == "cancel" && e.State == Queued {
			if err := SetState(ctx, tx, e.ID, Queued, Cancelled, time.Unix(0, r.NowNS).UTC().Format(time.RFC3339Nano)); err != nil {
				return ResourceChange{}, storageCode(err)
			}
		}
	} else {
		if pending == nil {
			return ResourceChange{}, BindingUnavailable
		}
		if err := pending(ctx, tx, r.Work, r.Action); err != nil {
			return ResourceChange{}, storageCode(err)
		}
	}
	targetStatus := "released"
	if r.Action == "cancel" {
		targetStatus = "cancelled"
	}
	var hold, quarantine any
	kind := "hold"
	if r.Legacy {
		kind = "quarantine"
		quarantine = id
		_, err = tx.ExecContext(ctx, "UPDATE migration_quarantine SET status=?,quarantine_version=? WHERE quarantine_id=?", targetStatus, next, id)
	} else {
		hold = id
		_, err = tx.ExecContext(ctx, "UPDATE security_holds SET status=?,hold_version=? WHERE hold_id=?", targetStatus, next, id)
	}
	if err != nil {
		return ResourceChange{}, storageCode(err)
	}
	disposition, err := uuid.NewRandom()
	if err != nil {
		return ResourceChange{}, TemporarilyUnavailable
	}
	var evidence any
	if r.EvidenceRef != "" {
		evidence = r.EvidenceRef
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO work_dispositions(disposition_id,hold_id,quarantine_id,previous_version,action,reason_code,reason_note,evidence_ref,audit_principal_id,audit_operation_id,created_at_ns) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, disposition.String(), hold, quarantine, version, r.Action, r.ReasonCode, r.ReasonNote, evidence, r.PrincipalID, r.OperationID, r.NowNS)
	if err != nil {
		return ResourceChange{}, storageCode(err)
	}
	return ResourceChange{Kind: kind, ID: id, Before: version, After: next}, nil
}
