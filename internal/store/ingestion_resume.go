package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"
)

// ReviewedInterval comes only from a trusted immutable human evidence resolver.
// Every event in the complete reviewed interval is retained as held, never ACKed.
// An empty interval is valid only when Before==After and the resolver proves it.
type ReviewedInterval struct {
	SourceID, Before, After string
	Events                  []SourceEvent
}
type ResumeIngestionRequest struct {
	BindingID                                      string
	ExpectedBindingVersion, ExpectedBarrierVersion int64
	EvidenceRef, PrincipalID, OperationID          string
	Interval                                       ReviewedInterval
}

func ResumeIngestion(ctx context.Context, tx *sql.Tx, r ResumeIngestionRequest) (ResourceChange, error) {
	if !validUUID(r.BindingID) || !validUUID(r.EvidenceRef) || !validUUID(r.PrincipalID) || !validUUID(r.OperationID) || !validUUID(r.Interval.SourceID) || r.ExpectedBindingVersion < 1 || r.ExpectedBarrierVersion < 1 || !validLocator(r.Interval.Before, true) || !validLocator(r.Interval.After, true) {
		return ResourceChange{}, InvalidRequest
	}
	// Evidence materialization is bounded. Larger intervals retain the barrier
	// and fail with capacity_exceeded; no partial resume or cursor advance occurs.
	if len(r.Interval.Events) > 1000 {
		return ResourceChange{}, CapacityExceeded
	}
	b, err := ReadBinding(ctx, tx, r.BindingID)
	if err != nil {
		return ResourceChange{}, err
	}
	if b.Version != r.ExpectedBindingVersion {
		return ResourceChange{}, VersionConflict
	}
	if b.Status != "enabled" {
		return ResourceChange{}, BindingUnavailable
	}
	var barrier int64
	var status string
	var paused sql.NullString
	err = tx.QueryRowContext(ctx, "SELECT barrier_version,status,paused_cursor FROM ingestion_barriers WHERE binding_id=?", b.ID).Scan(&barrier, &status, &paused)
	if errors.Is(err, sql.ErrNoRows) {
		return ResourceChange{}, NotFound
	}
	if err != nil {
		return ResourceChange{}, storageCode(err)
	}
	if barrier != r.ExpectedBarrierVersion {
		return ResourceChange{}, VersionConflict
	}
	if status != "held" {
		return ResourceChange{}, RequestTerminal
	}
	next, err := NextVersion(barrier)
	if err != nil {
		return ResourceChange{}, err
	}
	var source, cursor string
	err = tx.QueryRowContext(ctx, "SELECT source_id,cursor FROM ingestion_cursors WHERE binding_id=?", b.ID).Scan(&source, &cursor)
	if errors.Is(err, sql.ErrNoRows) {
		if paused.Valid {
			return ResourceChange{}, RecoveryRequired
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO ingestion_cursors(binding_id,source_id,cursor) VALUES(?,?,?)", b.ID, r.Interval.SourceID, r.Interval.Before)
		source, cursor = r.Interval.SourceID, r.Interval.Before
	}
	if err != nil {
		return ResourceChange{}, storageCode(err)
	}
	if source != r.Interval.SourceID || cursor != r.Interval.Before || (paused.Valid && paused.String != r.Interval.Before) {
		return ResourceChange{}, VersionConflict
	}
	for _, event := range r.Interval.Events {
		if event.BindingID != b.ID || event.SourceID != source || event.Before != cursor {
			return ResourceChange{}, InvalidRequest
		}
		prior, err := StageEvent(ctx, tx, event)
		if err != nil {
			return ResourceChange{}, err
		}
		if prior.Replayed {
			return ResourceChange{}, EventConflict
		}
		if err := FinishEvent(ctx, tx, event, EventResult{Classification: "held", Code: SecurityHold}, 0); err != nil {
			return ResourceChange{}, err
		}
		cursor = event.After
	}
	if cursor != r.Interval.After {
		return ResourceChange{}, InvalidRequest
	}
	// Pending events at the resume boundary may be retried afterwards; events in
	// the reviewed interval have permanent held results and cannot be accepted.
	id, err := uuid.NewRandom()
	if err != nil {
		return ResourceChange{}, TemporarilyUnavailable
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO ingestion_dispositions(disposition_id,binding_id,barrier_version,source_id,cursor_before,cursor_after,evidence_ref,audit_principal_id,audit_operation_id) VALUES(?,?,?,?,?,?,?,?,?)`, id.String(), b.ID, barrier, source, r.Interval.Before, cursor, r.EvidenceRef, r.PrincipalID, r.OperationID)
	if err != nil {
		return ResourceChange{}, storageCode(err)
	}
	_, err = tx.ExecContext(ctx, "UPDATE ingestion_barriers SET status='resolved',barrier_version=?,disposition_ref=? WHERE binding_id=?", next, id.String(), b.ID)
	if err != nil {
		return ResourceChange{}, storageCode(err)
	}
	return ResourceChange{Kind: "ingestion_barrier", ID: b.ID, Before: barrier, After: next}, nil
}
