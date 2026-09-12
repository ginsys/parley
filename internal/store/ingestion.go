package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"unicode/utf8"
)

// SourceEvent is verified by the deterministic host adapter before entering the
// writer. IDs, revisions and cursor links are host evidence, never reconnect IDs.
// Digest covers the source record plus its routing/interpretation context.
type SourceEvent struct {
	BindingID, EventID, SourceID, Revision string
	Digest                                 [32]byte
	Before, After                          string
}
type EventResult struct {
	Classification string `json:"classification"`
	EnvelopeID     string `json:"envelope_id,omitempty"`
	Code           Code   `json:"code,omitempty"`
	Replayed       bool   `json:"-"`
}

func validLocator(s string, empty bool) bool {
	return (empty || s != "") && len(s) <= MaxLocatorBytes && utf8.ValidString(s)
}
func (e SourceEvent) valid() bool {
	return validUUID(e.BindingID) && validUUID(e.SourceID) && validLocator(e.EventID, false) && validLocator(e.Revision, false) && validLocator(e.Before, true) && validLocator(e.After, true) && e.Before != e.After
}

// StageEvent retains a pending reference even if its original delivery or binding
// is not yet usable. Its caller must commit that reference without advancing the
// cursor. Event identity conflicts never produce a second effect.
func LookupEvent(ctx context.Context, tx *sql.Tx, e SourceEvent) (EventResult, bool, error) {
	if !e.valid() {
		return EventResult{}, false, InvalidRequest
	}
	var source, revision, before, after, classification string
	var digest []byte
	var result sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT source_id,source_revision,source_digest,cursor_before,cursor_after,classification,result_json FROM ingestion_evidence WHERE binding_id=? AND native_event_id=?`, e.BindingID, e.EventID).Scan(&source, &revision, &digest, &before, &after, &classification, &result)
	if err == nil {
		if source != e.SourceID || revision != e.Revision || string(digest) != string(e.Digest[:]) || before != e.Before || after != e.After {
			return EventResult{}, false, EventConflict
		}
		if classification == "pending" {
			return EventResult{Classification: "pending"}, true, nil
		}
		var saved EventResult
		if err := json.Unmarshal([]byte(result.String), &saved); err != nil {
			return EventResult{}, false, RecoveryRequired
		}
		saved.Replayed = true
		return saved, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return EventResult{}, false, storageCode(err)
	}
	return EventResult{}, false, nil
}

func StageEvent(ctx context.Context, tx *sql.Tx, e SourceEvent) (EventResult, error) {
	previous, found, err := LookupEvent(ctx, tx, e)
	if err != nil {
		return EventResult{}, err
	}
	if found {
		return previous, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO ingestion_evidence(binding_id,native_event_id,source_id,source_revision,source_digest,cursor_before,cursor_after,classification) VALUES(?,?,?,?,?,?,?,'pending')`, e.BindingID, e.EventID, e.SourceID, e.Revision, e.Digest[:], e.Before, e.After); err != nil {
		return EventResult{}, storageCode(err)
	}
	return EventResult{Classification: "pending"}, nil
}

// EventAtCursor checks the exact opaque predecessor. The initial cursor may only
// be established by trusted source initialization or an audited resume interval.
func EventAtCursor(ctx context.Context, tx *sql.Tx, e SourceEvent) error {
	var source, cursor string
	err := tx.QueryRowContext(ctx, "SELECT source_id,cursor FROM ingestion_cursors WHERE binding_id=?", e.BindingID).Scan(&source, &cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return HostUnverified
	}
	if err != nil {
		return storageCode(err)
	}
	if source != e.SourceID {
		return HostUnverified
	}
	if cursor != e.Before {
		return TemporarilyUnavailable
	}
	var other bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_evidence WHERE binding_id=? AND source_id=? AND cursor_before=? AND native_event_id!=?)`, e.BindingID, e.SourceID, e.Before, e.EventID).Scan(&other)
	if err != nil {
		return storageCode(err)
	}
	if other {
		return EventConflict
	}
	return nil
}

// InitializeIngestionSource requires freshly verified source origin evidence. It
// cannot replace a cursor, change source identity, or clear a revocation barrier.
func InitializeIngestionSource(ctx context.Context, tx *sql.Tx, binding, source, cursor string) error {
	if !validUUID(binding) || !validUUID(source) || !validLocator(cursor, true) {
		return InvalidRequest
	}
	var held bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM ingestion_barriers WHERE binding_id=? AND status='held')", binding).Scan(&held); err != nil {
		return storageCode(err)
	}
	if held {
		return SecurityHold
	}
	var oldSource, oldCursor string
	err := tx.QueryRowContext(ctx, "SELECT source_id,cursor FROM ingestion_cursors WHERE binding_id=?", binding).Scan(&oldSource, &oldCursor)
	if err == nil {
		if oldSource == source && oldCursor == cursor {
			return nil
		}
		return VersionConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return storageCode(err)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO ingestion_cursors(binding_id,source_id,cursor) VALUES(?,?,?)", binding, source, cursor)
	if err != nil {
		return storageCode(err)
	}
	return nil
}
func IngestionAllowed(ctx context.Context, tx *sql.Tx, binding string) error {
	var held bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_barriers WHERE binding_id=? AND status='held') OR EXISTS(SELECT 1 FROM retired_namespaces WHERE principal_id=?)`, binding, binding).Scan(&held)
	if err != nil {
		return storageCode(err)
	}
	if held {
		return SecurityHold
	}
	return nil
}

// FinishEvent is in the same writer transaction as any original ACK and reply
// insertion. Neither pending nor terminal evidence may be skipped or rewritten.
func FinishEvent(ctx context.Context, tx *sql.Tx, e SourceEvent, result EventResult, credential int64) error {
	if err := EventAtCursor(ctx, tx, e); err != nil {
		return err
	}
	switch result.Classification {
	case "accepted":
		if !validUUID(result.EnvelopeID) || credential < 1 {
			return InvalidRequest
		}
	case "no_marker", "malformed", "rejected", "held":
		if result.EnvelopeID != "" || credential != 0 {
			return InvalidRequest
		}
	default:
		return InvalidRequest
	}
	if !result.Code.valid() {
		return InvalidRequest
	}
	data, err := json.Marshal(result)
	if err != nil {
		return InvalidRequest
	}
	var version int64
	if err := tx.QueryRowContext(ctx, "SELECT cursor_version FROM ingestion_cursors WHERE binding_id=?", e.BindingID).Scan(&version); err != nil {
		return storageCode(err)
	}
	next, err := NextVersion(version)
	if err != nil {
		return err
	}
	var cv any
	if credential > 0 {
		cv = credential
	}
	res, err := tx.ExecContext(ctx, `UPDATE ingestion_evidence SET classification=?,result_json=?,accepted_credential_version=? WHERE binding_id=? AND native_event_id=? AND classification='pending' AND source_id=? AND source_revision=? AND source_digest=? AND cursor_before=? AND cursor_after=?`, result.Classification, string(data), cv, e.BindingID, e.EventID, e.SourceID, e.Revision, e.Digest[:], e.Before, e.After)
	if err != nil {
		return storageCode(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return storageCode(err)
	}
	if n != 1 {
		return EventConflict
	}
	_, err = tx.ExecContext(ctx, "UPDATE ingestion_cursors SET cursor=?,cursor_version=? WHERE binding_id=?", e.After, next, e.BindingID)
	if err != nil {
		return storageCode(err)
	}
	return nil
}
