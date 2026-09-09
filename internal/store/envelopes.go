package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var ErrEnvelopeNotFound = errors.New("envelope not found")

// InsertQueued writes a new envelope in the queued state. GrantVersion must
// already be stamped by the caller from CurrentGrant at accept time.
func InsertQueued(ctx context.Context, tx *Tx, e Envelope) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO envelopes (id, conversation, from_peer, to_peer, text, grant_version,
		                        in_reply_to, state, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)`,
		e.ID, e.Conversation, e.FromPeer, e.ToPeer, e.Text, e.GrantVersion,
		e.InReplyTo, e.CreatedAt, e.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert queued envelope: %w", err)
	}
	return nil
}

// ListQueued returns queued envelopes for a conversation, oldest first —
// the order the single-process dispatcher claims them in.
func ListQueued(ctx context.Context, tx *Tx, conversation string) ([]Envelope, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, conversation, from_peer, to_peer, text, grant_version, in_reply_to,
		       state, created_at, updated_at
		FROM envelopes
		WHERE conversation = ? AND state = 'queued'
		ORDER BY created_at ASC`, conversation)
	if err != nil {
		return nil, fmt.Errorf("list queued: %w", err)
	}
	defer rows.Close()
	return scanEnvelopes(rows)
}

// CancelQueued transitions every queued row for a conversation to cancelled,
// regardless of grant_version. Used by revoke.
func CancelQueued(ctx context.Context, tx *Tx, conversation, updatedAt string) (int64, error) {
	return cancelQueuedWhere(ctx, tx, `conversation = ? AND state = 'queued'`, updatedAt, conversation)
}

// CancelQueuedUnderVersion transitions queued rows stamped with the given
// (soon-to-be-superseded) grant_version to cancelled. Used by renewal: a
// message accepted under the old grant never silently carries forward under
// the new one.
func CancelQueuedUnderVersion(ctx context.Context, tx *Tx, conversation string, version int64, updatedAt string) (int64, error) {
	return cancelQueuedWhere(ctx, tx,
		`conversation = ? AND grant_version = ? AND state = 'queued'`, updatedAt, conversation, version)
}

func cancelQueuedWhere(ctx context.Context, tx *Tx, where, updatedAt string, args ...any) (int64, error) {
	execArgs := append([]any{updatedAt}, args...)
	res, err := tx.Exec(ctx, `UPDATE envelopes SET state = 'cancelled', updated_at = ? WHERE `+where, execArgs...)
	if err != nil {
		return 0, fmt.Errorf("cancel queued: %w", err)
	}
	return res.RowsAffected()
}

// TransitionToDispatching claims one queued envelope for dispatch. Returns
// false (no error) if the row is no longer queued — already cancelled by a
// concurrent revoke/renewal that committed first, which is not a bug, it's
// the serialization this design relies on.
func TransitionToDispatching(ctx context.Context, tx *Tx, id, updatedAt string) (bool, error) {
	res, err := tx.Exec(ctx, `
		UPDATE envelopes SET state = 'dispatching', updated_at = ?
		WHERE id = ? AND state = 'queued'`, updatedAt, id)
	if err != nil {
		return false, fmt.Errorf("transition to dispatching: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("transition to dispatching: rows affected: %w", err)
	}
	return n == 1, nil
}

// SetState sets an envelope's terminal (or acked) state outside the
// dispatching transaction — after the host call returns, success, failure,
// or ambiguous.
func SetState(ctx context.Context, tx *Tx, id string, state EnvelopeState, updatedAt string) error {
	res, err := tx.Exec(ctx, `UPDATE envelopes SET state = ?, updated_at = ? WHERE id = ?`,
		string(state), updatedAt, id)
	if err != nil {
		return fmt.Errorf("set envelope state: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("set envelope state: rows affected: %w", err)
	}
	if n == 0 {
		return ErrEnvelopeNotFound
	}
	return nil
}

// GetByID fetches one envelope by id within the caller's transaction.
func GetByID(ctx context.Context, tx *Tx, id string) (*Envelope, error) {
	row := tx.QueryRow(ctx, `
		SELECT id, conversation, from_peer, to_peer, text, grant_version, in_reply_to,
		       state, created_at, updated_at
		FROM envelopes WHERE id = ?`, id)
	var e Envelope
	var state string
	if err := row.Scan(&e.ID, &e.Conversation, &e.FromPeer, &e.ToPeer, &e.Text,
		&e.GrantVersion, &e.InReplyTo, &state, &e.CreatedAt, &e.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrEnvelopeNotFound
		}
		return nil, fmt.Errorf("get envelope %s: %w", id, err)
	}
	e.State = EnvelopeState(state)
	return &e, nil
}

func scanEnvelopes(rows *sql.Rows) ([]Envelope, error) {
	var out []Envelope
	for rows.Next() {
		var e Envelope
		var state string
		if err := rows.Scan(&e.ID, &e.Conversation, &e.FromPeer, &e.ToPeer, &e.Text,
			&e.GrantVersion, &e.InReplyTo, &state, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan envelope: %w", err)
		}
		e.State = EnvelopeState(state)
		out = append(out, e)
	}
	return out, rows.Err()
}
