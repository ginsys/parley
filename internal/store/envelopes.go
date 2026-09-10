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
// TrustedReply must only ever be true when the caller is codex.IngestTurn.
func InsertQueued(ctx context.Context, tx *sql.Tx, e Envelope) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO envelopes (id, conversation, from_peer, to_peer, text, grant_version,
		                        in_reply_to, is_trusted_reply, state, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)`,
		e.ID, e.Conversation, e.FromPeer, e.ToPeer, e.Text, e.GrantVersion,
		e.InReplyTo, e.TrustedReply, e.CreatedAt, e.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert queued envelope: %w", err)
	}
	return nil
}

// ListQueued returns queued envelopes for a conversation, oldest first —
// the order the single-process dispatcher claims them in.
func ListQueued(ctx context.Context, tx *sql.Tx, conversation string) ([]Envelope, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, conversation, from_peer, to_peer, text, grant_version, in_reply_to,
		       is_trusted_reply, state, created_at, updated_at
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
func CancelQueued(ctx context.Context, tx *sql.Tx, conversation, updatedAt string) (int64, error) {
	return cancelQueuedWhere(ctx, tx, `conversation = ? AND state = 'queued'`, updatedAt, conversation)
}

// CancelQueuedUnderVersion transitions queued rows stamped with the given
// (soon-to-be-superseded) grant_version to cancelled. Used by renewal: a
// message accepted under the old grant never silently carries forward under
// the new one.
func CancelQueuedUnderVersion(ctx context.Context, tx *sql.Tx, conversation string, version int64, updatedAt string) (int64, error) {
	return cancelQueuedWhere(ctx, tx,
		`conversation = ? AND grant_version = ? AND state = 'queued'`, updatedAt, conversation, version)
}

// CarryForwardQueuedReplies re-stamps queued reply rows from oldVersion to
// newVersion instead of letting CancelQueuedUnderVersion cancel them. A
// reply's own originating envelope was already unconditionally marked
// 'acked' by IngestTurn before the reply was queued, so unlike a fresh Send,
// there is no live sender left to notice the cancellation and resubmit —
// cancelling a reply here would strand its content with no path back to
// delivery, even though the whole point of a renewal is to let an
// already-in-progress exchange continue. Must run after the new
// grant_version row exists (InsertGrant), since
// envelopes(conversation, grant_version) has an immediate foreign key
// against grants.
//
// A row only qualifies if is_trusted_reply was set at insert time — which
// only codex.IngestTurn ever does, atomically, after validating in_reply_to
// against the specific original it just acked (replymarker.Validate). Bare
// "in_reply_to IS NOT NULL" (or even "in_reply_to names some envelope this
// conversation has acked") is not provenance: dispatch.Bridge.Send takes an
// arbitrary caller-supplied inReplyTo with no validation at all, so an
// ordinary send naming any acked envelope's id — not necessarily the one it
// is actually replying to — could otherwise claim reply status and get
// silently carried across a renewal instead of cancelled like every other
// old-grant message.
func CarryForwardQueuedReplies(ctx context.Context, tx *sql.Tx, conversation string, oldVersion, newVersion int64, updatedAt string) (int64, error) {
	res, err := tx.ExecContext(ctx, `
		UPDATE envelopes SET grant_version = ?, updated_at = ?
		WHERE conversation = ? AND grant_version = ? AND state = 'queued' AND is_trusted_reply = 1`,
		newVersion, updatedAt, conversation, oldVersion)
	if err != nil {
		return 0, fmt.Errorf("carry forward queued replies: %w", err)
	}
	return res.RowsAffected()
}

func cancelQueuedWhere(ctx context.Context, tx *sql.Tx, where, updatedAt string, args ...any) (int64, error) {
	execArgs := append([]any{updatedAt}, args...)
	res, err := tx.ExecContext(ctx, `UPDATE envelopes SET state = 'cancelled', updated_at = ? WHERE `+where, execArgs...)
	if err != nil {
		return 0, fmt.Errorf("cancel queued: %w", err)
	}
	return res.RowsAffected()
}

// TransitionToDispatching claims one queued envelope for dispatch. Returns
// false (no error) if the row is no longer queued — already cancelled by a
// concurrent revoke/renewal that committed first, which is not a bug, it's
// the serialization this design relies on.
func TransitionToDispatching(ctx context.Context, tx *sql.Tx, id, updatedAt string) (bool, error) {
	res, err := tx.ExecContext(ctx, `
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

// RequeueUnattempted reverts one 'dispatching' envelope back to 'queued'
// under grantVersion. Used when a Transport reports it never actually
// attempted delivery (the host process didn't start, or the transport
// rejected the message before ever calling the host) — the claimed budget
// slot must be refunded by the caller in the same transaction
// (store.RefundExchange) so an unattempted message doesn't count against
// max_exchanges. grantVersion lets the caller re-stamp the row onto whatever
// grant is current now, rather than blindly restoring the version it was
// claimed under: a revoke or renewal that committed while delivery was in
// flight can make that original version no longer active, and a row left
// 'queued' under a dead version can never be claimed again (ClaimExchange
// requires status = 'active') or picked up by a later renewal's
// CarryForwardQueuedReplies (which only ever matches the version it is
// itself superseding) — it would be silently stranded forever. Callers that
// confirm the version is still current simply pass it back unchanged.
// Guarded to only affect a row still 'dispatching': a concurrent state
// change (there shouldn't be one, since nothing else touches a 'dispatching'
// row) is surfaced as false rather than silently overwriting whatever it
// became.
func RequeueUnattempted(ctx context.Context, tx *sql.Tx, id string, grantVersion int64, updatedAt string) (bool, error) {
	res, err := tx.ExecContext(ctx, `
		UPDATE envelopes SET state = 'queued', grant_version = ?, updated_at = ?
		WHERE id = ? AND state = 'dispatching'`, grantVersion, updatedAt, id)
	if err != nil {
		return false, fmt.Errorf("requeue unattempted: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("requeue unattempted: rows affected: %w", err)
	}
	return n == 1, nil
}

// SetState sets an envelope's terminal (or acked) state outside the
// dispatching transaction — after the host call returns, success, failure,
// or ambiguous.
func SetState(ctx context.Context, tx *sql.Tx, id string, state EnvelopeState, updatedAt string) error {
	res, err := tx.ExecContext(ctx, `UPDATE envelopes SET state = ?, updated_at = ? WHERE id = ?`,
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
func GetByID(ctx context.Context, tx *sql.Tx, id string) (*Envelope, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT id, conversation, from_peer, to_peer, text, grant_version, in_reply_to,
		       is_trusted_reply, state, created_at, updated_at
		FROM envelopes WHERE id = ?`, id)
	var e Envelope
	var state string
	if err := row.Scan(&e.ID, &e.Conversation, &e.FromPeer, &e.ToPeer, &e.Text,
		&e.GrantVersion, &e.InReplyTo, &e.TrustedReply, &state, &e.CreatedAt, &e.UpdatedAt); err != nil {
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
			&e.GrantVersion, &e.InReplyTo, &e.TrustedReply, &state, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan envelope: %w", err)
		}
		e.State = EnvelopeState(state)
		out = append(out, e)
	}
	return out, rows.Err()
}
