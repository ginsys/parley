package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var ErrEnvelopeNotFound = errors.New("envelope not found")
var ErrStateConflict = errors.New("envelope is not in the expected state")

const MaxQueueBatch = 100

// QueueCursor contains only the stable sort key; no message body is loaded.
type QueueCursor struct {
	ID          string
	CreatedAtNS int64
}

// ListQueuedIDs bounds work and avoids reading bodies for other recipients.
func ListQueuedIDs(ctx context.Context, tx *sql.Tx, conversation, toPeer string, limit int, after *QueueCursor) ([]QueueCursor, error) {
	if limit < 1 || limit > MaxQueueBatch {
		return nil, fmt.Errorf("queue limit must be between 1 and %d", MaxQueueBatch)
	}
	query := `SELECT id,created_at_ns FROM envelopes WHERE conversation=? AND state='queued' AND to_peer=?`
	args := []any{conversation, toPeer}
	if after != nil {
		query += ` AND (created_at_ns,id)>(?,?)`
		args = append(args, after.CreatedAtNS, after.ID)
	}
	query += ` ORDER BY created_at_ns ASC, id ASC LIMIT ?`
	args = append(args, limit)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []QueueCursor
	for rows.Next() {
		var id QueueCursor
		if err := rows.Scan(&id.ID, &id.CreatedAtNS); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// InsertQueued writes a new envelope in the queued state. GrantVersion must
// already be stamped by the caller from CurrentGrant at accept time.
// TrustedReply must only ever be true when the caller performs authenticated ingestion.
func InsertQueued(ctx context.Context, tx *sql.Tx, e Envelope) error {
	ns, err := timestampNanos(e.CreatedAt)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO envelopes (id, conversation, from_peer, to_peer, text, grant_version,
		                        in_reply_to, is_trusted_reply, state, created_at, updated_at, created_at_ns)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)`,
		e.ID, e.Conversation, e.FromPeer, e.ToPeer, e.Text, e.GrantVersion,
		e.InReplyTo, e.TrustedReply, e.CreatedAt, e.CreatedAt, ns)
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
		       is_trusted_reply, state, created_at, updated_at, dispatch_attempt, error_code, error_detail
		FROM envelopes
		WHERE conversation = ? AND state = 'queued'
		ORDER BY created_at_ns ASC, id ASC`, conversation)
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
// 'acked' by authenticated ingestion before the reply was queued, so unlike a fresh Send,
// there is no live sender left to notice the cancellation and resubmit —
// cancelling a reply here would strand its content with no path back to
// delivery, even though the whole point of a renewal is to let an
// already-in-progress exchange continue. Must run after the new
// grant_version row exists (InsertGrant), since
// envelopes(conversation, grant_version) has an immediate foreign key
// against grants.
//
// A row only qualifies if is_trusted_reply was set at insert time — which
// only authenticated ingestion does, atomically, after validating in_reply_to
// against the specific original it just acked (replymarker.Validate). Bare
// "in_reply_to IS NOT NULL" (or even "in_reply_to names some envelope this
// conversation has acked") is not provenance: historical ordinary sends may
// contain unvalidated correlation IDs. Those rows cannot claim reply status
// or be carried across renewal. The authenticated Send API accepts neither
// correlation IDs nor a trusted-reply flag.
func CarryForwardQueuedReplies(ctx context.Context, tx *sql.Tx, conversation string, oldVersion, newVersion int64, updatedAt string) (int64, error) {
	g, err := CurrentGrant(ctx, tx, conversation)
	if err != nil {
		return 0, err
	}
	if g.GrantVersion != newVersion || g.CancelPendingReplies || g.RevokedAt != nil {
		return 0, nil
	}
	res, err := tx.ExecContext(ctx, `
 UPDATE envelopes AS reply SET grant_version=?,updated_at=?
 WHERE conversation=? AND grant_version=? AND state='queued' AND is_trusted_reply=1
 AND ((from_peer=? AND to_peer=? AND ? IN ('bidirectional','a_to_b'))
   OR (from_peer=? AND to_peer=? AND ? IN ('bidirectional','b_to_a')))
 AND EXISTS (SELECT 1 FROM envelopes AS original WHERE original.id=reply.in_reply_to
   AND original.conversation=reply.conversation AND original.state='acked'
   AND original.from_peer=reply.to_peer AND original.to_peer=reply.from_peer)
 AND NOT EXISTS (SELECT 1 FROM grants AS history WHERE history.conversation=reply.conversation
   AND history.grant_version>=? AND history.grant_version<=?
   AND (history.status='revoked' OR (history.grant_version>? AND history.cancel_pending_replies=1)))`,
		newVersion, updatedAt, conversation, oldVersion,
		g.PeerAID, g.PeerBID, string(g.Direction), g.PeerBID, g.PeerAID, string(g.Direction), oldVersion, newVersion, oldVersion)
	if err != nil {
		return 0, err
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
		UPDATE envelopes SET state = 'dispatching', dispatch_attempt=dispatch_attempt+1, error_code='', error_detail='', updated_at = ?
		WHERE id = ? AND state = 'queued' AND typeof(dispatch_attempt)='integer' AND dispatch_attempt<9223372036854775807`, updatedAt, id)
	if err != nil {
		return false, fmt.Errorf("transition to dispatching: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("transition to dispatching: rows affected: %w", err)
	}
	if n == 0 {
		var queued bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM envelopes WHERE id=? AND state='queued')", id).Scan(&queued); err != nil {
			return false, err
		}
		if queued {
			return false, InvalidRequest
		}
	}
	return n == 1, nil
}

// SetState changes a known prior state, for cancellation or acknowledgement.
// Dispatch outcomes use SettleDispatch to also match the attempt token.
func SetState(ctx context.Context, tx *sql.Tx, id string, expected, state EnvelopeState, updatedAt string) error {
	if state == Acked {
		held, err := WorkHeld(ctx, tx, WorkRef{Kind: "envelope", ID: id})
		if err != nil {
			return err
		}
		if held {
			return SecurityHold
		}
	}
	res, err := tx.ExecContext(ctx, `UPDATE envelopes SET state = ?, updated_at = ? WHERE id = ? AND state = ?`,
		string(state), updatedAt, id, string(expected))
	if err != nil {
		return fmt.Errorf("set envelope state: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("set envelope state: rows affected: %w", err)
	}
	if n == 0 {
		var exists int
		err := tx.QueryRowContext(ctx, "SELECT 1 FROM envelopes WHERE id=?", id).Scan(&exists)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrEnvelopeNotFound
		}
		if err != nil {
			return fmt.Errorf("check envelope existence: %w", err)
		}
		return ErrStateConflict
	}
	return nil
}

// GetByID fetches one envelope by id within the caller's transaction.
func GetByID(ctx context.Context, tx *sql.Tx, id string) (*Envelope, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT id, conversation, from_peer, to_peer, text, grant_version, in_reply_to,
		       is_trusted_reply, state, created_at, updated_at, dispatch_attempt, error_code, error_detail
		FROM envelopes WHERE id = ?`, id)
	var e Envelope
	var state string
	if err := row.Scan(&e.ID, &e.Conversation, &e.FromPeer, &e.ToPeer, &e.Text,
		&e.GrantVersion, &e.InReplyTo, &e.TrustedReply, &state, &e.CreatedAt, &e.UpdatedAt, &e.DispatchAttempt, &e.ErrorCode, &e.ErrorDetail); err != nil {
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
			&e.GrantVersion, &e.InReplyTo, &e.TrustedReply, &state, &e.CreatedAt, &e.UpdatedAt, &e.DispatchAttempt, &e.ErrorCode, &e.ErrorDetail); err != nil {
			return nil, fmt.Errorf("scan envelope: %w", err)
		}
		e.State = EnvelopeState(state)
		out = append(out, e)
	}
	return out, rows.Err()
}

// CanCarryReply validates provenance and every crossed renewal boundary. A
// cancellation choice or revocation cannot be bypassed by a later renewal.
func CanCarryReply(ctx context.Context, tx *sql.Tx, e *Envelope, target int64) (bool, error) {
	if !e.TrustedReply || e.InReplyTo == nil || target <= e.GrantVersion {
		return false, nil
	}
	g, err := CurrentGrant(ctx, tx, e.Conversation)
	if err != nil {
		return false, err
	}
	if g.GrantVersion != target || !g.PermitsDirection(e.FromPeer, e.ToPeer) {
		return false, nil
	}
	var valid int
	err = tx.QueryRowContext(ctx, `SELECT count(*) FROM envelopes WHERE id=? AND conversation=? AND state='acked' AND from_peer=? AND to_peer=?`, *e.InReplyTo, e.Conversation, e.ToPeer, e.FromPeer).Scan(&valid)
	if err != nil || valid != 1 {
		return false, err
	}
	var blockers int
	err = tx.QueryRowContext(ctx, `SELECT count(*) FROM grants WHERE conversation=? AND grant_version>=? AND grant_version<=? AND (status='revoked' OR (grant_version>? AND cancel_pending_replies=1))`, e.Conversation, e.GrantVersion, target, e.GrantVersion).Scan(&blockers)
	return blockers == 0, err
}

// SettleDispatch is conditional on the exact attempt, preventing an old
// settlement from changing a newer retry (even under the same grant version).
func SettleDispatch(ctx context.Context, tx *sql.Tx, claimed *Envelope, state EnvelopeState, version int64, code, detail, now string) (bool, error) {
	res, err := tx.ExecContext(ctx, `UPDATE envelopes SET state=?,grant_version=?,error_code=?,error_detail=?,updated_at=?
 WHERE id=? AND state='dispatching' AND grant_version=? AND dispatch_attempt=?`, state, version, code, detail, now, claimed.ID, claimed.GrantVersion, claimed.DispatchAttempt)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}
