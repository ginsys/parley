package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrNoActiveGrant means the conversation has no currently active grant —
// never enrolled, or its only grant was revoked/superseded.
var ErrNoActiveGrant = errors.New("no active grant for conversation")

// CurrentGrant returns the conversation's active grant. Callers must hold a
// Tx so the read is part of the same serialized transaction as whatever
// decision it feeds (send, revoke, renew) — this is not a passive read.
func CurrentGrant(ctx context.Context, tx *sql.Tx, conversation string) (*Grant, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT conversation, grant_version, peer_a_id, peer_b_id, direction,
		       max_exchanges, exchanges_used, granted_at, expires_at, status, revoked_at, cancel_pending_replies
		FROM grants
		WHERE conversation = ? AND status = 'active'`, conversation)

	var g Grant
	var direction, status string
	if err := row.Scan(&g.Conversation, &g.GrantVersion, &g.PeerAID, &g.PeerBID, &direction,
		&g.MaxExchanges, &g.ExchangesUsed, &g.GrantedAt, &g.ExpiresAt, &status, &g.RevokedAt, &g.CancelPendingReplies); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoActiveGrant
		}
		return nil, fmt.Errorf("query active grant: %w", err)
	}
	g.Direction = Direction(direction)
	g.Status = GrantStatus(status)
	return &g, nil
}

// EnsureConversation inserts the conversation row if it doesn't already
// exist. Idempotent so the controller can call it on every Grant.
func EnsureConversation(ctx context.Context, tx *sql.Tx, id, name, createdAt string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO conversations (id, name, created_at)
		VALUES (?, ?, ?)
		ON CONFLICT (id) DO NOTHING`, id, name, createdAt)
	if err != nil {
		return fmt.Errorf("ensure conversation: %w", err)
	}
	return nil
}

// InsertGrant writes a new grant version as 'active'. The schema's unique
// partial index (one active row per conversation) rejects this if the
// caller failed to supersede/revoke the prior active grant first — a bug in
// the controller, not a race, since both run under the same Tx.
func InsertGrant(ctx context.Context, tx *sql.Tx, g Grant) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO grants (conversation, grant_version, peer_a_id, peer_b_id, direction,
		                     max_exchanges, exchanges_used, granted_at, expires_at, status, revoked_at, cancel_pending_replies)
		VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, 'active', NULL, ?)`,
		g.Conversation, g.GrantVersion, g.PeerAID, g.PeerBID, string(g.Direction),
		g.MaxExchanges, g.GrantedAt, g.ExpiresAt, g.CancelPendingReplies)
	if err != nil {
		return fmt.Errorf("insert grant: %w", err)
	}
	return nil
}

// SetGrantStatus transitions a specific grant version away from active
// (revoked or superseded). timestamp is stored as revoked_at regardless of
// which of the two statuses is being set, since both mean "no longer live"
// for anything reading revoked_at.
func SetGrantStatus(ctx context.Context, tx *sql.Tx, conversation string, version int64, status GrantStatus, timestamp string) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE grants SET status = ?, revoked_at = ?
		WHERE conversation = ? AND grant_version = ? AND status = 'active'`,
		string(status), timestamp, conversation, version)
	if err != nil {
		return fmt.Errorf("set grant status: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("set grant status: rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("set grant status: no active grant %s/v%d to transition", conversation, version)
	}
	return nil
}

// ClaimExchange atomically increments exchanges_used for the given grant
// version, but only if it's still active and under max_exchanges. The
// affected-rows count is the budget check: 0 means exhausted or the version
// is no longer active, and the caller must treat that as a hard stop.
func ClaimExchange(ctx context.Context, tx *sql.Tx, conversation string, version int64) (bool, error) {
	res, err := tx.ExecContext(ctx, `
		UPDATE grants SET exchanges_used = exchanges_used + 1
		WHERE conversation = ? AND grant_version = ? AND status = 'active'
		  AND exchanges_used < max_exchanges`, conversation, version)
	if err != nil {
		return false, fmt.Errorf("claim exchange: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("claim exchange: rows affected: %w", err)
	}
	return n == 1, nil
}

// RefundExchange reverses one ClaimExchange call for a grant version whose
// claimed message was never actually attempted (the host process never
// started, or the transport rejected it before ever calling the host) — the
// budget slot was consumed for nothing and must not count against
// max_exchanges. Safe regardless of the grant's current status (revoked,
// superseded, or a since-expired active row): this only corrects the count
// already recorded against a specific historical version, it does not
// re-check eligibility. The exchanges_used > 0 guard makes a duplicate call
// a no-op instead of driving the counter negative.
func RefundExchange(ctx context.Context, tx *sql.Tx, conversation string, version int64) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE grants SET exchanges_used = exchanges_used - 1
		WHERE conversation = ? AND grant_version = ? AND exchanges_used > 0`, conversation, version)
	if err != nil {
		return fmt.Errorf("refund exchange: %w", err)
	}
	return nil
}
