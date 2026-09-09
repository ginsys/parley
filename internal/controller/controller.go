// Package controller is Parley's single protected write path for
// membership: Grant, Revoke, and Renew are the only code that ever writes a
// grant row. Per the design plan (§1), this package is meant to be invoked
// directly by a human in their own shell — cmd/parleyctl — never called as a
// tool by either peer session. Nothing here enforces that from inside the
// process; the protection is nah's hook-directory block on the state file
// path plus the Bash-classifier extension described in the plan, both
// external to this code.
package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/ginsys/parley/internal/store"
)

// Controller is a thin wrapper around a store.DB. It holds no state of its
// own — every operation is one BEGIN IMMEDIATE transaction.
type Controller struct {
	db *store.DB
}

func New(db *store.DB) *Controller {
	return &Controller{db: db}
}

// GrantParams describes a new enrollment. Conversation is used as both the
// conversation's id and its human-facing name — Parley conversations are
// human-created and named once; there's no separate rename-without-changing-
// identity use case yet, so collapsing id and name avoids a needless layer.
type GrantParams struct {
	Conversation string
	PeerAID      string
	PeerBID      string
	Direction    store.Direction
	MaxExchanges int64
	ExpiresAt    *time.Time
}

// Grant creates the first (version 1) grant for a conversation. It fails if
// the conversation already has an active grant — use Renew for that.
func (c *Controller) Grant(ctx context.Context, p GrantParams) (*store.Grant, error) {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback(ctx)
		}
	}()

	now := nowRFC3339()
	if err := store.EnsureConversation(ctx, tx, p.Conversation, p.Conversation, now); err != nil {
		return nil, err
	}
	if _, err := store.CurrentGrant(ctx, tx, p.Conversation); err == nil {
		return nil, fmt.Errorf("conversation %q already has an active grant; use Renew", p.Conversation)
	} else if err != store.ErrNoActiveGrant {
		return nil, err
	}

	g := store.Grant{
		Conversation: p.Conversation,
		GrantVersion: 1,
		PeerAID:      p.PeerAID,
		PeerBID:      p.PeerBID,
		Direction:    p.Direction,
		MaxExchanges: p.MaxExchanges,
		GrantedAt:    now,
		ExpiresAt:    formatOptionalTime(p.ExpiresAt),
	}
	if err := store.InsertGrant(ctx, tx, g); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	committed = true
	g.ExchangesUsed = 0
	g.Status = store.GrantActive
	return &g, nil
}

// RevokeResult is the exact split Revoke reports — never a bare "revoked",
// per the design plan's requirement that revocation's scope be stated
// plainly (it stops the bridge, not every path either peer could use).
type RevokeResult struct {
	Cancelled          int64
	AlreadyDispatching int64
	AlreadyHandedOff   int64
}

// Revoke ends the conversation's active grant. Every envelope still queued
// is cancelled in the same transaction; envelopes already claimed for
// dispatch or already handed off are reported, not touched — Revoke cannot
// undo a send that already committed to leaving this process.
func (c *Controller) Revoke(ctx context.Context, conversation string) (*RevokeResult, error) {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback(ctx)
		}
	}()

	g, err := store.CurrentGrant(ctx, tx, conversation)
	if err != nil {
		return nil, err
	}

	dispatching, err := countByState(ctx, tx, conversation, store.Dispatching)
	if err != nil {
		return nil, err
	}
	handedOff, err := countByState(ctx, tx, conversation, store.HandedOff)
	if err != nil {
		return nil, err
	}

	now := nowRFC3339()
	if err := store.SetGrantStatus(ctx, tx, conversation, g.GrantVersion, store.GrantRevoked, now); err != nil {
		return nil, err
	}
	cancelled, err := store.CancelQueued(ctx, tx, conversation, now)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	committed = true
	return &RevokeResult{Cancelled: cancelled, AlreadyDispatching: dispatching, AlreadyHandedOff: handedOff}, nil
}

// RenewParams updates the budget and/or expiry of a conversation's grant by
// creating a new version. Zero MaxExchanges means "keep the current value".
type RenewParams struct {
	Conversation string
	MaxExchanges int64
	ExpiresAt    *time.Time
}

// Renew supersedes the current active grant with a new version. Any
// envelope still queued under the old version is cancelled in the same
// transaction — a peer that still wants that content sent resubmits it
// fresh under the new grant, per the design plan's explicit rule against
// silently carrying old-grant messages forward.
func (c *Controller) Renew(ctx context.Context, p RenewParams) (*store.Grant, error) {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback(ctx)
		}
	}()

	current, err := store.CurrentGrant(ctx, tx, p.Conversation)
	if err != nil {
		return nil, err
	}

	now := nowRFC3339()
	if err := store.SetGrantStatus(ctx, tx, p.Conversation, current.GrantVersion, store.GrantSuperseded, now); err != nil {
		return nil, err
	}

	maxExchanges := p.MaxExchanges
	if maxExchanges == 0 {
		maxExchanges = current.MaxExchanges
	}
	expiresAt := formatOptionalTime(p.ExpiresAt)
	if p.ExpiresAt == nil {
		expiresAt = current.ExpiresAt
	}

	newVersion := current.GrantVersion + 1
	next := store.Grant{
		Conversation: p.Conversation,
		GrantVersion: newVersion,
		PeerAID:      current.PeerAID,
		PeerBID:      current.PeerBID,
		Direction:    current.Direction,
		MaxExchanges: maxExchanges,
		GrantedAt:    now,
		ExpiresAt:    expiresAt,
	}
	if err := store.InsertGrant(ctx, tx, next); err != nil {
		return nil, err
	}
	// Carry forward queued replies to the new version before cancelling
	// whatever's left queued under the old one — a reply has no live sender
	// left to resubmit it if cancelled (see CarryForwardQueuedReplies).
	// Must run after InsertGrant: envelopes(conversation, grant_version) has
	// an immediate foreign key against grants, so newVersion must already
	// exist as a row before any envelope can reference it.
	if _, err := store.CarryForwardQueuedReplies(ctx, tx, p.Conversation, current.GrantVersion, newVersion, now); err != nil {
		return nil, err
	}
	if _, err := store.CancelQueuedUnderVersion(ctx, tx, p.Conversation, current.GrantVersion, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	committed = true
	next.Status = store.GrantActive
	return &next, nil
}

func countByState(ctx context.Context, tx *store.Tx, conversation string, state store.EnvelopeState) (int64, error) {
	row := tx.QueryRow(ctx,
		`SELECT COUNT(*) FROM envelopes WHERE conversation = ? AND state = ?`, conversation, string(state))
	var n int64
	if err := row.Scan(&n); err != nil {
		return 0, fmt.Errorf("count envelopes in state %s: %w", state, err)
	}
	return n, nil
}

func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func formatOptionalTime(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := t.UTC().Format(time.RFC3339Nano)
	return &s
}
