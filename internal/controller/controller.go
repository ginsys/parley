// Package controller is the protected membership write path used by the
// human-operated cmd/parleyctl administrator. Ordinary adapters never invoke
// Grant, Revoke or Renew. This is a cooperative boundary, not enforced OS
// isolation; see docs/architecture.md for the implemented limits.
package controller

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ginsys/parley/internal/bridgetext"
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
//
// ExpectedVersion is optimistic-concurrency enforcement against the
// conversation's latest historical grant version (0 if none exists yet),
// active only when non-nil. It exists for a caller with an external,
// possibly-stale view of that history (internal/control's membership.enroll,
// driven by a wire operation's expected_grant_version field) and must not
// change GrantTx's observable behavior for a caller that leaves it nil: the
// legacy Grant method below always leaves it nil, so its own "conversation
// already has an active grant" check remains the only precondition enforced
// for every existing direct caller.
type GrantParams struct {
	Conversation    string
	PeerAID         string
	PeerBID         string
	Direction       store.Direction
	MaxExchanges    int64
	ExpiresAt       *time.Time
	ExpectedVersion *int64
}

// Grant creates the next historical version for a conversation. It fails if
// the conversation already has an active grant — use Renew for that. It
// opens and commits its own transaction; see GrantTx for the tx-accepting
// core this delegates to, used directly by a caller that already owns a
// writer transaction (internal/control's membership.enroll handler).
func (c *Controller) Grant(ctx context.Context, p GrantParams) (*store.Grant, error) {
	// This legacy writer has no coordinated recovery/clock contract.
	if c.db.RecoveryControlled() {
		return nil, store.RecoveryRequired
	}
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()
	if err := rejectRecoveryOwner(ctx, tx); err != nil {
		return nil, err
	}
	g, err := GrantTx(ctx, tx, p)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return g, nil
}

// GrantTx is Grant's actual validation and mutation body, taking an
// already-open transaction instead of opening its own. It is the single
// implementation shared by the legacy self-opening Grant above and
// internal/control's coordinator-driven membership.enroll handler, so their
// observable enrollment semantics (validation, version assignment, the
// active-grant conflict check) can never diverge between the two callers.
func GrantTx(ctx context.Context, tx *sql.Tx, p GrantParams) (*store.Grant, error) {
	if err := validateGrant(p); err != nil {
		return nil, err
	}
	now := nowRFC3339()
	if err := store.EnsureConversation(ctx, tx, p.Conversation, p.Conversation, now); err != nil {
		return nil, err
	}
	var latest int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(grant_version),0) FROM grants WHERE conversation=?", p.Conversation).Scan(&latest); err != nil {
		return nil, fmt.Errorf("latest grant version: %w", err)
	}
	if p.ExpectedVersion != nil && *p.ExpectedVersion != latest {
		return nil, store.StaleGrantVersion
	}
	if _, err := store.CurrentGrant(ctx, tx, p.Conversation); err == nil {
		if p.ExpectedVersion != nil {
			return nil, store.AlreadyActive
		}
		return nil, fmt.Errorf("conversation %q already has an active grant; use Renew", p.Conversation)
	} else if !errors.Is(err, store.ErrNoActiveGrant) {
		return nil, err
	}

	g := store.Grant{
		Conversation: p.Conversation,
		GrantVersion: latest + 1,
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
	g.ExchangesUsed = 0
	g.Status = store.GrantActive
	return &g, nil
}

// RevokeResult is the exact split Revoke reports — never a bare "revoked",
// so revocation's limited scope can be stated
// plainly (it stops the bridge, not every path either peer could use).
type RevokeResult struct {
	Cancelled          int64
	AlreadyDispatching int64
	AlreadyHandedOff   int64
}

// Revoke ends the conversation's active grant. Every envelope still queued
// is cancelled in the same transaction; envelopes already claimed for
// dispatch or already handed off are reported, not touched — Revoke cannot
// undo a send that already committed to leaving this process. It opens and
// commits its own transaction; see RevokeTx for the tx-accepting core.
func (c *Controller) Revoke(ctx context.Context, conversation string) (*RevokeResult, error) {
	// This legacy writer has no coordinated recovery/clock contract.
	if c.db.RecoveryControlled() {
		return nil, store.RecoveryRequired
	}
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()
	if err := rejectRecoveryOwner(ctx, tx); err != nil {
		return nil, err
	}
	result, err := RevokeTx(ctx, tx, conversation, nil)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return result, nil
}

// RevokeTx is Revoke's actual body, taking an already-open transaction.
// expectedVersion enforces optimistic concurrency against the conversation's
// current active grant version when non-nil (internal/control's
// membership.revoke, driven by a wire operation's expected_grant_version);
// the legacy Revoke above always passes nil, preserving its exact prior
// "act on whatever the current active grant is" behavior for every existing
// direct caller.
func RevokeTx(ctx context.Context, tx *sql.Tx, conversation string, expectedVersion *int64) (*RevokeResult, error) {
	g, err := store.CurrentGrant(ctx, tx, conversation)
	if err != nil {
		if errors.Is(err, store.ErrNoActiveGrant) && expectedVersion != nil {
			return nil, store.NoActiveGrant
		}
		return nil, err
	}
	if expectedVersion != nil && *expectedVersion != g.GrantVersion {
		return nil, store.StaleGrantVersion
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
	return &RevokeResult{Cancelled: cancelled, AlreadyDispatching: dispatching, AlreadyHandedOff: handedOff}, nil
}

// RenewParams updates the budget and/or expiry of a conversation's grant by
// creating a new version. Zero MaxExchanges means "keep the current value".
// ExpectedVersion enforces optimistic concurrency against the conversation's
// current active grant version when non-nil; nil preserves the legacy
// "act on whatever the current active grant is" behavior for every existing
// direct caller.
type RenewParams struct {
	CancelPendingReplies bool
	Conversation         string
	MaxExchanges         int64
	ExpiresAt            *time.Time
	ExpectedVersion      *int64
}

// Renew creates a successor grant, preserving the existing peers and
// direction unchanged. Ordinary queued messages are cancelled; proven
// replies carry by default because their originals were acknowledged.
// CancelPendingReplies explicitly opts out, including late unattempted
// rescue. It opens and commits its own transaction; see RenewTx for the
// tx-accepting core.
func (c *Controller) Renew(ctx context.Context, p RenewParams) (*store.Grant, error) {
	// This legacy writer has no coordinated recovery/clock contract.
	if c.db.RecoveryControlled() {
		return nil, store.RecoveryRequired
	}
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()
	if err := rejectRecoveryOwner(ctx, tx); err != nil {
		return nil, err
	}
	g, err := RenewTx(ctx, tx, p)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return g, nil
}

// RenewTx is Renew's actual body, taking an already-open transaction.
func RenewTx(ctx context.Context, tx *sql.Tx, p RenewParams) (*store.Grant, error) {
	if err := validateRenewalInput(p.Conversation, p.MaxExchanges, p.ExpiresAt); err != nil {
		return nil, err
	}
	current, err := currentGrantExpecting(ctx, tx, p.Conversation, p.ExpectedVersion)
	if err != nil {
		return nil, err
	}
	if err := validatePeerIDs(current.PeerAID, current.PeerBID); err != nil {
		return nil, err
	}
	return supersede(ctx, tx, current, p.Conversation, current.PeerAID, current.PeerBID, current.Direction,
		p.MaxExchanges, p.ExpiresAt, p.CancelPendingReplies)
}

// ReplaceParams updates a conversation's membership and/or policy by
// creating a new version, per membership.md's "Replace membership/policy"
// operation: a complete valid replacement, not an incremental patch. Budget
// and expiry follow Renew's own omission rules. PeerAID/PeerBID/Direction
// are the caller's already-translated replacement (internal/control's
// membership.replace handler translates the wire members/policy object via
// internal/membership before calling this) — ReplaceTx does not itself
// understand the members/policy shape.
type ReplaceParams struct {
	CancelPendingReplies bool
	Conversation         string
	PeerAID, PeerBID     string
	Direction            store.Direction
	MaxExchanges         int64
	ExpiresAt            *time.Time
	ExpectedVersion      *int64
}

// ReplaceTx is Replace's tx-accepting body. There is no legacy self-opening
// Replace: this operation has no CLI predecessor, only internal/control's
// membership.replace handler calls it, always inside a coordinator
// transaction, always with a concrete ExpectedVersion.
func ReplaceTx(ctx context.Context, tx *sql.Tx, p ReplaceParams) (*store.Grant, error) {
	if err := validateRenewalInput(p.Conversation, p.MaxExchanges, p.ExpiresAt); err != nil {
		return nil, err
	}
	if err := validateGrant(GrantParams{Conversation: p.Conversation, PeerAID: p.PeerAID, PeerBID: p.PeerBID, Direction: p.Direction, MaxExchanges: 1}); err != nil {
		return nil, err
	}
	current, err := currentGrantExpecting(ctx, tx, p.Conversation, p.ExpectedVersion)
	if err != nil {
		return nil, err
	}
	return supersede(ctx, tx, current, p.Conversation, p.PeerAID, p.PeerBID, p.Direction,
		p.MaxExchanges, p.ExpiresAt, p.CancelPendingReplies)
}

// currentGrantExpecting resolves the conversation's current active grant,
// translating a missing grant or version mismatch into the wire-audited
// store.Code values when a caller (internal/control) supplies a concrete
// expectedVersion; a nil expectedVersion (every existing legacy direct
// caller) preserves the exact prior "act on whatever the current active
// grant is" behavior and propagates store.ErrNoActiveGrant unwrapped.
func currentGrantExpecting(ctx context.Context, tx *sql.Tx, conversation string, expectedVersion *int64) (*store.Grant, error) {
	current, err := store.CurrentGrant(ctx, tx, conversation)
	if err != nil {
		if errors.Is(err, store.ErrNoActiveGrant) && expectedVersion != nil {
			return nil, store.NoActiveGrant
		}
		return nil, err
	}
	if expectedVersion != nil && *expectedVersion != current.GrantVersion {
		return nil, store.StaleGrantVersion
	}
	return current, nil
}

// supersede is Renew/Replace's shared version-bump body: mark the current
// active grant superseded, insert its successor with the given peers/
// direction/budget/expiry, then carry forward eligible queued trusted
// replies before cancelling whatever else is left queued under the old
// version. Called only after the caller's own validation and version check.
func supersede(ctx context.Context, tx *sql.Tx, current *store.Grant, conversation, peerA, peerB string, direction store.Direction,
	maxExchanges int64, expiresAt *time.Time, cancelPendingReplies bool) (*store.Grant, error) {
	now := nowRFC3339()
	if err := store.SetGrantStatus(ctx, tx, conversation, current.GrantVersion, store.GrantSuperseded, now); err != nil {
		return nil, err
	}
	if maxExchanges == 0 {
		maxExchanges = current.MaxExchanges
	}
	nextExpiresAt := formatOptionalTime(expiresAt)
	if expiresAt == nil {
		nextExpiresAt = current.ExpiresAt
	}
	newVersion := current.GrantVersion + 1
	next := store.Grant{
		CancelPendingReplies: cancelPendingReplies,
		Conversation:         conversation,
		GrantVersion:         newVersion,
		PeerAID:              peerA,
		PeerBID:              peerB,
		Direction:            direction,
		MaxExchanges:         maxExchanges,
		GrantedAt:            now,
		ExpiresAt:            nextExpiresAt,
	}
	if err := store.InsertGrant(ctx, tx, next); err != nil {
		return nil, err
	}
	// Carry forward queued replies to the new version before cancelling
	// whatever's left queued under the old one — a reply has no live sender
	// left to resubmit it if cancelled (see CarryForwardQueuedReplies).
	// Must run after InsertGrant: envelopes(conversation, grant_version) has
	// an immediate foreign key against grants, so newVersion must already
	// exist as a row before any envelope can reference it. CarryForward's
	// own membership/edge re-check (against the just-inserted successor's
	// stored peers/direction) is what correctly cancels a reply instead of
	// carrying it when Replace denies or removes its edge.
	if _, err := store.CarryForwardQueuedReplies(ctx, tx, conversation, current.GrantVersion, newVersion, now); err != nil {
		return nil, err
	}
	if _, err := store.CancelQueuedUnderVersion(ctx, tx, conversation, current.GrantVersion, now); err != nil {
		return nil, err
	}
	next.Status = store.GrantActive
	return &next, nil
}

// validateRenewalInput is Renew/Replace's shared conversation/budget/expiry
// input validation, run before any read against the conversation's history.
func validateRenewalInput(conversation string, maxExchanges int64, expiresAt *time.Time) error {
	if err := bridgetext.ValidateMetadata(conversation); err != nil {
		return fmt.Errorf("conversation identifier: %w", err)
	}
	if maxExchanges < 0 {
		return fmt.Errorf("renew requires conversation and nonnegative budget")
	}
	if expiresAt != nil && !expiresAt.After(time.Now()) {
		return fmt.Errorf("explicit expiry must be in the future")
	}
	return nil
}

func countByState(ctx context.Context, tx *sql.Tx, conversation string, state store.EnvelopeState) (int64, error) {
	row := tx.QueryRowContext(ctx,
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

func validateGrant(p GrantParams) error {
	if err := bridgetext.ValidateMetadata(p.Conversation); err != nil {
		return fmt.Errorf("conversation identifier: %w", err)
	}
	if p.PeerAID == p.PeerBID || p.MaxExchanges <= 0 {
		return fmt.Errorf("grant requires conversation, distinct peers and positive budget")
	}
	if err := validatePeerIDs(p.PeerAID, p.PeerBID); err != nil {
		return err
	}
	if p.Direction != store.Bidirectional && p.Direction != store.AToB && p.Direction != store.BToA {
		return fmt.Errorf("invalid grant direction %q", p.Direction)
	}
	if p.ExpiresAt != nil && !p.ExpiresAt.After(time.Now()) {
		return fmt.Errorf("explicit expiry must be in the future")
	}
	return nil
}

func validatePeerIDs(ids ...string) error {
	for _, id := range ids {
		if err := bridgetext.ValidateMetadata(id); err != nil {
			return fmt.Errorf("peer identifier: %w", err)
		}
	}
	return nil
}

// A separately opened legacy handle has no process-local recovery hooks. The
// durable checkpoint/incident evidence still establishes runtime ownership.
func rejectRecoveryOwner(ctx context.Context, tx *sql.Tx) error {
	checkpoint, err := store.ReadClockCheckpoint(ctx, tx)
	if err != nil {
		return err
	}
	var incident bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM recovery_incidents)").Scan(&incident); err != nil {
		return err
	}
	if checkpoint.Instant.Valid || incident {
		return store.RecoveryRequired
	}
	return nil
}
