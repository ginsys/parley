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
	if err := validateGrant(ctx, p); err != nil {
		return nil, err
	}
	now := authorityNow(ctx)
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
	// Restored active-grant precondition (EC-01, thread PRRT_kwDOUT1JT86j_9VC,
	// root 4053366763): the pre-refactor base Controller.Grant explicitly
	// called store.CurrentGrant and rejected an existing active grant before
	// ever computing a successor version. That check was lost when Grant was
	// split into this shared GrantTx body, so a matching-version enrollment
	// against an already-active conversation fell through to InsertGrant and
	// hit the idx_grants_one_active unique-index constraint instead of the
	// intended deterministic store.AlreadyActive rejection -- degrading to an
	// unaudited, non-terminal storage error via storageCode, and never
	// reserving the operation UUID (so the same UUID could later execute as
	// new work after an unrelated revocation). Checked before NextVersion
	// deliberately: an active grant sitting at math.MaxInt64 must still
	// report AlreadyActive, never the version-overflow rejection below, since
	// the version-exhaustion state is irrelevant while an active grant makes
	// this whole enrollment attempt illegitimate.
	if _, err := store.CurrentGrant(ctx, tx, p.Conversation); err == nil {
		return nil, store.AlreadyActive
	} else if !errors.Is(err, store.ErrNoActiveGrant) {
		return nil, err
	}
	// Checked before any business-state mutation below (thread
	// PRRT_kwDOUT1JT86j2RK8, root 4049498709): a bare latest+1 could wrap
	// to a negative version if a conversation's history ever reached
	// math.MaxInt64, silently minting an invalid grant_version instead of
	// refusing. NextVersion is the same checked-arithmetic helper
	// store.Coordinator.Execute already uses for its own sequence/revision
	// counters (internal/store/coordinator.go); reusing it here keeps one
	// overflow rule rather than a second, independently written one.
	nextVersion, err := store.NextVersion(latest)
	if err != nil {
		return nil, err
	}

	g := store.Grant{
		Conversation: p.Conversation,
		GrantVersion: nextVersion,
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

	now := authorityNow(ctx)
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
	result, err := RenewTx(ctx, tx, p)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return result.Grant, nil
}

// RenewTx is Renew's actual body, taking an already-open transaction.
func RenewTx(ctx context.Context, tx *sql.Tx, p RenewParams) (*SupersedeResult, error) {
	if err := validateRenewalInput(ctx, p.Conversation, p.MaxExchanges, p.ExpiresAt); err != nil {
		return nil, err
	}
	current, err := currentGrantExpecting(ctx, tx, p.Conversation, p.ExpectedVersion)
	if err != nil {
		return nil, err
	}
	if err := validatePeerIDs(current.PeerAID, current.PeerBID); err != nil {
		return nil, err
	}
	// Peer binding/credential eligibility (store.EnabledPeer) is
	// deliberately NOT checked here: RenewTx is also Controller.Renew's
	// legacy self-opening body (internal/controller/controller_test.go,
	// internal/dispatch's fixtures), which predates the connection
	// registry and operates on bare peer-ID strings with no bindings row
	// at all -- requiring one here would reject every legacy caller.
	// internal/control's handleMembershipRenew (the only PR2 wire caller)
	// performs this recheck itself, against current.PeerAID/PeerBID, after
	// this call returns -- see that handler's comment.
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
func ReplaceTx(ctx context.Context, tx *sql.Tx, p ReplaceParams) (*SupersedeResult, error) {
	if err := validateRenewalInput(ctx, p.Conversation, p.MaxExchanges, p.ExpiresAt); err != nil {
		return nil, err
	}
	if err := validateGrant(ctx, GrantParams{Conversation: p.Conversation, PeerAID: p.PeerAID, PeerBID: p.PeerBID, Direction: p.Direction, MaxExchanges: 1}); err != nil {
		return nil, err
	}
	current, err := currentGrantExpecting(ctx, tx, p.Conversation, p.ExpectedVersion)
	if err != nil {
		return nil, err
	}
	// Review 5255666571 (root 4053127303, thread PRRT_kwDOUT1JT86j_Wm8):
	// RenewTx validates the conversation's current, already-stored peer IDs
	// before superseding (see RenewTx above); this call was missing here,
	// letting a byte-malformed legacy pair -- recorded before today's
	// ASCII-compatibility rule existed -- be silently superseded by an
	// otherwise-valid replacement instead of durably rejected the way a
	// renewal of the same legacy grant already is. p.PeerAID/p.PeerBID
	// (the caller's NEW replacement peers) are validated separately above,
	// via validateGrant; this checks the OLD, stored peers being replaced.
	if err := validatePeerIDs(current.PeerAID, current.PeerBID); err != nil {
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

// SupersedeResult is Renew/Replace's shared result: the successor grant plus
// the exact counts of what happened to the predecessor's queued envelopes,
// mirroring RevokeResult's own three-way split (Cancelled/AlreadyDispatching/
// AlreadyHandedOff) so a renew/replace receipt can report its lifecycle
// effect just as precisely as revoke's does, instead of silently discarding
// CarryForwardQueuedReplies/CancelQueuedUnderVersion's own return counts, or
// -- for AlreadyDispatching/AlreadyHandedOff -- simply never counting them
// at all. Carried has no RevokeResult analogue: revoke has no successor
// version for a reply to carry forward to.
type SupersedeResult struct {
	Grant              *store.Grant
	Carried            int64
	Cancelled          int64
	AlreadyDispatching int64
	AlreadyHandedOff   int64
}

// supersede is Renew/Replace's shared version-bump body: mark the current
// active grant superseded, insert its successor with the given peers/
// direction/budget/expiry, then carry forward eligible queued trusted
// replies before cancelling whatever else is left queued under the old
// version. Called only after the caller's own validation and version check.
func supersede(ctx context.Context, tx *sql.Tx, current *store.Grant, conversation, peerA, peerB string, direction store.Direction,
	maxExchanges int64, expiresAt *time.Time, cancelPendingReplies bool) (*SupersedeResult, error) {
	// Counted before any mutation below, mirroring RevokeTx's identical
	// ordering and identical countByState call: this count is intentionally
	// conversation-wide, not scoped to current.GrantVersion. A hosted review
	// finding (PR #63, issuecomment-5740801367) correctly flagged an earlier
	// version of this comment for claiming the opposite -- that any envelope
	// presently dispatching/handed_off "was necessarily claimed under
	// current.GrantVersion" -- which is false after two successive
	// renewals: an envelope claimed under version N stays dispatching/
	// handed_off across N's own supersession (dispatch claims are never
	// retargeted to a successor version, and Revoke/RenewTx/ReplaceTx never
	// touch an envelope already past Queued), so a later supersede of N+1
	// still finds and reports it, attributed only to "this conversation has
	// outstanding in-flight work," not to the version being superseded right
	// now. Scoping this count by conversation alone is therefore the
	// deliberate design (matching RevokeTx's own identical choice), not an
	// approximation that happens to be exact only for a single renewal --
	// see TestMembershipRenewResultReportsDispatchingAndHandedOffCounts
	// (internal/control/membership_test.go) for the two-renewal regression
	// coverage proving work retained from an earlier grant version is still
	// counted.
	dispatching, err := countByState(ctx, tx, conversation, store.Dispatching)
	if err != nil {
		return nil, err
	}
	handedOff, err := countByState(ctx, tx, conversation, store.HandedOff)
	if err != nil {
		return nil, err
	}
	// Checked before any business-state mutation below (thread
	// PRRT_kwDOUT1JT86j2RK8, root 4049498709): the same NextVersion
	// checked-arithmetic helper GrantTx now uses above, applied to
	// supersede's own successor-version calculation, so a conversation
	// history that has already reached math.MaxInt64 is refused rather
	// than silently wrapping to a negative grant_version. Computed before
	// SetGrantStatus marks the current grant superseded, so a refusal here
	// leaves the current grant's status untouched inside this transaction
	// (which the coordinator also rolls back wholesale on any mutate
	// error, but the ordering itself documents that this check gates
	// supersession, not merely storage).
	newVersion, err := store.NextVersion(current.GrantVersion)
	if err != nil {
		return nil, err
	}
	now := authorityNow(ctx)
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
	carried, err := store.CarryForwardQueuedReplies(ctx, tx, conversation, current.GrantVersion, newVersion, now)
	if err != nil {
		return nil, err
	}
	cancelled, err := store.CancelQueuedUnderVersion(ctx, tx, conversation, current.GrantVersion, now)
	if err != nil {
		return nil, err
	}
	next.Status = store.GrantActive
	return &SupersedeResult{
		Grant: &next, Carried: carried, Cancelled: cancelled,
		AlreadyDispatching: dispatching, AlreadyHandedOff: handedOff,
	}, nil
}

// validateRenewalInput is Renew/Replace's shared conversation/budget/expiry
// input validation, run before any read against the conversation's history.
// The expiry check compares against ctx's single authority instant
// (authorityInstant), not an independently sampled time.Now() -- the same
// MC-03 correction as validateGrant's -- and returns store.RequestExpired,
// an existing, previously never-returned terminal store.Code (already
// documented generically in docs/specifications/control.md and
// connections.md's error-contract tables) rather than a plain error that
// store.Coordinator.Execute's own classification would degrade to
// TemporarilyUnavailable, discarding the specific, audited reason.
func validateRenewalInput(ctx context.Context, conversation string, maxExchanges int64, expiresAt *time.Time) error {
	if err := bridgetext.ValidateMetadata(conversation); err != nil {
		return fmt.Errorf("conversation identifier: %w", err)
	}
	if maxExchanges < 0 {
		return fmt.Errorf("renew requires conversation and nonnegative budget")
	}
	if expiresAt != nil && !expiresAt.After(authorityInstant(ctx)) {
		return store.RequestExpired
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

// authorityInstant returns store.Coordinator.Execute's single
// writer-validated instant when one is installed on ctx (every
// coordinator-driven mutation callback: internal/control's
// membership.enroll/renew/replace/revoke handlers), falling back to the
// process wall clock only for a caller with no coordinator context (the
// legacy self-opening Grant/Revoke/Renew wrappers above). This is the raw
// time.Time extraction authorityNow formats for storage; validateGrant and
// validateRenewalInput also compare an explicit expiry against this exact
// value (MC-03), rather than each independently sampling time.Now() as
// before -- without this, a single membership.enroll's own stored
// granted_at and its explicit-expiry-in-the-future check could disagree
// about "now" whenever the coordinator's resolved authority instant
// diverges from wall time (a clock-rollback/recovery scenario).
func authorityInstant(ctx context.Context) time.Time {
	return store.AuthorityTime(ctx, func() time.Time { return time.Now() })
}

// authorityNow returns authorityInstant formatted for storage. Before this
// split, GrantTx/RevokeTx/supersede each minted their own independent
// time.Now() sample even when called from inside a coordinator transaction
// that had already resolved and validated a single authoritative instant
// for the whole command -- letting a single membership.enroll's EnabledPeer
// check and its own grant's granted_at timestamp disagree about "now".
// store.AuthorityTime is the same time authority
// internal/control/membership.go's handlers already use for their own
// EnabledPeer checks.
func authorityNow(ctx context.Context) string {
	return authorityInstant(ctx).UTC().Format(time.RFC3339Nano)
}

func formatOptionalTime(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := t.UTC().Format(time.RFC3339Nano)
	return &s
}

func validateGrant(ctx context.Context, p GrantParams) error {
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
	if p.ExpiresAt != nil && !p.ExpiresAt.After(authorityInstant(ctx)) {
		return store.RequestExpired
	}
	return nil
}

// validatePeerIDs checks each id against the same ASCII-compatibility rule
// enforced everywhere else (bridgetext.ValidateMetadata). Its callers are
// not equivalent: validateGrant applies it to freshly supplied peer IDs on
// an enroll/replace path membership.Validate has already filtered upstream
// on every reachable wire route (so a malformed value here is unreachable
// in practice via the coordinator); RenewTx (below) applies it to a
// conversation's already-stored, historical peer IDs, where a legacy value
// predating today's rule is a real, reachable condition. Returning
// store.IncompatibleIdentifier here -- rather than a plain wrapped error --
// gives that reachable RenewTx case a terminal, durably audited rejection
// instead of degrading to TemporarilyUnavailable (see
// store.IncompatibleIdentifier's own doc comment); the never-actually-taken
// validateGrant path inherits the same return value, which is harmless
// since it is not reachable with malformed input via the coordinator.
func validatePeerIDs(ids ...string) error {
	for _, id := range ids {
		if err := bridgetext.ValidateMetadata(id); err != nil {
			return store.IncompatibleIdentifier
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
