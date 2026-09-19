package store

import (
	"context"
	"database/sql"
	"time"
)

// RecoveryHooks are trusted runtime wiring, installed before admitting services.
// Before/After may persist external markers; neither runs under the writer gate.
// Time validates one instant under the writer and performs no external I/O.
type RecoveryHooks struct {
	Before func(context.Context, string) error
	Time   func(context.Context, *sql.Tx, string) (time.Time, error)
	After  func() error
}
type recoveryContextKey struct{}
type authorityTimeKey struct{}
type commandIdentityKey struct{}

func CommandIdentity(ctx context.Context) (CommandPrincipal, bool) {
	p, ok := ctx.Value(commandIdentityKey{}).(CommandPrincipal)
	return p, ok
}

type RecoveryMaintenance struct{ coordinator *Coordinator }

func (c *Coordinator) InstallRecovery(h RecoveryHooks) (*RecoveryMaintenance, error) {
	if h.Before == nil || h.Time == nil || h.After == nil {
		return nil, InvalidRequest
	}
	ctx, cancel := context.WithTimeout(context.Background(), AuthenticationDeadline)
	defer cancel()
	if err := c.lock(ctx); err != nil {
		return nil, storageCode(err)
	}
	defer func() { <-c.gate }()
	if c.revision != 0 || c.connectionsClaimed || c.recovery.Load() != nil {
		return nil, InvalidRequest
	}
	c.recovery.Store(&h)
	return &RecoveryMaintenance{c}, nil
}
func (m *RecoveryMaintenance) Transition(ctx context.Context, change func(context.Context, *sql.Tx, CommitView) (TransitionResult, error), publish func(CommitView)) (Code, error) {
	return m.coordinator.Transition(context.WithValue(ctx, recoveryContextKey{}, m.coordinator), change, publish)
}
func (m *RecoveryMaintenance) Inspect(ctx context.Context, read func(context.Context, *sql.Tx) error) error {
	return m.coordinator.Inspect(context.WithValue(ctx, recoveryContextKey{}, m.coordinator), read)
}
func (c *Coordinator) operationHooks(ctx context.Context) *RecoveryHooks {
	if owner, _ := ctx.Value(recoveryContextKey{}).(*Coordinator); owner == c {
		return nil
	}
	return c.recovery.Load()
}
func (c *Coordinator) transactionContext(ctx context.Context, tx *sql.Tx, kind string) (context.Context, error) {
	h := c.operationHooks(ctx)
	if h == nil {
		return ctx, nil
	}
	instant, err := h.Time(ctx, tx, kind)
	if err != nil {
		return ctx, storageCode(err)
	}
	if _, err := InstantNanos(instant); err != nil {
		return ctx, storageCode(err)
	}
	return context.WithValue(ctx, authorityTimeKey{}, instant), nil
}

// AuthorityTime uses the single validated writer instant when recovery owns the
// runtime. The fallback remains for explicitly injected controlled unit fixtures.
func AuthorityTime(ctx context.Context, fallback func() time.Time) time.Time {
	if instant, ok := ctx.Value(authorityTimeKey{}).(time.Time); ok {
		return instant
	}
	return fallback()
}
func (c *Coordinator) Execute(ctx context.Context, p CommandPrincipal, r CommandRequest, authorize func(context.Context, *sql.Tx) error, mutate func(context.Context, *sql.Tx) (CommandResult, error), publish func(CommitView)) (receipt CommandReceipt, err error) {
	ctx = context.WithValue(ctx, commandIdentityKey{}, p)
	h := c.operationHooks(ctx)
	if h != nil {
		defer func() {
			if afterErr := h.After(); afterErr != nil {
				// EC-02 (2026-09-19 review): if err is already nil here, the
				// business transaction below has already durably committed
				// -- both a genuine success and a terminal domain rejection
				// commit their operation_results/command_audit rows before
				// this deferred call ever runs (c.execute's own commit is
				// unconditional in either case; only an ambiguous
				// tx.Commit() itself already sets err to OutcomeUnknown
				// before reaching here, which the `err == nil` guard below
				// correctly excludes). After's own failure is a completely
				// separate concern -- an independent external marker/
				// checkpoint flush (internal/recovery.Service.after) that
				// has already triggered its own FailStop callback -- and must
				// never be reported as if the mutation itself never
				// happened or was rejected: doing so previously discarded
				// the caller's only evidence (the receipt's OperationID,
				// AuditID and CommitView) that a real, committed operation
				// exists, making a well-formed error response look like a
				// proven non-commitment when it was not one. Preserve the
				// receipt and report OutcomeUnknown -- the same "uncertain,
				// safe to retry with the same operation ID" contract a
				// client-side *TimeoutError already carries -- instead of
				// zeroing the receipt and surfacing whatever incidental
				// code afterErr happened to carry (often RecoveryRequired,
				// which elsewhere always means "provably never committed").
				//
				// Review 5256660570 (comment 4053958839): the original
				// EC-02 fix above only special-cased err == nil, missing
				// the case where c.execute's own ambiguous tx.Commit had
				// already set err = OutcomeUnknown before this deferred
				// call ever runs (comment above). That is already the
				// correct, most-cautious "uncertain, safe to retry"
				// classification for a commit whose outcome could not be
				// observed -- After's own unrelated failure must not
				// downgrade it to a different storage code that discards
				// the receipt and asserts a stronger, unproven claim.
				if err == nil || err == OutcomeUnknown {
					err = OutcomeUnknown
					return
				}
				receipt = CommandReceipt{}
				err = storageCode(afterErr)
			}
		}()
		if err := h.Before(ctx, r.kind); err != nil {
			return CommandReceipt{}, storageCode(err)
		}
	}
	return c.execute(ctx, p, r, authorize, mutate, publish)
}
func (c *Coordinator) Transition(ctx context.Context, change func(context.Context, *sql.Tx, CommitView) (TransitionResult, error), publish func(CommitView)) (Code, error) {
	return c.transitionWithHooks(ctx, "connection", change, publish)
}

// Settle records only exact already-claimed dispatch outcomes and refunds. It
// runs recovery detection but remains available during a hold; callers must not
// introduce new authority or work through this trusted internal path.
func (c *Coordinator) Settle(ctx context.Context, change func(context.Context, *sql.Tx, CommitView) (TransitionResult, error)) (Code, error) {
	return c.transitionWithHooks(ctx, "dispatch.settle", change, nil)
}
func (c *Coordinator) transitionWithHooks(ctx context.Context, kind string, change func(context.Context, *sql.Tx, CommitView) (TransitionResult, error), publish func(CommitView)) (code Code, err error) {
	h := c.operationHooks(ctx)
	if h != nil {
		defer func() {
			if afterErr := h.After(); afterErr != nil {
				code = ""
				err = storageCode(afterErr)
			}
		}()
		if err := h.Before(ctx, kind); err != nil {
			return "", storageCode(err)
		}
	}
	return c.transition(ctx, kind, change, publish)
}
func (c *Coordinator) Inspect(ctx context.Context, read func(context.Context, *sql.Tx) error) (err error) {
	h := c.operationHooks(ctx)
	if h != nil {
		defer func() {
			if afterErr := h.After(); afterErr != nil {
				err = storageCode(afterErr)
			}
		}()
		if err := h.Before(ctx, "human_inspection"); err != nil {
			return storageCode(err)
		}
	}
	return c.inspect(ctx, read)
}

// RecoveryControlled reports whether the owning runtime installed recovery policy.
func (d *DB) RecoveryControlled() bool { return d.coordinator.recovery.Load() != nil }
