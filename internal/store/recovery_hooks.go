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
func (c *Coordinator) Transition(ctx context.Context, change func(context.Context, *sql.Tx, CommitView) (TransitionResult, error), publish func(CommitView)) (code Code, err error) {
	h := c.operationHooks(ctx)
	if h != nil {
		defer func() {
			if afterErr := h.After(); afterErr != nil {
				code = ""
				err = storageCode(afterErr)
			}
		}()
		if err := h.Before(ctx, "connection"); err != nil {
			return "", storageCode(err)
		}
	}
	return c.transition(ctx, change, publish)
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
