package store

import (
	"context"
	"database/sql"
)

// TransitionResult describes a connection-specific transition, which has no
// ordinary operation receipt. A rejection may still commit terminal credential
// expiry. Errors instead roll back all effects and suppress publication.
type TransitionResult struct {
	Changed bool
	Code    Code
}

// Transition serializes connection state with administrative commands. change
// may stage SQL but must not mutate process state; publish installs process state
// after commit under the same gate. Neither callback may perform external I/O.
// Unchanged transitions roll back even accidental SQL writes.
func (c *Coordinator) transition(ctx context.Context,
	change func(context.Context, *sql.Tx, CommitView) (TransitionResult, error),
	publish func(CommitView),
) (Code, error) {
	if change == nil {
		return "", InvalidRequest
	}
	if err := c.lock(ctx); err != nil {
		return "", storageCode(err)
	}
	defer func() { <-c.gate }()
	if c.failed {
		return "", RecoveryRequired
	}
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return "", storageCode(err)
	}
	defer tx.Rollback()
	ctx, err = c.transactionContext(ctx, tx, "connection")
	if err != nil {
		return "", err
	}
	result, err := change(ctx, tx, CommitView{c.epoch, c.revision})
	if err != nil {
		return "", storageCode(err)
	}
	if !result.Code.valid() {
		return "", InvalidRequest
	}
	if !result.Changed {
		return result.Code, nil
	}
	revision, err := NextVersion(c.revision)
	if err != nil {
		c.failed = true
		return "", err
	}
	if err := c.commit(tx); err != nil {
		return "", err
	}
	c.revision = revision
	if publish != nil {
		publish(CommitView{c.epoch, revision})
	}
	return result.Code, nil
}

// ClaimConnections reserves one process-local connection registry for this
// writer's lifetime. A second manager cannot bypass the first manager's slots.
func (c *Coordinator) ClaimConnections(ctx context.Context) error {
	if err := c.lock(ctx); err != nil {
		return storageCode(err)
	}
	defer func() { <-c.gate }()
	if c.connectionsClaimed {
		return InvalidRequest
	}
	c.connectionsClaimed = true
	return nil
}
