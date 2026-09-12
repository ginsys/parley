package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ginsys/parley/internal/bridgetext"
	"github.com/google/uuid"
)

// CommandPrincipal is trusted internal input, never an authentication mechanism.
// The endpoint must derive it from authenticated server state, not a payload.
type CommandPrincipal struct {
	ID           string
	ConnectorUID uint32
}

// ResourceChange retains safe identifiers and version evidence, never message
// bodies, credential verifiers, provisioning paths or arbitrary diagnostic text.
type ResourceChange struct {
	Kind   string `json:"kind"`
	ID     string `json:"id"`
	Before int64  `json:"before"`
	After  int64  `json:"after"`
}
type CommandResult struct {
	Code      Code             `json:"code"`
	Resources []ResourceChange `json:"resources"`
}
type CommitView struct {
	Epoch    string
	Revision int64
}
type CommandReceipt struct {
	Result        CommandResult
	AuditID       string
	AuditSequence int64
	View          CommitView
	Replayed      bool
}

// Coordinator is one instance per writer. Every mutation callback uses its
// immediate transaction. Callbacks must not perform I/O, start another writer
// transaction, or reenter the coordinator. Publication is process-local and
// infallible; socket writes and credential publication happen after Execute.
type Coordinator struct {
	db                 *DB
	gate               chan struct{}
	once               sync.Once
	epoch              string
	initErr            error
	revision           int64
	failed             bool
	connectionsClaimed bool
	// Exact credential expiries observed but not yet persisted survive failed calls.
	credentialExpiries sync.Map
	recovery           atomic.Pointer[RecoveryHooks]
	now                func() time.Time
}

func (d *DB) Coordinator() *Coordinator { return d.coordinator }
func (c *Coordinator) lock(ctx context.Context) error {
	select {
	case c.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-c.gate
		return err
	}
	c.once.Do(func() { id, err := uuid.NewRandom(); c.initErr = err; c.epoch = id.String() })
	if c.initErr != nil {
		<-c.gate
		return TemporarilyUnavailable
	}
	return nil
}

// Execute reauthorizes even a replay before reading its private receipt. The
// authorization callback checks access to the command/result; mutation-specific
// version/hold checks belong in mutate so a valid replay precedes stale guards.
// A terminal domain rejection is returned in Result.Code and commits an audit
// with NO business effects. Infrastructure errors roll the entire transaction back.
func (c *Coordinator) execute(ctx context.Context, p CommandPrincipal, r CommandRequest,
	authorize func(context.Context, *sql.Tx) error,
	mutate func(context.Context, *sql.Tx) (CommandResult, error),
	publish func(CommitView),
) (CommandReceipt, error) {
	if !validUUID(p.ID) || !validUUID(r.id) || r.kind == "" || authorize == nil || mutate == nil {
		return CommandReceipt{}, InvalidRequest
	}
	if err := c.lock(ctx); err != nil {
		return CommandReceipt{}, storageCode(err)
	}
	defer func() { <-c.gate }()
	if c.failed {
		return CommandReceipt{}, RecoveryRequired
	}
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return CommandReceipt{}, storageCode(err)
	}
	defer tx.Rollback()
	ctx, err = c.transactionContext(ctx, tx, r.kind)
	if err != nil {
		return CommandReceipt{}, err
	}
	if err := authorize(ctx, tx); err != nil {
		return CommandReceipt{}, storageCode(err)
	}
	receipt, err := lookupReceipt(ctx, tx, p.ID, r)
	if err == nil {
		receipt.Replayed = true
		return receipt, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return CommandReceipt{}, storageCode(err)
	}
	var sequence int64
	if err := tx.QueryRowContext(ctx, "SELECT audit_sequence FROM installation WHERE singleton=1").Scan(&sequence); err != nil {
		return CommandReceipt{}, storageCode(err)
	}
	nextSequence, err := NextVersion(sequence)
	if err != nil {
		return CommandReceipt{}, err
	}
	nextRevision, err := NextVersion(c.revision)
	if err != nil {
		c.failed = true
		return CommandReceipt{}, err
	}
	now, err := InstantNanos(AuthorityTime(ctx, c.now))
	if err != nil {
		return CommandReceipt{}, err
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return CommandReceipt{}, TemporarilyUnavailable
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT command_effect"); err != nil {
		return CommandReceipt{}, storageCode(err)
	}
	result, err := mutate(ctx, tx)
	if err != nil {
		return CommandReceipt{}, storageCode(err)
	}
	if !result.Code.valid() {
		return CommandReceipt{}, InvalidRequest
	}
	if !result.Code.terminalResult() {
		return CommandReceipt{}, result.Code
	}
	if result.Code != "" {
		// Even a buggy rejecting callback cannot commit a partial state transition.
		if _, err := tx.ExecContext(ctx, "ROLLBACK TO command_effect"); err != nil {
			return CommandReceipt{}, storageCode(err)
		}
	}
	if _, err := tx.ExecContext(ctx, "RELEASE command_effect"); err != nil {
		return CommandReceipt{}, storageCode(err)
	}
	if len(result.Resources) > 100 {
		return CommandReceipt{}, InvalidRequest
	}
	for _, resource := range result.Resources {
		if len(resource.Kind) > 64 || bridgetext.ValidateMetadata(resource.Kind) != nil || len(resource.ID) > MaxIdentityBytes || bridgetext.ValidateMetadata(resource.ID) != nil || resource.Before < 0 || resource.After < 0 {
			return CommandReceipt{}, InvalidRequest
		}
	}
	data, err := json.Marshal(result)
	if err != nil {
		return CommandReceipt{}, InvalidRequest
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO operation_results(principal_id,operation_id,operation_kind,request_digest,result_json) VALUES(?,?,?,?,?)`, p.ID, r.id, r.kind, r.digest[:], string(data)); err != nil {
		return CommandReceipt{}, storageCode(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO command_audit(audit_id,audit_sequence,principal_id,connector_uid,operation_id,operation_kind,request_digest,server_time_ns,result_json,commit_epoch,commit_revision) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, id.String(), nextSequence, p.ID, p.ConnectorUID, r.id, r.kind, r.digest[:], now, string(data), c.epoch, nextRevision); err != nil {
		return CommandReceipt{}, storageCode(err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE installation SET audit_sequence=? WHERE singleton=1", nextSequence); err != nil {
		return CommandReceipt{}, storageCode(err)
	}
	if err := c.commit(tx); err != nil {
		return CommandReceipt{}, err
	}
	c.revision = nextRevision
	view := CommitView{c.epoch, nextRevision}
	if publish != nil {
		publish(view)
	}
	return CommandReceipt{Result: result, AuditID: id.String(), AuditSequence: nextSequence, View: view}, nil
}
func lookupReceipt(ctx context.Context, tx *sql.Tx, principal string, r CommandRequest) (CommandReceipt, error) {
	var result CommandReceipt
	var kind, data string
	var digest []byte
	err := tx.QueryRowContext(ctx, `SELECT o.operation_kind,o.request_digest,o.result_json,a.audit_id,a.audit_sequence,a.commit_epoch,a.commit_revision FROM operation_results o JOIN command_audit a USING(principal_id,operation_id) WHERE o.principal_id=? AND o.operation_id=?`, principal, r.id).Scan(&kind, &digest, &data, &result.AuditID, &result.AuditSequence, &result.View.Epoch, &result.View.Revision)
	if err != nil {
		return result, err
	}
	if kind != r.kind || string(digest) != string(r.digest[:]) {
		return CommandReceipt{}, OperationConflict
	}
	if err := json.Unmarshal([]byte(data), &result.Result); err != nil {
		return CommandReceipt{}, TemporarilyUnavailable
	}
	return result, nil
}

func (c *Coordinator) commit(tx *sql.Tx) error {
	if err := tx.Commit(); err != nil {
		// With the pinned driver, Commit executes under context.Background. These
		// sentinels therefore come from database/sql's pre-commit cancellation/done
		// checks: the driver commit was not invoked. Do not infer this merely from
		// ctx.Err(), which could become non-nil after an ambiguous driver failure.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, sql.ErrTxDone) {
			return TemporarilyUnavailable
		}
		c.failed = true
		return OutcomeUnknown
	}
	return nil
}
