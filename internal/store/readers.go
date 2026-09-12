package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const ReadTimeout = 5 * time.Second
const MaxReaders = 4

var ErrReadersNotReady = errors.New("readers_not_ready")

func readerDSN(path string) (string, error) {
	u, err := normalizedDatabaseURL(path)
	if err != nil {
		return "", err
	}
	q := u.Query()
	if u.Opaque == ":memory:" || u.Path == ":memory:" || q.Get("mode") == "memory" {
		return "", fmt.Errorf("readers require a file-backed database")
	}
	q.Set("mode", "ro")
	q.Set("_txlock", "deferred")
	q.Set("_query_only", "on")
	q.Set("_busy_timeout", "5000")
	q.Set("_foreign_keys", "on")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// OpenReaders is a startup-only operation, after migration and recovery. It
// publishes the pool only after all four connections have opened successfully.
// Memory/URI library writers remain supported; runtime readers require a file.
func (d *DB) OpenReaders(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, ReadTimeout)
	defer cancel()
	d.lifecycle.Lock()
	defer d.lifecycle.Unlock()
	if d.closed {
		return sql.ErrConnDone
	}
	if d.readers.Load() != nil {
		return nil
	}
	dsn, err := readerDSN(d.path)
	if err != nil {
		return err
	}
	pool, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}
	pool.SetMaxOpenConns(MaxReaders)
	pool.SetMaxIdleConns(MaxReaders)
	var conns []*sql.Conn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for range MaxReaders {
		c, err := pool.Conn(ctx)
		if err != nil {
			for _, c := range conns {
				c.Close()
			}
			pool.Close()
			return err
		}
		conns = append(conns, c)
	}
	if err := ctx.Err(); err != nil {
		for _, c := range conns {
			c.Close()
		}
		pool.Close()
		return err
	}
	d.readers.Store(pool)
	return nil
}

// Queries cannot execute caller SQL or expose a transaction/connection. Returned
// values are detached from SQLite before the caller can perform external I/O.
type Queries struct{ db *DB }

func (d *DB) Queries() Queries { return Queries{db: d} }

func (q Queries) snapshot(ctx context.Context, read func(context.Context, *sql.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, ReadTimeout)
	defer cancel()
	if q.db == nil {
		return ErrReadersNotReady
	}
	pool := q.db.readers.Load()
	if pool == nil {
		return ErrReadersNotReady
	}
	tx, err := pool.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := read(ctx, tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return tx.Commit()
}

// QueueBatch includes tail wrap in the same snapshot and deadline. wrapped tells
// the poller to discard its old cursor even when dispatch subsequently fails.
func (q Queries) QueueBatch(ctx context.Context, conversation, toPeer string, limit int, after *QueueCursor) (batch []QueueCursor, wrapped bool, err error) {
	err = q.snapshot(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var e error
		batch, e = ListQueuedIDs(ctx, tx, conversation, toPeer, limit, after)
		if e == nil && len(batch) == 0 && after != nil {
			wrapped = true
			batch, e = ListQueuedIDs(ctx, tx, conversation, toPeer, limit, nil)
		}
		return e
	})
	if err != nil {
		return nil, false, err
	}
	return batch, wrapped, nil
}

// EnvelopeOutcome omits payload and authorization data. This is an internal
// observation, never evidence permitting delivery or disclosure to an agent.
type EnvelopeOutcome struct {
	ID                     string
	State                  EnvelopeState
	ErrorCode, ErrorDetail string
}

func (q Queries) Outcome(ctx context.Context, id string) (out EnvelopeOutcome, err error) {
	err = q.snapshot(ctx, func(ctx context.Context, tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `SELECT id,state,error_code,error_detail FROM envelopes WHERE id=?`, id).Scan(&out.ID, &out.State, &out.ErrorCode, &out.ErrorDetail)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrEnvelopeNotFound
		}
		return err
	})
	if err != nil {
		return EnvelopeOutcome{}, err
	}
	return out, nil
}
