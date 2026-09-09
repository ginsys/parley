package claude

import (
	"context"
	"errors"

	"github.com/ginsys/parley/internal/dispatch"
	"github.com/ginsys/parley/internal/store"
)

// Poller drives ordinary Dispatch for one conversation's envelopes addressed
// to this adapter's peer, gated on the current connection's Handshake. Per
// the design plan, a message accepted before readiness must stay queued,
// never advance to dispatching, until the handshake's nonce is acknowledged.
type Poller struct {
	db           *store.DB
	bridge       *dispatch.Bridge
	handshake    *Handshake
	conversation string
	toPeer       string
}

func NewPoller(db *store.DB, bridge *dispatch.Bridge, handshake *Handshake, conversation, toPeer string) *Poller {
	return &Poller{db: db, bridge: bridge, handshake: handshake, conversation: conversation, toPeer: toPeer}
}

// Tick attempts dispatch of every currently queued envelope addressed to
// this adapter's peer, but only while the handshake is ready — re-checked
// before each individual dispatch, not just once before the batch, since a
// reconnect (Reset/Stop) can revoke readiness mid-batch and the remaining
// envelopes must stop being dispatched immediately, not finish the run.
// Not ready is not an error — it means every such envelope correctly stays
// queued. Returns the envelope ids actually dispatched, for tests.
func (p *Poller) Tick(ctx context.Context) ([]string, error) {
	if !p.handshake.Ready() {
		return nil, nil
	}
	candidates, err := p.queuedForMe(ctx)
	if err != nil {
		return nil, err
	}
	var attempted []string
	for _, id := range candidates {
		if !p.handshake.Ready() {
			break
		}
		attempted = append(attempted, id)
		if _, err := p.bridge.Dispatch(ctx, id); err != nil && !errors.Is(err, dispatch.ErrBudgetExhausted) {
			return attempted, err
		}
	}
	return attempted, nil
}

func (p *Poller) queuedForMe(ctx context.Context) ([]string, error) {
	tx, err := p.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	envs, err := store.ListQueued(ctx, tx, p.conversation)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range envs {
		if e.ToPeer == p.toPeer {
			ids = append(ids, e.ID)
		}
	}
	return ids, nil
}
