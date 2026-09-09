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
// queued. The generation captured at the start of the batch is also
// rechecked before each dispatch: Ready() alone can't tell a still-current
// connection from a brand new one that raced back to ready by the time this
// loop gets to a later envelope, and only the connection that authorized
// this batch may keep authorizing it. On the grant's budget running out,
// the whole batch stops rather than continuing through the remaining
// candidates, each of which would hit the same exhausted budget. Returns
// the envelope ids actually dispatched, for tests.
func (p *Poller) Tick(ctx context.Context) ([]string, error) {
	if !p.handshake.Ready() {
		return nil, nil
	}
	gen := p.handshake.Generation()

	candidates, err := p.queuedForMe(ctx)
	if err != nil {
		return nil, err
	}
	var attempted []string
	for _, id := range candidates {
		if !p.handshake.Ready() || p.handshake.Generation() != gen {
			break
		}
		attempted = append(attempted, id)
		if _, err := p.bridge.Dispatch(ctx, id); err != nil {
			if errors.Is(err, dispatch.ErrBudgetExhausted) {
				break
			}
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
