package codex

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/ginsys/parley/internal/replymarker"
	"github.com/ginsys/parley/internal/store"
)

// ErrNoMarker re-exports replymarker.ErrNoMarker for callers that only need
// to distinguish "ordinary conversation, nothing to do" from every other
// outcome, without importing replymarker directly.
var ErrNoMarker = replymarker.ErrNoMarker

// ErrDirectionNotPermitted means the current grant does not permit a
// message from fromPeer to the marker's "to" — either because Direction is
// one-directional and this is the disallowed way, or because one of the two
// identities isn't the grant's enrolled pair at all.
var ErrDirectionNotPermitted = errors.New("grant does not permit this message direction")

// IngestTurn examines one Codex turn's final-answer text for a BRIDGE-REPLY
// marker (internal/replymarker) and, if it validates, atomically marks the
// envelope it replies to 'acked' and queues the reply as a new outbound
// envelope from fromPeer to the marker's "to". Ordinary conversation with no
// marker is reported as ErrNoMarker, never forwarded — this is the expected,
// common case, not a failure the caller needs to alarm on.
//
// expectedTo is the enrolled peer this conversation forwards replies to
// (the Claude-side identity) — a marker addressed elsewhere is rejected
// (§1b), never forwarded on the strength of syntax alone.
func IngestTurn(ctx context.Context, db *store.DB, conversation, fromPeer, expectedTo, turnText string) (*store.Envelope, error) {
	marker, err := replymarker.Extract(turnText)
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	repliedTo, err := replymarker.Validate(ctx, tx, conversation, fromPeer, expectedTo, marker)
	if err != nil {
		return nil, err
	}

	g, err := store.CurrentGrant(ctx, tx, conversation)
	if err != nil {
		return nil, err
	}
	nowTime := time.Now().UTC()
	if !g.Permits(fromPeer, marker.To, nowTime) {
		return nil, fmt.Errorf("%w: %s -> %s under grant version %d", ErrDirectionNotPermitted, fromPeer, marker.To, g.GrantVersion)
	}

	now := nowTime.Format(time.RFC3339Nano)
	if err := store.SetState(ctx, tx, repliedTo.ID, store.Acked, now); err != nil {
		return nil, fmt.Errorf("ack %s: %w", repliedTo.ID, err)
	}

	inReplyTo := marker.InReplyTo
	reply := store.Envelope{
		ID:           uuid.NewString(),
		Conversation: conversation,
		FromPeer:     fromPeer,
		ToPeer:       marker.To,
		Text:         marker.Text,
		GrantVersion: g.GrantVersion,
		InReplyTo:    &inReplyTo,
		TrustedReply: true,
		State:        store.Queued,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := store.InsertQueued(ctx, tx, reply); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return &reply, nil
}

// IsNoMarker reports whether err means the turn had no BRIDGE-REPLY marker —
// ordinary conversation, not an error condition.
func IsNoMarker(err error) bool {
	return errors.Is(err, replymarker.ErrNoMarker)
}
