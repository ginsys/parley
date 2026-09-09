// Package codex will hold Parley's Codex-side adapter: a Transport that
// hands text to a Codex thread via `codex queue` (Probe A), and a reply
// ingester that reads a thread's turns for BRIDGE-REPLY markers
// (internal/replymarker).
package codex

import (
	"context"
	"errors"
	"fmt"
	"os/exec"

	"github.com/ginsys/parley/internal/bridgetext"
	"github.com/ginsys/parley/internal/dispatch"
	"github.com/ginsys/parley/internal/store"
)

// ErrQueueAmbiguous marks a QueueMessage outcome where whether the
// underlying `codex queue` write actually committed can't be determined
// (e.g. a context deadline during the call) — Transport.Deliver reports this
// as dispatch.ErrAmbiguous, which Bridge records as 'uncertain' and never
// retries automatically (design plan §3).
var ErrQueueAmbiguous = errors.New("codex queue outcome ambiguous")

// QueueSender is the one host operation this adapter needs: hand text to a
// specific thread's queue. ExecSender is the real implementation; tests use
// a fake.
type QueueSender interface {
	QueueMessage(ctx context.Context, threadID, text string) error
}

// ExecSender shells out to the codex CLI directly, per Probe A's verified
// invocation: `codex queue --thread <id> --message <text>`, exit 0 is the
// only observable success event.
type ExecSender struct{}

func (ExecSender) QueueMessage(ctx context.Context, threadID, text string) error {
	cmd := exec.CommandContext(ctx, "codex", "queue", "--thread", threadID, "--message", text)
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %v", ErrQueueAmbiguous, err)
		}
		return fmt.Errorf("codex queue: %w", err)
	}
	return nil
}

// Transport implements dispatch.Transport for one Codex thread. fromLabel is
// the bridge-assigned sender name stamped on every wrapped message (§2), not
// a free-text field the message content can override.
type Transport struct {
	sender    QueueSender
	threadID  string
	fromLabel string
}

func NewTransport(sender QueueSender, threadID, fromLabel string) *Transport {
	return &Transport{sender: sender, threadID: threadID, fromLabel: fromLabel}
}

func (t *Transport) Deliver(ctx context.Context, e store.Envelope) error {
	wrapped := bridgetext.Wrap(t.fromLabel, e.Text)
	if err := t.sender.QueueMessage(ctx, t.threadID, wrapped); err != nil {
		if errors.Is(err, ErrQueueAmbiguous) {
			return fmt.Errorf("%w: %v", dispatch.ErrAmbiguous, err)
		}
		return err
	}
	return nil
}
