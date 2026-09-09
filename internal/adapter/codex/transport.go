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

// ErrRecipientMismatch marks an envelope addressed to a peer other than the
// one this Transport is bound to. Nothing upstream of Deliver (Bridge.Send,
// IngestTurn, Bridge.Dispatch) filters envelopes by ToPeer before calling a
// Transport, so a single Codex Transport wired into a Bridge alongside other
// peers' envelopes must reject a misdirected one itself, not queue it into
// its own thread under its own fromLabel.
var ErrRecipientMismatch = errors.New("envelope addressed to a different peer than this transport is bound to")

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
	return runAndClassify(ctx, cmd)
}

// runAndClassify runs cmd and classifies a failure. Only a context outcome
// observed after the process actually started is ambiguous — cmd.Process is
// set once exec successfully forks/execs it. A context already
// canceled/expired before that point means the command never ran at all, so
// nothing could have committed: a plain, safely retryable failure, not
// 'uncertain'. Split out from QueueMessage so it can be exercised directly
// against a real short-lived process, without depending on the codex binary.
func runAndClassify(ctx context.Context, cmd *exec.Cmd) error {
	err := cmd.Run()
	if err == nil {
		return nil
	}
	if ctx.Err() != nil && cmd.Process != nil {
		return fmt.Errorf("%w: %v", ErrQueueAmbiguous, err)
	}
	return fmt.Errorf("codex queue: %w", err)
}

// Transport implements dispatch.Transport for one Codex thread, bound to
// exactly the one peer identity (toPeer) that thread represents. fromLabel is
// the bridge-assigned sender name stamped on every wrapped message (§2), not
// a free-text field the message content can override.
type Transport struct {
	sender    QueueSender
	threadID  string
	fromLabel string
	toPeer    string
}

func NewTransport(sender QueueSender, threadID, fromLabel, toPeer string) *Transport {
	return &Transport{sender: sender, threadID: threadID, fromLabel: fromLabel, toPeer: toPeer}
}

func (t *Transport) Deliver(ctx context.Context, e store.Envelope) error {
	if e.ToPeer != t.toPeer {
		return fmt.Errorf("%w: envelope to %q, transport bound to %q", ErrRecipientMismatch, e.ToPeer, t.toPeer)
	}
	wrapped, err := bridgetext.Wrap(t.fromLabel, e.Text)
	if err != nil {
		return err
	}
	if err := t.sender.QueueMessage(ctx, t.threadID, wrapped); err != nil {
		if errors.Is(err, ErrQueueAmbiguous) {
			return fmt.Errorf("%w: %v", dispatch.ErrAmbiguous, err)
		}
		return err
	}
	return nil
}
