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
	"strings"

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

// ErrQueueNotAttempted marks a QueueMessage outcome where the codex process
// definitely never started — the context was already canceled/expired
// before exec forked it (cmd.Process stays nil in that case) — or the
// message was rejected before exec was even attempted (ErrMessageTooLarge).
// Transport.Deliver reports this as dispatch.ErrNoAttempt, which Dispatch
// refunds and requeues rather than terminally failing (design plan §3/§4):
// unlike ErrQueueAmbiguous, there is no possibility the host already saw
// this message.
var ErrQueueNotAttempted = errors.New("codex queue never attempted")

// ErrRecipientMismatch marks an envelope whose ToPeer or FromPeer doesn't
// match the peer identities this Transport is bound to. Nothing upstream of
// Deliver (Bridge.Send, IngestTurn, Bridge.Dispatch) filters envelopes by
// peer before calling a Transport, so a single Codex Transport wired into a
// Bridge alongside other peers' envelopes must reject a misdirected or
// misattributed one itself — a FromPeer mismatch would otherwise queue a
// message into this thread stamped with this transport's own fromLabel as
// if it came from the bound peer, regardless of who it actually came from.
var ErrRecipientMismatch = errors.New("envelope addressed to or from a different peer than this transport is bound to")

// maxMessageBytes bounds the wrapped text ExecSender ever puts in a single
// argv element. Linux caps a single exec argument at MAX_ARG_STRLEN (128
// KiB); `codex queue --help` offers no stdin or file-based alternative to
// --message, so a message any larger fails deep inside exec with an opaque
// E2BIG rather than a message this package can classify. This limit stays
// safely under that kernel ceiling so oversized text is rejected here, with
// a clear error, before ever calling exec.
const maxMessageBytes = 96 * 1024

// ErrMessageTooLarge marks a wrapped message that would risk exceeding
// Linux's per-argument exec limit if handed to `codex queue --message`.
var ErrMessageTooLarge = errors.New("message exceeds codex queue's argv size limit")

// ErrMessageContainsNUL marks a wrapped message containing a NUL byte.
// os/exec rejects a NUL-containing argv element before ever forking the
// process, leaving cmd.Process nil — indistinguishable, to runAndClassify,
// from any other exec-start failure, and so classified as
// ErrQueueNotAttempted (transient, safely retryable) by default. That's
// wrong here: a NUL byte is immutable content of this exact envelope, not a
// transient host condition, so retrying would reproduce the identical
// rejection forever. Checked and classified permanent here, before exec is
// ever attempted, the same way ErrMessageTooLarge is.
var ErrMessageContainsNUL = errors.New("message contains a NUL byte, which os/exec cannot pass as an argument")

// ErrQueuePermanentlyRejected marks a QueueMessage outcome that is rejected
// before exec is ever called, for a reason the message's own content
// guarantees will still hold on every future retry (currently: size).
// Transport.Deliver reports this as dispatch.ErrPermanentlyRejected, which
// Dispatch refunds but never requeues — unlike ErrQueueNotAttempted's
// transient causes (a canceled context), retrying an oversized message can
// only ever reproduce the identical rejection, so refund-and-requeue would
// loop forever instead of resolving.
var ErrQueuePermanentlyRejected = errors.New("codex queue permanently rejected the message")

// QueueSender is the one host operation this adapter needs: hand text to a
// specific thread's queue. ExecSender is the real implementation; tests use
// a fake.
type QueueSender interface {
	QueueMessage(ctx context.Context, threadID, text string) error
}

// ExecSender shells out to the codex CLI directly, per Probe A's verified
// invocation: `codex queue --thread <id> --message <text>`, exit 0 is the
// only observable success event.
type ExecSender struct {
	// command is an optional process factory for controlled adapter tests.
	command func(context.Context, string, ...string) *exec.Cmd
}

func (s ExecSender) QueueMessage(ctx context.Context, threadID, text string) error {
	if len(text) > maxMessageBytes {
		return fmt.Errorf("%w: %d bytes > %d: %w", ErrMessageTooLarge, len(text), maxMessageBytes, ErrQueuePermanentlyRejected)
	}
	if strings.ContainsRune(text, 0) {
		return fmt.Errorf("%w: %w", ErrMessageContainsNUL, ErrQueuePermanentlyRejected)
	}
	makeCommand := s.command
	if makeCommand == nil {
		makeCommand = exec.CommandContext
	}
	cmd := makeCommand(ctx, "codex", "queue", "--thread", threadID, "--message", text)
	return runAndClassify(ctx, cmd)
}

// runAndClassify runs cmd and classifies a failure. A context outcome
// observed after the process actually started is ambiguous — cmd.Process is
// set once exec successfully forks/execs it. cmd.Process staying nil means
// the command never ran at all — definitely not attempted, safely retryable
// without any risk of duplicate delivery, not merely 'uncertain' — whether
// that's because the context was already canceled/expired before exec forked
// it, or because exec itself failed to start the process at all (binary not
// found, not executable, argv too large): neither case ever reached the
// host. Any other failure (a real exit error from a process that did start)
// is a plain terminal failure. Split out from QueueMessage so it can be
// exercised directly against a real short-lived process, without depending
// on the codex binary.
func runAndClassify(ctx context.Context, cmd *exec.Cmd) error {
	err := cmd.Run()
	if err == nil {
		return nil
	}
	if cmd.Process == nil {
		return fmt.Errorf("codex queue: %v: %w", err, ErrQueueNotAttempted)
	}
	if ctx.Err() != nil {
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

// wrapMessage is bridgetext.Wrap, indirected so a test can force its one
// failure mode (a crypto/rand read error) without depending on actually
// exhausting system randomness.
var wrapMessage = bridgetext.Wrap

func (t *Transport) Deliver(ctx context.Context, e store.Envelope) error {
	// A recipient/sender mismatch is rejected before QueueMessage is ever
	// called — the host was never at risk of duplicate delivery — and it is
	// permanent for this exact envelope: it is bound to the wrong peers
	// regardless of how many times it's retried, unlike a transient
	// never-attempted cause. Wrapping it as dispatch.ErrPermanentlyRejected
	// (not a plain error) tells Dispatch to refund the claimed budget slot
	// rather than burning it on a routing mistake that was never going to
	// reach the host.
	if e.ToPeer != t.toPeer {
		return fmt.Errorf("%w: %w: envelope to %q, transport bound to %q", dispatch.ErrPermanentlyRejected, ErrRecipientMismatch, e.ToPeer, t.toPeer)
	}
	if e.FromPeer != t.fromLabel {
		return fmt.Errorf("%w: %w: envelope from %q, transport bound to %q", dispatch.ErrPermanentlyRejected, ErrRecipientMismatch, e.FromPeer, t.fromLabel)
	}
	wrapped, err := wrapMessage(e.ID, t.fromLabel, e.Text)
	if err != nil {
		// Wrap only fails on a crypto/rand read error generating the
		// boundary — QueueMessage is never reached, and unlike the
		// recipient/sender mismatch above, the failure isn't a property of
		// this envelope that a retry would keep reproducing. Report it as
		// dispatch.ErrNoAttempt so Dispatch refunds and requeues it rather
		// than terminally failing on a transient host condition.
		return fmt.Errorf("%w: %v", dispatch.ErrNoAttempt, err)
	}
	if err := t.sender.QueueMessage(ctx, t.threadID, wrapped); err != nil {
		if errors.Is(err, ErrQueueAmbiguous) {
			return fmt.Errorf("%w: %v", dispatch.ErrAmbiguous, err)
		}
		if errors.Is(err, ErrQueuePermanentlyRejected) {
			return fmt.Errorf("%w: %v", dispatch.ErrPermanentlyRejected, err)
		}
		if errors.Is(err, ErrQueueNotAttempted) {
			return fmt.Errorf("%w: %v", dispatch.ErrNoAttempt, err)
		}
		return err
	}
	return nil
}
