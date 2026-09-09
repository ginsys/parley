// Package claude will hold Parley's Claude-side Channels adapter. This file
// implements the readiness handshake Codex specified as a correction to the
// fixed-delay workaround found during the Go MCP SDK compatibility check:
// neither MCP initialization nor session idleness proves the channel
// listener is ready to receive, so real readiness is established by a
// nonce'd probe, acknowledged back through Claude's reply tool, before any
// application message is allowed to leave the queued state.
package claude

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// Handshake tracks readiness for exactly one connection. A new connection —
// including a reconnect after a crash or session restart — must never
// inherit a prior connection's ack; callers construct (or Reset) one
// Handshake per connection, never reuse readiness across connections.
type Handshake struct {
	sendProbe func(nonce string) error
	timeout   time.Duration

	mu      sync.Mutex
	nonce   string
	ready   bool
	stopped bool
	timer   *time.Timer
	// generation identifies one connection lifecycle, incremented only by
	// Start/Reset (a genuinely new connection), never by a timeout retry
	// within the same connection. A timer callback captures its generation
	// at schedule time and no-ops if it has since changed — this is what
	// lets a stale callback from a superseded connection recognize itself
	// as stale even after a Stop()-then-Reset() cycle has cleared stopped
	// again, which a stopped flag alone cannot distinguish.
	generation int
	// sending guards against overlapping sendProbe calls for the same
	// connection: if a send is still blocked when its own timeout fires,
	// the retry must not launch a second concurrent send stacked on top of
	// the first — it skips this round and the next timer tick checks again.
	sending bool
	// retries counts handshake probes actually sent, for tests and
	// observability. Only the handshake itself is ever retried — never an
	// application message with uncertain delivery.
	retries int
}

// NewHandshake builds a Handshake that calls sendProbe with a fresh nonce
// each time it (re)starts, and retries the probe — never an application
// message — if no matching Ack arrives within timeout. A timeout retry
// resends the same nonce rather than minting a new one, so a genuine but
// slow acknowledgement for the original probe still lands; only Start/Reset
// (a real new connection) ever rotates the nonce.
func NewHandshake(sendProbe func(nonce string) error, timeout time.Duration) *Handshake {
	return &Handshake{sendProbe: sendProbe, timeout: timeout}
}

// Start begins the handshake: generates a nonce, sends the probe, and arms
// the timeout. Call once per connection.
func (h *Handshake) Start() error {
	return h.begin()
}

// Reset marks the handshake not-ready and starts a fresh one with a new
// nonce. Call on every reconnect — a prior connection's pending or completed
// ack must not carry over to the new one.
func (h *Handshake) Reset() error {
	return h.begin()
}

// begin starts a new connection lifecycle: resets state, mints a fresh
// nonce, bumps the generation, arms the timeout, and sends the first probe
// outside the lock.
func (h *Handshake) begin() error {
	h.mu.Lock()
	h.ready = false
	h.stopped = false
	nonce, err := generateNonce()
	if err != nil {
		// Leave no stale nonce behind: a superseded connection's nonce must
		// not remain ackable, and stopped guards against a leftover timer
		// from a prior connection retrying into this failed state.
		h.nonce = ""
		h.stopped = true
		h.mu.Unlock()
		return fmt.Errorf("generate handshake nonce: %w", err)
	}
	h.nonce = nonce
	h.generation++
	gen := h.generation
	h.sending = false
	if h.timer != nil {
		h.timer.Stop()
	}
	h.timer = time.AfterFunc(h.timeout, func() { h.onTimeout(gen) })
	h.mu.Unlock()

	return h.attemptSend(gen, nonce)
}

// onTimeout fires when a probe for generation gen hasn't been acked within
// timeout. gen is captured at schedule time so a callback left over from a
// superseded connection recognizes itself as stale, even if Stop() and a
// subsequent Reset() ran in between and cleared stopped again.
func (h *Handshake) onTimeout(gen int) {
	h.mu.Lock()
	// stopped guards against a timer that had already fired (and is now
	// merely blocked on h.mu) by the time Stop() ran and released the lock —
	// time.Timer.Stop() cannot cancel a callback that already started
	// running, only a future firing. gen guards the case Stop()'s stopped
	// guard cannot: a stale callback that doesn't acquire the lock until
	// after a subsequent Reset() has already cleared stopped.
	if h.ready || h.stopped || gen != h.generation {
		h.mu.Unlock()
		return
	}
	nonce := h.nonce
	h.timer = time.AfterFunc(h.timeout, func() { h.onTimeout(gen) })
	h.mu.Unlock()

	// Best-effort: a send failure here just means another timeout fires and
	// tries again. There is no application message to protect from a retry
	// at this layer — that guarantee lives in the poller only dispatching
	// once Ready() is true.
	_ = h.attemptSend(gen, nonce)
}

// attemptSend calls sendProbe outside h.mu, guarded so at most one send is
// ever in flight per connection: if a prior send for this generation hasn't
// returned yet, this attempt is skipped rather than stacking a second
// concurrent write on top of it — the next timeout tick will try again once
// the in-flight one completes. It also re-checks liveness (ready/stopped/
// generation) immediately before sending, since time may have passed since
// the caller decided to attempt this.
func (h *Handshake) attemptSend(gen int, nonce string) error {
	h.mu.Lock()
	if h.sending || h.ready || h.stopped || gen != h.generation {
		h.mu.Unlock()
		return nil
	}
	h.sending = true
	h.retries++
	h.mu.Unlock()

	err := h.sendProbe(nonce)

	h.mu.Lock()
	h.sending = false
	h.mu.Unlock()
	return err
}

// Ack processes a candidate acknowledgement carrying nonce. Returns true
// only when it matches the currently pending probe; a stale or foreign
// nonce (e.g. from a superseded connection) is rejected and readiness is
// unaffected.
func (h *Handshake) Ack(nonce string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.nonce == "" || nonce != h.nonce {
		return false
	}
	h.ready = true
	if h.timer != nil {
		h.timer.Stop()
	}
	return true
}

// Ready reports whether the current connection's handshake has been
// acknowledged. The poller must not dispatch any application message while
// this is false.
func (h *Handshake) Ready() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ready
}

// Generation reports the current connection lifecycle's generation number,
// incremented by every Start/Reset. A caller that must ensure an entire
// multi-step operation (e.g. a batch of dispatches) stays authorized by one
// single acknowledged handshake — not a newer connection that happened to
// also reach Ready() by the time the operation finishes — captures this
// once up front and confirms it hasn't changed before each step, in
// addition to checking Ready().
func (h *Handshake) Generation() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.generation
}

// Retries reports how many times the probe has actually been sent, for
// tests. A retry skipped because a prior send was still in flight doesn't
// count — this counts sends, not timeout firings.
func (h *Handshake) Retries() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.retries
}

// Stop disarms the timeout timer and clears readiness — a stopped
// connection is by definition no longer proven ready, so a caller that only
// checks Ready() (e.g. Poller) must see false immediately, not keep
// dispatching into a dead transport until a future Reset(). It also
// prevents a timeout callback that was already running (blocked on the same
// lock) from re-arming a fresh probe once Stop returns — time.Timer.Stop
// cannot cancel a callback that has already started. Call when tearing down
// a connection. A subsequent Start or Reset clears the stopped flag, since
// that begins a new lifecycle.
func (h *Handshake) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopped = true
	h.ready = false
	h.nonce = ""
	if h.timer != nil {
		h.timer.Stop()
	}
}

// generateNonce is a variable, not a direct call to newNonce, purely so
// tests can simulate a crypto/rand failure without depending on an actual
// entropy-source outage.
var generateNonce = newNonce

func newNonce() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
