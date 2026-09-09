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

	mu    sync.Mutex
	nonce string
	ready bool
	timer *time.Timer
	// retries counts handshake (re)sends, for tests and observability. Only
	// the handshake itself is ever retried — never an application message
	// with uncertain delivery.
	retries int
}

// NewHandshake builds a Handshake that calls sendProbe with a fresh nonce
// each time it (re)starts, and retries the probe — never an application
// message — if no matching Ack arrives within timeout.
func NewHandshake(sendProbe func(nonce string) error, timeout time.Duration) *Handshake {
	return &Handshake{sendProbe: sendProbe, timeout: timeout}
}

// Start begins the handshake: generates a nonce, sends the probe, and arms
// the timeout. Call once per connection.
func (h *Handshake) Start() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.startLocked()
}

// Reset marks the handshake not-ready and starts a fresh one with a new
// nonce. Call on every reconnect — a prior connection's pending or completed
// ack must not carry over to the new one.
func (h *Handshake) Reset() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.startLocked()
}

func (h *Handshake) startLocked() error {
	h.ready = false
	nonce, err := newNonce()
	if err != nil {
		return fmt.Errorf("generate handshake nonce: %w", err)
	}
	h.nonce = nonce
	h.retries++
	if h.timer != nil {
		h.timer.Stop()
	}
	h.timer = time.AfterFunc(h.timeout, h.onTimeout)
	return h.sendProbe(nonce)
}

func (h *Handshake) onTimeout() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ready {
		return
	}
	// Best-effort: a send failure here just means another timeout fires and
	// tries again. There is no application message to protect from a retry
	// at this layer — that guarantee lives in the poller only dispatching
	// once Ready() is true.
	_ = h.startLocked()
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

// Retries reports how many times the probe has been (re)sent, for tests.
func (h *Handshake) Retries() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.retries
}

// Stop disarms the timeout timer without changing readiness. Call when
// tearing down a connection so a stale timer can't fire a probe into a dead
// transport.
func (h *Handshake) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.timer != nil {
		h.timer.Stop()
	}
}

func newNonce() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
