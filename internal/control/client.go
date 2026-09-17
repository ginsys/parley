package control

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ginsys/parley/internal/connection"
)

// Client is a single-connection, single-in-flight-request session against
// one parley-control/1 server. It performs no retry and no reconnect --
// callers needing those build them on top -- matching the server's own
// strictly sequential per-socket design (internal/control/listener_linux.go).
// Call enforces the single-in-flight precondition itself (mandate CP-07):
// this is a caller-misuse guard against silent ID/reader races, not a
// concurrency guarantee -- a rejected concurrent call is not queued,
// retried or serialized on the caller's behalf.
type Client struct {
	conn   *net.UnixConn
	br     *bufio.Reader
	nextID uint64

	// broken is set once this connection's request/response correlation
	// can no longer be trusted (a write/read/decode failure, or a
	// response whose ID did not match the outstanding request). Once
	// set, every subsequent Call refuses immediately rather than risk
	// reading a stale, unread response left on the wire from an earlier
	// call as if it belonged to a new one.
	broken bool

	// inFlight guards against two concurrent Call invocations racing
	// nextID, br and the connection deadline against each other (mandate
	// CP-07). A single atomic flag is sufficient because Call always
	// clears it before returning, on every path, via defer.
	inFlight atomic.Bool
}

// errClientBroken is returned by Call once a prior call left this
// connection's request/response correlation in an unknown state.
var errClientBroken = errors.New("control: connection is no longer usable after a prior unresolved call")

// errClientConcurrentCall is returned by Call when another Call on the
// same Client is already in flight. Client is documented single-in-flight
// (mandate CP-07); this turns a caller's concurrency bug into an
// immediate, clear error instead of a silent nextID/reader race.
var errClientConcurrentCall = errors.New("control: concurrent Call on a single-in-flight client")

// Dial connects to cfg.Endpoint, authenticating the server's kernel UID via
// connection.DialTrustedServer, then performs the first-call server.hello
// handshake the profile requires before any other method
// (docs/specifications/control.md). On any error, including a rejected,
// mismatched or malformed hello, the connection is closed and a nil Client
// is returned. A matching correlation ID and a well-formed envelope are
// not by themselves a completed negotiation -- the decoded HelloResult
// itself is validated too (mandate R5), since a zero-value or
// protocol-mismatched result could otherwise slip through Call's own
// success path.
func Dial(ctx context.Context, cfg ClientConfig) (*Client, HelloResult, error) {
	conn, err := connection.DialTrustedServer(ctx, cfg.Endpoint, cfg.ServerUID)
	if err != nil {
		return nil, HelloResult{}, err
	}
	c := &Client{conn: conn, br: bufio.NewReader(conn)}
	var hello HelloResult
	if err := c.Call(ctx, "server.hello", map[string]any{"protocol": ProtocolVersion}, &hello); err != nil {
		conn.Close()
		return nil, HelloResult{}, err
	}
	if err := hello.validate(); err != nil {
		conn.Close()
		return nil, HelloResult{}, err
	}
	return c, hello, nil
}

// Close closes the underlying connection. It does not send anything.
func (c *Client) Close() error { return c.conn.Close() }

// Call issues one request and decodes its result into out (nil to discard
// the result entirely). It never sends a second request before this one's
// response arrives, so correlation IDs are simply incremented, never reused
// concurrently.
//
// ctx governs both the write and the response read, via both its deadline
// (if any) and its cancellation, observed even with no deadline set (mandate
// R3): a context.AfterFunc callback forces the pending I/O to unblock by
// setting an immediate deadline, then this call waits for that callback to
// actually finish (not merely for ctx.Done(), which only proves the
// callback was scheduled -- see the stdlib's own
// context.ExampleAfterFunc_connection) before returning, so a later Call on
// this connection can never race a still-running deadline reset left over
// from an earlier one.
//
// An already-cancelled context is refused before anything is sent: nothing
// was dispatched, so this is a plain ctx.Err(), never *TimeoutError or a
// broken connection -- this is the one case positively established as
// "not dispatched" (mandate T2); every other failure path below is not.
// Once Write is entered, a failure there does not prove the request was
// never seen either: a partial write can still leave bytes in the
// connection's send buffer, so it is classified exactly like a post-write
// read failure, not as a proven non-dispatch. And once the request is
// fully written, an interrupted, truncated or oversized-without-terminator
// read leaves the outcome of a mutating call genuinely unknown, whatever
// its concrete transport cause -- a deadline, a cancellation, or the
// connection closing or resetting with no valid matching response. Call
// reports all of these uniformly as *TimeoutError (kept as the exported
// type name for compatibility; despite the name it does not assert an
// elapsed deadline specifically -- see TimeoutError's own doc comment) and
// marks the connection broken so a later call can never mistake a stale or
// still-pending response for its own (see
// docs/specifications/control.md's outcome-uncertainty guidance). A
// well-formed, ID-matched response -- success or RemoteError -- is by
// contrast a definite, resolved outcome and is never wrapped this way.
func (c *Client) Call(ctx context.Context, method string, params map[string]any, out any) error {
	if !c.inFlight.CompareAndSwap(false, true) {
		return errClientConcurrentCall
	}
	defer c.inFlight.Store(false)
	if c.broken {
		return errClientBroken
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.nextID++
	id := strconv.FormatUint(c.nextID, 10)
	if params == nil {
		params = map[string]any{}
	}
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	if err != nil {
		return err
	}
	deadline := time.Time{}
	if dl, ok := ctx.Deadline(); ok {
		deadline = dl
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return err
	}

	// stopc is closed by the AfterFunc callback itself once it has finished
	// forcing the deadline -- the only reliable "the callback is done"
	// signal (stop() returning false means only "launched", per
	// context.AfterFunc's documented contract).
	stopc := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		c.conn.SetDeadline(time.Now())
		close(stopc)
	})
	defer func() {
		if !stop() {
			<-stopc
		}
		c.conn.SetDeadline(time.Time{})
	}()

	request = append(request, '\n')
	if _, err := c.conn.Write(request); err != nil {
		// A failed or partial write leaves both the server's read position
		// and whether it ever saw this request unknown to us -- this is
		// not a proven non-dispatch, so it is classified exactly like a
		// post-write read failure below, not returned bare (mandate T2).
		// A later call on this connection could otherwise read whatever
		// response the server eventually sends for it.
		c.broken = true
		if ctxErr := ctx.Err(); ctxErr != nil {
			return wrapCancel(ctxErr, err)
		}
		return wrapTimeout(err)
	}
	line, err := readBoundedFrame(c.br)
	if err != nil {
		// A deadline, a cancellation, an EOF/reset from the peer closing,
		// or an oversized response with no terminator: none of these is a
		// valid matching response, and none proves the request was not
		// received and acted on. The response, if any, is unread and
		// still on the wire; a later call must never read it mistaking it
		// for its own (mandate T2).
		c.broken = true
		if ctxErr := ctx.Err(); ctxErr != nil {
			return wrapCancel(ctxErr, err)
		}
		return wrapTimeout(err)
	}
	// The frame is validated against the same strict grammar the server
	// itself enforces on incoming requests (envelope.go's parseJSON) before
	// any decode is attempted -- a peer's malformed response (duplicate
	// keys, invalid UTF-8, unpaired surrogate escapes, excess nesting,
	// trailing bytes) gets the identical rejection an equivalently
	// malformed request would (mandate CP-06). A malformed response after
	// an attempted write is not a proven non-dispatch -- classified as an
	// unresolved outcome via wrapTimeout, not returned bare (mandate CP-02),
	// matching the write/read-failure classification above.
	if _, err := parseJSON(line); err != nil {
		c.broken = true
		return wrapTimeout(fmt.Errorf("control: malformed response: %w", err))
	}
	var resp incomingResponse
	if err := json.Unmarshal(line, &resp); err != nil {
		// parseJSON already accepted this exact byte sequence, so a
		// subsequent json.Unmarshal failure here would indicate an internal
		// inconsistency between the two decoders, not a hostile/malformed
		// peer -- still classified as an unresolved outcome, never a bare
		// error, for the same reason.
		c.broken = true
		return wrapTimeout(fmt.Errorf("control: malformed response: %w", err))
	}
	// Exclusivity is judged on key presence for both members, not on
	// either member's decoded value: presence and an allowed nullable
	// value are separate schema questions (mandate R5). A conformant peer
	// never emits `"error":null` or `"error":{}` at all, so error's mere
	// presence is already self-contradictory alongside a result -- but a
	// legitimately present `"result":null` (a success value) must not be
	// confused with result being absent, which is why both checks are
	// presence-only here and null-tolerance is applied later, only to a
	// present error's own value, and only to reject it.
	resultPresent := len(resp.Result) > 0
	errorPresent := len(resp.Error) > 0
	if resp.JSONRPC != "2.0" {
		c.broken = true
		return fmt.Errorf("control: response has unsupported jsonrpc version %q", resp.JSONRPC)
	}
	if resultPresent == errorPresent {
		// Exactly one of result/error must be present -- neither (a bare
		// `{"id":"1"}`) is not a completed call, and both is a
		// self-contradictory envelope (mandate R5). Neither is safe to
		// treat as success.
		c.broken = true
		return errors.New("control: response must carry exactly one of result or error")
	}
	if resp.ID == nil || *resp.ID != id {
		// The server's own serialization (internal/control/listener_linux.go)
		// never has more than one request in flight per socket, so a
		// mismatched ID means this connection's framing is desynchronized,
		// not merely that a request was skipped -- never proceed as if the
		// mismatched response belonged to this call.
		c.broken = true
		return fmt.Errorf("control: response id %s does not match request id %q", formatResponseID(resp.ID), id)
	}
	if errorPresent {
		// A present error must be an actual, valid error object -- a bare
		// `"error":null` or `"error":{}` reaches here only because it is
		// present (not absent), and must still be rejected rather than
		// decoded into a zero-valued incomingError and returned as if it
		// were a genuine RemoteError with code 0 and an empty message
		// (mandate R5).
		if string(resp.Error) == "null" {
			c.broken = true
			return errors.New("control: response carries an explicit null error, not a valid error object")
		}
		var wireErr incomingError
		if err := json.Unmarshal(resp.Error, &wireErr); err != nil {
			c.broken = true
			return fmt.Errorf("control: malformed error object: %w", err)
		}
		if err := wireErr.validate(); err != nil {
			c.broken = true
			return err
		}
		remote := &RemoteError{RPC: wireErr.Code, Message: wireErr.Message}
		if wireErr.Data != nil {
			remote.Domain = wireErr.Data.Code
		}
		return remote
	}
	if out == nil || len(resp.Result) == 0 {
		return nil
	}
	return json.Unmarshal(resp.Result, out)
}

// formatResponseID renders resp.ID for a diagnostic message without
// dereferencing a nil pointer and without printing its address: an earlier
// version passed the *string itself to %v, printing a pointer address
// (e.g. "0xc0001a2030") instead of the id it carried, or "<nil>" for a
// missing id -- neither is useful in an error message a human has to act
// on (mandate H3).
func formatResponseID(id *string) string {
	if id == nil {
		return "<missing>"
	}
	return strconv.Quote(*id)
}

// readBoundedFrame reads one LF-terminated response line, refusing to grow
// past MaxFrameBytes -- the same bound the server enforces on its own
// writes (see writeResponse in listener_linux.go). Without this, a
// malformed or hostile peer sending an unterminated stream would make
// bufio.Reader.ReadBytes buffer without limit.
func readBoundedFrame(br *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		b, err := br.ReadByte()
		if err != nil {
			return nil, err
		}
		buf = append(buf, b)
		if b == '\n' {
			return buf, nil
		}
		if len(buf) >= MaxFrameBytes {
			return nil, fmt.Errorf("control: response exceeds the frame bound")
		}
	}
}

// incomingResponse mirrors wireResponse (response.go) for decoding rather
// than encoding: json.RawMessage on Result defers the shape to the caller's
// out value, and Error uses its own struct since wireError's Code/Message
// are unexported-shape-compatible but Data must round-trip DomainCode.
//
// Both Result and Error are json.RawMessage, not typed/pointer fields:
// unmarshaling JSON `null` into a pointer field sets it to nil,
// indistinguishable from the key being absent entirely. json.RawMessage
// instead captures the raw bytes ("null", length 4) whenever the key is
// present at all, so presence can be judged uniformly for both members
// (len(raw) > 0) before either one's value is inspected (mandate R5) --
// without this, a self-contradictory envelope carrying both a real result
// and an explicit `"error":null` could slip past the result/error
// exclusivity check as if error had never been present.
type incomingResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *string         `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   json.RawMessage `json:"error"`
}

type incomingError struct {
	Code    RPCCode    `json:"code"`
	Message string     `json:"message"`
	Data    *errorData `json:"data"`
}

// validate enforces the error object's own required shape (mandate R5):
// unmarshaling `{}` or a partially-typed object into incomingError succeeds
// with Go zero values (Code 0, empty Message, nil Data) with no decode
// error at all, so this is the only thing standing between a malformed
// error object and a RemoteError carrying code 0 and an empty message.
// Reuses the server's own envelope/domain code vocabulary
// (internal/control/errors.go) rather than inventing a second one: a
// genuine peer only ever emits one of the five envelope RPCCodes or
// ServerError, so any other value -- including the zero value a missing or
// null "code" decodes to -- is rejected outright.
func (e incomingError) validate() error {
	switch e.Code {
	case ParseError, InvalidRequest, MethodNotFound, InvalidParams, InternalError:
		if e.Data != nil {
			return fmt.Errorf("control: envelope error %d must not carry error.data", e.Code)
		}
	case ServerError:
		// domainErrorResponse (response.go) always sets Data on a
		// ServerError; a missing or blank data.code is malformed, not a
		// legitimate domain error this client has simply never seen
		// before.
		if e.Data == nil || strings.TrimSpace(string(e.Data.Code)) == "" {
			return errors.New("control: domain error is missing its required error.data.code")
		}
		// A peer is never trusted to introduce an arbitrary domain code
		// merely by sending one that happens to be nonblank -- it must be a
		// member of the accepted error.data.code vocabulary this client and
		// the server both enforce (mandate CP-03).
		if !e.Data.Code.valid() {
			return fmt.Errorf("control: domain error carries an unrecognized error.data.code %q", e.Data.Code)
		}
	default:
		return fmt.Errorf("control: response carries an unrecognized error code %d", e.Code)
	}
	if strings.TrimSpace(e.Message) == "" {
		return errors.New("control: error object is missing a required message")
	}
	return nil
}

// RemoteError is a well-formed error response from the server: either an
// envelope-level RPCCode (Domain empty) or a domain error carrying a fixed
// DomainCode. It is never returned for a transport failure or a timeout --
// see TimeoutError for that.
type RemoteError struct {
	RPC     RPCCode
	Message string
	Domain  DomainCode
}

func (e *RemoteError) Error() string {
	if e.Domain != "" {
		return fmt.Sprintf("control: %s: %s", e.Domain, e.Message)
	}
	return fmt.Sprintf("control: rpc %d: %s", e.RPC, e.Message)
}

// TimeoutError represents an incomplete exchange: a write or read failure
// observed after the request had already been handed to Write, once no
// valid matching response can establish what actually happened. Despite
// the name (kept for API compatibility -- every existing caller matches on
// this concrete type), it is not asserted to mean an elapsed deadline
// specifically: ctx's deadline elapsing is one cause, but so is ctx's
// cancellation, the peer closing the connection (EOF), a reset, or an
// oversized response with no terminator -- wrapTimeout and wrapCancel
// below wrap all of these identically, deliberately, because the caller's
// only correct response to any of them is the same: the outcome is
// unknown, never labelled a proven failure (mandate T2, correcting an
// earlier version of this type whose Error() text claimed "timed out"
// unconditionally, which was simply false for an EOF/reset cause). It
// carries no information about whether the server received, executed or
// committed the request -- an unresolved mutation must be retried with the
// same operation ID, never assumed failed. See
// docs/specifications/control.md's command atomicity section.
type TimeoutError struct{ err error }

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("control: incomplete exchange, outcome unknown: %v", e.err)
}
func (e *TimeoutError) Unwrap() error { return e.err }

// wrapCancel classifies an I/O error observed after ctx became Done. A
// deadline-exceeded ctx and a plain Canceled ctx both wrap identically as
// *TimeoutError: either way the request may already have reached and
// mutated the server, and the caller must treat the outcome as unknown
// (mandate R3), never as a proof of failure.
func wrapCancel(ctxErr, ioErr error) error {
	return &TimeoutError{err: fmt.Errorf("%w (ctx: %w)", ioErr, ctxErr)}
}

// wrapTimeout classifies a write/read failure observed with ctx not (yet)
// Done at the point of the check in Call, so a deadline exceeding is only
// one of the possible causes here. A server-side close (io.EOF), a reset,
// or any other transport error at this point leaves the request's outcome
// exactly as unknown as an actual deadline would: the request may already
// have reached and executed on the server. Every caller of this function
// is past the point where the request was (at least partially) handed to
// Write, so unconditional wrapping is correct here; a positively-observed
// pre-dispatch refusal (Call's own ctx.Err() check before Write) never
// reaches this function (mandate T2 -- an earlier version here only
// wrapped errors satisfying net.Error.Timeout(), so a plain EOF/reset
// escaped as a bare, unwrapped error that a caller matching on
// *TimeoutError would then treat as "definitely not performed").
func wrapTimeout(err error) error {
	return &TimeoutError{err: err}
}
