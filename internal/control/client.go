package control

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/ginsys/parley/internal/connection"
)

// Client is a single-connection, single-in-flight-request session against
// one parley-control/1 server. It performs no retry and no reconnect --
// callers needing those build them on top -- matching the server's own
// strictly sequential per-socket design (internal/control/listener_linux.go).
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
}

// errClientBroken is returned by Call once a prior call left this
// connection's request/response correlation in an unknown state.
var errClientBroken = errors.New("control: connection is no longer usable after a prior unresolved call")

// Dial connects to cfg.Endpoint, authenticating the server's kernel UID via
// connection.DialTrustedServer, then performs the first-call server.hello
// handshake the profile requires before any other method
// (docs/specifications/control.md). On any error, including a rejected or
// mismatched hello, the connection is closed and a nil Client is returned.
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
	return c, hello, nil
}

// Close closes the underlying connection. It does not send anything.
func (c *Client) Close() error { return c.conn.Close() }

// Call issues one request and decodes its result into out (nil to discard
// the result entirely). It never sends a second request before this one's
// response arrives, so correlation IDs are simply incremented, never reused
// concurrently. ctx's deadline, if any, bounds both the write and the
// response read; a deadline exceeded here is reported as *TimeoutError, not
// folded into a domain or envelope error -- the caller must not treat a
// client-observed timeout as proof the request failed on the server (see
// docs/specifications/control.md's outcome-uncertainty guidance).
func (c *Client) Call(ctx context.Context, method string, params map[string]any, out any) error {
	if c.broken {
		return errClientBroken
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
	defer c.conn.SetDeadline(time.Time{})

	request = append(request, '\n')
	if _, err := c.conn.Write(request); err != nil {
		// A partially written request leaves the server's read position
		// unknown to us; a later call on this connection could read
		// whatever response the server eventually sends for it.
		c.broken = true
		return wrapTimeout(err)
	}
	line, err := readBoundedFrame(c.br)
	if err != nil {
		// Timeout or any other read failure: the response, if any, is
		// unread and still on the wire. A later call must never read it
		// mistaking it for its own.
		c.broken = true
		return wrapTimeout(err)
	}
	var resp incomingResponse
	if err := json.Unmarshal(line, &resp); err != nil {
		c.broken = true
		return fmt.Errorf("control: malformed response: %w", err)
	}
	if resp.ID == nil || *resp.ID != id {
		// The server's own serialization (internal/control/listener_linux.go)
		// never has more than one request in flight per socket, so a
		// mismatched ID means this connection's framing is desynchronized,
		// not merely that a request was skipped -- never proceed as if the
		// mismatched response belonged to this call.
		c.broken = true
		return fmt.Errorf("control: response id %v does not match request id %q", resp.ID, id)
	}
	if resp.Error != nil {
		remote := &RemoteError{RPC: resp.Error.Code, Message: resp.Error.Message}
		if resp.Error.Data != nil {
			remote.Domain = resp.Error.Data.Code
		}
		return remote
	}
	if out == nil || len(resp.Result) == 0 {
		return nil
	}
	return json.Unmarshal(resp.Result, out)
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
type incomingResponse struct {
	ID     *string         `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *incomingError  `json:"error"`
}

type incomingError struct {
	Code    RPCCode    `json:"code"`
	Message string     `json:"message"`
	Data    *errorData `json:"data"`
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

// TimeoutError wraps a transport error observed while ctx's deadline (or a
// prior SetDeadline) elapsed. It carries no information about whether the
// server received, executed or committed the request -- an unresolved
// mutation must be retried with the same operation ID, never assumed
// failed. See docs/specifications/control.md's command atomicity section.
type TimeoutError struct{ err error }

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("control: timed out waiting for a response: %v", e.err)
}
func (e *TimeoutError) Unwrap() error { return e.err }

func wrapTimeout(err error) error {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return &TimeoutError{err: err}
	}
	return err
}
