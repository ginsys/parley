//go:build linux

package control

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/runtime"
)

func TestDialPerformsHelloAndReturnsResult(t *testing.T) {
	fx := newListenerFixture(t)
	client, hello, err := Dial(context.Background(), ClientConfig{Endpoint: fx.socketPath, ServerUID: fx.serverUID})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if hello.Protocol != ProtocolVersion || hello.AdministratorID != fx.adminID {
		t.Fatalf("%#v", hello)
	}
	if len(hello.Methods) != len(ImplementedMethods) {
		t.Fatalf("%#v", hello.Methods)
	}
}

func TestClientCallOperationGetNotFound(t *testing.T) {
	fx := newListenerFixture(t)
	client, _, err := Dial(context.Background(), ClientConfig{Endpoint: fx.socketPath, ServerUID: fx.serverUID})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var result OperationGetResult
	err = client.Call(context.Background(), "operation.get", map[string]any{"operation_id": "60000000-0000-4000-8000-000000000099"}, &result)
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.Domain != OperationNotFound {
		t.Fatalf("err=%#v", err)
	}
}

func TestClientCallSequentialRequestsBothSucceed(t *testing.T) {
	fx := newListenerFixture(t)
	client, _, err := Dial(context.Background(), ClientConfig{Endpoint: fx.socketPath, ServerUID: fx.serverUID})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for range 2 {
		err := client.Call(context.Background(), "operation.get", map[string]any{"operation_id": "60000000-0000-4000-8000-000000000099"}, nil)
		var remote *RemoteError
		if !errors.As(err, &remote) || remote.Domain != OperationNotFound {
			t.Fatalf("err=%#v", err)
		}
	}
}

// TestClientCallRejectsConcurrentCall is mandate CP-07's regression:
// Client is documented single-connection, single-in-flight-request --
// a second Call while one is still outstanding must fail fast with
// errClientConcurrentCall rather than race nextID/br/the connection
// deadline against the first call. This is a caller-misuse guard, not a
// queue: the second call is never retried or serialized on its behalf.
//
// release is registered for idempotent, guaranteed closing via t.Cleanup
// before the fake server goroutine or either Call is launched -- an early
// t.Fatal in any assertion below must not strand the fake server on
// <-release forever, since t.Cleanup still runs after Fatal even though
// the code after it does not.
//
// The second Call is itself given a bounded, test-owned deadline rather
// than context.Background() (mandate RC-05), and is run in its own
// goroutine observed through an outer bounded select exactly like the
// first Call above, rather than invoked inline and blocked on directly.
// With the guard intact, Call rejects it before dispatching anything and
// both bounds are irrelevant to a passing run's timing. But if the guard
// regresses, this Call would actually dispatch onto the shared connection
// and race the first Call's own still-pending read for the connection's
// one shared deadline (net.Conn.SetDeadline is per-connection, not
// per-call): each Call's own deferred cleanup unconditionally resets that
// shared deadline when it returns, so the *inner* ctx timeout on the
// second call is not itself a reliable bound once two Calls are actually
// concurrent on one connection -- that race is precisely what the CP-07
// guard exists to prevent, so disabling it to demonstrate the regression
// makes the inner bound racy too, observed directly: an inline
// `client.Call(secondCtx, ...)` here hung past its own 2s context deadline
// in roughly half of repeated fault-injection runs. The outer select
// below is therefore the actual bound the test relies on: it always
// proceeds within its own fixed wait regardless of whether the inner Call
// returns on its own, and the single combined cleanup (registered before
// either Call goroutine is launched) unconditionally closes the shared
// connection, which -- unlike SetDeadline -- cannot be raced back open,
// so it deterministically unblocks any still-pending read on either Call
// goroutine. The cleanup then observes both Call goroutines and the fake
// server's own goroutine actually finishing, each under its own bound, so
// a broken guard implementation fails this test promptly instead of
// hanging it and leaves no probe goroutine surviving past the test
// regardless of which assertion (if any) fails first. The *GoroutineDone
// channels (closed once, safe to observe repeatedly) are used for that
// observation instead of firstDone/secondDone themselves, each of which
// the test body below consumes at most once on the success path --
// reading a one-shot buffered channel a second time in cleanup would
// otherwise block for the full timeout and report a false failure on
// every ordinary passing run.
func TestClientCallRejectsConcurrentCall(t *testing.T) {
	dir := privateSocketDir(t)
	path := dir + "/admin.sock"
	uid := uint32(os.Getuid())
	raw, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	received := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseNow := func() { releaseOnce.Do(func() { close(release) }) }

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := raw.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		if _, err := br.ReadBytes('\n'); err != nil {
			return
		}
		close(received)
		<-release
		conn.Write([]byte(`{"jsonrpc":"2.0","id":"1","result":{}}` + "\n"))
	}()

	conn, err := connection.DialTrustedServer(context.Background(), path, uid)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{conn: conn, br: bufio.NewReader(conn)}

	firstDone := make(chan error, 1)
	firstGoroutineDone := make(chan struct{})
	go func() {
		defer close(firstGoroutineDone)
		firstDone <- client.Call(context.Background(), "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
	}()

	secondDone := make(chan error, 1)
	secondGoroutineDone := make(chan struct{})

	// Registered before any work above can fail, per the doc comment above.
	t.Cleanup(func() {
		releaseNow()
		conn.Close()
		for name, done := range map[string]chan struct{}{"first": firstGoroutineDone, "second": secondGoroutineDone} {
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Errorf("%s Call goroutine did not complete after release and close", name)
			}
		}
		select {
		case <-serverDone:
		case <-time.After(2 * time.Second):
			t.Error("fake server goroutine did not exit after release")
		}
	})

	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed the first request")
	}

	secondCtx, secondCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer secondCancel()
	go func() {
		defer close(secondGoroutineDone)
		secondDone <- client.Call(secondCtx, "server.hello", nil, nil)
	}()
	select {
	case err := <-secondDone:
		if !errors.Is(err, errClientConcurrentCall) {
			t.Fatalf("err=%v, want errClientConcurrentCall", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("second call did not return within its own bounded deadline -- the concurrent-call guard may be broken")
	}

	releaseNow()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first call failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first call never completed after release")
	}
	if client.broken {
		t.Fatal("the rejected concurrent call must not mark the connection broken")
	}
}

func TestDialFailsWhenAdministratorUIDUnconfigured(t *testing.T) {
	dir := privateSocketDir(t)
	socketPath := dir + "/admin.sock"
	uid := uint32(os.Getuid())
	cfg, err := NewConfig(socketPath, uid, map[string]uint32{"60000000-0000-4000-8000-000000000001": uid + 1})
	if err != nil {
		t.Fatal(err)
	}
	db := controlTestDB(t)
	if err := db.OpenReaders(context.Background()); err != nil {
		t.Fatal(err)
	}
	service := NewListenerService(cfg, 0600)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := service.Start(context.Background(), runtime.Resources{WorkerContext: ctx, Writer: db, Queries: db.Queries(), Mode: runtime.Normal}); err != nil {
		t.Fatal(err)
	}
	defer func() { service.StopAdmission(); cancel(); service.Wait() }()

	_, _, err = Dial(context.Background(), ClientConfig{Endpoint: socketPath, ServerUID: uid})
	if err == nil {
		t.Fatal("expected dial/hello to fail for an unconfigured administrator UID")
	}
	// The listener closes an unconfigured administrator's socket immediately
	// at accept time, before serveSession ever runs -- but that happens
	// concurrently with, not strictly before, the client's own hello write,
	// so the client cannot structurally distinguish this from a connection
	// reset mid-flight (mandate T2: a post-write failure is an unresolved
	// exchange unless it is positively established as pre-dispatch, and a
	// race with the peer's accept-time close is not provable from here).
	// It must never be misreported as a *RemoteError (no such response was
	// ever sent).
	var remote *RemoteError
	var timeout *TimeoutError
	if errors.As(err, &remote) {
		t.Fatalf("expected a transport failure, not a server response, got %#v", err)
	}
	if !errors.As(err, &timeout) {
		t.Fatalf("err=%v (%T), want *TimeoutError (outcome unknown)", err, err)
	}
}

// TestClientCallTimesOutWithoutFalseFailureSemantics constructs a Client
// directly (unexported fields, same package) against a raw listener that
// accepts but never responds, proving Call reports *TimeoutError -- never a
// *RemoteError -- when ctx's deadline elapses. The caller must not treat
// this as proof the request failed on the server.
// TestClientCallRejectsMismatchedResponseIDAndBreaksTheConnection proves a
// response whose id does not match the outstanding request's id is never
// treated as this call's own result -- the call fails and the connection
// is marked broken so no later call can read whatever response was
// actually meant for the mismatched id.
func TestClientCallRejectsMismatchedResponseIDAndBreaksTheConnection(t *testing.T) {
	dir := privateSocketDir(t)
	path := dir + "/admin.sock"
	uid := uint32(os.Getuid())
	raw, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	go func() {
		conn, err := raw.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		bufio.NewReader(conn).ReadBytes('\n')
		conn.Write([]byte(`{"jsonrpc":"2.0","id":"not-the-request-id","result":{}}` + "\n"))
	}()

	conn, err := connection.DialTrustedServer(context.Background(), path, uid)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{conn: conn, br: bufio.NewReader(conn)}
	defer client.Close()

	err = client.Call(context.Background(), "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
	if err == nil {
		t.Fatal("expected a mismatched response id to be rejected")
	}
	// The request was already fully written and a full response line was
	// actually read -- this connection's framing is desynchronized, not
	// proof our request failed, so this must classify as an unresolved
	// outcome, not a bare error (mandate T2; found by the hosted review of
	// this batch's own CP-02 fix, which had left this and its sibling
	// post-write rejection paths below still returning bare errors).
	var timeout *TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("err=%v, want *TimeoutError", err)
	}
	if !client.broken {
		t.Fatal("connection not marked broken after a mismatched response id")
	}
	if err := client.Call(context.Background(), "server.hello", nil, nil); !errors.Is(err, errClientBroken) {
		t.Fatalf("err=%v, want errClientBroken", err)
	}
}

// TestClientCallClassifiesPostWriteEnvelopeRejectionsAsUnresolvedOutcome
// covers the sibling post-write rejection paths TestClientCallRejects...
// MismatchedResponseId... above does not: every one of these is reached
// only after a full response line was actually read, so the request was
// already fully written and its outcome is unknown -- each must be
// *TimeoutError, never a bare error (mandate T2).
func TestClientCallClassifiesPostWriteEnvelopeRejectionsAsUnresolvedOutcome(t *testing.T) {
	uid := uint32(os.Getuid())
	cases := []struct {
		name     string
		response string
	}{
		{"unsupported jsonrpc version", `{"jsonrpc":"1.0","id":"1","result":{}}` + "\n"},
		{"neither result nor error present", `{"jsonrpc":"2.0","id":"1"}` + "\n"},
		{"both result and error present", `{"jsonrpc":"2.0","id":"1","result":{},"error":{"code":-32000,"message":"x","data":{"code":"not_found"}}}` + "\n"},
		{"explicit null error alongside no result", `{"jsonrpc":"2.0","id":"1","error":null}` + "\n"},
		{"malformed error object", `{"jsonrpc":"2.0","id":"1","error":{"code":"not-a-number","message":"x"}}` + "\n"},
		{"error object fails validate (unrecognized code)", `{"jsonrpc":"2.0","id":"1","error":{"code":-1,"message":"x"}}` + "\n"},
		// The remaining cases are mandate CP-06's response-contract
		// completion: encoding/json's struct decode is case-insensitive on
		// field names when no exact match exists and silently ignores
		// unknown object keys, so without validateResponseEnvelope/
		// validateErrorObjectShape these would otherwise decode as ordinary
		// responses (or, for CRLF, slip past parseJSON's own trailing-
		// content check, since a trailing CR is insignificant JSON
		// whitespace).
		{"unknown outer member", `{"jsonrpc":"2.0","id":"1","result":{},"extra":true}` + "\n"},
		{"wrong-case jsonrpc alias", `{"JSONRPC":"2.0","id":"1","result":{}}` + "\n"},
		{"case-distinct competing keys", `{"jsonrpc":"2.0","JSONRPC":"2.0","id":"1","result":{}}` + "\n"},
		{"CRLF line ending", `{"jsonrpc":"2.0","id":"1","result":{}}` + "\r\n"},
		{"unknown error member", `{"jsonrpc":"2.0","id":"1","error":{"code":-32000,"message":"x","data":{"code":"not_found"},"extra":true}}` + "\n"},
		{"unknown error.data member", `{"jsonrpc":"2.0","id":"1","error":{"code":-32000,"message":"x","data":{"code":"not_found","extra":true}}}` + "\n"},
		{"error.data explicitly null", `{"jsonrpc":"2.0","id":"1","error":{"code":-32000,"message":"x","data":null}}` + "\n"},
		// RC-03's exact reproduction: an *envelope*-level RPCCode (not
		// ServerError) with an explicit `"data":null`. incomingError.Data
		// is an ordinary *errorData pointer field, so encoding/json decodes
		// both an absent "data" key and an explicit null one to the same
		// nil value -- indistinguishable to incomingError.validate's own
		// `e.Data != nil` check for these codes. Before RC-03,
		// validateErrorObjectShape's own `data != nil` guard let this
		// explicit null through unexamined (it is legitimately absent for
		// these codes), so this exact envelope became a valid RemoteError
		// with an unbroken connection -- silently accepting a field with no
		// null-is-permitted allowance in the accepted wire contract.
		{"envelope error carries an explicit null data (RC-03)", `{"jsonrpc":"2.0","id":"1","error":{"code":-32601,"message":"unknown method","data":null}}` + "\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := rawResponder(t, c.response)
			conn, err := connection.DialTrustedServer(context.Background(), path, uid)
			if err != nil {
				t.Fatal(err)
			}
			client := &Client{conn: conn, br: bufio.NewReader(conn)}
			defer client.Close()
			err = client.Call(context.Background(), "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
			var timeout *TimeoutError
			if !errors.As(err, &timeout) {
				t.Fatalf("err=%v, want *TimeoutError", err)
			}
			if !client.broken {
				t.Fatal("connection not marked broken")
			}
		})
	}
}

// TestClientCallMarksConnectionBrokenAfterTimeout proves a timed-out call
// leaves the connection unusable for a subsequent call: the response, if
// the server eventually sends one, is still unread on the wire and must
// never be attributed to a later, unrelated call.
func TestClientCallMarksConnectionBrokenAfterTimeout(t *testing.T) {
	dir := privateSocketDir(t)
	path := dir + "/admin.sock"
	uid := uint32(os.Getuid())
	raw, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	release := make(chan struct{})
	defer close(release)
	go func() {
		conn, err := raw.Accept()
		if err != nil {
			return
		}
		<-release
		conn.Close()
	}()

	conn, err := connection.DialTrustedServer(context.Background(), path, uid)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{conn: conn, br: bufio.NewReader(conn)}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = client.Call(ctx, "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
	var timeout *TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("err=%v, want *TimeoutError", err)
	}
	if !client.broken {
		t.Fatal("connection not marked broken after a timeout")
	}
	if err := client.Call(context.Background(), "server.hello", nil, nil); !errors.Is(err, errClientBroken) {
		t.Fatalf("err=%v, want errClientBroken", err)
	}
}

// TestClientCallReportsServerSideCloseAsIncompleteExchangeNotBareEOF covers
// a post-write failure that is not a deadline timeout at all: the peer
// demonstrably receives the full request (synchronized on the server
// goroutine's own successful ReadString, not on timing) and then closes
// without ever writing a response. ctx carries no deadline and is never
// cancelled, so the resulting read failure here is a plain io.EOF -- not a
// net.Error with Timeout() true, and not something wrapCancel's ctx.Err()
// branch would touch either. The request may already have reached and
// been acted on by the server -- the outcome is exactly as unknown as an
// actual timeout's -- so this must still surface as *TimeoutError, never
// as a bare io.EOF a caller could mistake for "definitely not performed"
// (mandate T2). Before the fix, wrapTimeout only wrapped
// net.Error.Timeout() failures, so this exact scenario returned a bare,
// unwrapped io.EOF: this test fails against that prior behavior.
func TestClientCallReportsServerSideCloseAsIncompleteExchangeNotBareEOF(t *testing.T) {
	dir := privateSocketDir(t)
	path := dir + "/admin.sock"
	uid := uint32(os.Getuid())
	raw, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	received := make(chan struct{})
	go func() {
		conn, err := raw.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
			return
		}
		close(received) // the request was genuinely received before this closes
	}()

	conn, err := connection.DialTrustedServer(context.Background(), path, uid)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{conn: conn, br: bufio.NewReader(conn)}
	defer client.Close()

	err = client.Call(context.Background(), "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("server goroutine never observed the request")
	}
	var timeout *TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("err=%v (%T), want *TimeoutError", err, err)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err=%v, want the underlying io.EOF preserved via Unwrap", err)
	}
	if !client.broken {
		t.Fatal("connection not marked broken after a server-side close")
	}
}

// TestClientCallReportsTruncatedResponseAsIncompleteExchange covers the
// other unresolved-outcome shape mandate T2 names explicitly: the peer
// writes part of a response and then closes without ever sending the
// terminating LF readBoundedFrame requires. This is a distinct failure
// path from a close with zero bytes written (the previous test) -- some
// response bytes did arrive, just not a complete, parseable one -- and
// must be classified the same way: outcome unknown, never a proven
// failure and never confused with the well-formed responses
// TestClientCallAcceptsWellFormedResultsAndErrorsAndKeepsConnectionUsable
// covers.
func TestClientCallReportsTruncatedResponseAsIncompleteExchange(t *testing.T) {
	dir := privateSocketDir(t)
	path := dir + "/admin.sock"
	uid := uint32(os.Getuid())
	raw, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	go func() {
		conn, err := raw.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
			return
		}
		conn.Write([]byte(`{"jsonrpc":"2.0","id":"1","resu`)) // deliberately no closing LF
	}()

	conn, err := connection.DialTrustedServer(context.Background(), path, uid)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{conn: conn, br: bufio.NewReader(conn)}
	defer client.Close()

	err = client.Call(context.Background(), "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
	var timeout *TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("err=%v (%T), want *TimeoutError", err, err)
	}
	if !client.broken {
		t.Fatal("connection not marked broken after a truncated response")
	}
}

// infiniteXReader is a per-Read-unbounded in-memory io.Reader that never
// produces the LF readBoundedFrame is waiting for and never itself returns
// an error. It is always wrapped in io.LimitReader by its callers (mandate
// RC-04): supplying it unwrapped would make a regressed size guard grow
// memory without bound instead of failing, since this reader alone never
// terminates and never errors. Wrapped with a finite limit, the only ways
// readBoundedFrame can return are (a) tripping its own size bound, the
// intended outcome, or (b) a regressed guard exhausting the finite input
// and failing on EOF instead -- both finite, no hang, no unbounded growth.
type infiniteXReader struct{}

func (infiniteXReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

// TestReadBoundedFrameRefusesOversizedUnterminatedStream proves the
// client's response reader will not buffer without limit -- mirroring the
// server's own MaxFrameBytes write bound (writeResponse in
// listener_linux.go) -- when a peer never sends the terminating LF, and
// that the returned error is the actual size-rejection outcome, not a
// coincidental deadline or EOF standing in for it. A prior version of this
// test read from a real socket under a 10s read deadline and accepted any
// non-nil error, so a regression that let readBoundedFrame buffer far past
// MaxFrameBytes (or hang) would have still passed once that deadline fired.
// The source is also now finite (mandate RC-04): an unlimited never-failing
// reader meant a regressed guard would instead grow memory indefinitely
// rather than fail this test at all -- io.LimitReader here supplies more
// bytes than the allowed frame length but a bounded amount, so a missing
// bound exhausts the input and fails with a distinct, diagnosable error
// (io.ErrUnexpectedEOF from bufio.Reader.ReadString) instead of hanging or
// leaking.
func TestReadBoundedFrameRefusesOversizedUnterminatedStream(t *testing.T) {
	src := io.LimitReader(infiniteXReader{}, 2*MaxFrameBytes)
	_, err := readBoundedFrame(bufio.NewReader(src))
	if !errors.Is(err, errFrameTooLarge) {
		t.Fatalf("err=%v, want errFrameTooLarge -- a deadline or EOF must not be mistaken for the size rejection", err)
	}
}

// rawResponder starts a raw Unix listener that accepts exactly one
// connection, reads (and discards) one request line, then writes back
// response verbatim (already including its own trailing newline). It
// returns the listener's path.
func rawResponder(t *testing.T, response string) string {
	t.Helper()
	dir := privateSocketDir(t)
	path := dir + "/admin.sock"
	raw, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	go func() {
		conn, err := raw.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		bufio.NewReader(conn).ReadBytes('\n')
		conn.Write([]byte(response))
	}()
	return path
}

// TestClientCallAlreadyCancelledSendsNoRequest covers mandate R3: a
// context cancelled before Call ever dispatches must be refused as a
// plain ctx.Err(), with nothing written to the wire and the connection
// left usable (not marked broken) -- this is provably not an uncertain
// mutation, since nothing was ever sent.
func TestClientCallAlreadyCancelledSendsNoRequest(t *testing.T) {
	dir := privateSocketDir(t)
	path := dir + "/admin.sock"
	uid := uint32(os.Getuid())
	raw, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	received := make(chan bool, 1)
	go func() {
		conn, err := raw.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		buf := make([]byte, 1)
		_, err = conn.Read(buf)
		received <- err == nil
	}()

	conn, err := connection.DialTrustedServer(context.Background(), path, uid)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{conn: conn, br: bufio.NewReader(conn)}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = client.Call(ctx, "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want context.Canceled", err)
	}
	if client.broken {
		t.Fatal("an already-cancelled pre-dispatch call must not mark the connection broken")
	}
	if got := <-received; got {
		t.Fatal("a request was sent despite an already-cancelled context")
	}
}

// TestClientCallCancellationWithoutDeadlineUnblocksBlockedRead covers
// mandate R3's central claim: Call must observe ctx.Done() even when ctx
// carries no deadline. Without this, a blocked read here would hang until
// the peer eventually closes (or the test's own timeout), not until
// cancellation.
func TestClientCallCancellationWithoutDeadlineUnblocksBlockedRead(t *testing.T) {
	dir := privateSocketDir(t)
	path := dir + "/admin.sock"
	uid := uint32(os.Getuid())
	raw, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	release := make(chan struct{})
	defer close(release)
	go func() {
		conn, err := raw.Accept()
		if err != nil {
			return
		}
		bufio.NewReader(conn).ReadBytes('\n') // read the request, never respond
		<-release
		conn.Close()
	}()

	conn, err := connection.DialTrustedServer(context.Background(), path, uid)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{conn: conn, br: bufio.NewReader(conn)}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err = client.Call(ctx, "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
	elapsed := time.Since(start)
	var timeout *TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("err=%v, want *TimeoutError", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("cancellation did not unblock the read promptly: %v", elapsed)
	}
	if !client.broken {
		t.Fatal("connection not marked broken after a cancelled read")
	}
}

// TestClientCallCancellationWithoutDeadlineUnblocksBlockedWrite mirrors
// the read case for a blocked write: the peer accepts but never reads,
// eventually filling the kernel socket buffer so conn.Write blocks: only
// cancellation (no deadline set) unblocks it.
func TestClientCallCancellationWithoutDeadlineUnblocksBlockedWrite(t *testing.T) {
	dir := privateSocketDir(t)
	path := dir + "/admin.sock"
	uid := uint32(os.Getuid())
	raw, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	release := make(chan struct{})
	defer close(release)
	accepted := make(chan struct{})
	go func() {
		conn, err := raw.Accept()
		if err != nil {
			return
		}
		close(accepted)
		<-release // never read: keeps the kernel socket send buffer full
		conn.Close()
	}()

	conn, err := connection.DialTrustedServer(context.Background(), path, uid)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{conn: conn, br: bufio.NewReader(conn)}
	defer client.Close()
	<-accepted // the peer is connected before we start writing

	big := strings.Repeat("x", 32*1024*1024) // far larger than any default socket buffer
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err = client.Call(ctx, "server.hello", map[string]any{"protocol": ProtocolVersion, "padding": big}, nil)
	elapsed := time.Since(start)
	var timeout *TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("err=%v, want *TimeoutError", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("cancellation did not unblock the write promptly: %v", elapsed)
	}
	if !client.broken {
		t.Fatal("connection not marked broken after a cancelled write")
	}
}

// TestClientCallCancellationRaceWithResponseHasCoherentOutcomeAndNoLeak
// races cancellation against a real, fast response repeatedly: every
// outcome must be one of the well-defined possibilities, never a panic or
// a hang, and the context.AfterFunc observer for each call must not leak
// goroutines across many iterations (mandate R3's "release any
// cancellation observer promptly" requirement).
func TestClientCallCancellationRaceWithResponseHasCoherentOutcomeAndNoLeak(t *testing.T) {
	fx := newListenerFixture(t)
	client, _, err := Dial(context.Background(), ClientConfig{Endpoint: fx.socketPath, ServerUID: fx.serverUID})
	if err != nil {
		t.Fatal(err)
	}
	// A plain `defer client.Close()` here would bind the *original* client
	// value at defer-registration time: the loop below reassigns `client`
	// on every redial, so that defer would close only the first-dialed
	// connection and leak every replacement if a later Fatal exits before
	// the explicit `client.Close()` at the end of this function. Look up
	// the current, owned client at defer-execution time instead, with a
	// nil guard: a failed redial can leave `client` nil right before the
	// test terminates.
	defer func() {
		if client != nil {
			_ = client.Close()
		}
	}()

	before := goruntime.NumGoroutine()
	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		go cancel()
		err := client.Call(ctx, "operation.get", map[string]any{"operation_id": "60000000-0000-4000-8000-000000000099"}, nil)
		var remote *RemoteError
		var timeout *TimeoutError
		switch {
		case err == nil:
			// Success: the response won the race before cancellation took
			// effect.
		case errors.As(err, &timeout):
			// Mid-flight: the request may already have been dispatched
			// before cancellation interrupted a blocked read/write.
			// wrapCancel/wrapTimeout always surface this as *TimeoutError,
			// wrapping the same context.Canceled cause a pre-dispatch
			// refusal also carries -- errors.Is(err, context.Canceled)
			// alone cannot distinguish the two outcomes (mandate H4), so
			// the concrete *TimeoutError type is checked first, and the
			// connection must be broken here, never left usable.
			if !client.broken {
				t.Fatalf("iteration %d: mid-flight cancellation outcome was not marked broken: %#v", i, err)
			}
		case errors.As(err, &remote):
			// The response (a domain error) won the race.
		case errors.Is(err, context.Canceled):
			// Pre-dispatch: Call's own ctx.Err() check refused before
			// writing anything, so this must never be marked broken --
			// checked only after ruling out *TimeoutError above, since
			// the bare cause-chain match is shared with the mid-flight
			// case and would otherwise misclassify it (mandate H4).
			if client.broken {
				t.Fatalf("iteration %d: a pre-dispatch cancellation must never mark the connection broken: %#v", i, err)
			}
		default:
			t.Fatalf("iteration %d: incoherent outcome (not nil, not remote, not timeout, not cancelled-pre-dispatch): %#v", i, err)
		}
		if client.broken {
			// This connection can no longer be reused; redial so later
			// iterations still exercise the race. The server's own socket
			// slot for the just-closed connection is released by its
			// accept-loop goroutine asynchronously, so a redial
			// immediately after Close can transiently race
			// MaxSocketsPerAdministrator -- retry briefly rather than
			// treating that harness race as a defect in Call itself.
			client.Close()
			var dialErr error
			for attempt := 0; attempt < 50; attempt++ {
				client, _, dialErr = Dial(context.Background(), ClientConfig{Endpoint: fx.socketPath, ServerUID: fx.serverUID})
				if dialErr == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if dialErr != nil {
				t.Fatalf("iteration %d: redial: %v", i, dialErr)
			}
		}
	}
	client.Close()
	// Let any AfterFunc callback goroutines that were mid-flight finish.
	deadline := time.Now().Add(2 * time.Second)
	for goruntime.NumGoroutine() > before+5 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if after := goruntime.NumGoroutine(); after > before+5 {
		t.Fatalf("possible cancellation-observer goroutine leak: before=%d after=%d", before, after)
	}
}

// TestClientCallRejectsMalformedResponseEnvelopes covers mandate R5: a
// matching correlation ID alone is not proof of a completed call. Each
// case must be rejected and must mark the connection broken, exactly like
// the existing mismatched-ID and timeout cases.
func TestClientCallRejectsMalformedResponseEnvelopes(t *testing.T) {
	uid := uint32(os.Getuid())
	cases := []struct {
		name     string
		response string
	}{
		{"missing jsonrpc", `{"id":"1","result":{}}` + "\n"},
		{"wrong version", `{"jsonrpc":"1.0","id":"1","result":{}}` + "\n"},
		{"matching id but neither result nor error", `{"jsonrpc":"2.0","id":"1"}` + "\n"},
		{"both result and error", `{"jsonrpc":"2.0","id":"1","result":{},"error":{"code":-32000,"message":"x"}}` + "\n"},
		{"result present with an explicit null error", `{"jsonrpc":"2.0","id":"1","result":{},"error":null}` + "\n"},
		{"result null and error present (both keys, per JSON-RPC exclusivity)", `{"jsonrpc":"2.0","id":"1","result":null,"error":{"code":-32601,"message":"unknown"}}` + "\n"},
		{"error alone, explicit null", `{"jsonrpc":"2.0","id":"1","error":null}` + "\n"},
		{"error alone, empty object", `{"jsonrpc":"2.0","id":"1","error":{}}` + "\n"},
		{"error missing required code", `{"jsonrpc":"2.0","id":"1","error":{"message":"x"}}` + "\n"},
		{"error missing required message", `{"jsonrpc":"2.0","id":"1","error":{"code":-32601}}` + "\n"},
		{"error with null message", `{"jsonrpc":"2.0","id":"1","error":{"code":-32601,"message":null}}` + "\n"},
		{"error with mistyped code", `{"jsonrpc":"2.0","id":"1","error":{"code":"not-a-number","message":"x"}}` + "\n"},
		{"error with unrecognized code", `{"jsonrpc":"2.0","id":"1","error":{"code":-1,"message":"x"}}` + "\n"},
		{"domain error missing required data.code", `{"jsonrpc":"2.0","id":"1","error":{"code":-32000,"message":"x"}}` + "\n"},
		{"domain error with blank data.code", `{"jsonrpc":"2.0","id":"1","error":{"code":-32000,"message":"x","data":{"code":""}}}` + "\n"},
		{"envelope error must not carry data", `{"jsonrpc":"2.0","id":"1","error":{"code":-32601,"message":"x","data":{"code":"forbidden"}}}` + "\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := rawResponder(t, c.response)
			conn, err := connection.DialTrustedServer(context.Background(), path, uid)
			if err != nil {
				t.Fatal(err)
			}
			client := &Client{conn: conn, br: bufio.NewReader(conn)}
			defer client.Close()
			err = client.Call(context.Background(), "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
			if err == nil {
				t.Fatal("expected rejection")
			}
			if !client.broken {
				t.Fatal("connection not marked broken after a malformed response envelope")
			}
		})
	}
}

// TestClientCallMalformedResponseIsClassifiedAsUnresolvedOutcome is
// mandate CP-02's regression: a response that fails to decode after the
// request has already been written is not a proven non-dispatch -- it must
// surface as *TimeoutError (an unresolved-outcome, same-operation-ID-retry
// classification), exactly like the write/read-failure paths above, never
// as a bare error a caller could mistake for "definitely not performed".
func TestClientCallMalformedResponseIsClassifiedAsUnresolvedOutcome(t *testing.T) {
	uid := uint32(os.Getuid())
	// Syntactically invalid JSON: json.Unmarshal alone would already
	// reject this, but the classification -- not the rejection itself --
	// is what this test targets.
	path := rawResponder(t, `{"jsonrpc":"2.0","id":"1",`+"\n")
	conn, err := connection.DialTrustedServer(context.Background(), path, uid)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{conn: conn, br: bufio.NewReader(conn)}
	defer client.Close()
	err = client.Call(context.Background(), "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
	var timeout *TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("err=%v, want *TimeoutError", err)
	}
	if !client.broken {
		t.Fatal("connection not marked broken after a malformed response")
	}
}

// TestClientCallRejectsResponseViolatingStrictJSONGrammar is mandate
// CP-06's regression: a response with a duplicate top-level key is exactly
// the kind of malformed frame encoding/json's lenient decoder would accept
// silently (last value wins, no error) but the server's own parseJSON
// grammar rejects on the request path. The response path must apply the
// identical strict grammar, not a looser one just because it is decoding a
// reply instead of a request.
func TestClientCallRejectsResponseViolatingStrictJSONGrammar(t *testing.T) {
	uid := uint32(os.Getuid())
	path := rawResponder(t, `{"jsonrpc":"2.0","id":"1","id":"1","result":{}}`+"\n")
	conn, err := connection.DialTrustedServer(context.Background(), path, uid)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{conn: conn, br: bufio.NewReader(conn)}
	defer client.Close()
	err = client.Call(context.Background(), "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
	if err == nil {
		t.Fatal("expected rejection of a duplicate-key response")
	}
	var timeout *TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("err=%v, want *TimeoutError (mandate CP-02 classification)", err)
	}
	if !client.broken {
		t.Fatal("connection not marked broken after a strict-grammar violation")
	}
}

// TestClientCallDomainCodeVocabulary is mandate CP-03's regression, in both
// directions: a peer must not be able to introduce an arbitrary
// error.data.code merely by sending one that happens to be nonblank -- it
// must be a member of the accepted domain-code vocabulary this client and
// the server both enforce -- but that vocabulary must also actually accept
// every code the wire contract names, including the six wire-only
// additions (resnapshot_required, subscription_conflict,
// stale_grant_version, invalid_membership, unsupported_membership,
// incompatible_identifier) that have no store.Code counterpart. A valid
// code must decode into a genuine, usable RemoteError; an invalid one must
// leave the exchange incomplete (*TimeoutError) on a now-unusable
// connection, never a bare error a caller could mistake for something
// else.
func TestClientCallDomainCodeVocabulary(t *testing.T) {
	uid := uint32(os.Getuid())
	valid := []DomainCode{
		ProtocolMismatch, OperationNotFound,
		ResnapshotRequired, SubscriptionConflict, StaleGrantVersion,
		InvalidMembership, UnsupportedMembership, IncompatibleIdentifier,
		// A representative pre-existing store.Code, proving the switch
		// added by CP-03 did not shadow the store.Code fallback beneath it.
		DomainCode("capacity_exceeded"),
	}
	for _, code := range valid {
		t.Run("valid/"+string(code), func(t *testing.T) {
			response := fmt.Sprintf(`{"jsonrpc":"2.0","id":"1","error":{"code":-32000,"message":"x","data":{"code":%q}}}`+"\n", code)
			path := rawResponder(t, response)
			conn, err := connection.DialTrustedServer(context.Background(), path, uid)
			if err != nil {
				t.Fatal(err)
			}
			client := &Client{conn: conn, br: bufio.NewReader(conn)}
			defer client.Close()
			err = client.Call(context.Background(), "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
			var remote *RemoteError
			if !errors.As(err, &remote) || remote.Domain != code {
				t.Fatalf("err=%#v, want RemoteError with Domain %q", err, code)
			}
			if client.broken {
				t.Fatal("a valid domain code must not mark the connection broken")
			}
		})
	}

	invalid := []struct {
		name     string
		response string
	}{
		{"unrecognized", `{"jsonrpc":"2.0","id":"1","error":{"code":-32000,"message":"x","data":{"code":"not_a_real_domain_code"}}}` + "\n"},
		{"blank", `{"jsonrpc":"2.0","id":"1","error":{"code":-32000,"message":"x","data":{"code":""}}}` + "\n"},
		{"wrong type", `{"jsonrpc":"2.0","id":"1","error":{"code":-32000,"message":"x","data":{"code":123}}}` + "\n"},
	}
	for _, c := range invalid {
		t.Run("invalid/"+c.name, func(t *testing.T) {
			path := rawResponder(t, c.response)
			conn, err := connection.DialTrustedServer(context.Background(), path, uid)
			if err != nil {
				t.Fatal(err)
			}
			client := &Client{conn: conn, br: bufio.NewReader(conn)}
			defer client.Close()
			err = client.Call(context.Background(), "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
			var timeout *TimeoutError
			if !errors.As(err, &timeout) {
				t.Fatalf("err=%#v, want *TimeoutError -- an invalid domain code is an incomplete exchange, not a bare error", err)
			}
			if !client.broken {
				t.Fatal("connection not marked broken after an invalid domain code")
			}
		})
	}
}

// rawSequentialResponder starts a raw Unix listener that accepts exactly
// one connection and answers each request line, in order, with the
// matching entry in responses (already including its own trailing
// newline) -- unlike rawResponder, which only ever answers one request.
// It returns the listener's path.
func rawSequentialResponder(t *testing.T, responses []string) string {
	t.Helper()
	dir := privateSocketDir(t)
	path := dir + "/admin.sock"
	raw, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	go func() {
		conn, err := raw.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		for _, resp := range responses {
			if _, err := br.ReadBytes('\n'); err != nil {
				return
			}
			if _, err := conn.Write([]byte(resp)); err != nil {
				return
			}
		}
	}()
	return path
}

// TestClientCallAcceptsWellFormedResultsAndErrorsAndKeepsConnectionUsable is
// R5's positive control: the stricter presence-based exclusivity and error-
// object validation added above must not make rejection unconditional. A
// well-formed null result, a well-formed envelope error and a well-formed
// domain error must each be accepted on their own terms without marking the
// connection broken, and a genuine domain/envelope error must still leave
// the connection usable for a subsequent, ordinary successful call --
// distinguishing a valid rejection (RemoteError) from the malformed-
// transport rejections above, which do mark the connection broken.
func TestClientCallAcceptsWellFormedResultsAndErrorsAndKeepsConnectionUsable(t *testing.T) {
	uid := uint32(os.Getuid())

	t.Run("null result is a legitimate success value", func(t *testing.T) {
		path := rawResponder(t, `{"jsonrpc":"2.0","id":"1","result":null}`+"\n")
		conn, err := connection.DialTrustedServer(context.Background(), path, uid)
		if err != nil {
			t.Fatal(err)
		}
		client := &Client{conn: conn, br: bufio.NewReader(conn)}
		defer client.Close()
		if err := client.Call(context.Background(), "server.hello", nil, nil); err != nil {
			t.Fatalf("unexpected rejection of a legitimate null result: %v", err)
		}
		if client.broken {
			t.Fatal("a legitimate null result must not mark the connection broken")
		}
	})

	t.Run("well-formed envelope error, then a subsequent successful call", func(t *testing.T) {
		path := rawSequentialResponder(t, []string{
			`{"jsonrpc":"2.0","id":"1","error":{"code":-32601,"message":"unknown method"}}` + "\n",
			`{"jsonrpc":"2.0","id":"2","result":{}}` + "\n",
		})
		conn, err := connection.DialTrustedServer(context.Background(), path, uid)
		if err != nil {
			t.Fatal(err)
		}
		client := &Client{conn: conn, br: bufio.NewReader(conn)}
		defer client.Close()

		err = client.Call(context.Background(), "bogus.method", nil, nil)
		var remote *RemoteError
		if !errors.As(err, &remote) || remote.RPC != MethodNotFound || remote.Message != "unknown method" {
			t.Fatalf("err=%#v", err)
		}
		if client.broken {
			t.Fatal("a well-formed envelope error must not mark the connection broken")
		}
		if err := client.Call(context.Background(), "server.hello", nil, nil); err != nil {
			t.Fatalf("connection unusable after a well-formed envelope error: %v", err)
		}
	})

	t.Run("well-formed domain error, then a subsequent successful call", func(t *testing.T) {
		path := rawSequentialResponder(t, []string{
			`{"jsonrpc":"2.0","id":"1","error":{"code":-32000,"message":"forbidden","data":{"code":"forbidden"}}}` + "\n",
			`{"jsonrpc":"2.0","id":"2","result":{}}` + "\n",
		})
		conn, err := connection.DialTrustedServer(context.Background(), path, uid)
		if err != nil {
			t.Fatal(err)
		}
		client := &Client{conn: conn, br: bufio.NewReader(conn)}
		defer client.Close()

		err = client.Call(context.Background(), "operation.get", nil, nil)
		var remote *RemoteError
		if !errors.As(err, &remote) || remote.RPC != ServerError || remote.Domain != DomainCode("forbidden") {
			t.Fatalf("err=%#v", err)
		}
		if client.broken {
			t.Fatal("a well-formed domain error must not mark the connection broken")
		}
		if err := client.Call(context.Background(), "server.hello", nil, nil); err != nil {
			t.Fatalf("connection unusable after a well-formed domain error: %v", err)
		}
	})
}

// fixedProfileLimitsJSON renders this client's own fixed parley-control/1
// profile limits as a `"limits":{...}` JSON fragment -- the one genuinely
// valid baseline every hello-result test case is derived from, so a case
// targeting an unrelated field never incidentally fails (or is
// incidentally saved) by a limits mismatch (mandate RC-02).
func fixedProfileLimitsJSON() string {
	return fmt.Sprintf(`"limits":{"max_frame_bytes":%d,"max_nesting_depth":%d,"max_sockets_per_administrator":%d,"max_sockets_total":%d,"max_executing_per_socket":%d,"max_queued_per_socket":%d}`,
		MaxFrameBytes, maxDepth, MaxSocketsPerAdministrator, MaxSocketsTotal, MaxExecutingPerSocket, MaxQueuedPerSocket)
}

const validHelloID = "60000000-0000-4000-8000-000000000001"

// TestDialAcceptsAGenuinelyValidHelloResult is RC-02's positive control: a
// hello result matching the fixed parley-control/1 profile exactly, with
// canonical identities, a valid epoch, state and methods, must negotiate
// successfully through the real Dial path and yield a usable client. This
// proves the baseline every negative case in
// TestDialRejectsInvalidHelloResult mutates a single field from is itself
// valid, not merely "not obviously wrong."
func TestDialAcceptsAGenuinelyValidHelloResult(t *testing.T) {
	uid := uint32(os.Getuid())
	response := `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validHelloID + `","server_epoch":"e","administrator_id":"` + validHelloID + `","state":"running",` + fixedProfileLimitsJSON() + `,"methods":["server.hello"]}}` + "\n"
	path := rawResponder(t, response)
	client, hello, err := Dial(context.Background(), ClientConfig{Endpoint: path, ServerUID: uid})
	if err != nil {
		t.Fatalf("a genuinely valid, profile-matching hello result was rejected: %v", err)
	}
	defer client.Close()
	if hello.ServerID != validHelloID || hello.AdministratorID != validHelloID || hello.State != "running" {
		t.Fatalf("Dial returned an unexpected HelloResult: %+v", hello)
	}
}

// TestDialRejectsInvalidHelloResult covers mandate R5's requirement that
// Dial validate the decoded HelloResult itself, not just the envelope
// Call's own checks already accept: a bare `{"result":{}}`, an actual
// `"result":null`, an unsupported protocol, a missing/malformed required
// identity field, an unknown state, an invalid (non-positive) limits
// shape, and a missing or blank advertised method must all fail
// negotiation. Every case below holds every field but one at the same
// genuinely valid baseline TestDialAcceptsAGenuinelyValidHelloResult
// proves negotiates successfully (fixedProfileLimitsJSON, canonical UUIDs,
// "running", one valid method), so each case fails for the single
// property it names -- not incidentally masked or incidentally caused by
// an unrelated field, such as the limits mismatch that previously made
// "missing methods" and "blank method name" fail at the limits check
// instead of ever reaching the methods check they claim to exercise
// (mandate RC-02). want is checked as a substring of the rejection's own
// error text, and the rejection is confirmed to be a plain hello.validate()
// error, not a *TimeoutError from an unrelated connection-level failure --
// together these rule out a transport timeout, a malformed fixture, or an
// unrelated field silently making the case pass for the wrong reason.
func TestDialRejectsInvalidHelloResult(t *testing.T) {
	uid := uint32(os.Getuid())
	validID := validHelloID
	realLimits := fixedProfileLimitsJSON()
	cases := []struct {
		name     string
		response string
		want     string
	}{
		// CP-11: parley-control/1 is a fixed, not negotiated, profile -- a
		// peer advertising a merely-positive-but-different limit must be
		// rejected, not silently tolerated as if this client would then
		// frame/queue against whatever the peer claims.
		{"limits incompatible with the fixed profile", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running","limits":{"max_frame_bytes":` + fmt.Sprint(MaxFrameBytes+1) + `,"max_nesting_depth":` + fmt.Sprint(maxDepth) + `,"max_sockets_per_administrator":` + fmt.Sprint(MaxSocketsPerAdministrator) + `,"max_sockets_total":` + fmt.Sprint(MaxSocketsTotal) + `,"max_executing_per_socket":` + fmt.Sprint(MaxExecutingPerSocket) + `,"max_queued_per_socket":` + fmt.Sprint(MaxQueuedPerSocket) + `},"methods":["server.hello"]}}` + "\n", "limits incompatible"},
		// CP-10: reuses envelope.go's validMethodSyntax -- a method name
		// with a space is not in [A-Za-z0-9._]{1,64}.
		{"invalid method syntax", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running",` + realLimits + `,"methods":["bad method"]}}` + "\n", "invalid method name"},
		// CP-10: a duplicated advertisement is a hello contract violation
		// even though every individual name is syntactically valid.
		{"duplicate method name", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running",` + realLimits + `,"methods":["server.hello","server.hello"]}}` + "\n", "more than once"},
		// An empty object and an explicit null are distinct wire shapes
		// (RC-02); both must still be rejected, both decode to a zero-value
		// HelloResult and so are both caught by the very first check.
		{"empty hello result object", `{"jsonrpc":"2.0","id":"1","result":{}}` + "\n", "unsupported protocol"},
		{"null hello result", `{"jsonrpc":"2.0","id":"1","result":null}` + "\n", "unsupported protocol"},
		{"unsupported protocol", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"other","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running",` + realLimits + `,"methods":["server.hello"]}}` + "\n", "unsupported protocol"},
		{"missing server_id", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running",` + realLimits + `,"methods":["server.hello"]}}` + "\n", "invalid server_id"},
		{"non-uuid server_id", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"  ","server_epoch":"e","administrator_id":"` + validID + `","state":"running",` + realLimits + `,"methods":["server.hello"]}}` + "\n", "invalid server_id"},
		{"non-uuid administrator_id", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"not-a-uuid","state":"running",` + realLimits + `,"methods":["server.hello"]}}` + "\n", "invalid administrator_id"},
		{"unknown state", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"unknown",` + realLimits + `,"methods":["server.hello"]}}` + "\n", "unknown state"},
		{"invalid limits shape", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running","limits":{},"methods":["server.hello"]}}` + "\n", "limits incompatible"},
		{"missing methods", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running",` + realLimits + `}}` + "\n", "advertises no methods"},
		{"blank method name", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running",` + realLimits + `,"methods":["  "]}}` + "\n", "invalid method name"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := rawResponder(t, c.response)
			_, _, err := Dial(context.Background(), ClientConfig{Endpoint: path, ServerUID: uid})
			if err == nil {
				t.Fatal("expected rejection")
			}
			var timeout *TimeoutError
			if errors.As(err, &timeout) {
				t.Fatalf("rejected via a connection-level error (%v), not hello.validate() as this case intends", err)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err=%q, want it to contain %q", err.Error(), c.want)
			}
		})
	}
}

func TestClientCallTimesOutWithoutFalseFailureSemantics(t *testing.T) {
	dir := privateSocketDir(t)
	path := dir + "/admin.sock"
	uid := uint32(os.Getuid())
	raw, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	release := make(chan struct{})
	defer close(release)
	go func() {
		conn, err := raw.Accept()
		if err != nil {
			return
		}
		<-release
		conn.Close()
	}()

	conn, err := connection.DialTrustedServer(context.Background(), path, uid)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{conn: conn, br: bufio.NewReader(conn)}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = client.Call(ctx, "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
	var timeout *TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("err=%v, want *TimeoutError", err)
	}
}
