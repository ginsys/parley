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
	go func() {
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
	defer client.Close()

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- client.Call(context.Background(), "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
	}()

	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed the first request")
	}

	if err := client.Call(context.Background(), "server.hello", nil, nil); !errors.Is(err, errClientConcurrentCall) {
		t.Fatalf("err=%v, want errClientConcurrentCall", err)
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first call failed: %v", err)
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

// TestReadBoundedFrameRefusesOversizedUnterminatedStream proves the
// client's response reader will not buffer without limit -- mirroring the
// server's own MaxFrameBytes write bound (writeResponse in
// listener_linux.go) -- when a peer never sends the terminating LF.
func TestReadBoundedFrameRefusesOversizedUnterminatedStream(t *testing.T) {
	dir := privateSocketDir(t)
	path := dir + "/admin.sock"
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
		chunk := make([]byte, 4096)
		for i := range chunk {
			chunk[i] = 'x'
		}
		for {
			if _, err := conn.Write(chunk); err != nil {
				return
			}
		}
	}()

	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, err = readBoundedFrame(bufio.NewReader(conn))
	if err == nil {
		t.Fatal("expected refusal of an unbounded unterminated stream")
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

// TestClientCallRejectsUnrecognizedDomainCode is mandate CP-03's
// regression: a peer must not be able to introduce an arbitrary
// error.data.code merely by sending one that happens to be nonblank -- it
// must be a member of the accepted domain-code vocabulary this client and
// the server both enforce.
func TestClientCallRejectsUnrecognizedDomainCode(t *testing.T) {
	uid := uint32(os.Getuid())
	path := rawResponder(t, `{"jsonrpc":"2.0","id":"1","error":{"code":-32000,"message":"x","data":{"code":"not_a_real_domain_code"}}}`+"\n")
	conn, err := connection.DialTrustedServer(context.Background(), path, uid)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{conn: conn, br: bufio.NewReader(conn)}
	defer client.Close()
	err = client.Call(context.Background(), "server.hello", map[string]any{"protocol": ProtocolVersion}, nil)
	if err == nil {
		t.Fatal("expected rejection of an unrecognized error.data.code")
	}
	if !client.broken {
		t.Fatal("connection not marked broken after an unrecognized domain code")
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

// TestDialRejectsInvalidHelloResult covers mandate R5's requirement that
// Dial validate the decoded HelloResult itself, not just the envelope
// Call's own checks already accept: a bare `{"result":{}}`, an
// unsupported protocol, a missing/malformed required identity field, an
// unknown state, an invalid (non-positive) limits shape, and a missing or
// blank advertised method must all fail negotiation. server_id and
// administrator_id use valid canonical UUIDs except in the cases that
// specifically target those fields, so each case fails for the reason it
// claims to test, not incidentally for a different one.
func TestDialRejectsInvalidHelloResult(t *testing.T) {
	uid := uint32(os.Getuid())
	const validID = "60000000-0000-4000-8000-000000000001"
	const validLimits = `"limits":{"max_frame_bytes":1,"max_nesting_depth":1,"max_sockets_per_administrator":1,"max_sockets_total":1,"max_executing_per_socket":1,"max_queued_per_socket":1}`
	// realLimits matches this client's own fixed parley-control/1 profile
	// exactly -- used to isolate the CP-10/CP-11 cases below from the
	// pre-existing (and intentionally non-matching) validLimits above, so
	// those cases fail for the one property under test, not incidentally
	// for a limits mismatch too.
	realLimits := fmt.Sprintf(`"limits":{"max_frame_bytes":%d,"max_nesting_depth":%d,"max_sockets_per_administrator":%d,"max_sockets_total":%d,"max_executing_per_socket":%d,"max_queued_per_socket":%d}`,
		MaxFrameBytes, maxDepth, MaxSocketsPerAdministrator, MaxSocketsTotal, MaxExecutingPerSocket, MaxQueuedPerSocket)
	cases := []struct {
		name     string
		response string
	}{
		// CP-11: parley-control/1 is a fixed, not negotiated, profile -- a
		// peer advertising a merely-positive-but-different limit must be
		// rejected, not silently tolerated as if this client would then
		// frame/queue against whatever the peer claims.
		{"limits incompatible with the fixed profile", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running","limits":{"max_frame_bytes":` + fmt.Sprint(MaxFrameBytes+1) + `,"max_nesting_depth":` + fmt.Sprint(maxDepth) + `,"max_sockets_per_administrator":` + fmt.Sprint(MaxSocketsPerAdministrator) + `,"max_sockets_total":` + fmt.Sprint(MaxSocketsTotal) + `,"max_executing_per_socket":` + fmt.Sprint(MaxExecutingPerSocket) + `,"max_queued_per_socket":` + fmt.Sprint(MaxQueuedPerSocket) + `},"methods":["server.hello"]}}` + "\n"},
		// CP-10: reuses envelope.go's validMethodSyntax -- a method name
		// with a space is not in [A-Za-z0-9._]{1,64}.
		{"invalid method syntax", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running",` + realLimits + `,"methods":["bad method"]}}` + "\n"},
		// CP-10: a duplicated advertisement is a hello contract violation
		// even though every individual name is syntactically valid.
		{"duplicate method name", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running",` + realLimits + `,"methods":["server.hello","server.hello"]}}` + "\n"},
		{"null hello result", `{"jsonrpc":"2.0","id":"1","result":{}}` + "\n"},
		{"unsupported protocol", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"other","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running",` + validLimits + `,"methods":["server.hello"]}}` + "\n"},
		{"missing server_id", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running",` + validLimits + `,"methods":["server.hello"]}}` + "\n"},
		{"non-uuid server_id", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"  ","server_epoch":"e","administrator_id":"` + validID + `","state":"running",` + validLimits + `,"methods":["server.hello"]}}` + "\n"},
		{"non-uuid administrator_id", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"not-a-uuid","state":"running",` + validLimits + `,"methods":["server.hello"]}}` + "\n"},
		{"unknown state", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"unknown",` + validLimits + `,"methods":["server.hello"]}}` + "\n"},
		{"invalid limits shape", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running","limits":{},"methods":["server.hello"]}}` + "\n"},
		{"missing methods", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running",` + validLimits + `}}` + "\n"},
		{"blank method name", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"` + validID + `","server_epoch":"e","administrator_id":"` + validID + `","state":"running",` + validLimits + `,"methods":["  "]}}` + "\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := rawResponder(t, c.response)
			_, _, err := Dial(context.Background(), ClientConfig{Endpoint: path, ServerUID: uid})
			if err == nil {
				t.Fatal("expected rejection")
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
