//go:build linux

package control

import (
	"bufio"
	"context"
	"errors"
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
	var remote *RemoteError
	var timeout *TimeoutError
	if errors.As(err, &remote) || errors.As(err, &timeout) {
		t.Fatalf("expected a plain transport failure (closed connection), got %#v", err)
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
	if !client.broken {
		t.Fatal("connection not marked broken after a mismatched response id")
	}
	if err := client.Call(context.Background(), "server.hello", nil, nil); !errors.Is(err, errClientBroken) {
		t.Fatalf("err=%v, want errClientBroken", err)
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
	defer client.Close()

	before := goruntime.NumGoroutine()
	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		go cancel()
		err := client.Call(ctx, "operation.get", map[string]any{"operation_id": "60000000-0000-4000-8000-000000000099"}, nil)
		var remote *RemoteError
		switch {
		case err == nil:
			// Success: the response won the race before cancellation took
			// effect.
		case errors.Is(err, context.Canceled):
			// Pre-dispatch: cancellation was already observed before
			// anything was sent -- correctly left unbroken.
		case errors.As(err, &remote):
			// The response (a domain error) won the race.
		case client.broken:
			// Cancellation was observed mid-flight, racing an in-flight
			// read/write. The exact error shape here (a raw net error,
			// *TimeoutError, etc.) depends on precisely when the forced
			// deadline reset raced the connection; what matters is that
			// the connection is consistently marked broken so a later
			// call can never mistake a stale response for its own.
		default:
			t.Fatalf("iteration %d: incoherent outcome (not nil, not remote, not cancelled-pre-dispatch, not broken): %#v", i, err)
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

// TestDialRejectsInvalidHelloResult covers mandate R5's requirement that
// Dial validate the decoded HelloResult itself, not just the envelope
// Call's own checks already accept: a bare `{"result":{}}`, an
// unsupported protocol, a missing required identity field, an unknown
// state and an invalid (non-positive) limits shape must all fail
// negotiation.
func TestDialRejectsInvalidHelloResult(t *testing.T) {
	uid := uint32(os.Getuid())
	const validLimits = `"limits":{"max_frame_bytes":1,"max_nesting_depth":1,"max_sockets_per_administrator":1,"max_sockets_total":1,"max_executing_per_socket":1,"max_queued_per_socket":1}`
	cases := []struct {
		name     string
		response string
	}{
		{"null hello result", `{"jsonrpc":"2.0","id":"1","result":{}}` + "\n"},
		{"unsupported protocol", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"other","server_id":"s","server_epoch":"e","administrator_id":"a","state":"running",` + validLimits + `,"methods":[]}}` + "\n"},
		{"missing server_id", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_epoch":"e","administrator_id":"a","state":"running",` + validLimits + `,"methods":[]}}` + "\n"},
		{"unknown state", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"s","server_epoch":"e","administrator_id":"a","state":"unknown",` + validLimits + `,"methods":[]}}` + "\n"},
		{"invalid limits shape", `{"jsonrpc":"2.0","id":"1","result":{"protocol":"` + ProtocolVersion + `","server_id":"s","server_epoch":"e","administrator_id":"a","state":"running","limits":{},"methods":[]}}` + "\n"},
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
