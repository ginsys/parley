//go:build linux

package control

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
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
	service := NewListenerService(cfg, 0600, "epoch-fixture")
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
