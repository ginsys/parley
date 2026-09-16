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
	l, err := Listen(cfg, 0600)
	if err != nil {
		t.Fatal(err)
	}
	db := controlTestDB(t)
	if err := db.OpenReaders(context.Background()); err != nil {
		t.Fatal(err)
	}
	service := NewListenerService(l, cfg, "epoch-fixture")
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
