//go:build linux

package control

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/runtime"
	"github.com/ginsys/parley/internal/store"
)

func privateSocketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "parley-control-listener-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestListenBindsWithRequestedModeAndAccepts(t *testing.T) {
	dir := privateSocketDir(t)
	path := filepath.Join(dir, "admin.sock")
	l, err := Listen(Config{AdminSocket: path, ServerUID: uint32(os.Getuid())}, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode=%v", info.Mode())
	}
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
}

func TestListenRefusesNonSocketEntry(t *testing.T) {
	dir := privateSocketDir(t)
	path := filepath.Join(dir, "admin.sock")
	if err := os.WriteFile(path, []byte("not a socket"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := Listen(Config{AdminSocket: path, ServerUID: uint32(os.Getuid())}, 0600)
	if err == nil {
		t.Fatal("expected refusal")
	}
}

func TestListenRefusesLiveSocket(t *testing.T) {
	dir := privateSocketDir(t)
	path := filepath.Join(dir, "admin.sock")
	live, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	_, err = Listen(Config{AdminSocket: path, ServerUID: uint32(os.Getuid())}, 0600)
	if err == nil {
		t.Fatal("expected refusal: a live socket must never be replaced")
	}
}

func TestListenRefusesUnsupportedModes(t *testing.T) {
	for _, mode := range []os.FileMode{0777, 0644, 0700, 0} {
		dir := privateSocketDir(t)
		path := filepath.Join(dir, "admin.sock")
		_, err := Listen(Config{AdminSocket: path, ServerUID: uint32(os.Getuid())}, mode)
		if err == nil {
			t.Fatalf("mode %v: expected refusal, got none", mode)
		}
	}
}

func TestListenReplacesProvablyStaleSocket(t *testing.T) {
	dir := privateSocketDir(t)
	path := filepath.Join(dir, "admin.sock")
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	// The socket file still exists, but nothing is listening on it: a
	// connect attempt now returns a definite ECONNREFUSED.
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("stale file missing before replacement: %v", err)
	}
	l, err := Listen(Config{AdminSocket: path, ServerUID: uint32(os.Getuid())}, 0600)
	if err != nil {
		t.Fatalf("expected the stale socket to be replaced: %v", err)
	}
	defer l.Close()
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
}

func TestAcquireSocketSlotEnforcesCaps(t *testing.T) {
	ln := &Listener{perAdmin: make(map[string]int)}
	for range MaxSocketsPerAdministrator {
		if !ln.acquireSocketSlot("admin-a") {
			t.Fatal("expected slot within per-administrator cap")
		}
	}
	if ln.acquireSocketSlot("admin-a") {
		t.Fatal("per-administrator cap not enforced")
	}
	ln.releaseSocketSlot("admin-a")
	if !ln.acquireSocketSlot("admin-a") {
		t.Fatal("released slot not reusable")
	}
}

func TestAcquireSocketSlotEnforcesTotalCapAcrossAdministrators(t *testing.T) {
	ln := &Listener{perAdmin: make(map[string]int)}
	admins := MaxSocketsTotal/MaxSocketsPerAdministrator + 1
	acquired := 0
	for a := range admins {
		for range MaxSocketsPerAdministrator {
			id := string(rune('a' + a))
			if ln.totalSockets >= MaxSocketsTotal {
				if ln.acquireSocketSlot(id) {
					t.Fatal("total cap not enforced")
				}
				continue
			}
			if !ln.acquireSocketSlot(id) {
				t.Fatal("unexpected refusal within total cap")
			}
			acquired++
		}
	}
	if acquired != MaxSocketsTotal {
		t.Fatalf("acquired=%d want=%d", acquired, MaxSocketsTotal)
	}
}

func TestAcquireSocketSlotRefusesAfterStop(t *testing.T) {
	ln := &Listener{perAdmin: make(map[string]int), stopped: true}
	if ln.acquireSocketSlot("admin-a") {
		t.Fatal("stopped listener accepted a new slot")
	}
}

// listenerFixture wires a real Listen()'d socket to a full Listener
// service against a real, migrated store.DB -- the same components
// cmd/parleyd will assemble.
type listenerFixture struct {
	socketPath string
	adminID    string
	serverUID  uint32
	db         *store.DB
	ln         *Listener
}

func newListenerFixture(t *testing.T) *listenerFixture {
	t.Helper()
	dir := privateSocketDir(t)
	socketPath := filepath.Join(dir, "admin.sock")
	adminID := "60000000-0000-4000-8000-000000000001"
	uid := uint32(os.Getuid())
	cfg, err := NewConfig(socketPath, uid, map[string]uint32{adminID: uid})
	if err != nil {
		t.Fatal(err)
	}
	db := controlTestDB(t)
	if err := db.OpenReaders(context.Background()); err != nil {
		t.Fatal(err)
	}
	service := NewListenerService(cfg, 0600, "epoch-fixture")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := service.Start(context.Background(), runtime.Resources{WorkerContext: ctx, Writer: db, Queries: db.Queries(), Mode: runtime.Normal}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		service.StopAdmission()
		cancel()
		service.Wait()
	})
	return &listenerFixture{socketPath: socketPath, adminID: adminID, serverUID: uid, db: db, ln: service}
}

func dialAndRoundTrip(t *testing.T, path string, serverUID uint32, requestLine string) map[string]any {
	t.Helper()
	conn, err := connection.DialTrustedServer(context.Background(), path, serverUID)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(requestLine + "\n")); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(line, &decoded); err != nil {
		t.Fatalf("response %q: %v", line, err)
	}
	return decoded
}

func TestListenerServiceHelloRoundTrip(t *testing.T) {
	fx := newListenerFixture(t)
	resp := dialAndRoundTrip(t, fx.socketPath, fx.serverUID, `{"jsonrpc":"2.0","id":"1","method":"server.hello","params":{"protocol":"parley-control/1"}}`)
	if resp["error"] != nil {
		t.Fatalf("%#v", resp)
	}
	result, ok := resp["result"].(map[string]any)
	if !ok || result["administrator_id"] != fx.adminID || result["protocol"] != ProtocolVersion {
		t.Fatalf("%#v", resp)
	}
}

func TestListenerServiceRejectsRequestsBeforeHello(t *testing.T) {
	fx := newListenerFixture(t)
	resp := dialAndRoundTrip(t, fx.socketPath, fx.serverUID, `{"jsonrpc":"2.0","id":"1","method":"operation.get","params":{"operation_id":"60000000-0000-4000-8000-000000000099"}}`)
	errObj, ok := resp["error"].(map[string]any)
	if !ok {
		t.Fatalf("%#v", resp)
	}
	data, ok := errObj["data"].(map[string]any)
	if !ok || data["code"] != string(store.Forbidden) {
		t.Fatalf("%#v", errObj)
	}
}

func TestListenerServiceUnconfiguredUIDIsRefusedSilently(t *testing.T) {
	dir := privateSocketDir(t)
	socketPath := filepath.Join(dir, "admin.sock")
	uid := uint32(os.Getuid())
	// Configure an administrator UID that is not this test process's UID.
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

	conn, err := connection.DialTrustedServer(context.Background(), socketPath, uid)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected the connection to be closed for an unconfigured UID")
	}
}

func TestListenerServiceStopAdmissionDrainsCleanly(t *testing.T) {
	fx := newListenerFixture(t)
	resp := dialAndRoundTrip(t, fx.socketPath, fx.serverUID, `{"jsonrpc":"2.0","id":"1","method":"server.hello","params":{"protocol":"parley-control/1"}}`)
	if resp["error"] != nil {
		t.Fatalf("%#v", resp)
	}
	if err := fx.ln.StopAdmission(); err != nil {
		t.Fatal(err)
	}
	if _, err := net.DialTimeout("unix", fx.socketPath, time.Second); err == nil {
		t.Fatal("listener still accepting after StopAdmission")
	}
	done := make(chan error, 1)
	go func() { done <- fx.ln.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after StopAdmission")
	}
}

func TestListenerServiceRecoveryOnlyState(t *testing.T) {
	dir := privateSocketDir(t)
	socketPath := filepath.Join(dir, "admin.sock")
	adminID := "60000000-0000-4000-8000-000000000001"
	uid := uint32(os.Getuid())
	cfg, err := NewConfig(socketPath, uid, map[string]uint32{adminID: uid})
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
	if err := service.Start(context.Background(), runtime.Resources{WorkerContext: ctx, Writer: db, Queries: db.Queries(), Mode: runtime.Held}); err != nil {
		t.Fatal(err)
	}
	defer func() { service.StopAdmission(); cancel(); service.Wait() }()
	resp := dialAndRoundTrip(t, socketPath, uid, `{"jsonrpc":"2.0","id":"1","method":"server.hello","params":{"protocol":"parley-control/1"}}`)
	result, ok := resp["result"].(map[string]any)
	if !ok || result["state"] != string(StateRecoveryOnly) {
		t.Fatalf("%#v", resp)
	}
}

// TestListenerServiceClosesConnectionOnIdlessObject proves
// docs/specifications/control.md:64-65's "not executed and receive no
// response; close" -- an earlier version of serveSession merely
// `continue`d, leaving the connection (and the client's socket slot)
// open indefinitely after an ID-less object.
func TestListenerServiceClosesConnectionOnIdlessObject(t *testing.T) {
	fx := newListenerFixture(t)
	conn, err := connection.DialTrustedServer(context.Background(), fx.socketPath, fx.serverUID)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(`{"jsonrpc":"2.0","method":"server.hello","params":{"protocol":"parley-control/1"}}` + "\n")); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected the connection to be closed after an ID-less object")
	}
}

// TestListenerServiceDisconnectsSilentPreHelloConnection proves
// docs/specifications/control.md:77's "the first call within five
// seconds is server.hello" is actually enforced -- a connection that is
// accepted (kernel-authenticated) but never sends a single byte must not
// hold its socket slot forever.
func TestListenerServiceDisconnectsSilentPreHelloConnection(t *testing.T) {
	fx := newListenerFixture(t)
	conn, err := connection.DialTrustedServer(context.Background(), fx.socketPath, fx.serverUID)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(FrameDeadline + 3*time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected the silent connection to be closed once FrameDeadline elapsed")
	}
}

// TestWriteResponseReplacesOversizedResponseWithInternalError proves
// writeResponse never puts an over-MaxFrameBytes frame on the wire: a
// response whose encoded body would exceed the profile's own frame bound
// is replaced with a bounded InternalError before writing, preserving the
// original request's ID.
func TestWriteResponseReplacesOversizedResponseWithInternalError(t *testing.T) {
	dir := privateSocketDir(t)
	path := filepath.Join(dir, "admin.sock")
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan *net.UnixConn, 1)
	go func() {
		c, err := ln.AcceptUnix()
		if err != nil {
			return
		}
		accepted <- c
	}()
	dialed, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer dialed.Close()
	serverConn := <-accepted
	defer serverConn.Close()

	id := "1"
	oversized := make([]byte, MaxFrameBytes)
	resp := successResponse(id, map[string]string{"padding": string(oversized)})
	writeErr := make(chan error, 1)
	go func() { writeErr <- writeResponse(serverConn, resp) }()

	dialed.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := bufio.NewReader(dialed).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}
	if len(line) >= MaxFrameBytes {
		t.Fatalf("wrote an oversized frame: %d bytes", len(line))
	}
	var decoded map[string]any
	if err := json.Unmarshal(line, &decoded); err != nil {
		t.Fatalf("response %q: %v", line, err)
	}
	if decoded["id"] != id {
		t.Fatalf("id=%v, want %q", decoded["id"], id)
	}
	errObj, ok := decoded["error"].(map[string]any)
	if !ok || int(errObj["code"].(float64)) != int(InternalError) {
		t.Fatalf("%#v", decoded)
	}
}
