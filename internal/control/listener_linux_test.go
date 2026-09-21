//go:build linux

package control

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/runtime"
	"github.com/ginsys/parley/internal/store"
	"golang.org/x/sys/unix"
)

// fakeListener implements net.Listener with a scripted queue of Accept
// results, letting R4's tests inject exact Accept errors deterministically
// -- the mandate requires injecting Accept errors, never exhausting real
// host file descriptors.
type fakeListener struct {
	mu      sync.Mutex
	results []fakeAcceptResult
	acceptN int
}

type fakeAcceptResult struct {
	conn net.Conn
	err  error
}

func (f *fakeListener) Accept() (net.Conn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acceptN++
	if len(f.results) == 0 {
		return nil, errors.New("fakeListener: accept results exhausted")
	}
	r := f.results[0]
	f.results = f.results[1:]
	return r.conn, r.err
}

func (f *fakeListener) Close() error { return nil }

func (f *fakeListener) Addr() net.Addr { return &net.UnixAddr{Name: "fake", Net: "unix"} }

func (f *fakeListener) acceptCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.acceptN
}

// syntheticAcceptError is a controlled net.Error for injecting a specific
// Temporary()/Timeout() classification without needing a real syscall
// errno (mandate R4: inject Accept errors, never exhaust real fds).
type syntheticAcceptError struct {
	msg              string
	temporary, timeo bool
}

func (e *syntheticAcceptError) Error() string { return e.msg }
func (e *syntheticAcceptError) Timeout() bool { return e.timeo }

//nolint:staticcheck // SA1019: intentional -- matches isTemporaryAcceptError's own use of Temporary()
func (e *syntheticAcceptError) Temporary() bool { return e.temporary }

func TestAcceptLoopReturnsCleanlyOnOwnedStop(t *testing.T) {
	fl := &fakeListener{results: []fakeAcceptResult{{err: net.ErrClosed}}}
	ln := &Listener{listener: fl, perAdmin: make(map[string]int), stopped: true}
	done := make(chan struct{})
	ln.wg.Add(1)
	go func() { ln.acceptLoop(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("acceptLoop did not return on an owned stop")
	}
	ln.wg.Wait()
	if err := ln.Wait(); err != nil {
		t.Fatalf("owned stop must not be surfaced as a failure: %v", err)
	}
}

func TestAcceptLoopEscalatesAfterBoundedTemporaryRetries(t *testing.T) {
	sentinel := &syntheticAcceptError{msg: "synthetic: resource exhaustion", temporary: true}
	results := make([]fakeAcceptResult, maxAcceptRetries+1)
	for i := range results {
		results[i] = fakeAcceptResult{err: sentinel}
	}
	fl := &fakeListener{results: results}
	ln := &Listener{listener: fl, perAdmin: make(map[string]int)}
	var failed atomic.Pointer[error]
	ln.OnAcceptFailure(func(err error) { e := err; failed.Store(&e) })
	ln.wg.Add(1)
	done := make(chan struct{})
	go func() { ln.acceptLoop(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("acceptLoop did not escalate within the bound")
	}
	if fl.acceptCount() != maxAcceptRetries+1 {
		t.Fatalf("accept called %d times, want exactly %d (bounded retry)", fl.acceptCount(), maxAcceptRetries+1)
	}
	got := failed.Load()
	if got == nil || !errors.Is(*got, error(sentinel)) {
		t.Fatalf("OnAcceptFailure not invoked with the synthetic error: %v", got)
	}
	if err := ln.Wait(); !errors.Is(err, sentinel) {
		t.Fatalf("Wait()=%v, want the synthetic error", err)
	}
}

func TestAcceptLoopSurfacesUnexpectedNonTemporaryFailureImmediately(t *testing.T) {
	sentinel := errors.New("synthetic: permanent accept failure")
	fl := &fakeListener{results: []fakeAcceptResult{{err: sentinel}}}
	ln := &Listener{listener: fl, perAdmin: make(map[string]int)}
	var failed atomic.Pointer[error]
	ln.OnAcceptFailure(func(err error) { e := err; failed.Store(&e) })
	ln.wg.Add(1)
	done := make(chan struct{})
	go func() { ln.acceptLoop(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("acceptLoop did not surface an unexpected non-temporary failure")
	}
	if fl.acceptCount() != 1 {
		t.Fatalf("accept called %d times, want exactly 1 (no retry for a non-temporary error)", fl.acceptCount())
	}
	got := failed.Load()
	if got == nil || !errors.Is(*got, sentinel) {
		t.Fatalf("OnAcceptFailure not invoked with the synthetic error: %v", got)
	}
	if err := ln.Wait(); !errors.Is(err, sentinel) {
		t.Fatalf("Wait()=%v, want the synthetic error", err)
	}
}

func TestAcceptLoopBackoffIsCancellable(t *testing.T) {
	sentinel := &syntheticAcceptError{msg: "synthetic: transient", temporary: true}
	results := make([]fakeAcceptResult, maxAcceptRetries)
	for i := range results {
		results[i] = fakeAcceptResult{err: sentinel}
	}
	fl := &fakeListener{results: results}
	ln := &Listener{listener: fl, perAdmin: make(map[string]int)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	ln.wg.Add(1)
	go func() { ln.acceptLoop(ctx); close(done) }()

	// Cancel partway through the bounded retry sequence (well before its
	// full worst-case backoff, ~2.5s for 8 retries) and confirm the loop
	// exits promptly rather than waiting out the remaining delays.
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not interrupt the accept retry backoff")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("backoff was not interrupted promptly: %v", elapsed)
	}
	if err := ln.Wait(); err != nil {
		t.Fatalf("a cancelled backoff must not be surfaced as a failure: %v", err)
	}
}

// assertConnectionActuallyClosed fails t unless err proves the server
// actually closed the connection (io.EOF, the error a peer's Read sees
// once the other side closes) -- never merely that the caller's own read
// deadline expired. A prior version of these tests accepted any non-nil
// read error, including the caller's own timeout; removing the fix under
// test still made both pass, since an un-fixed server that never closes
// the connection would just make the caller's own deadline fire instead.
func assertConnectionActuallyClosed(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected the connection to be closed, got no error")
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatalf("connection was not actually closed within the deadline; the caller's own read deadline expired instead: %v", err)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF from the server closing the connection, got: %v", err)
	}
}

// socketCountForTest reads ln.totalSockets under its own lock -- a
// test-only synchronization point proving the accept loop has actually
// registered a newly dialed connection (acquireSocketSlot runs
// synchronously, before that connection's serving goroutine is spawned),
// not merely that the kernel accepted it into its listen backlog.
func (ln *Listener) socketCountForTest() int {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	return ln.totalSockets
}

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

// TestControlSocketAddrAcceptsAndRejectsAtTheExactBoundary exercises the
// pure address-length arithmetic directly, at the precise byte boundary
// x/sys/unix's SockaddrUnix enforces (S15-2): a constructed-address helper,
// not a large number of real file descriptors, is what a descriptor-width
// boundary needs -- controlSocketAddr performs no syscalls, so an arbitrary
// small fd number exercises the same string arithmetic a large one would.
func TestControlSocketAddrAcceptsAndRejectsAtTheExactBoundary(t *testing.T) {
	const parent = 3 // arbitrary; controlSocketAddr never dereferences it
	prefix := fmt.Sprintf("/proc/self/fd/%d/", parent)
	fit := maxUnixSockAddrLen - len(prefix)
	if fit < 1 {
		t.Fatalf("test setup: prefix %q already exceeds the boundary", prefix)
	}
	name := strings.Repeat("a", fit)

	addr, err := controlSocketAddr(parent, name)
	if err != nil {
		t.Fatalf("exact boundary (%d bytes) rejected: %v", len(prefix)+fit, err)
	}
	if len(addr) != maxUnixSockAddrLen {
		t.Fatalf("addr len=%d, want exactly %d", len(addr), maxUnixSockAddrLen)
	}

	if _, err := controlSocketAddr(parent, name+"a"); !errors.Is(err, errSocketAddressTooLong) {
		t.Fatalf("one byte over the boundary: err=%v, want errSocketAddressTooLong", err)
	}
}

// TestListenRejectsAdminSocketAddressTooLongForSockaddrUn proves the
// rejection actually reaches real Listen callers, not just the pure helper
// above, and that startup fails with the documented error rather than
// falling back to an unprotected bind.
func TestListenRejectsAdminSocketAddressTooLongForSockaddrUn(t *testing.T) {
	dir := privateSocketDir(t)
	// Long enough that /proc/self/fd/<parent>/<name> cannot fit a struct
	// sockaddr_un regardless of the parent descriptor's numeric width.
	name := strings.Repeat("a", maxUnixSockAddrLen+1)
	path := filepath.Join(dir, name)
	_, err := Listen(Config{AdminSocket: path, ServerUID: uint32(os.Getuid())}, 0600)
	if !errors.Is(err, errSocketAddressTooLong) {
		t.Fatalf("err=%v, want errSocketAddressTooLong", err)
	}
	if _, statErr := os.Lstat(path); statErr == nil {
		t.Fatal("Listen created an entry at the rejected pathname")
	}
}

// TestListenRejectsTooLongAddressWithoutTouchingExistingEntry proves the
// address-length check runs before prepareSocketPath can probe or remove
// anything: an existing entry at a nonrepresentable pathname must survive
// the rejected Listen call untouched.
func TestListenRejectsTooLongAddressWithoutTouchingExistingEntry(t *testing.T) {
	dir := privateSocketDir(t)
	name := strings.Repeat("a", maxUnixSockAddrLen+1)
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := Listen(Config{AdminSocket: path, ServerUID: uint32(os.Getuid())}, 0600)
	if !errors.Is(err, errSocketAddressTooLong) {
		t.Fatalf("err=%v, want errSocketAddressTooLong", err)
	}
	data, statErr := os.ReadFile(path)
	if statErr != nil {
		t.Fatalf("existing entry removed on rejection: %v", statErr)
	}
	if string(data) != "existing" {
		t.Fatalf("existing entry modified: %q", data)
	}
}

// TestListenAcceptsAdminSocketAtTheAddressBoundaryAndBinds proves the
// boundary above isn't merely rejecting everything: a path whose
// constructed address is short enough still binds and accepts real
// connections. TestListenBindsWithRequestedModeAndAccepts already covers
// the ordinary short-path case; this covers the accepted edge next to the
// rejected one above.
func TestListenAcceptsAdminSocketAtTheAddressBoundaryAndBinds(t *testing.T) {
	dir := privateSocketDir(t)
	// A long name, but kept far enough below maxUnixSockAddrLen that the
	// plain "dir+name" absolute path (this test's own verification dial
	// path, which does not go through /proc/self/fd/ addressing) also
	// stays representable -- that is a separate, real client-side
	// constraint this test must not confuse with the fix under test. The
	// margin below the boundary accounts for the dir prefix length
	// (os.MkdirTemp's random suffix varies) and leaves room for the
	// descriptor-relative address (typically slightly longer than the
	// plain path for a small parent fd number) to still fit.
	name := strings.Repeat("a", maxUnixSockAddrLen-50)
	path := filepath.Join(dir, name)
	l, err := Listen(Config{AdminSocket: path, ServerUID: uint32(os.Getuid())}, 0600)
	if err != nil {
		t.Fatalf("long-but-representable path rejected: %v", err)
	}
	defer l.Close()
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
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
	cancel     context.CancelFunc // the worker context runtime shutdown would cancel
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
	service := NewListenerService(cfg, 0600)
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
	return &listenerFixture{socketPath: socketPath, adminID: adminID, serverUID: uid, db: db, ln: service, cancel: cancel}
}

// dialAndRoundTripErr is dialAndRoundTrip's actual I/O core, returning an
// error instead of calling t.Fatal so TestDialAndRoundTripBoundsHandshakeIODeadline
// can observe a deliberately induced failure directly -- a *testing.T
// subtest's failure would otherwise unconditionally propagate to and fail
// its parent, regardless of what the parent asserts afterward, making
// "assert this call fails quickly, then still pass" unrepresentable via
// t.Run. dialAndRoundTrip below is the thin, unchanged-behavior wrapper
// every other test in this file uses.
func dialAndRoundTripErr(ctx context.Context, path string, serverUID uint32, requestLine string) (map[string]any, error) {
	conn, err := connection.DialTrustedServer(ctx, path, serverUID)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	// DialTrustedServer only bounds the dial itself; without this, an
	// accepted peer that never completes its side of the exchange (see
	// TestDialAndRoundTripBoundsHandshakeIODeadline) would block this
	// helper -- and every test using it -- indefinitely. restartTeardownWait
	// is reused rather than inventing a second finite bound; a deadline
	// expiring here surfaces as a plain error, never as a false "peer
	// closed" or "hello succeeded" reading (mandate TC-R1).
	if err := conn.SetDeadline(time.Now().Add(restartTeardownWait)); err != nil {
		return nil, err
	}
	if _, err := conn.Write([]byte(requestLine + "\n")); err != nil {
		return nil, err
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	var decoded map[string]any
	if err := json.Unmarshal(line, &decoded); err != nil {
		return nil, fmt.Errorf("response %q: %w", line, err)
	}
	return decoded, nil
}

func dialAndRoundTrip(t *testing.T, path string, serverUID uint32, requestLine string) map[string]any {
	t.Helper()
	decoded, err := dialAndRoundTripErr(context.Background(), path, serverUID, requestLine)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

// TestDialAndRoundTripBoundsHandshakeIODeadline proves dialAndRoundTripErr's
// deadline (above) actually bounds a stalled exchange, not just a
// configured duration that is never reached: a peer that accepts the
// connection but never writes a response must make the call fail within
// restartTeardownWait plus scheduling slack, never hang. The accepted
// connection is closed afterward so this probe does not strand it
// (mandate TC-R1 focused verification #4).
func TestDialAndRoundTripBoundsHandshakeIODeadline(t *testing.T) {
	dir := privateSocketDir(t)
	path := filepath.Join(dir, "admin.sock")
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- c // accepted, but deliberately never written to or closed
	}()
	// Close the accepted connection whenever the accept goroutine actually
	// publishes it -- closing the listener above unblocks a still-pending
	// Accept but does not close a connection Accept has already returned.
	// Registered now, before the fallible round-trip call below, so an
	// early Fatal still runs this; bounded rather than a non-blocking
	// select, so a connection published only after this goroutine schedules
	// (a race the original non-blocking check could lose) is still closed
	// instead of leaked (mandate PC-F2).
	t.Cleanup(func() {
		select {
		case c := <-accepted:
			c.Close()
		case <-time.After(restartTeardownWait):
			t.Errorf("accepted connection was never published by the accept goroutine")
		}
	})

	start := time.Now()
	_, err = dialAndRoundTripErr(context.Background(), path, uint32(os.Getuid()), `{"jsonrpc":"2.0","id":"1","method":"server.hello","params":{"protocol":"parley-control/1"}}`)
	elapsed := time.Since(start)
	var netErr net.Error
	if err == nil || !(errors.As(err, &netErr) && netErr.Timeout()) {
		t.Fatalf("expected a deadline-timeout error against a peer that never responds, got: %v", err)
	}
	if elapsed > restartTeardownWait+2*time.Second {
		t.Fatalf("dialAndRoundTripErr did not respect its own deadline: took %s", elapsed)
	}
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
	service := NewListenerService(cfg, 0600)
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
	_, err = conn.Read(buf)
	// assertConnectionActuallyClosed, not a bare err==nil check: an earlier
	// version of this test accepted any non-nil read error, including the
	// caller's own read-deadline timeout -- which would also pass if the
	// server never closed the connection at all and this test's deadline
	// simply expired first.
	assertConnectionActuallyClosed(t, err)
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

// TestListenerServiceStopAdmissionUnlinksTheOwnedSocketPath is mandate
// CP-09's regression: a clean shutdown must remove the on-disk pathname
// socket entry it owns, not merely stop accepting on it -- previously the
// socket file was left behind indefinitely after every clean StopAdmission,
// looking indistinguishable from a stale/crashed entry to anything that
// only inspects the filesystem.
func TestListenerServiceStopAdmissionUnlinksTheOwnedSocketPath(t *testing.T) {
	fx := newListenerFixture(t)
	resp := dialAndRoundTrip(t, fx.socketPath, fx.serverUID, `{"jsonrpc":"2.0","id":"1","method":"server.hello","params":{"protocol":"parley-control/1"}}`)
	if resp["error"] != nil {
		t.Fatalf("%#v", resp)
	}
	if err := fx.ln.StopAdmission(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(fx.socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket path still exists after clean StopAdmission: err=%v", err)
	}
}

// TestUnlinkOwnedSocketNeverRemovesAReplacementAtTheSamePath is CP-09's
// companion negative control: unlinkOwnedSocket must compare device/inode
// against the identity captured at bind time, not merely the pathname
// string, so it never removes a different socket a separate process has
// already bound at the same path after this listener's own entry was
// replaced.
func TestUnlinkOwnedSocketNeverRemovesAReplacementAtTheSamePath(t *testing.T) {
	dir := privateSocketDir(t)
	path := filepath.Join(dir, "admin.sock")
	original, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	var originalStat unix.Stat_t
	if err := unix.Lstat(path, &originalStat); err != nil {
		t.Fatal(err)
	}
	// Simulate replacement: remove the original entry and bind a fresh
	// socket at the identical path, exactly like a subsequent process
	// legitimately taking over the same pathname.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()

	unlinkOwnedSocket(path, uint64(originalStat.Dev), originalStat.Ino)

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("replacement socket was removed by an unrelated owner's unlink: %v", err)
	}
}

// TestListenerServiceStartCleansUpTheSocketWhenIdentityReadFails is F2's
// regression, surfaced by the hosted review of this batch's own CP-09 fix:
// Start used to capture boundDev/boundIno and install them on ln only after
// both the installation-identity read (Coordinator().Inspect) and the epoch
// read (Coordinator().Epoch) succeeded, so a failure in either one returned
// an error without ever unlinking the socket Start had just bound,
// orphaning the file. The fix captures the identity immediately after a
// successful bind and calls unlinkOwnedSocket on both failure returns,
// before Start ever returns to its caller.
func TestListenerServiceStartCleansUpTheSocketWhenIdentityReadFails(t *testing.T) {
	dir := privateSocketDir(t)
	socketPath := filepath.Join(dir, "admin.sock")
	adminID := "60000000-0000-4000-8000-000000000001"
	uid := uint32(os.Getuid())
	cfg, err := NewConfig(socketPath, uid, map[string]uint32{adminID: uid})
	if err != nil {
		t.Fatal(err)
	}
	db := controlTestDB(t)
	// Closing the store's underlying *sql.DB before Start ever calls
	// Coordinator().Inspect forces that call's own `c.db.Begin(ctx)` to fail
	// cleanly (sql.ErrConnDone) -- reaching exactly the installation-
	// identity-read failure branch Start's error wrapping names, with no
	// test-only injection seam added to production code.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	service := NewListenerService(cfg, 0600)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = service.Start(context.Background(), runtime.Resources{WorkerContext: ctx, Writer: db, Queries: db.Queries(), Mode: runtime.Normal})
	if err == nil {
		t.Fatal("Start succeeded against a closed store, want the installation-identity-read failure")
	}
	if _, statErr := os.Stat(socketPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("socket path still exists after Start's identity-read failure: err=%v", statErr)
	}
}

// TestListenerServiceStartFailsClosedWhenPostBindIdentityCannotBeRead is
// RC-01's regression: a failed post-bind Lstat used to leave
// boundDev/boundIno at zero while Start still returned success, silently
// and permanently disabling unlinkOwnedSocket's cleanup for that
// incarnation. Start must instead fail closed: report the error, close the
// listener it just bound, and leave the on-disk pathname untouched -- an
// Lstat failure establishes nothing about what currently occupies that
// pathname, so removing it would repeat exactly the unverified-removal
// mistake unlinkOwnedSocket's own identity check exists to avoid.
func TestListenerServiceStartFailsClosedWhenPostBindIdentityCannotBeRead(t *testing.T) {
	dir := privateSocketDir(t)
	socketPath := filepath.Join(dir, "admin.sock")
	adminID := "60000000-0000-4000-8000-000000000001"
	uid := uint32(os.Getuid())
	cfg, err := NewConfig(socketPath, uid, map[string]uint32{adminID: uid})
	if err != nil {
		t.Fatal(err)
	}
	db := controlTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	service := NewListenerService(cfg, 0600)
	injectedErr := errors.New("injected lstat failure")
	service.lstatSocket = func(string, *unix.Stat_t) error { return injectedErr }

	ctx, cancel := context.WithCancel(context.Background())
	// Registered before Start, not merely before the assertions below: if
	// Start ever regresses to succeed despite the injected lstat failure --
	// exactly the condition this test exists to rule out -- the t.Fatal a
	// few lines down would Goexit past this test's own manual
	// StopAdmission/Wait calls near its end, leaking that incarnation's
	// real accept loop for the rest of the package run. StopAdmission and
	// Wait are safe to call again here even along the intended-failure
	// path below: ln.listener stays nil throughout it (Start returns
	// before ever assigning it), making both a no-op the second time.
	t.Cleanup(func() {
		service.StopAdmission()
		cancel()
		service.Wait()
	})
	err = service.Start(context.Background(), runtime.Resources{WorkerContext: ctx, Writer: db, Queries: db.Queries(), Mode: runtime.Normal})
	if err == nil {
		t.Fatal("Start succeeded despite a failed post-bind identity read")
	}
	if !errors.Is(err, injectedErr) {
		t.Fatalf("err=%v, want it to wrap the injected identity-read failure", err)
	}

	// The pathname must be left untouched, not speculatively unlinked: an
	// Lstat failure does not establish that this incarnation's own entry
	// -- rather than some other, already-replaced entry -- is what is
	// present now.
	if _, statErr := os.Stat(socketPath); statErr != nil {
		t.Fatalf("socket path was removed after an unverified identity read: %v", statErr)
	}

	// No usable server was installed and no accept loop was launched: a
	// fresh dial against the same path must not succeed through this
	// failed incarnation.
	if conn, dialErr := net.DialTimeout("unix", socketPath, 200*time.Millisecond); dialErr == nil {
		conn.Close()
		t.Fatal("dial succeeded against a listener that failed to Start")
	}

	// A subsequent StopAdmission/Wait must not hang, start work, or invent
	// a successful pathname removal.
	stopDone := make(chan error, 1)
	go func() { stopDone <- service.StopAdmission() }()
	select {
	case stopErr := <-stopDone:
		if stopErr != nil {
			t.Fatalf("StopAdmission on a failed Start returned an error: %v", stopErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("StopAdmission on a failed Start did not return")
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- service.Wait() }()
	select {
	case waitErr := <-waitDone:
		if waitErr != nil {
			t.Fatalf("Wait on a failed Start returned an error: %v", waitErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait on a failed Start did not return")
	}
	if _, statErr := os.Stat(socketPath); statErr != nil {
		t.Fatalf("socket path was removed by StopAdmission after a failed Start: %v", statErr)
	}
}

// TestListenerServiceStartSucceedsWhenPostBindIdentityIsReadable is the
// healthy control for the regression above: an uninjected, real Lstat
// still lets Start succeed exactly as before.
func TestListenerServiceStartSucceedsWhenPostBindIdentityIsReadable(t *testing.T) {
	fx := newListenerFixture(t)
	resp := dialAndRoundTrip(t, fx.socketPath, fx.serverUID, `{"jsonrpc":"2.0","id":"1","method":"server.hello","params":{"protocol":"parley-control/1"}}`)
	if resp["error"] != nil {
		t.Fatalf("%#v", resp)
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
	service := NewListenerService(cfg, 0600)
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
	_, err = conn.Read(buf)
	assertConnectionActuallyClosed(t, err)
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
	_, err = conn.Read(buf)
	assertConnectionActuallyClosed(t, err)
}

// TestWriteResponseReplacesOversizedResponseWithInternalError proves
// writeResponse never puts an over-MaxFrameBytes frame on the wire: a
// response whose encoded body would exceed the profile's own frame bound
// is replaced with a bounded InternalError before writing, preserving the
// original request's ID.
// TestListenerServicePreHelloDeadlineIsAbsoluteDespiteSlowFrame proves the
// pre-hello negotiation window is a single absolute deadline, not
// renewable by ongoing traffic. Before this fix, ReadFrame's own
// per-frame deadline (reset to now+FrameDeadline once a frame's first
// byte arrives -- frame.go) let a slow-trickled frame push the effective
// negotiation window past its documented five-second bound; a synthetic
// fixture demonstrated hello succeeding at six seconds through exactly
// this path.
func TestListenerServicePreHelloDeadlineIsAbsoluteDespiteSlowFrame(t *testing.T) {
	fx := newListenerFixture(t)
	conn, err := connection.DialTrustedServer(context.Background(), fx.socketPath, fx.serverUID)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Send only a frame's opening byte, then withhold the rest (including
	// the terminating LF) well past FrameDeadline. This keeps ReadFrame
	// itself blocked mid-frame -- exercising its own internal deadline
	// reset -- rather than exercising the already-covered "never sends a
	// single byte" case.
	if _, err := conn.Write([]byte("{")); err != nil {
		t.Fatal(err)
	}
	// A read deadline comfortably past FrameDeadline but short of what
	// the pre-fix behavior would have allowed (up to roughly
	// 2*FrameDeadline): if the fix regresses, this read times out on the
	// caller's own deadline instead of observing the server's close.
	conn.SetReadDeadline(time.Now().Add(FrameDeadline + 2*time.Second))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	assertConnectionActuallyClosed(t, err)
}

func TestWriteResponseReplacesOversizedResponseWithInternalError(t *testing.T) {
	dir := privateSocketDir(t)
	path := filepath.Join(dir, "admin.sock")
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// gotConn is unconditionally closed on every AcceptUnix outcome, success
	// or failure, so a select waiting on it never blocks forever the way a
	// prior version's `serverConn := <-accepted` could when AcceptUnix
	// failed without publishing anything at all. acceptedConn is guarded by
	// acceptedMu since the accept goroutine and this test's own goroutine
	// (via t.Cleanup, which can run concurrently with nothing else by then,
	// but still reads the same variable) both touch it.
	var acceptedMu sync.Mutex
	var acceptedConn *net.UnixConn
	gotConn := make(chan struct{})
	go func() {
		c, err := ln.AcceptUnix()
		acceptedMu.Lock()
		if err == nil {
			acceptedConn = c
		}
		acceptedMu.Unlock()
		close(gotConn)
	}()
	// Registered before any fallible assertion below (mandate: named test
	// evidence): whatever AcceptUnix eventually does, and even if it
	// finishes only after a t.Fatal elsewhere already gave up waiting, the
	// accepted connection -- if any -- is still closed here rather than
	// leaked.
	t.Cleanup(func() {
		select {
		case <-gotConn:
		case <-time.After(2 * time.Second):
			t.Error("accept goroutine never finished")
			return
		}
		acceptedMu.Lock()
		c := acceptedConn
		acceptedMu.Unlock()
		if c != nil {
			c.Close()
		}
	})

	dialed, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer dialed.Close()

	select {
	case <-gotConn:
	case <-time.After(5 * time.Second):
		t.Fatal("accept never completed")
	}
	acceptedMu.Lock()
	serverConn := acceptedConn
	acceptedMu.Unlock()
	if serverConn == nil {
		t.Fatal("accept failed")
	}

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
	select {
	case err := <-writeErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writeResponse never completed after the client read its response")
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

// TestListenerServiceHelloEpochMatchesCoordinatorEpoch is mandate R6's core
// regression: the control surface must use the owning coordinator's own
// process-local epoch, not a second, independently minted identity for the
// same server incarnation.
func TestListenerServiceHelloEpochMatchesCoordinatorEpoch(t *testing.T) {
	fx := newListenerFixture(t)
	resp := dialAndRoundTrip(t, fx.socketPath, fx.serverUID, `{"jsonrpc":"2.0","id":"1","method":"server.hello","params":{"protocol":"parley-control/1"}}`)
	result, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("%#v", resp)
	}
	helloEpoch, _ := result["server_epoch"].(string)
	if helloEpoch == "" {
		t.Fatalf("missing server_epoch: %#v", result)
	}
	coordEpoch, err := fx.db.Coordinator().Epoch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if helloEpoch != coordEpoch {
		t.Fatalf("hello server_epoch %q != coordinator epoch %q", helloEpoch, coordEpoch)
	}
}

// TestListenerServiceReceiptEpochMatchesHelloEpoch confirms a receipt
// generated through the same runtime carries that same coordinator epoch,
// not some other value -- the epoch hello reports is the epoch mutations
// are actually committed under.
func TestListenerServiceReceiptEpochMatchesHelloEpoch(t *testing.T) {
	fx := newListenerFixture(t)
	resp := dialAndRoundTrip(t, fx.socketPath, fx.serverUID, `{"jsonrpc":"2.0","id":"1","method":"server.hello","params":{"protocol":"parley-control/1"}}`)
	result := resp["result"].(map[string]any)
	helloEpoch, _ := result["server_epoch"].(string)
	if helloEpoch == "" {
		t.Fatalf("missing server_epoch: %#v", result)
	}

	req, err := store.NewCommandRequest("binding.register", "60000000-0000-4000-8000-000000000050")
	if err != nil {
		t.Fatal(err)
	}
	principal := store.CommandPrincipal{ID: fx.adminID, ConnectorUID: fx.serverUID}
	receipt, err := fx.db.Coordinator().Execute(context.Background(), principal, req, allowedCommand, insertSyntheticCommand, nil)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.View.Epoch != helloEpoch {
		t.Fatalf("receipt epoch %q != hello epoch %q", receipt.View.Epoch, helloEpoch)
	}
}

// TestListenerServiceBoundsMutationByRequestDeadline is review 5257748895
// (comment 4054786967): serveSession used to hand the long-lived runtime
// worker context straight to Session.Handle, so a membership mutation stuck
// behind the coordinator gate waited for as long as the gate stayed held.
// The gate is held here by a real, deliberately blocked Transition; the
// mutation must come back as temporarily_unavailable once RequestDeadline
// passes, while the gate is still held, and must not have committed.
func TestListenerServiceBoundsMutationByRequestDeadline(t *testing.T) {
	fx := newListenerFixture(t)
	entered, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := fx.db.Coordinator().Transition(context.Background(), func(context.Context, *sql.Tx, store.CommitView) (store.TransitionResult, error) {
			close(entered)
			<-release
			return store.TransitionResult{}, nil
		}, nil)
		held <- err
	}()
	<-entered
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)

	conn, err := connection.DialTrustedServer(context.Background(), fx.socketPath, fx.serverUID)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(RequestDeadline + restartTeardownWait)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	roundTrip := func(line string) map[string]any {
		t.Helper()
		if _, err := conn.Write([]byte(line + "\n")); err != nil {
			t.Fatal(err)
		}
		reply, err := br.ReadBytes('\n')
		if err != nil {
			t.Fatalf("no bounded response while the gate was held: %v", err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(reply, &decoded); err != nil {
			t.Fatal(err)
		}
		return decoded
	}
	if hello := roundTrip(`{"jsonrpc":"2.0","id":"1","method":"server.hello","params":{"protocol":"parley-control/1"}}`); hello["error"] != nil {
		t.Fatalf("hello failed: %#v", hello)
	}

	// Two mutations pipelined in one write (review 5259563170, comment
	// 4056153933): the second waits behind the first, and that wait counts
	// toward its own deadline, so both are refused at about RequestDeadline
	// from arrival -- not the second a further RequestDeadline later. The
	// gate wait honors the request context, so these are the coordinator's
	// own provable non-commitments (temporarily_unavailable), delivered
	// handlerContextLead before the deadline; a third, context-free request
	// behind them therefore still has budget when dispatched and is answered
	// normally. Its refusal once a context-ignoring handler used up the whole
	// deadline is TestListenerServiceAnswersUncertainWhileRecoveryPreparationBlocks.
	started := time.Now()
	if _, err := conn.Write([]byte(`{"jsonrpc":"2.0","id":"2","method":"membership.revoke","params":{"operation_id":"80000000-0000-4000-8000-00000000f001","conversation":"conv-deadline","expected_grant_version":"1"}}` + "\n" +
		`{"jsonrpc":"2.0","id":"3","method":"membership.revoke","params":{"operation_id":"80000000-0000-4000-8000-00000000f002","conversation":"conv-deadline","expected_grant_version":"1"}}` + "\n" +
		`{"jsonrpc":"2.0","id":"4","method":"no.such.method","params":{}}` + "\n")); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"2", "3", "4"} {
		reply, err := br.ReadBytes('\n')
		if err != nil {
			t.Fatalf("no bounded response for request %s while the gate was held: %v", id, err)
		}
		elapsed := time.Since(started)
		var resp map[string]any
		if err := json.Unmarshal(reply, &resp); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-held:
			t.Fatalf("the gate was released before the response arrived (err=%v); the deadline was not what bounded it", err)
		default:
		}
		wireErr, _ := resp["error"].(map[string]any)
		data, _ := wireErr["data"].(map[string]any)
		if id == "4" {
			if resp["id"] != id || wireErr["code"] != float64(MethodNotFound) {
				t.Fatalf("expected method-not-found for request %s (it still had budget when dispatched), got %#v", id, resp)
			}
		} else if resp["id"] != id || data["code"] != string(store.TemporarilyUnavailable) {
			t.Fatalf("expected temporarily_unavailable for request %s at the request deadline, got %#v", id, resp)
		}
		if elapsed < RequestDeadline-time.Second || elapsed > RequestDeadline+3*time.Second {
			t.Fatalf("response %s after %v, want about RequestDeadline (%v) from arrival", id, elapsed, RequestDeadline)
		}
	}

	releaseOnce()
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	var count int
	if err := fx.db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT count(*) FROM operation_results WHERE operation_id='80000000-0000-4000-8000-00000000f001'").Scan(&count)
	}); err != nil || count != 0 {
		t.Fatalf("a deadline-refused mutation must not have recorded a result: count=%d err=%v", count, err)
	}
}

// TestListenerServiceClosesOnQueueOverflowAndReusedCorrelationID holds the
// coordinator gate so a pipelined mutation stays executing, then proves the
// bounds the per-socket queue enforces before any deadline reply: a
// correlation ID reused while outstanding, and one frame more than
// MaxExecutingPerSocket + MaxQueuedPerSocket -- whatever the frames are
// (review 5264559000, comment 4060461953: ID-less objects and unechoable
// violations occupy capacity like requests) -- each close the socket instead
// of executing. The boundary cases prove the bound itself is admitted, served
// once the gate frees, and its capacity reusable afterwards.
func TestListenerServiceClosesOnQueueOverflowAndReusedCorrelationID(t *testing.T) {
	revoke := func(id string, op int) string {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":"membership.revoke","params":{"operation_id":"80000000-0000-4000-8000-0000000000%02d","conversation":"conv-queue","expected_grant_version":"1"}}`+"\n", id, op)
	}
	const queued = MaxQueuedPerSocket
	// Every burst starts with one executing revoke (id "2"); the rest follow it.
	behind := func(kinds []frameKind) string { return revoke("2", 1) + burstOf(kinds, 3) }
	mixed := append(repeatKind(validFrame, queued/2), repeatKind(unechoableFrame, queued-queued/2)...)
	closing := map[string]string{
		"reused correlation id": revoke("2", 1) + revoke("2", 2),
		// Review 5259780995 (comment 4056295482): a malformed envelope whose
		// ID can be echoed would be answered under that ID, so it counts as
		// a reuse too (positional params are an envelope violation).
		"violation reusing an outstanding id": revoke("2", 1) + `{"jsonrpc":"2.0","id":"2","method":"membership.revoke","params":[]}` + "\n",
		"queue overflow":                      behind(repeatKind(validFrame, queued+1)),
		"queue overflow of unechoable frames": behind(repeatKind(unechoableFrame, queued+1)),
		"queue overflow by an id-less frame":  behind(append(mixed, idlessFrame)),
	}
	open := func(t *testing.T) (*listenerFixture, net.Conn, *bufio.Reader, func()) {
		t.Helper()
		fx := newListenerFixture(t)
		release, _ := holdCoordinator(t, fx.db, nil)
		conn, err := connection.DialTrustedServer(context.Background(), fx.socketPath, fx.serverUID)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		// Shorter than RequestDeadline: a close must come from the queue
		// bound, not from the held mutation timing out first.
		if err := conn.SetDeadline(time.Now().Add(RequestDeadline - 2*time.Second)); err != nil {
			t.Fatal(err)
		}
		br := bufio.NewReader(conn)
		if _, err := conn.Write([]byte(`{"jsonrpc":"2.0","id":"1","method":"server.hello","params":{"protocol":"parley-control/1"}}` + "\n")); err != nil {
			t.Fatal(err)
		}
		if _, err := br.ReadBytes('\n'); err != nil {
			t.Fatalf("hello: %v", err)
		}
		return fx, conn, br, release
	}
	for name, burst := range closing {
		t.Run(name, func(t *testing.T) {
			_, conn, br, _ := open(t)
			if _, err := conn.Write([]byte(burst)); err != nil {
				t.Fatal(err)
			}
			_, err := br.ReadBytes('\n')
			assertConnectionActuallyClosed(t, err)
		})
	}
	boundary := map[string][]frameKind{
		"the bound of requests":          repeatKind(validFrame, queued),
		"the bound of unechoable frames": repeatKind(unechoableFrame, queued),
		"a mixed bound":                  mixed,
	}
	for name, kinds := range boundary {
		t.Run(name+" is admitted, served and its capacity reusable", func(t *testing.T) {
			_, conn, br, release := open(t)
			if _, err := conn.Write([]byte(behind(kinds))); err != nil {
				t.Fatal(err)
			}
			release()
			if err := conn.SetDeadline(time.Now().Add(restartTeardownWait)); err != nil {
				t.Fatal(err)
			}
			// The executing revoke answers first (a domain rejection: no
			// grant exists), then every frame behind it in order.
			if resp := readReply(t, br); resp["id"] != "2" || resp["error"] == nil {
				t.Fatalf("expected the executing revoke's rejection first, got %#v", resp)
			}
			expectRepliesInOrder(t, br, kinds, 3)
			again := append(repeatKind(validFrame, MaxExecutingPerSocket), kinds...)
			if _, err := conn.Write([]byte(burstOf(again, 20))); err != nil {
				t.Fatal(err)
			}
			expectRepliesInOrder(t, br, again, 20)
		})
	}
}

// restartTeardownWait bounds every finite wait used by serveIncarnation
// and its regression below, so a stuck accept loop cannot hang either the
// test or its own cleanup indefinitely.
const restartTeardownWait = 5 * time.Second

// teardownOutcome distinguishes a teardown that genuinely completed --
// observed the accept loop actually stop within its bound, whether or not
// the service itself then reported an error -- from one that did not
// observe completion at all. Only a completed teardown may safely close
// the store or let a caller proceed to open the next incarnation of the
// same on-disk file; a completed-with-error teardown still safely released
// its resources but is not a successful restart precondition (mandate
// TC-R1: these are two different things, not one boolean).
type teardownOutcome struct {
	completed bool
	err       error
	stage     string // "stop_admission" | "wait_timeout" | "wait_error" | "close" | "" (clean)
}

// serveIncarnation bundles one restart incarnation's store/listener
// lifetime and guarantees its stop/close sequence runs exactly once,
// whether invoked explicitly (the normal restart path, before the next
// incarnation opens the same on-disk file) or as a t.Cleanup fallback if a
// fallible operation between resource acquisition and that explicit
// teardown fails first. Register teardown via t.Cleanup as soon as the
// struct exists, before acquiring any resource, so a failure at any later
// point -- not only after the explicit teardown -- still stops admission,
// cancels, waits for the accept loop to actually exit, and closes the
// store, instead of leaking them past the test (mandate T1 companion).
//
// waitTimeout overrides restartTeardownWait when nonzero, letting a test
// force a deterministic, genuine (not raced) non-completion -- see
// TestServeIncarnationTeardownRetainsResourcesWhenServiceDoesNotStopInTime
// -- without an injected fake service.
type serveIncarnation struct {
	once        sync.Once
	db          *store.DB
	svc         *Listener
	cancel      context.CancelFunc
	waitTimeout time.Duration
	outcome     teardownOutcome
}

// teardownCore performs the actual stop/cancel/wait/close sequence and
// computes the outcome without touching a *testing.T. It is the single
// place the sequence and its ordering are implemented; teardown (below)
// wraps it for the two real restart tests, reporting any problem through
// t.Errorf, while
// TestServeIncarnationTeardownRetainsResourcesWhenServiceDoesNotStopInTime
// calls it directly so that deliberately forcing the timeout branch does
// not itself fail that otherwise-passing regression -- a *testing.T
// subtest's failure unconditionally propagates to and fails its parent
// regardless of what the parent asserts afterward, making "expect this to
// report non-completion, then still pass" unrepresentable through t.Run
// (mandate TC-R1).
//
// On a service-wait timeout, it deliberately does NOT close the store --
// a service that has not observably stopped may still be using it -- and
// reports completed=false so a caller must neither proceed to the next
// incarnation nor let a fixture-removal cleanup race a possibly-still-live
// listener or store.
func (s *serveIncarnation) teardownCore() teardownOutcome {
	var outcome teardownOutcome
	wait := s.waitTimeout
	if wait <= 0 {
		wait = restartTeardownWait
	}
	outcome.completed = true
	if s.svc != nil {
		if err := s.svc.StopAdmission(); err != nil {
			outcome.err = err
			outcome.stage = "stop_admission"
		}
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.svc != nil {
		done := make(chan error, 1)
		go func() { done <- s.svc.Wait() }()
		select {
		case err := <-done:
			if err != nil && outcome.err == nil {
				outcome.err = err
				outcome.stage = "wait_error"
			}
		case <-time.After(wait):
			outcome.completed = false
			outcome.stage = "wait_timeout"
			outcome.err = fmt.Errorf("listener did not stop accepting within %s", wait)
			return outcome
		}
	}
	if s.db != nil {
		if err := s.db.Close(); err != nil && outcome.err == nil {
			outcome.err = err
			outcome.stage = "close"
		}
	}
	return outcome
}

// teardown runs teardownCore exactly once (sync.Once), safe to call both
// explicitly (the normal restart path, before the next incarnation opens
// the same on-disk file) and as a t.Cleanup fallback without double-
// closing the listener, re-running the sequence, or manufacturing a
// cleanup error on a healthy restart; nil fields (an early failure before
// a later resource was acquired) are skipped inside teardownCore. Any
// problem -- including a service-wait timeout -- is reported via
// t.Errorf, tagged with its stage, so a real regression surfaces as a
// failing test (mandate T1 companion / TC-R1). It returns the same
// teardownOutcome on every call, computed only once.
func (s *serveIncarnation) teardown(t *testing.T) teardownOutcome {
	t.Helper()
	s.once.Do(func() {
		s.outcome = s.teardownCore()
		if s.outcome.err != nil {
			t.Errorf("teardown (%s): %v", s.outcome.stage, s.outcome.err)
		}
	})
	return s.outcome
}

// retainOnIncompleteTeardown calls inc.teardown and, when it did not
// observe completion, marks *retain so a paired privateSocketDirRetainable
// keeps the fixture directory instead of racing its removal against a
// possibly-still-live listener or open store (mandate TC-R1). Idempotent
// like teardown itself -- safe to call from both an explicit call site and
// its t.Cleanup fallback.
func retainOnIncompleteTeardown(t *testing.T, inc *serveIncarnation, retain *bool) teardownOutcome {
	t.Helper()
	outcome := inc.teardown(t)
	if !outcome.completed {
		*retain = true
	}
	return outcome
}

// privateSocketDirRetainable behaves exactly like privateSocketDir, except
// its removal is skipped when *retain is true at cleanup time -- set by
// retainOnIncompleteTeardown when a serveIncarnation's teardown did not
// observe the service actually stop within its bound (mandate TC-R1).
func privateSocketDirRetainable(t *testing.T, retain *bool) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "parley-control-listener-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if *retain {
			t.Logf("retaining %s: a service teardown did not confirm shutdown within its wait", dir)
			return
		}
		os.RemoveAll(dir)
	})
	return dir
}

// TestListenerRestartMintsNewEpochButPreservesServerIDAndHistory exercises
// an actual restart: reopening the same on-disk database and starting a
// fresh Listener/coordinator produces a new epoch (mandate R6: "restarting
// creates a new epoch"), while server_id and previously committed receipt
// history are preserved untouched -- a historical receipt's commit_epoch
// legitimately differs from today's hello and must never be rewritten or
// rejected merely for that difference.
func TestListenerRestartMintsNewEpochButPreservesServerIDAndHistory(t *testing.T) {
	ctx := context.Background()
	var retainFixtures bool
	dir := privateSocketDirRetainable(t, &retainFixtures)
	dbPath := filepath.Join(dir, "parley.db")
	adminID := "60000000-0000-4000-8000-000000000001"
	uid := uint32(os.Getuid())
	req, err := store.NewCommandRequest("binding.register", "60000000-0000-4000-8000-000000000060")
	if err != nil {
		t.Fatal(err)
	}
	principal := store.CommandPrincipal{ID: adminID, ConnectorUID: uid}

	// First incarnation: init, serve, capture hello's epoch/server_id and a
	// real committed receipt. inc1's teardown is registered before any
	// resource is acquired, so a failure at any point below -- not only
	// after the explicit teardown further down -- still tears it down.
	inc1 := &serveIncarnation{}
	t.Cleanup(func() { retainOnIncompleteTeardown(t, inc1, &retainFixtures) })

	db1, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	inc1.db = db1
	if err := db1.OpenReaders(ctx); err != nil {
		t.Fatal(err)
	}
	socketPath1 := filepath.Join(dir, "admin1.sock")
	cfg1, err := NewConfig(socketPath1, uid, map[string]uint32{adminID: uid})
	if err != nil {
		t.Fatal(err)
	}
	svc1 := NewListenerService(cfg1, 0600)
	wctx1, cancel1 := context.WithCancel(ctx)
	inc1.svc = svc1
	inc1.cancel = cancel1
	if err := svc1.Start(ctx, runtime.Resources{WorkerContext: wctx1, Writer: db1, Queries: db1.Queries(), Mode: runtime.Normal}); err != nil {
		t.Fatal(err)
	}

	resp1 := dialAndRoundTrip(t, socketPath1, uid, `{"jsonrpc":"2.0","id":"1","method":"server.hello","params":{"protocol":"parley-control/1"}}`)
	result1 := resp1["result"].(map[string]any)
	serverID1, _ := result1["server_id"].(string)
	epoch1, _ := result1["server_epoch"].(string)
	if epoch1 == "" || serverID1 == "" {
		t.Fatalf("missing epoch/server_id: %#v", result1)
	}

	receipt1, err := db1.Coordinator().Execute(ctx, principal, req, allowedCommand, insertSyntheticCommand, nil)
	if err != nil {
		t.Fatal(err)
	}
	if receipt1.View.Epoch != epoch1 {
		t.Fatalf("receipt epoch %q != hello epoch %q", receipt1.View.Epoch, epoch1)
	}

	// Explicit normal teardown of the first incarnation, before the second
	// one opens the same on-disk file. inc1.teardown is idempotent
	// (sync.Once), so the t.Cleanup fallback registered above becomes a
	// no-op rather than double-closing anything. A restart against the
	// same on-disk file is only safe once the first incarnation has
	// genuinely, cleanly stopped -- not merely once the test's own failed
	// flag was set (mandate TC-R1): a timed-out or service-error teardown
	// must stop this test here, before store.OpenExisting ever runs.
	if outcome := retainOnIncompleteTeardown(t, inc1, &retainFixtures); !outcome.completed || outcome.err != nil {
		t.Fatalf("first incarnation did not shut down cleanly before restart: completed=%v err=%v", outcome.completed, outcome.err)
	}

	// Second incarnation: a real restart against the same on-disk file.
	// Same registration discipline as inc1.
	inc2 := &serveIncarnation{}
	t.Cleanup(func() { retainOnIncompleteTeardown(t, inc2, &retainFixtures) })

	db2, err := store.OpenExisting(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	inc2.db = db2
	if err := db2.OpenReaders(ctx); err != nil {
		t.Fatal(err)
	}
	socketPath2 := filepath.Join(dir, "admin2.sock")
	cfg2, err := NewConfig(socketPath2, uid, map[string]uint32{adminID: uid})
	if err != nil {
		t.Fatal(err)
	}
	svc2 := NewListenerService(cfg2, 0600)
	wctx2, cancel2 := context.WithCancel(ctx)
	inc2.svc = svc2
	inc2.cancel = cancel2
	if err := svc2.Start(ctx, runtime.Resources{WorkerContext: wctx2, Writer: db2, Queries: db2.Queries(), Mode: runtime.Normal}); err != nil {
		t.Fatal(err)
	}

	resp2 := dialAndRoundTrip(t, socketPath2, uid, `{"jsonrpc":"2.0","id":"1","method":"server.hello","params":{"protocol":"parley-control/1"}}`)
	result2 := resp2["result"].(map[string]any)
	epoch2, _ := result2["server_epoch"].(string)
	serverID2, _ := result2["server_id"].(string)
	if epoch2 == "" || serverID2 == "" {
		t.Fatalf("missing epoch/server_id: %#v", result2)
	}

	if epoch2 == epoch1 {
		t.Fatal("restart did not mint a new epoch")
	}
	if serverID2 != serverID1 {
		t.Fatalf("server_id changed across restart: %q -> %q", serverID1, serverID2)
	}

	// Replaying the same operation through the new coordinator must return
	// the original, historical receipt untouched -- proving history is
	// preserved and not rewritten or rejected merely because its
	// commit_epoch differs from today's hello.
	replay, err := db2.Coordinator().Execute(ctx, principal, req, allowedCommand, insertSyntheticCommand, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replayed || replay.AuditID != receipt1.AuditID || replay.View.Epoch != epoch1 {
		t.Fatalf("historical receipt not preserved: replayed=%v auditID=%q (want %q) epoch=%q (want %q)",
			replay.Replayed, replay.AuditID, receipt1.AuditID, replay.View.Epoch, epoch1)
	}
}

// TestListenerRestartFirstIncarnationCleansUpOnEarlyReturnAfterGenuineStart
// is the restart test's T1-companion regression. After synchronizing on a
// genuinely started first incarnation (a real authenticated server.hello
// round trip succeeds, not merely a raw dial), it deliberately returns
// early -- no explicit StopAdmission/cancel/Wait/Close -- relying solely
// on serveIncarnation's t.Cleanup fallback.
//
// Registration order controls the property under test: privateSocketDirRetainable's
// own directory-removal cleanup is registered first (so it fires *last*),
// the post-teardown verification below is registered second (so it fires
// *second*), and inc's fallback teardown is registered last (so it fires
// *first*) -- meaning fixture removal never races a still-live listener or
// open store, and the verification below only ever runs after teardown has
// already completed (mandate T1 companion: "verify its service stopped
// and DB resources settled before fixture removal"). If that teardown
// itself does not observe completion in time, retainOnIncompleteTeardown
// marks the directory for retention instead of letting it be removed out
// from under a possibly-still-live listener or store (mandate TC-R1).
func TestListenerRestartFirstIncarnationCleansUpOnEarlyReturnAfterGenuineStart(t *testing.T) {
	ctx := context.Background()
	var retainFixtures bool
	dir := privateSocketDirRetainable(t, &retainFixtures)
	dbPath := filepath.Join(dir, "parley.db")
	adminID := "60000000-0000-4000-8000-000000000002"
	uid := uint32(os.Getuid())
	socketPath := filepath.Join(dir, "admin.sock")

	var svc *Listener
	// Registered before inc's fallback teardown (so LIFO runs it *after*
	// teardown has already completed): proves the listener was actually
	// closed -- a fresh dial must fail -- and that the accept loop
	// actually exited, the same evidentiary shape as
	// TestServeCleanupWaitsForCompletionEvenOnEarlyReturn in cmd/parleyd.
	t.Cleanup(func() {
		if svc == nil {
			return // never got far enough to start; nothing to verify
		}
		if _, dialErr := connection.DialTrustedServer(context.Background(), socketPath, uid); dialErr == nil {
			t.Error("socket still accepts connections after the wait-for-completion cleanup returned")
		}
		done := make(chan error, 1)
		go func() { done <- svc.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("listener accept loop reported failure: %v", err)
			}
		case <-time.After(restartTeardownWait):
			t.Error("accept loop did not exit promptly after teardown")
		}
	})

	inc := &serveIncarnation{}
	t.Cleanup(func() { retainOnIncompleteTeardown(t, inc, &retainFixtures) })

	db, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	inc.db = db
	if err := db.OpenReaders(ctx); err != nil {
		t.Fatal(err)
	}
	cfg, err := NewConfig(socketPath, uid, map[string]uint32{adminID: uid})
	if err != nil {
		t.Fatal(err)
	}
	svc = NewListenerService(cfg, 0600)
	wctx, cancel := context.WithCancel(ctx)
	inc.svc = svc
	inc.cancel = cancel
	if err := svc.Start(ctx, runtime.Resources{WorkerContext: wctx, Writer: db, Queries: db.Queries(), Mode: runtime.Normal}); err != nil {
		t.Fatal(err)
	}

	// Synchronize on actual readiness -- a full authenticated hello round
	// trip, not merely a raw dial -- before deliberately taking the
	// early-return path below.
	resp := dialAndRoundTrip(t, socketPath, uid, `{"jsonrpc":"2.0","id":"1","method":"server.hello","params":{"protocol":"parley-control/1"}}`)
	if resp["error"] != nil {
		t.Fatalf("hello failed on a genuinely started incarnation: %#v", resp)
	}

	// Deliberately nothing else here: no explicit StopAdmission/cancel/
	// Wait/Close. Reaching the end of the test function having only
	// confirmed startup above is the "early exit after a genuinely started
	// first incarnation" this regression demonstrates recovers cleanly.
}

// probeOwner owns every resource that
// TestServeIncarnationTeardownRetainsResourcesWhenServiceDoesNotStopInTime
// acquires, tracking exactly what has actually been created so far so an
// early failure at any acquisition step still releases only what exists
// (mandate PC-F2's "early resource registration": a probe owner registered
// once, over zero-value/nil fields, before the first fallible acquisition,
// not one built and registered only after several of them already
// succeeded). It is deliberately independent of inc's own borrowed-resource
// teardownCore call below: release stops admission and observes completion
// itself rather than assuming teardownCore already did so, since an early
// exit before that call would otherwise leave the accept loop permanently
// blocked in Accept with nothing left to unblock it -- cancelling the
// worker context alone does not close the listening socket, only
// StopAdmission does.
type probeOwner struct {
	db     *store.DB
	svc    *Listener
	cancel context.CancelFunc
	idle   net.Conn
}

// release independently stops admission -- tolerating an already-performed
// stop of this exact listener (a repeated net.Listener.Close() returns a
// wrapped net.ErrClosed, not a genuine new failure, and StopAdmission
// itself is documented safe even before a listener ever bound) -- releases
// the idle connection, invokes the cancellation withheld from the borrowed
// incarnation under test, and observes Wait within its bound before ever
// closing the database. A Wait that does not complete within the bound
// returns before the database is closed or any retention decision is made,
// applying the same stop-before-close rule used for the borrowed
// incarnation to this probe's own real resources; a completed Wait that
// itself reports a service error still safely proceeds through ordered
// close, with that error preserved and reported.
func (o *probeOwner) release() error {
	var stopErr error
	if o.svc != nil {
		if err := o.svc.StopAdmission(); err != nil && !errors.Is(err, net.ErrClosed) {
			stopErr = fmt.Errorf("stop admission: %w", err)
		}
	}
	if o.idle != nil {
		o.idle.Close()
	}
	if o.cancel != nil {
		o.cancel()
	}
	var waitErr error
	if o.svc != nil {
		done := make(chan error, 1)
		go func() { done <- o.svc.Wait() }()
		select {
		case waitErr = <-done:
		case <-time.After(restartTeardownWait):
			return fmt.Errorf("listener did not actually stop within %s", restartTeardownWait)
		}
	}
	var closeErr error
	if o.db != nil {
		closeErr = o.db.Close()
	}
	return errors.Join(stopErr, waitErr, closeErr)
}

// TestServeIncarnationTeardownRetainsResourcesWhenServiceDoesNotStopInTime
// is TC-R1's core regression: it forces serveIncarnation.teardown's bounded
// wait to observe a genuinely not-yet-stopped service, using a real
// connected-but-silent peer whose own several-second pre-hello deadline
// (FrameDeadline) has not yet elapsed -- never an injected fake -- against
// a deliberately short waitTimeout. It then proves the required gate: the
// store is not closed and the fixture directory is marked for retention.
//
// The worker-context cancel func is deliberately withheld from inc (left
// nil): serveSession's ctx.Done() watcher closes any connection the
// instant its context is cancelled, which would make the wait complete
// almost immediately and defeat this probe. The real cancellation
// capability, along with every other resource, is instead owned by a
// probeOwner (above), released from a t.Cleanup registered before this
// function acquires anything, so an early Fatal at any point still
// releases exactly what was actually acquired instead of stranding it
// (mandate TC-R1 focused verification #3; mandate PC-F2). That release
// only clears the retention flag once it has observed every owned resource
// genuinely settle -- a successful run must not permanently retain the
// fixture just because retention was required while the forced wait was
// still outstanding.
func TestServeIncarnationTeardownRetainsResourcesWhenServiceDoesNotStopInTime(t *testing.T) {
	ctx := context.Background()
	var retainFixtures bool
	dir := privateSocketDirRetainable(t, &retainFixtures)
	dbPath := filepath.Join(dir, "parley.db")
	adminID := "60000000-0000-4000-8000-000000000004"
	uid := uint32(os.Getuid())
	socketPath := filepath.Join(dir, "admin.sock")

	owner := &probeOwner{}
	t.Cleanup(func() {
		if err := owner.release(); err != nil {
			retainFixtures = true
			t.Errorf("probe cleanup did not settle cleanly, retaining %s: %v", dir, err)
			return
		}
		retainFixtures = false
	})

	db, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	owner.db = db
	if err := db.OpenReaders(ctx); err != nil {
		t.Fatal(err)
	}
	cfg, err := NewConfig(socketPath, uid, map[string]uint32{adminID: uid})
	if err != nil {
		t.Fatal(err)
	}
	svc := NewListenerService(cfg, 0600)
	owner.svc = svc
	wctx, cancel := context.WithCancel(ctx)
	owner.cancel = cancel
	if err := svc.Start(ctx, runtime.Resources{WorkerContext: wctx, Writer: db, Queries: db.Queries(), Mode: runtime.Normal}); err != nil {
		t.Fatal(err)
	}

	// A connected but silent peer: accepted (kernel-authenticated) and
	// occupying one of the listener's per-connection goroutines, but not
	// yet past its own pre-hello deadline -- keeping the listener's accept
	// loop from fully stopping for several seconds, comfortably longer
	// than waitTimeout below.
	idle, err := connection.DialTrustedServer(ctx, socketPath, uid)
	if err != nil {
		t.Fatal(err)
	}
	owner.idle = idle

	// A successful Dial only proves the kernel accepted the connection
	// into its backlog, not that the server's own accept loop has called
	// Accept() and spawned this connection's serving goroutine yet.
	// Without this synchronization, teardown below can race ahead of that
	// and observe a wg count of zero (only the accept loop's own count),
	// making it complete immediately instead of genuinely timing out --
	// acquireSocketSlot is incremented synchronously, in program order,
	// strictly before that goroutine is spawned, so waiting for it here
	// is a solid proxy with no separate exported hook needed.
	deadline := time.Now().Add(2 * time.Second)
	for svc.socketCountForTest() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("server never registered the idle connection as accepted")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// teardownCore is called directly, not teardown/retainOnIncompleteTeardown:
	// this deliberately forces the timeout branch, and teardown's own
	// t.Errorf on that branch would otherwise unconditionally fail this
	// test regardless of the assertions below (see teardownCore's doc
	// comment). The retention gate itself (retainOnIncompleteTeardown) is
	// exercised faithfully by applying its exact rule to the outcome here.
	inc := &serveIncarnation{db: db, svc: svc, waitTimeout: 500 * time.Millisecond}
	outcome := inc.teardownCore()
	if outcome.completed {
		t.Fatal("expected teardown to observe non-completion while a connection is still being served")
	}
	if outcome.stage != "wait_timeout" {
		t.Fatalf("expected the wait_timeout stage, got %q (err: %v)", outcome.stage, outcome.err)
	}
	if !outcome.completed {
		retainFixtures = true
	}
	if !retainFixtures {
		t.Fatal("expected the fixture directory to be marked for retention")
	}
	// The store must not have been closed while the service may still be
	// using it: a fresh read through it must still succeed.
	if _, err := db.Coordinator().Epoch(ctx); err != nil {
		t.Fatalf("store appears closed after a timed-out teardown: %v", err)
	}

	// Releasing and joining the probe's own controlled work, and the
	// resulting retention decision, happen in the registered t.Cleanup
	// above -- run on every exit path, not only this one.
}
