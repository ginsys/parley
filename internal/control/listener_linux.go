//go:build linux

package control

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/runtime"
	"golang.org/x/sys/unix"
)

// WriteDeadline bounds one response write; a stalled client is
// disconnected rather than allowed to hold a goroutine or database
// resources indefinitely.
const WriteDeadline = 5 * time.Second

// staleSocketProbeTimeout bounds the connect attempt used to prove an
// existing socket abandoned. Any outcome other than a definite connection
// refusal within this window is treated as "not provably abandoned."
const staleSocketProbeTimeout = 2 * time.Second

var (
	errUnexpectedSocketEntry  = errors.New("control: an unexpected file exists at admin_socket")
	errSocketNotProvablyStale = errors.New("control: existing admin_socket is not provably abandoned; refusing to replace it")
)

// Listen binds cfg.AdminSocket, after validating every ancestor directory
// is trusted (see connection.TrustedDirectory) and, if an entry already
// exists at that path, that it is a stale socket left by a crashed prior
// instance -- proven only by a bounded connect attempt returning a
// definite connection refusal. A live socket, a non-socket entry, an
// unexpected owner, a timeout or any other probe outcome fails startup
// instead of replacing it. mode is the socket file's permission bits
// (0600, or 0660 with an explicitly provisioned administrator-only group
// -- provisioning that group is the caller's responsibility).
func Listen(cfg Config, mode os.FileMode) (net.Listener, error) {
	dir := filepath.Dir(cfg.AdminSocket)
	name := filepath.Base(cfg.AdminSocket)
	parent, err := connection.TrustedDirectory(dir, cfg.ServerUID)
	if err != nil {
		return nil, err
	}
	defer unix.Close(parent)

	if err := prepareSocketPath(parent, name, cfg.ServerUID); err != nil {
		return nil, err
	}

	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	addr := fmt.Sprintf("/proc/self/fd/%d/%s", parent, name)
	if err := unix.Bind(fd, &unix.SockaddrUnix{Name: addr}); err != nil {
		unix.Close(fd)
		return nil, err
	}
	if err := unix.Fchmodat(parent, name, uint32(mode), 0); err != nil {
		unix.Close(fd)
		unix.Unlinkat(parent, name, 0)
		return nil, err
	}
	if err := unix.Listen(fd, 128); err != nil {
		unix.Close(fd)
		unix.Unlinkat(parent, name, 0)
		return nil, err
	}
	f := os.NewFile(uintptr(fd), cfg.AdminSocket)
	listener, err := net.FileListener(f)
	f.Close() // net.FileListener dup's the fd; this copy is no longer needed
	if err != nil {
		unix.Unlinkat(parent, name, 0)
		return nil, err
	}
	return listener, nil
}

// prepareSocketPath ensures name does not already exist under parent,
// unlinking it first only when it is provably an abandoned socket from a
// crashed prior server instance.
func prepareSocketPath(parent int, name string, serverUID uint32) error {
	var st unix.Stat_t
	err := unix.Fstatat(parent, name, &st, unix.AT_SYMLINK_NOFOLLOW)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil
		}
		return err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFSOCK || st.Uid != serverUID {
		return errUnexpectedSocketEntry
	}
	refused, err := probeConnectionRefused(parent, name)
	if err != nil {
		return err
	}
	if !refused {
		return errSocketNotProvablyStale
	}
	return unix.Unlinkat(parent, name, 0)
}

// probeConnectionRefused reports true only when connecting to the
// existing socket returns a definite ECONNREFUSED -- proof no listener is
// bound to it. A successful connect, a timeout, a permission failure or
// any other outcome is not proof of abandonment.
func probeConnectionRefused(parent int, name string) (bool, error) {
	addr := fmt.Sprintf("/proc/self/fd/%d/%s", parent, name)
	conn, err := net.DialTimeout("unix", addr, staleSocketProbeTimeout)
	if err == nil {
		conn.Close()
		return false, nil
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true, nil
	}
	return false, nil
}

// Listener implements runtime.Service over an already-bound net.Listener
// (see Listen). It authenticates each accepted connection via
// connection.PeerUID against the server's configured administrators
// (never a client-supplied identity), enforces the per-administrator and
// total socket caps, and serves each session strictly sequentially: read
// one frame, dispatch it, write its response, then read the next. This
// trivially satisfies the profile's "one executing plus at most eight
// queued requests per socket" bound -- there is never more than one
// request in flight per socket -- at the cost of not implementing
// per-socket request pipelining in PR1.
type Listener struct {
	listener net.Listener
	cfg      Config
	serverID string
	epoch    string

	mu           sync.Mutex
	server       *Server
	stopped      bool
	totalSockets int
	perAdmin     map[string]int
	wg           sync.WaitGroup
}

// NewListenerService wraps l for runtime.Start's Registration.Service.
// serverID and epoch are resolved by the caller (cmd/parleyd) before
// construction: serverID from the installation row, epoch minted once per
// process start.
func NewListenerService(l net.Listener, cfg Config, serverID, epoch string) *Listener {
	return &Listener{listener: l, cfg: cfg, serverID: serverID, epoch: epoch, perAdmin: make(map[string]int)}
}

func (ln *Listener) Start(_ context.Context, res runtime.Resources) error {
	state := StateRunning
	if res.Mode == runtime.Held {
		state = StateRecoveryOnly
	}
	ln.mu.Lock()
	ln.server = NewServer(ln.cfg, res.Queries, ln.serverID, ln.epoch, state)
	ln.mu.Unlock()
	ln.wg.Add(1)
	go ln.acceptLoop(res.WorkerContext)
	return nil
}

func (ln *Listener) StopAdmission() error {
	ln.mu.Lock()
	ln.stopped = true
	ln.mu.Unlock()
	return ln.listener.Close()
}

func (ln *Listener) Wait() error {
	ln.wg.Wait()
	return nil
}

func (ln *Listener) acceptLoop(ctx context.Context) {
	defer ln.wg.Done()
	for {
		conn, err := ln.listener.Accept()
		if err != nil {
			return // listener closed by StopAdmission
		}
		uconn, ok := conn.(*net.UnixConn)
		if !ok {
			conn.Close()
			continue
		}
		uid, err := connection.PeerUID(uconn)
		if err != nil {
			uconn.Close()
			continue
		}
		ln.mu.Lock()
		identity, ok := ln.server.IdentifyPeer(uid)
		ln.mu.Unlock()
		if !ok {
			uconn.Close()
			continue
		}
		if !ln.acquireSocketSlot(identity.PrincipalID) {
			// capacity_exceeded: refused before hello, so no JSON-RPC
			// response is possible; the client observes a closed socket.
			uconn.Close()
			continue
		}
		ln.wg.Add(1)
		go func() {
			defer ln.wg.Done()
			defer ln.releaseSocketSlot(identity.PrincipalID)
			ln.mu.Lock()
			sess := ln.server.NewSession(identity)
			ln.mu.Unlock()
			serveSession(ctx, sess, uconn)
		}()
	}
}

func (ln *Listener) acquireSocketSlot(principal string) bool {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.stopped {
		return false
	}
	if ln.totalSockets >= MaxSocketsTotal || ln.perAdmin[principal] >= MaxSocketsPerAdministrator {
		return false
	}
	ln.totalSockets++
	ln.perAdmin[principal]++
	return true
}

func (ln *Listener) releaseSocketSlot(principal string) {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.totalSockets--
	ln.perAdmin[principal]--
	if ln.perAdmin[principal] <= 0 {
		delete(ln.perAdmin, principal)
	}
}

// serveSession runs one authenticated connection until it closes, a
// fatal framing error occurs, a handled request signals closeAfter, or
// ctx is cancelled (runtime shutdown). Correlation IDs cannot be reused
// while outstanding on the same socket (docs/specifications/control.md);
// because this loop never has more than one request in flight, that
// constraint holds vacuously here and needs no separate tracking.
func serveSession(ctx context.Context, sess *Session, conn *net.UnixConn) {
	defer conn.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close() // unblocks any pending Read/Write for shutdown
		case <-done:
		}
	}()

	br := bufio.NewReader(conn)
	for {
		if ctx.Err() != nil {
			return
		}
		frame, err := ReadFrame(br, conn, FrameDeadline)
		if err != nil {
			return
		}
		req, violation, idless := classifyEnvelope(frame)
		if idless {
			continue // no response, per profile; connection stays open
		}
		if violation != nil {
			if writeResponse(conn, responseForViolation(violation)) != nil {
				return
			}
			continue
		}
		resp, closeAfter := sess.Handle(ctx, req)
		if writeResponse(conn, resp) != nil || closeAfter {
			return
		}
	}
}

func writeResponse(conn *net.UnixConn, resp Response) error {
	data, err := resp.Encode()
	if err != nil {
		return err
	}
	if err := conn.SetWriteDeadline(time.Now().Add(WriteDeadline)); err != nil {
		return err
	}
	defer conn.SetWriteDeadline(time.Time{})
	data = append(data, '\n')
	_, err = conn.Write(data)
	return err
}
