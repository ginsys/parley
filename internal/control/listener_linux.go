//go:build linux

package control

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/runtime"
	"github.com/ginsys/parley/internal/store"
	"golang.org/x/sys/unix"
)

// WriteDeadline bounds one response write; a stalled client is
// disconnected rather than allowed to hold a goroutine or database
// resources indefinitely.
const WriteDeadline = 5 * time.Second

// RequestDeadline is docs/specifications/control.md's total server request
// deadline: one dispatched request, including any wait for the coordinator
// gate, must finish within it.
const RequestDeadline = 5 * time.Second

// handlerContextLead is how far before its RequestDeadline a dispatched
// handler's context expires. A handler that honors its context -- the
// coordinator gate wait, a reader query -- then returns its own definite
// classification (temporarily_unavailable is a proven non-commitment when
// the gate was never acquired) before serveSession has to answer for it at
// the deadline with the weaker outcome_unknown. Review 5259801666 (comment
// 4056340196): the recovery hooks' marker-I/O mutex and marker writes are
// not bounded by any context, so a handler may outlive its budget; see
// serveSession for what happens then.
const handlerContextLead = 500 * time.Millisecond

// staleSocketProbeTimeout bounds the connect attempt used to prove an
// existing socket abandoned. Any outcome other than a definite connection
// refusal within this window is treated as "not provably abandoned."
const staleSocketProbeTimeout = 2 * time.Second

var (
	errUnexpectedSocketEntry  = errors.New("control: an unexpected file exists at admin_socket")
	errSocketNotProvablyStale = errors.New("control: existing admin_socket is not provably abandoned; refusing to replace it")
	errSocketAddressTooLong   = errors.New("control: admin_socket's descriptor-relative bind address does not fit a struct sockaddr_un")
)

// maxUnixSockAddrLen is the largest address x/sys/unix's SockaddrUnix will
// accept on Linux: its sockaddr() rejects any name with len(name) >=
// len(raw.Path) (an 108-byte array), returning EINVAL otherwise -- so 107 is
// the largest representable length, not 108. Verified against
// golang.org/x/sys/unix's syscall_linux.go SockaddrUnix.sockaddr, and
// independently empirically: a 107-byte address binds successfully; a
// 108-byte one is rejected.
const maxUnixSockAddrLen = len(unix.RawSockaddrUnix{}.Path) - 1

// controlSocketAddr builds the descriptor-relative "/proc/self/fd/<parent>/
// <name>" address used for both Bind (TOCTOU-safe: it resolves the name
// lookup through the already-verified trusted directory descriptor, never a
// re-walked absolute path) and prepareSocketPath's stale-entry probe, so the
// two can never diverge on what they consider a valid address.
//
// This spelling can be LONGER than the original configured admin_socket
// path -- and, depending on parent's numeric width, can also be shorter --
// so validating the length of the original configured path is not
// sufficient: only the actual constructed address's length determines
// whether the platform's struct sockaddr_un can represent it. Validating
// here, before prepareSocketPath ever probes or removes an existing entry,
// means a nonrepresentable address is rejected without touching whatever
// currently occupies that pathname.
func controlSocketAddr(parent int, name string) (string, error) {
	addr := fmt.Sprintf("/proc/self/fd/%d/%s", parent, name)
	if len(addr) > maxUnixSockAddrLen {
		return "", fmt.Errorf("%w: constructed address is %d bytes, exceeds the %d-byte limit (admin_socket=%q)", errSocketAddressTooLong, len(addr), maxUnixSockAddrLen, name)
	}
	return addr, nil
}

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
	if mode != 0600 && mode != 0660 {
		return nil, fmt.Errorf("control: admin_socket mode must be exactly 0600 or 0660, got %v", mode)
	}
	dir := filepath.Dir(cfg.AdminSocket)
	name := filepath.Base(cfg.AdminSocket)
	parent, err := connection.TrustedDirectory(dir, cfg.ServerUID)
	if err != nil {
		return nil, err
	}
	defer unix.Close(parent)

	// Validated before prepareSocketPath can probe or remove any existing
	// entry: a nonrepresentable address must be rejected without touching
	// whatever currently occupies that pathname.
	addr, err := controlSocketAddr(parent, name)
	if err != nil {
		return nil, err
	}

	if err := prepareSocketPath(parent, name, addr, cfg.ServerUID); err != nil {
		return nil, err
	}

	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
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
// crashed prior server instance. addr is the already-validated
// controlSocketAddr for this same parent/name, reused for the probe so it
// can never diverge from the address Listen will actually Bind.
func prepareSocketPath(parent int, name, addr string, serverUID uint32) error {
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
	refused, err := probeConnectionRefused(addr)
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
//
// ECONNREFUSED is not exclusive to an abandoned socket, though: a socket
// that has been Bind'd but not yet had Listen called on it can also
// return ECONNREFUSED for that same brief interval, indistinguishable
// from this probe's intended "abandoned" case. This matters only if a
// second, concurrent Listen (this function or another process's) could
// observe an in-progress first Listen's Bind-but-not-yet-Listen socket at
// this exact pathname. In the accepted production assembly this cannot
// happen for two *cooperating* contenders on the *same* database:
// runtime.Start acquires the canonical database ownership lock
// (internal/runtime/runtime.go's Acquire call, before any
// runtime.Service.Start including this listener's) before this
// listener's own Start ever calls Listen, so only the single lock holder
// ever reaches Bind/Listen for that database at a time. This is a
// configuration/ownership-scoped mitigation, not a general proof: it says
// nothing about an arbitrary standalone Listen caller outside that
// assembly, or about two different databases whose configuration happens
// to share one socket pathname -- neither of those is protected by the
// database ownership lock, and this function does not claim otherwise.
func probeConnectionRefused(addr string) (bool, error) {
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
// total socket caps, and executes each session's requests strictly
// sequentially in arrival order: one executing plus at most
// MaxQueuedPerSocket complete frames waiting, each charged for its wait
// (see serveSession).
type Listener struct {
	cfg  Config
	mode os.FileMode

	mu           sync.Mutex
	listener     net.Listener // nil until Start binds it
	boundDev     uint64       // device of the on-disk socket entry Start bound, for unlinkOwnedSocket
	boundIno     uint64       // inode of the on-disk socket entry Start bound, for unlinkOwnedSocket
	server       *Server
	stopped      bool
	totalSockets int
	perAdmin     map[string]int
	acceptErr    error       // recorded by fail; returned by Wait
	onFailure    func(error) // optional, set via OnAcceptFailure before Start
	wg           sync.WaitGroup

	// lstatSocket, when set, replaces unix.Lstat for Start's post-bind
	// identity read. Instance-local (a field on this Listener, not a
	// package-level variable) so a test exercising this failure cannot
	// race an unrelated test's ordinary Start call in the same package.
	// Nil in production; Start falls back to unix.Lstat itself.
	lstatSocket func(path string, stat *unix.Stat_t) error
}

// NewListenerService builds a Listener for runtime.Start's
// Registration.Service. Unlike an already-bound net.Listener, cfg/mode are
// only turned into a bound socket inside Start, after runtime.Start has
// already acquired exclusive database ownership -- a process that loses
// that race never creates, probes or replaces the admin socket at all.
// Neither serverID nor epoch are constructor arguments: Start resolves
// both itself from the writer runtime.Start supplies (Resources.Writer) --
// serverID by reading the installation row directly (the same one-shot
// pattern recovery.Service.New uses for the same row), and epoch via
// Coordinator().Epoch, the owning coordinator's own process-local identity
// (mandate R6), never a second, independently minted one for the same
// server incarnation.
func NewListenerService(cfg Config, mode os.FileMode) *Listener {
	return &Listener{cfg: cfg, mode: mode, perAdmin: make(map[string]int)}
}

// OnAcceptFailure registers f to be invoked, at most once, the first time
// acceptLoop gives up on an Accept error that is neither an owned stop
// (StopAdmission) nor a bounded transient condition. f is called directly
// from the accept goroutine, without ln.mu held; like
// recovery.Config.FailStop, it must be nonblocking, perform no I/O and
// never reenter the store. Call before Start; a call after Start races
// acceptLoop's own read of it and is not supported.
func (ln *Listener) OnAcceptFailure(f func(error)) {
	ln.mu.Lock()
	ln.onFailure = f
	ln.mu.Unlock()
}

func (ln *Listener) Start(ctx context.Context, res runtime.Resources) error {
	listener, err := Listen(ln.cfg, ln.mode)
	if err != nil {
		return fmt.Errorf("control: bind admin socket: %w", err)
	}
	// Captured immediately after a successful bind, from the on-disk
	// pathname entry itself (not the socket fd -- Linux's fstat on an
	// AF_UNIX socket fd reports the socket's own pseudo "sockfs" identity,
	// not the bound directory entry's real filesystem inode, so comparing
	// against a socket-fd-derived stat would never match the path's actual
	// inode at all). Captured before the two failure returns below too, not
	// only on the success path: ln.listener/boundDev/boundIno are only
	// assigned under the lock further down, so a failure here previously
	// left StopAdmission's `listener == nil` check short-circuiting past
	// unlinkOwnedSocket entirely, orphaning the just-bound socket file
	// (found by the hosted review of this batch's own CP-09 fix).
	//
	// A failed Lstat here is itself a startup failure, not a degraded but
	// otherwise successful Start: leaving boundDev/boundIno at zero would
	// make unlinkOwnedSocket a permanent, silent no-op for this incarnation
	// (found by a later hosted review of that fix). This does NOT mean
	// blindly unlinking the pathname on this failure -- an Lstat failure
	// establishes nothing about what, if anything, currently occupies that
	// pathname, so removing it here would be exactly the unverified-removal
	// mistake unlinkOwnedSocket's own identity check exists to avoid. Fail
	// closed instead: close the listener this attempt just bound and leave
	// the pathname untouched, its state and cleanup explicitly unverified.
	lstat := ln.lstatSocket
	if lstat == nil {
		lstat = unix.Lstat
	}
	var boundStat unix.Stat_t
	if statErr := lstat(ln.cfg.AdminSocket, &boundStat); statErr != nil {
		return errors.Join(
			fmt.Errorf("control: read bound socket identity: %w", statErr),
			listener.Close(),
		)
	}
	boundDev, boundIno := uint64(boundStat.Dev), boundStat.Ino
	state := StateRunning
	if res.Mode == runtime.Held {
		state = StateRecoveryOnly
	}
	var serverID string
	if err := res.Writer.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT server_id FROM installation WHERE singleton=1").Scan(&serverID)
	}); err != nil {
		unlinkOwnedSocket(ln.cfg.AdminSocket, boundDev, boundIno)
		listener.Close()
		return fmt.Errorf("control: read installation identity: %w", err)
	}
	epoch, err := res.Writer.Coordinator().Epoch(ctx)
	if err != nil {
		unlinkOwnedSocket(ln.cfg.AdminSocket, boundDev, boundIno)
		listener.Close()
		return fmt.Errorf("control: read coordinator epoch: %w", err)
	}
	ln.mu.Lock()
	ln.listener = listener
	ln.boundDev, ln.boundIno = boundDev, boundIno
	ln.server = NewServer(ln.cfg, res.Queries, res.Writer, serverID, epoch, state)
	ln.mu.Unlock()
	ln.wg.Add(1)
	go ln.acceptLoop(res.WorkerContext)
	return nil
}

// StopAdmission is safe even when Start never got as far as binding a
// listener (e.g. Listen itself failed): runtime.Start registers a service
// for cleanup before calling its Start (internal/runtime/runtime.go), so a
// failed Start still receives a StopAdmission/Wait pair. Setting ln.stopped
// before closing the listener is what lets acceptLoop distinguish this
// intentional close from an unexpected one -- net.ErrClosed alone is never
// sufficient proof of an owned stop (mandate R4).
func (ln *Listener) StopAdmission() error {
	ln.mu.Lock()
	ln.stopped = true
	listener := ln.listener
	boundDev, boundIno := ln.boundDev, ln.boundIno
	ln.mu.Unlock()
	if listener == nil {
		return nil
	}
	// Remove the on-disk pathname entry so a clean shutdown does not leave
	// a stale-looking socket behind (mandate CP-09). prepareSocketPath's
	// own startup-side stale-socket probe already handles a leftover entry
	// safely regardless of clean-vs-crashed shutdown, so this is hygiene,
	// not a correctness fix -- any error here is silently ignored.
	unlinkOwnedSocket(ln.cfg.AdminSocket, boundDev, boundIno)
	return listener.Close()
}

// unlinkOwnedSocket removes the on-disk socket entry at path, but only when
// a fresh Lstat of that path still reports the identity (device/inode)
// captured from the pathname itself at bind time, not merely the pathname
// string. This is best-effort, identity-checked cleanup: it protects
// against a replacement that is already visible (a different device/inode)
// at the moment this check runs, and it still relies on this service
// having exclusive/trusted control of the socket pathname for its own
// lifecycle -- the same precondition prepareSocketPath's own startup-side
// stale-socket probe depends on. The identity captured after Listen
// returns is a sampled post-bind pathname identity, not proof of atomic
// binding; boundDev/boundIno of 0 (Start's Lstat failed, or never bound)
// never matches a real entry, so this is a no-op then.
//
// This does NOT close every replacement race, and no inode-reuse argument
// is required for one to matter. Lstat and Unlink below are two separate,
// non-atomic syscalls on the same pathname. A different actor can replace
// the entry -- move or remove the original and install a different-inode
// socket at the same name -- strictly after Lstat has already matched the
// originally-bound identity but before Unlink runs; Unlink then removes
// whatever now occupies that name, regardless of its inode. No reuse of
// the original device/inode pair is needed for this to happen: the
// replacement is simply the thing present at unlink time, identity check
// or not. Retaining a parent-directory descriptor across the two calls
// (as prepareSocketPath does on the startup side) would not close this
// window either -- descriptor-relative Fstatat/Unlinkat resolve by name
// within that directory fd, which stabilizes the parent-directory *lookup*
// against a symlink or rename higher in the path, but performs two
// separate syscalls, not one atomic compare-and-unlink of a directory
// entry; a concurrent replace of the same name races an Unlinkat exactly
// as it races a plain Unlink. Closing this window structurally would
// require exclusive/serialized control of the socket pathname for this
// service's entire lifecycle, which this fix does not add -- that remains
// a known, accepted limitation of a single-owner deployment, not evidence
// of a guarantee this code provides.
func unlinkOwnedSocket(path string, boundDev, boundIno uint64) {
	if boundDev == 0 && boundIno == 0 {
		return
	}
	var pathStat unix.Stat_t
	if err := unix.Lstat(path, &pathStat); err != nil {
		return
	}
	if boundDev != uint64(pathStat.Dev) || boundIno != pathStat.Ino {
		return
	}
	_ = unix.Unlink(path)
}

// Wait returns the accept failure acceptLoop recorded, if any (mandate
// R4): a persistent or unexpected Accept error must be surfaced through
// the normal service-failure path, not silently swallowed as if shutdown
// were always clean.
func (ln *Listener) Wait() error {
	ln.wg.Wait()
	ln.mu.Lock()
	server := ln.server
	ln.mu.Unlock()
	if server != nil {
		// Every session has ended, so no new expiry persistence can start;
		// drain what finished requests left running before the runtime
		// closes the writer underneath it.
		server.WaitBackground()
	}
	ln.mu.Lock()
	defer ln.mu.Unlock()
	return ln.acceptErr
}

// maxAcceptRetries bounds a resource-exhaustion (EMFILE/ENFILE/EINTR-class)
// accept retry loop before escalating to failure -- never an unbounded
// retry or busy-loop.
const maxAcceptRetries = 8

// acceptRetryBaseDelay is the first backoff delay for a transient accept
// error; it doubles each subsequent retry up to maxAcceptRetries.
const acceptRetryBaseDelay = 10 * time.Millisecond

// isTemporaryAcceptError reports whether err is a bounded, retry-worthy
// accept-time condition (EINTR, EMFILE, ENFILE, or any Timeout()-true
// errno) rather than a permanent failure. net.Error.Temporary() is
// deprecated but remains the functionally correct predicate for exactly
// this class: net.OpError.Temporary() treats accept-time ECONNRESET/
// ECONNABORTED as temporary and otherwise delegates to
// syscall.Errno.Temporary(), which is true for EINTR/EMFILE/ENFILE or any
// Timeout()-true errno (verified against the Go 1.27.1 stdlib source).
func isTemporaryAcceptError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Temporary() //nolint:staticcheck // SA1019: intentional, see comment above
}

// fail records an unexpected, non-owned-stop, non-transient accept
// failure for Wait to return, and notifies the optional OnAcceptFailure
// callback at most once. Never called while holding ln.mu; never blocks.
func (ln *Listener) fail(err error) {
	ln.mu.Lock()
	if ln.acceptErr != nil {
		ln.mu.Unlock()
		return
	}
	ln.acceptErr = err
	onFailure := ln.onFailure
	ln.mu.Unlock()
	if onFailure != nil {
		onFailure(err)
	}
}

func (ln *Listener) acceptLoop(ctx context.Context) {
	defer ln.wg.Done()
	retries := 0
	for {
		conn, err := ln.listener.Accept()
		if err != nil {
			ln.mu.Lock()
			stopped := ln.stopped
			ln.mu.Unlock()
			if stopped || ctx.Err() != nil {
				// An owned stop (StopAdmission set ln.stopped before
				// closing the listener) or runtime shutdown -- clean, not
				// a failure to surface.
				return
			}
			if isTemporaryAcceptError(err) && retries < maxAcceptRetries {
				retries++
				delay := acceptRetryBaseDelay * time.Duration(uint(1)<<uint(retries-1))
				select {
				case <-time.After(delay):
					continue
				case <-ctx.Done():
					return
				}
			}
			// Neither an owned stop nor a bounded-retryable transient
			// condition: an unexpected failure that would otherwise leave
			// the process apparently listening while nothing accepts
			// further administrators. Surface it rather than silently
			// returning, busy-looping or retrying forever.
			ln.fail(err)
			return
		}
		retries = 0
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
// the reader below tracks queued and executing IDs to enforce that.
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

	// preHelloTimer enforces docs/specifications/control.md's "After
	// kernel authentication, the first call within five seconds is
	// server.hello" as a single absolute deadline for the whole
	// connection, independent of ReadFrame's own per-frame deadline.
	// Renewing a read deadline on every loop iteration is not enough:
	// ReadFrame resets its own deadline to now+FrameDeadline once a
	// frame's first byte arrives (frame.go), which can push a slow-
	// trickled or repeatedly rejected pre-hello frame's effective
	// deadline past the original cutoff. This timer fires exactly once,
	// unconditionally, and is stopped only after hello actually
	// succeeds -- matching the existing ctx.Done() goroutine above,
	// which closes the connection the same way to unblock a pending
	// read.
	var helloDone atomic.Bool
	preHelloTimer := time.AfterFunc(FrameDeadline, func() {
		if !helloDone.Load() {
			conn.Close()
		}
	})
	defer preHelloTimer.Stop()

	// Review 5259563170 (comment 4056153933): a frame pipelined behind an
	// executing request must be charged for that wait. One reader goroutine
	// per socket (never one per frame) stamps each complete frame's arrival
	// and hands it to this loop through the profile's bounded queue, so the
	// request deadline below runs from arrival, not from dequeue. A frame
	// that finds the queue full closes the socket: this loop owns the
	// socket's only output path, so there is no bounded output space for a
	// capacity_exceeded reply (docs/specifications/control.md's "otherwise
	// close"). Correlation IDs stay unique while outstanding the same way:
	// a reused one closes the socket rather than executing.
	type arrival struct {
		req       Request
		violation *Violation
		idless    bool
		at        time.Time
		id        string // the correlation ID its response will carry
		tracked   bool   // id is held in outstanding until that response is written
	}
	// outstanding holds the correlation IDs whose responses are not yet
	// written; occupied counts every admitted complete frame -- in the
	// channel, dequeued as pending, or executing -- so the queue holds that
	// many and a send never blocks. Review 5264346948 (comment 4060293329):
	// the two part ways once a request is answered at its deadline while its
	// handler still runs -- the ID is released with the reply (a conforming
	// client may reuse it), but the executing slot stays occupied until the
	// handler returns. Review 5264559000 (comment 4060461953): an ID-less
	// object or an unechoable violation has no ID to track but still occupies
	// a slot, so capacity is charged independently of tracking. Either way
	// the reader admits at most MaxQueuedPerSocket frames behind a live
	// handler, as advertised. Each admitted frame releases its slot exactly
	// once: with its written response, or -- for a request answered at the
	// deadline -- when its handler returns; an ID-less frame ends the session.
	const maxOutstanding = MaxExecutingPerSocket + MaxQueuedPerSocket
	queue := make(chan arrival, maxOutstanding)
	readerDone := make(chan struct{})
	var outstandingMu sync.Mutex
	outstanding := make(map[string]struct{}, maxOutstanding)
	occupied := 0
	go func() {
		defer close(readerDone)
		defer close(queue)
		br := bufio.NewReader(conn)
		for {
			frame, err := ReadFrame(br, conn, FrameDeadline)
			if err != nil {
				return
			}
			next := arrival{at: time.Now()}
			next.req, next.violation, next.idless = classifyEnvelope(frame)
			// Review 5259780995 (comment 4056295482): a violation whose ID
			// can be echoed is answered under that ID, so it is tracked like
			// a valid request; only an ID-less or unechoable frame is not.
			switch {
			case next.idless:
			case next.violation == nil:
				next.id, next.tracked = next.req.ID, true
			case next.violation.ID != nil:
				next.id, next.tracked = *next.violation.ID, true
			}
			outstandingMu.Lock()
			full := occupied >= maxOutstanding
			occupied++
			reused := false
			if next.tracked {
				_, reused = outstanding[next.id]
				outstanding[next.id] = struct{}{}
			}
			outstandingMu.Unlock()
			if reused || full {
				conn.Close()
				return
			}
			select {
			case queue <- next:
			default:
				conn.Close()
				return
			}
		}
	}()
	defer func() {
		conn.Close() // unblocks the reader's pending ReadFrame
		<-readerDone
	}()

	// Review 5259679438 (comment 4056232738) and 5259801666 (comment
	// 4056315885): a call stays outstanding until its response is written,
	// so its ID is released only after the write, and the lock is not held
	// across the write -- the reader keeps classifying frames meanwhile, so a
	// reuse pipelined during a slow write is detected and closes the socket
	// rather than being admitted once the write completes. A conforming
	// client reuses an ID only after reading its response; the release
	// follows the write immediately, so such a reuse is practically never
	// mistaken for a violation, and a client that never reuses IDs (the
	// package's own Client) is unaffected.
	respond := func(next arrival, resp Response) error {
		err := writeResponse(conn, resp)
		outstandingMu.Lock()
		if next.tracked {
			delete(outstanding, next.id)
		}
		occupied--
		outstandingMu.Unlock()
		return err
	}

	// Review 5257748895 (comment 4054786967) and 5259801666 (comment
	// 4056340196): ctx is the long-lived runtime worker context, so a
	// handler gets a request-scoped deadline -- docs/specifications/
	// control.md's five-second server request deadline covers queue wait,
	// gate wait and execution together. That context bounds what honors it
	// (the coordinator gate, reader queries); it does not bound the recovery
	// hooks' marker-I/O mutex or a marker write, and the deferred After hook
	// runs even when Before refused. So a handler runs in its own goroutine
	// -- one per executing request, never one per frame: MaxExecutingPerSocket
	// is one, so this is bounded by the socket caps -- and this loop stops
	// waiting for it at the deadline. What it answers then is not a
	// non-commitment claim: the handler may have committed and be finishing
	// recovery evidence, so a mutation gets outcome_unknown (same-ID retry
	// replays the durable receipt or executes once) and a read, which has no
	// commitment to be uncertain about, temporarily_unavailable. The handler
	// stays owned: no further request on this socket dispatches until it
	// returns (queued ones expire at their own deadlines meanwhile), its late
	// result is discarded, and serveSession does not return -- so
	// Listener.Wait does not, and the runtime keeps the writer open -- until
	// it has. store.Coordinator's commit runs independently of the request
	// context, so an expiring context never tears a commit.
	type outcome struct {
		resp       Response
		closeAfter bool
	}
	type execution struct {
		next     arrival
		done     chan outcome // buffered: a handler never blocks on a loop that stopped waiting
		answered bool         // the deadline reply was written; the late result is discarded
	}
	var running *execution
	defer func() {
		if running != nil {
			// The socket's close does not wait for the handler -- an ID-less
			// frame's bounded close, a write failure and shutdown all close
			// now; the drain below is what keeps the writer open for it.
			conn.Close()
			<-running.done
		}
	}()
	dispatch := func(next arrival) *execution {
		ex := &execution{next: next, done: make(chan outcome, 1)}
		reqCtx, cancelReq := context.WithDeadline(ctx, next.at.Add(RequestDeadline-handlerContextLead))
		go func() {
			defer cancelReq()
			resp, closeAfter := sess.Handle(reqCtx, next.req)
			ex.done <- outcome{resp: resp, closeAfter: closeAfter}
		}()
		return ex
	}
	finish := func(out outcome) (keepServing bool) {
		ex := *running
		running = nil
		if sess.negotiated {
			// sess.negotiated is written by the handler goroutine, whose
			// send on done happens-before this read; helloDone is the only
			// field preHelloTimer's goroutine reads.
			helloDone.Store(true)
		}
		if ex.answered {
			// The deadline reply released the correlation ID; the executing
			// slot it kept occupied is free only now that the handler returned.
			// The ID is not touched here: a newer request may hold it by now.
			outstandingMu.Lock()
			occupied--
			outstandingMu.Unlock()
			return !out.closeAfter
		}
		return respond(ex.next, out.resp) == nil && !out.closeAfter
	}
	// answerAtDeadline writes the deadline reply for the running request. Its
	// correlation ID is released with the reply, as for any written response;
	// its capacity is not (see finish), so the reader still admits at most
	// MaxQueuedPerSocket behind the live handler.
	answerAtDeadline := func(ex *execution) error {
		ex.answered = true
		err := writeResponse(conn, deadlineResponse(ex.next.req))
		outstandingMu.Lock()
		delete(outstanding, ex.next.id)
		outstandingMu.Unlock()
		return err
	}
	// refuse settles a dequeued frame without dispatching it. An ID-less
	// object closes the socket (docs/specifications/control.md:64-65: "ID-less
	// client objects (including notifications) are not executed and receive
	// no response; close with a bounded operational diagnostic" -- the
	// deferred conn.Close() above is that close); a malformed envelope gets
	// its envelope error under its echoable ID; and a request that waited out
	// its whole deadline in the queue is refused before dispatch (review
	// 5259780995, comment 4056295483: handlers that never consult the context
	// -- a repeated server.hello, an unknown method, parameter validation --
	// would otherwise still run and answer after the advertised limit;
	// nothing has executed, so this is a provable non-commitment). Review
	// 5264346948 (comment 4060293322): the same classification governs a
	// frame that expires while an earlier request is still executing.
	refuse := func(next arrival) (keepServing bool) {
		switch {
		case next.idless:
			return false
		case next.violation != nil:
			return respond(next, responseForViolation(next.violation)) == nil
		default:
			return respond(next, domainErrorResponse(&next.req.ID, DomainCode(store.TemporarilyUnavailable))) == nil
		}
	}

	// pending is a dequeued request waiting for the running execution to
	// end; it expires on its own arrival-based deadline while it waits.
	var pending *arrival
	for {
		if running == nil && pending != nil {
			next := *pending
			pending = nil
			if next.idless || next.violation != nil || !time.Now().Before(next.at.Add(RequestDeadline)) {
				if !refuse(next) {
					return
				}
				continue
			}
			running = dispatch(next)
			continue
		}
		queueIn := queue
		if pending != nil {
			queueIn = nil // hold one; the reader's bounded queue holds the rest
		}
		var (
			execDone       <-chan outcome
			cutoff, expiry <-chan time.Time
			timers         []*time.Timer
		)
		if running != nil {
			execDone = running.done
			if !running.answered {
				timer := time.NewTimer(time.Until(running.next.at.Add(RequestDeadline)))
				timers = append(timers, timer)
				cutoff = timer.C
			}
		}
		if pending != nil {
			timer := time.NewTimer(time.Until(pending.at.Add(RequestDeadline)))
			timers = append(timers, timer)
			expiry = timer.C
		}
		keepServing := true
		select {
		case <-ctx.Done():
			keepServing = false
		case next, ok := <-queueIn:
			// !ok: the reader ended -- the socket closed or a frame was fatal.
			keepServing = ok
			if ok {
				pending = &next
			}
		case out := <-execDone:
			keepServing = finish(out)
		case <-cutoff:
			select {
			case out := <-running.done:
				// Both ready: a definite result beats an uncertain one.
				keepServing = finish(out)
			default:
				keepServing = answerAtDeadline(running) == nil
			}
		case <-expiry:
			next := *pending
			pending = nil
			keepServing = refuse(next)
		}
		for _, timer := range timers {
			timer.Stop()
		}
		if !keepServing {
			return
		}
	}
}

// deadlineResponse answers a request whose handler is still running at its
// deadline. A mutation's outcome cannot be established from outside the
// handler -- it may have committed and be finishing recovery evidence -- so
// it is outcome_unknown, never a non-commitment claim; a read has nothing to
// be uncertain about and is temporarily_unavailable.
func deadlineResponse(req Request) Response {
	if isMutationMethod(req.Method) {
		return domainErrorResponse(&req.ID, DomainCode(store.OutcomeUnknown))
	}
	return domainErrorResponse(&req.ID, DomainCode(store.TemporarilyUnavailable))
}

func writeResponse(conn *net.UnixConn, resp Response) error {
	data, err := resp.Encode()
	if err != nil {
		return err
	}
	if len(data)+1 > MaxFrameBytes {
		// A response that cannot fit the profile's own frame bound must
		// never be written truncated or oversized; replace it with a
		// bounded internal-error response instead.
		data, err = envelopeErrorResponse(InternalError, resp.ID).Encode()
		if err != nil {
			return err
		}
	}
	if err := conn.SetWriteDeadline(time.Now().Add(WriteDeadline)); err != nil {
		return err
	}
	defer conn.SetWriteDeadline(time.Time{})
	data = append(data, '\n')
	_, err = conn.Write(data)
	return err
}
