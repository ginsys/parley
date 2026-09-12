package connection

import (
	"context"
	"database/sql"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ginsys/parley/internal/store"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

// Authentication is secret-bearing input, never a principal or a log payload.
// Only deterministic adapter code should construct it from a private credential.
type Authentication struct {
	credentialID string
	secret       [32]byte
	native       NativeTuple
}

func NewAuthentication(id string, secret []byte, native NativeTuple) (Authentication, error) {
	parsed, err := uuid.Parse(id)
	if err != nil || parsed == uuid.Nil || parsed.String() != id || len(secret) != 32 {
		return Authentication{}, store.AuthenticationFailed
	}
	a := Authentication{credentialID: id, native: native}
	copy(a.secret[:], secret)
	return a, nil
}
func (Authentication) String() string   { return "[authentication redacted]" }
func (Authentication) GoString() string { return "[authentication redacted]" }

// Token is evidence supplied to a trusted verifier, not a constructible principal.
// Ordinary APIs accept only the private Session capability made by Attach.
type Token struct {
	BindingID, PeerID, Epoch      string
	CredentialVersion, Generation int64
	ConnectorUID                  uint32
}
type Snapshot struct {
	Epoch      string
	Generation int64
	Active     bool
}
type ManagerConfig struct {
	// AfterFunc must schedule asynchronously; injected only with a controlled clock.
	AfterFunc      func(time.Duration, func()) func()
	Store          *store.DB
	MaxNonattached int
	Now            func() time.Time
	Guard          func(context.Context, *sql.Tx, string) error
	Verify         func(context.Context, NativeTuple, Token) error
}
type Manager struct {
	pending   chan struct{}
	afterFunc func(time.Duration, func()) func()
	store     *store.DB
	now       func() time.Time
	guard     func(context.Context, *sql.Tx, string) error
	verify    func(context.Context, NativeTuple, Token) error
	// All mutable fields below, including socket/session fields, use the store gate.
	sockets map[*Socket]struct{}
	slots   map[string]*Session
}
type Socket struct {
	// Timer expiry and deadline publication serialize independently of the writer.
	deadlineMu              sync.Mutex
	deadline                time.Time
	authGate                chan struct{}
	authenticated           atomic.Bool
	releaseOnce             sync.Once
	manager                 *Manager
	conn                    *net.UnixConn
	uid                     uint32
	ctx                     context.Context
	cancel                  context.CancelFunc
	closeOnce               sync.Once
	accepted, lastHeartbeat time.Time
	credentialID, bindingID string
	native                  NativeTuple
	session                 *Session
	timer                   func()
}
type Session struct {
	socket            *Socket
	token             Token
	ready             bool
	hostVerified      bool
	nonce             string
	readinessDeadline time.Time
}

func NewManager(cfg ManagerConfig) (*Manager, error) {
	if cfg.Store == nil || cfg.MaxNonattached <= 0 || cfg.Guard == nil || cfg.Verify == nil {
		return nil, store.InvalidRequest
	}
	if cfg.AfterFunc == nil {
		cfg.AfterFunc = func(d time.Duration, f func()) func() { timer := time.AfterFunc(d, f); return func() { timer.Stop() } }
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	ctx, cancel := context.WithTimeout(context.Background(), store.AuthenticationDeadline)
	defer cancel()
	if err := cfg.Store.Coordinator().ClaimConnections(ctx); err != nil {
		return nil, err
	}
	return &Manager{pending: make(chan struct{}, cfg.MaxNonattached), afterFunc: cfg.AfterFunc, store: cfg.Store, now: cfg.Now, guard: cfg.Guard, verify: cfg.Verify, sockets: make(map[*Socket]struct{}), slots: make(map[string]*Session)}, nil
}
func peerUID(conn *net.UnixConn) (uint32, error) {
	if conn == nil {
		return 0, store.AuthenticationFailed
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, store.AuthenticationFailed
	}
	var cred *unix.Ucred
	var sockErr error
	err = raw.Control(func(fd uintptr) {
		kind, err := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TYPE)
		if err != nil || kind != unix.SOCK_STREAM {
			sockErr = store.AuthenticationFailed
			return
		}
		cred, sockErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	})
	if err != nil || sockErr != nil || cred == nil {
		return 0, store.AuthenticationFailed
	}
	return cred.Uid, nil
}

// Accept consumes a Unix stream accepted by the future control endpoint. It reads
// kernel UID itself. No caller-supplied UID is accepted. It owns closure on error.
func (m *Manager) Accept(ctx context.Context, conn *net.UnixConn) (*Socket, error) {
	if conn == nil {
		return nil, store.AuthenticationFailed
	}
	select {
	case m.pending <- struct{}{}:
	default:
		conn.Close()
		return nil, store.CapacityExceeded
	}
	life, cancel := context.WithCancel(context.Background())
	s := &Socket{manager: m, conn: conn, ctx: life, cancel: cancel, accepted: m.now(), authGate: make(chan struct{}, 1)}
	s.lastHeartbeat = s.accepted
	// Capacity and the initial deadline include waiting for the coordinator.
	stopInitial := m.afterFunc(store.AuthenticationDeadline, func() {
		if !s.authenticated.Load() {
			s.cancel()
			s.releaseCapacity()
			conn.Close()
		}
	})
	defer stopInitial()
	waitCtx, stopWait := context.WithTimeout(ctx, store.AuthenticationDeadline)
	defer stopWait()
	stopLifetime := context.AfterFunc(life, stopWait)
	defer stopLifetime()
	uid, err := peerUID(conn)
	if err == nil {
		s.uid = uid
		_, err = m.store.Coordinator().Transition(waitCtx, func(context.Context, *sql.Tx, store.CommitView) (store.TransitionResult, error) {
			if s.ctx.Err() != nil || m.expired(s, m.now()) {
				return store.TransitionResult{}, store.AuthenticationFailed
			}
			return store.TransitionResult{Changed: true}, nil
		}, func(store.CommitView) {
			m.prune()
			m.sockets[s] = struct{}{}
			s.arm(store.AuthenticationDeadline - m.now().Sub(s.accepted))
		})
	}
	if err != nil {
		cancel()
		s.releaseCapacity()
		conn.Close()
		return nil, err
	}
	go func() { <-life.Done(); s.releaseCapacity(); conn.Close() }()
	return s, nil
}
func (s *Socket) releaseCapacity() { s.releaseOnce.Do(func() { <-s.manager.pending }) }

// Authentication calls are serialized per socket through their failure cleanup.
// No coordinator callback takes this gate, so the lock order cannot be reversed.
func (s *Socket) lockAuthentication(ctx context.Context) error {
	if ctx.Err() != nil {
		return store.TemporarilyUnavailable
	}
	select {
	case s.authGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return store.TemporarilyUnavailable
	case <-s.ctx.Done():
		return store.AuthenticationFailed
	}
}
func (m *Manager) hasDueSockets() bool {
	for s := range m.sockets {
		if s.ctx.Err() != nil || m.expired(s, m.now()) {
			return true
		}
	}
	return false
}
func (m *Manager) prune() {
	for s := range m.sockets {
		if s.ctx.Err() != nil || m.expired(s, m.now()) {
			m.remove(s)
		}
	}
}
func (s *Socket) Context() context.Context { return s.ctx }
func (s *Socket) arm(after time.Duration) {
	s.deadlineMu.Lock()
	if s.authenticated.Load() {
		s.deadline = s.lastHeartbeat.Add(store.LivenessDeadline)
	} else {
		s.deadline = s.accepted.Add(store.AuthenticationDeadline)
	}
	s.deadlineMu.Unlock()
	if s.timer != nil {
		s.timer()
	}
	initial := !s.authenticated.Load()
	s.timer = s.manager.afterFunc(after, func() {
		if initial && !s.authenticated.Load() {
			s.cancel()
			s.releaseCapacity()
			s.conn.Close()
		}
		s.manager.expire(s)
	})
}
func (s *Socket) Close() {
	s.closeOnce.Do(func() {
		s.cancel()
		s.releaseCapacity()
		ctx, cancel := context.WithTimeout(context.Background(), store.AuthenticationDeadline)
		defer cancel()
		_, _ = s.manager.store.Coordinator().Transition(ctx, func(context.Context, *sql.Tx, store.CommitView) (store.TransitionResult, error) {
			return store.TransitionResult{Changed: true}, nil
		}, func(store.CommitView) { s.manager.remove(s) })
		// Even a failed writer cannot keep this transport operational. Every operation
		// checks cancellation before using an otherwise still-present slot.
		s.cancel()
		s.conn.Close()
	})
}
func (m *Manager) remove(s *Socket) {
	if s.session != nil && m.slots[s.bindingID] == s.session {
		delete(m.slots, s.bindingID)
	}
	delete(m.sockets, s)
	if s.timer != nil {
		s.timer()
	}
	s.cancel()
	s.releaseCapacity()
}

// Invalidate is an infallible publication callback for trusted rotation/revoke.
// Invoke only under this store's coordinator gate, never from a request payload.
func (m *Manager) Invalidate(binding string) {
	for s := range m.sockets {
		if s.bindingID == binding {
			m.remove(s)
		}
	}
}
func (m *Manager) active(binding string) bool {
	v := m.slots[binding]
	return v != nil && v.socket.ctx.Err() == nil && !m.expired(v.socket, m.now())
}
func (m *Manager) owned(s *Socket) bool {
	if s == nil || s.manager != m || s.ctx.Err() != nil {
		return false
	}
	_, ok := m.sockets[s]
	return ok
}
func (m *Manager) expired(s *Socket, now time.Time) bool {
	if s.credentialID == "" {
		return !now.Before(s.accepted.Add(store.AuthenticationDeadline))
	}
	return !now.Before(s.lastHeartbeat.Add(store.LivenessDeadline))
}
func (m *Manager) expire(s *Socket) {
	if s == nil || s.manager != m {
		return
	}
	s.deadlineMu.Lock()
	closeIt := s.ctx.Err() != nil || (!s.deadline.IsZero() && !m.now().Before(s.deadline))
	if closeIt {
		// Cancel while holding the deadline lock so a concurrent heartbeat cannot
		// revive a socket whose published deadline already expired.
		s.cancel()
	}
	s.deadlineMu.Unlock()
	if closeIt {
		s.Close()
	}
}

func (m *Manager) authenticate(ctx context.Context, tx *sql.Tx, s *Socket, a Authentication) (store.BindingRecord, store.CredentialRecord, bool, error) {
	if !m.owned(s) || m.expired(s, m.now()) {
		return store.BindingRecord{}, store.CredentialRecord{}, false, store.AuthenticationFailed
	}
	c, err := store.ReadCredential(ctx, tx, a.credentialID)
	if err != nil {
		if err == store.BindingUnavailable {
			err = store.AuthenticationFailed
		}
		return store.BindingRecord{}, c, false, err
	}
	if !c.Matches(a.secret) {
		return store.BindingRecord{}, c, false, store.AuthenticationFailed
	}
	b, err := store.ReadBinding(ctx, tx, c.BindingID)
	if err != nil {
		if err == store.BindingUnavailable {
			err = store.AuthenticationFailed
		}
		return b, c, false, err
	}
	if b.ConnectorUID != s.uid || a.native != (NativeTuple{b.HostKind, b.NamespaceID, b.SessionID}) || b.Status != "enabled" || c.Status != "current" {
		return b, c, false, store.AuthenticationFailed
	}
	if s.credentialID != "" && (s.credentialID != a.credentialID || s.bindingID != b.ID || s.native != a.native) {
		return b, c, false, store.AuthenticationFailed
	}
	if !m.now().Before(time.Unix(0, c.ExpiresAtNS)) {
		_, err := tx.ExecContext(ctx, "UPDATE credentials SET status='expired' WHERE credential_id=? AND status='current'", c.ID)
		if err != nil {
			return b, c, false, err
		}
		return b, c, true, store.AuthenticationFailed
	}
	return b, c, false, nil
}
func (m *Manager) bind(s *Socket, b store.BindingRecord, c store.CredentialRecord) {
	if s.credentialID == "" {
		s.authenticated.Store(true)
		s.credentialID = c.ID
		s.bindingID = b.ID
		s.native = NativeTuple{b.HostKind, b.NamespaceID, b.SessionID}
		s.lastHeartbeat = m.now()
		s.arm(store.LivenessDeadline)
	}
}
func (m *Manager) Inspect(ctx context.Context, s *Socket, a Authentication) (Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, store.AuthenticationDeadline)
	defer cancel()
	if s == nil || s.manager != m {
		return Snapshot{}, store.AuthenticationFailed
	}
	if err := s.lockAuthentication(ctx); err != nil {
		return Snapshot{}, err
	}
	defer func() { <-s.authGate }()
	var snapshot Snapshot
	var b store.BindingRecord
	var c store.CredentialRecord
	var expired bool
	var rejected bool
	code, err := m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, view store.CommitView) (store.TransitionResult, error) {
		var authErr error
		b, c, expired, authErr = m.authenticate(ctx, tx, s, a)
		if authErr != nil {
			if authErr == store.AuthenticationFailed {
				rejected = true
				return store.TransitionResult{Changed: true, Code: store.AuthenticationFailed}, nil
			}
			return store.TransitionResult{}, authErr
		}
		if err := m.guard(ctx, tx, b.ID); err != nil {
			return store.TransitionResult{}, err
		}
		if !m.owned(s) || m.expired(s, m.now()) {
			rejected = true
			return store.TransitionResult{Changed: true, Code: store.AuthenticationFailed}, nil
		}
		snapshot = Snapshot{view.Epoch, b.Generation, m.active(b.ID)}
		return store.TransitionResult{Changed: s.credentialID == "" || m.hasDueSockets()}, nil
	}, func(store.CommitView) {
		if expired {
			m.Invalidate(b.ID)
		} else if rejected {
			m.remove(s)
		} else {
			m.prune()
			if !m.owned(s) {
				rejected = true
				return
			}
			m.bind(s, b, c)
		}
	})
	if err == nil && code != "" {
		err = code
	}
	if err == nil && rejected {
		err = store.AuthenticationFailed
	}
	if err != nil {
		if (rejected || err == store.AuthenticationFailed) && s != nil && s.manager == m {
			s.Close()
		}
		return Snapshot{}, err
	}
	return snapshot, nil
}
func (m *Manager) Attach(ctx context.Context, s *Socket, a Authentication, expected int64) (*Session, error) {
	ctx, cancel := context.WithTimeout(ctx, store.AuthenticationDeadline)
	defer cancel()
	if s == nil || s.manager != m {
		return nil, store.AuthenticationFailed
	}
	if err := s.lockAuthentication(ctx); err != nil {
		return nil, err
	}
	defer func() { <-s.authGate }()
	var b store.BindingRecord
	var c store.CredentialRecord
	var expired bool
	var rejected bool
	var result *Session
	code, err := m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, view store.CommitView) (store.TransitionResult, error) {
		var authErr error
		b, c, expired, authErr = m.authenticate(ctx, tx, s, a)
		if authErr != nil {
			if authErr == store.AuthenticationFailed {
				rejected = true
				return store.TransitionResult{Changed: true, Code: store.AuthenticationFailed}, nil
			}
			return store.TransitionResult{}, authErr
		}
		if err := m.guard(ctx, tx, b.ID); err != nil {
			return store.TransitionResult{}, err
		}
		if !m.owned(s) || m.expired(s, m.now()) {
			rejected = true
			return store.TransitionResult{Changed: true, Code: store.AuthenticationFailed}, nil
		}
		if s.session != nil && m.slots[b.ID] == s.session {
			result = s.session
			return store.TransitionResult{}, nil
		}
		if m.active(b.ID) {
			return store.TransitionResult{Changed: s.credentialID == "", Code: store.AlreadyConnected}, nil
		}
		if expected < 0 || b.Generation != expected {
			return store.TransitionResult{Changed: s.credentialID == "", Code: store.GenerationConflict}, nil
		}
		next, err := store.NextVersion(b.Generation)
		if err != nil {
			return store.TransitionResult{}, err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE bindings SET connection_generation=? WHERE binding_id=?", next, b.ID); err != nil {
			return store.TransitionResult{}, err
		}
		result = &Session{socket: s, token: Token{b.ID, b.PeerID, view.Epoch, c.Version, next, s.uid}}
		return store.TransitionResult{Changed: true}, nil
	}, func(store.CommitView) {
		if expired {
			m.Invalidate(b.ID)
		} else if rejected {
			m.remove(s)
		} else {
			m.prune()
			if !m.owned(s) {
				rejected = true
				return
			}
			m.bind(s, b, c)
			if result != nil {
				s.releaseCapacity()
				s.session = result
				m.slots[b.ID] = result
			}
		}
	})
	if err == nil && code != "" {
		err = code
	}
	if err == nil && rejected {
		err = store.AuthenticationFailed
	}
	if err != nil {
		if (rejected || err == store.AuthenticationFailed) && s != nil && s.manager == m {
			s.Close()
		}
		return nil, err
	}
	return result, nil
}
