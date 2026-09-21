package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ginsys/parley/internal/store"
)

// ProtocolVersion is the only wire protocol this server negotiates.
// Unsupported versions produce protocol_mismatch and close; there is no
// silent downgrade.
const ProtocolVersion = "parley-control/1"

// Profile-constant socket/queue caps (docs/specifications/control.md's
// "Wire profile and limits" section). Changing them requires an advertised
// protocol capability revision, not a runtime configuration knob.
const (
	MaxSocketsPerAdministrator = 16
	MaxSocketsTotal            = 64
	MaxExecutingPerSocket      = 1
	MaxQueuedPerSocket         = 8
)

// ServerState is the endpoint's operational mode, echoed in server.hello.
type ServerState string

const (
	StateRunning      ServerState = "running"
	StateRecoveryOnly ServerState = "recovery_only"
)

// ImplementedMethods is the exact, exhaustive set of methods this PR1
// server advertises and dispatches. The full parley-control/1 method
// table (docs/specifications/control.md) defines many more; every one of
// them, until wired in a later change, is method-not-found here -- this
// package does not claim to implement them.
var ImplementedMethods = []string{
	"server.hello", "operation.get",
	"membership.enroll", "membership.renew", "membership.replace", "membership.revoke",
}

// isMutationMethod reports whether method durably commits through
// store.Coordinator.Execute -- the methods whose outcome a caller must treat
// as unknown, not absent, when no result reaches it (see serveSession).
func isMutationMethod(method string) bool {
	return strings.HasPrefix(method, "membership.")
}

// Identity is one session's negotiated administrator identity, resolved
// from the kernel-verified connecting UID via the server's configured
// administrator map. It is never derived from a request field.
type Identity struct {
	PrincipalID string // administrator UUID
	UID         uint32
}

// Server holds what dispatch needs across every session: the immutable
// administrator configuration (for the UID->principal reverse lookup),
// the reader pool for read methods, the owning writer's coordinator (for
// membership.* mutations) and this process's identity.
type Server struct {
	Config   Config
	Queries  store.Queries
	Store    *store.DB // the owning writer; membership.* mutations use its Coordinator()
	Now      func() time.Time
	ServerID string // installation.server_id: stable across restarts
	Epoch    string // minted once per process start; changes on restart
	State    ServerState

	byUID map[uint32]string // reverse of Config.administrators, built once

	// Credential-expiry persistence runs off the request path (see
	// persistExpiries): at most one goroutine at a time, rerun once more if
	// another request observed an expiry meanwhile.
	background    sync.WaitGroup
	expiryMu      sync.Mutex
	expiryRunning bool
	expiryAgain   bool
}

// persistExpiries durably records the credential expiries a membership
// handler observed, without holding that handler's response. Review
// 5256660570 (comment 4053958828): this write is a separate
// coordinator.Transition from the already-committed, already-audited
// mutation, so its failure must never overwrite that response. Review
// 5259563170 (comment 4056153935): store.ExpiryEvidence.Persist waits on the
// coordinator under its own fresh deadline, independent of caller
// cancellation as AGENTS.md requires, so running it inline could hold a
// response -- and its socket slot -- well past RequestDeadline. An
// observation is never lost by deferring it: recordExpiry already retained
// it in the coordinator's credentialExpiries map, which every Persist call
// retries, so a single in-flight goroutine covers concurrent observers.
func (s *Server) persistExpiries(e *store.ExpiryEvidence) {
	s.expiryMu.Lock()
	if s.expiryRunning {
		s.expiryAgain = true
		s.expiryMu.Unlock()
		return
	}
	s.expiryRunning = true
	s.background.Add(1)
	s.expiryMu.Unlock()
	go func() {
		defer s.background.Done()
		for {
			_ = e.Persist(s.Store, nil)
			s.expiryMu.Lock()
			if !s.expiryAgain {
				s.expiryRunning = false
				s.expiryMu.Unlock()
				return
			}
			s.expiryAgain = false
			s.expiryMu.Unlock()
		}
	}()
}

// WaitBackground blocks until expiry persistence started by finished
// requests has drained. Call it only while no request is in flight.
func (s *Server) WaitBackground() { s.background.Wait() }

// now returns Server.Now, defaulting to time.Now -- Now is injectable for
// tests, never required of a production caller.
func (s *Server) now() func() time.Time {
	if s.Now != nil {
		return s.Now
	}
	return time.Now
}

// NewServer builds a Server. serverID and epoch are resolved by the
// caller (cmd/parleyd) before construction; this package does not open or
// query the store beyond the injected Queries. writer is the same
// *store.DB runtime.Start already opened (Resources.Writer); membership.*
// handlers call writer.Coordinator().Execute directly, never a second
// coordinator or writer.
func NewServer(cfg Config, queries store.Queries, writer *store.DB, serverID, epoch string, state ServerState) *Server {
	admins := cfg.Administrators()
	byUID := make(map[uint32]string, len(admins))
	for id, uid := range admins {
		byUID[uid] = id
	}
	return &Server{Config: cfg, Queries: queries, Store: writer, ServerID: serverID, Epoch: epoch, State: state, byUID: byUID}
}

// IdentifyPeer resolves a kernel-verified UID to its configured
// administrator principal. false means the connecting UID is not a
// configured administrator; the caller must refuse the connection before
// this session can ever be negotiated.
func (s *Server) IdentifyPeer(uid uint32) (Identity, bool) {
	id, ok := s.byUID[uid]
	if !ok {
		return Identity{}, false
	}
	return Identity{PrincipalID: id, UID: uid}, true
}

// Session is one accepted connection's negotiation and dispatch state.
// It is not the listener; see listener_linux.go for socket handling,
// framing and output serialization.
type Session struct {
	server     *Server
	identity   Identity
	negotiated bool
}

// NewSession starts a session for an already kernel-authenticated
// administrator identity. No method may run before server.hello succeeds
// on this session.
func (s *Server) NewSession(identity Identity) *Session {
	return &Session{server: s, identity: identity}
}

// Handle dispatches one classified, envelope-valid request (see
// classifyEnvelope) and returns the response to write. closeAfter reports
// that the connection must be closed once this response has been
// written -- currently only true for hello's protocol_mismatch outcome.
// Handle never panics on a well-formed Request; a malformed params shape
// produces InvalidParams, not a crash.
func (sess *Session) Handle(ctx context.Context, req Request) (resp Response, closeAfter bool) {
	if !sess.negotiated && req.Method != "server.hello" {
		// "Every other request before hello fails." No capability is
		// disclosed by distinguishing this from any other authorization
		// failure.
		return domainErrorResponse(&req.ID, DomainCode(store.Forbidden)), false
	}
	switch req.Method {
	case "server.hello":
		return sess.handleHello(req)
	case "operation.get":
		return sess.handleOperationGet(ctx, req)
	case "membership.enroll", "membership.renew", "membership.replace", "membership.revoke":
		if sess.server.Store == nil {
			// Store is an ordinary field, not enforced non-nil by NewServer
			// -- server_test.go's testServer helper constructs one with a
			// nil Store for hello/operation.get-only fixtures, and this
			// method's own doc comment promises Handle never panics on a
			// well-formed request. A production Listener.Start always
			// passes its real writer (res.Writer), so this is defense in
			// depth against a misconfigured/test Server, not a reachable
			// production path.
			return domainErrorResponse(&req.ID, DomainCode(store.TemporarilyUnavailable)), false
		}
		switch req.Method {
		case "membership.enroll":
			return sess.handleMembershipEnroll(ctx, req)
		case "membership.renew":
			return sess.handleMembershipRenew(ctx, req)
		case "membership.replace":
			return sess.handleMembershipReplace(ctx, req)
		default:
			return sess.handleMembershipRevoke(ctx, req)
		}
	default:
		// Reachable only pre-negotiation would already have been caught
		// above; post-negotiation this is any method PR1 does not wire.
		return envelopeErrorResponse(MethodNotFound, &req.ID), false
	}
}

func (sess *Session) handleHello(req Request) (Response, bool) {
	protocol, ok := exactlyOneStringParam(req.Params, "protocol")
	if !ok {
		return envelopeErrorResponse(InvalidParams, &req.ID), false
	}
	if protocol != ProtocolVersion {
		return domainErrorResponse(&req.ID, ProtocolMismatch), true
	}
	sess.negotiated = true
	result := HelloResult{
		Protocol:        ProtocolVersion,
		ServerID:        sess.server.ServerID,
		ServerEpoch:     sess.server.Epoch,
		AdministratorID: sess.identity.PrincipalID,
		State:           string(sess.server.State),
		Limits: Limits{
			MaxFrameBytes:              MaxFrameBytes,
			MaxNestingDepth:            maxDepth,
			MaxSocketsPerAdministrator: MaxSocketsPerAdministrator,
			MaxSocketsTotal:            MaxSocketsTotal,
			MaxExecutingPerSocket:      MaxExecutingPerSocket,
			MaxQueuedPerSocket:         MaxQueuedPerSocket,
		},
		Methods: ImplementedMethods,
	}
	return successResponse(req.ID, result), false
}

func (sess *Session) handleOperationGet(ctx context.Context, req Request) (Response, bool) {
	operationID, ok := exactlyOneStringParam(req.Params, "operation_id")
	if !ok || !canonicalUUID(operationID) {
		return envelopeErrorResponse(InvalidParams, &req.ID), false
	}
	rec, err := sess.server.Queries.OperationRecord(ctx, sess.identity.PrincipalID, operationID)
	if err != nil {
		if errors.Is(err, store.ErrOperationNotFound) {
			return domainErrorResponse(&req.ID, OperationNotFound), false
		}
		return domainErrorResponse(&req.ID, domainCode(err)), false
	}
	resultJSON, err := recodeCommandResult(rec.ResultJSON)
	if err != nil {
		return envelopeErrorResponse(InternalError, &req.ID), false
	}
	result := OperationGetResult{
		OperationID:   operationID,
		OperationKind: rec.OperationKind,
		Result:        resultJSON,
		AuditID:       rec.AuditID,
		CommitView: CommitView{
			Epoch:    rec.CommitEpoch,
			Revision: strconv.FormatInt(rec.CommitRevision, 10),
		},
	}
	return successResponse(req.ID, result), false
}

// wireResourceChange mirrors store.ResourceChange but encodes Before/After
// as canonical decimal strings, per the profile's 64-bit codec
// (docs/specifications/control.md): store.ResourceChange keeps plain int64
// fields for safe internal Go round-trips, but republishing them as bare
// JSON numbers to an external client risks silent precision loss past
// 2^53 for a client that decodes into float64.
type wireResourceChange struct {
	Kind   string `json:"kind"`
	ID     string `json:"id"`
	Before string `json:"before"`
	After  string `json:"after"`
}

type wireCommandResult struct {
	Code      store.Code           `json:"code"`
	Resources []wireResourceChange `json:"resources"`
}

// recodeCommandResult reinterprets a stored store.CommandResult's raw JSON
// and re-encodes it with decimal-string Before/After fields instead of
// republishing the stored bytes verbatim.
func recodeCommandResult(raw string) (json.RawMessage, error) {
	var stored store.CommandResult
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		return nil, err
	}
	resources := make([]wireResourceChange, len(stored.Resources))
	for i, r := range stored.Resources {
		resources[i] = wireResourceChange{
			Kind:   r.Kind,
			ID:     r.ID,
			Before: strconv.FormatInt(r.Before, 10),
			After:  strconv.FormatInt(r.After, 10),
		}
	}
	return json.Marshal(wireCommandResult{Code: stored.Code, Resources: resources})
}

// exactlyOneStringParam reports the value of key when params contains
// exactly that one key and its value is a JSON string. Any other shape
// (extra fields, wrong type, missing key) is a params violation -- named
// parameters are validated per method, not merely decoded loosely.
func exactlyOneStringParam(params map[string]any, key string) (string, bool) {
	if len(params) != 1 {
		return "", false
	}
	v, ok := params[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// HelloResult is server.hello's result. Its exact field names are this
// package's implementation choice: docs/specifications/control.md
// specifies the required content (protocol, server/epoch/admin IDs,
// state, limits, methods) but not a JSON schema for hello specifically,
// unlike the mutation methods it fixes field names for.
type HelloResult struct {
	Protocol        string   `json:"protocol"`
	ServerID        string   `json:"server_id"`    // installation.server_id: stable across restarts
	ServerEpoch     string   `json:"server_epoch"` // minted once per process start
	AdministratorID string   `json:"administrator_id"`
	State           string   `json:"state"` // "running" or "recovery_only"
	Limits          Limits   `json:"limits"`
	Methods         []string `json:"methods"`
}

// validate reports whether h is a well-formed, negotiated hello result:
// the exact supported protocol, a real (non-zero-value) server/epoch/
// administrator identity, a known operational state, and sane profile
// limits. A response that already passed Call's own envelope/success
// checks can still carry a zero-value or otherwise nonsensical result (a
// bare `{"result":{}}` decodes into a HelloResult with every field at its
// zero value without error); Dial must not treat a merely well-formed
// JSON-RPC success as a completed protocol negotiation (mandate R5). A
// matching correlation ID alone is not proof of negotiation either.
func (h HelloResult) validate() error {
	if h.Protocol != ProtocolVersion {
		return fmt.Errorf("control: hello result claims unsupported protocol %q", h.Protocol)
	}
	if !canonicalUUID(h.ServerID) {
		return fmt.Errorf("control: hello result has an invalid server_id %q", h.ServerID)
	}
	if !canonicalUUID(h.AdministratorID) {
		return fmt.Errorf("control: hello result has an invalid administrator_id %q", h.AdministratorID)
	}
	if strings.TrimSpace(h.ServerEpoch) == "" {
		return errors.New("control: hello result is missing a required identity field")
	}
	if h.State != string(StateRunning) && h.State != string(StateRecoveryOnly) {
		return fmt.Errorf("control: hello result has an unknown state %q", h.State)
	}
	// parley-control/1 is a fixed profile, not a negotiated one: this
	// client has no per-field tolerance for a peer that advertises
	// different limits than its own build uses, since it would then be
	// framing/queuing against a profile the server does not actually honor
	// (mandate CP-11).
	if h.Limits.MaxFrameBytes != MaxFrameBytes || h.Limits.MaxNestingDepth != maxDepth ||
		h.Limits.MaxSocketsPerAdministrator != MaxSocketsPerAdministrator ||
		h.Limits.MaxSocketsTotal != MaxSocketsTotal || h.Limits.MaxExecutingPerSocket != MaxExecutingPerSocket ||
		h.Limits.MaxQueuedPerSocket != MaxQueuedPerSocket {
		return fmt.Errorf("control: hello result advertises limits incompatible with this client's fixed parley-control/1 profile: %+v", h.Limits)
	}
	if len(h.Methods) == 0 {
		return errors.New("control: hello result advertises no methods")
	}
	// Reuse the exact syntax the server itself enforces on incoming request
	// methods (envelope.go's validMethodSyntax), and reject a duplicate
	// advertisement outright -- a hello contract violation here means the
	// method table this client would dispatch against cannot be trusted
	// (mandate CP-10).
	seen := make(map[string]bool, len(h.Methods))
	for _, m := range h.Methods {
		if !validMethodSyntax(m) {
			return fmt.Errorf("control: hello result advertises an invalid method name %q", m)
		}
		if seen[m] {
			return fmt.Errorf("control: hello result advertises method %q more than once", m)
		}
		seen[m] = true
	}
	return nil
}

// Limits mirrors the profile constants advertised at hello.
type Limits struct {
	MaxFrameBytes              int `json:"max_frame_bytes"`
	MaxNestingDepth            int `json:"max_nesting_depth"`
	MaxSocketsPerAdministrator int `json:"max_sockets_per_administrator"`
	MaxSocketsTotal            int `json:"max_sockets_total"`
	MaxExecutingPerSocket      int `json:"max_executing_per_socket"`
	MaxQueuedPerSocket         int `json:"max_queued_per_socket"`
}

// CommitView is the durable commit position a receipt was recorded
// under: epoch plus decimal-string revision, never a signed snapshot
// token.
type CommitView struct {
	Epoch    string `json:"epoch"`
	Revision string `json:"revision"`
}

// OperationGetResult is operation.get's result for a found record. Result
// is NOT the stored ResultJSON bytes republished verbatim: handleOperationGet
// always passes them through recodeCommandResult first, which reconstructs
// the durable store.CommandResult and re-encodes its resource counters as
// canonical decimal strings (see wireResourceChange) per the profile's
// 64-bit codec. The durable stored representation and this wire
// representation are related but distinct; only the latter is what
// operation.get actually returns.
type OperationGetResult struct {
	OperationID   string          `json:"operation_id"`
	OperationKind string          `json:"operation_kind"`
	Result        json.RawMessage `json:"result"`
	AuditID       string          `json:"audit_id"`
	CommitView    CommitView      `json:"commit_view"`
}
