package store

import (
	"errors"
	"math"
	"time"

	"github.com/google/uuid"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Connection contract constants. These are not evidence of a live host integration.
const (
	MaxIdentityBytes       = 256
	MaxLocatorBytes        = 4096
	AuthenticationDeadline = 5 * time.Second
	ReadinessDeadline      = 30 * time.Second
	HeartbeatInterval      = 10 * time.Second
	LivenessDeadline       = 30 * time.Second
	ReconnectRetries       = 3
	ReconnectInterval      = time.Second
)

// MaxLegacyLocatorBytes bounds membership.revoke's own exact-key legacy
// conversation identifier (AGENTS.md's exact-key human revocation escape) --
// internal/control/membership.go's auditRepresentable, and this package's
// own "membership.revoke" resource-ID exception in Coordinator.execute
// (coordinator.go), are its only two call sites. EC-04 (2026-09-19 review)
// replaced an earlier reuse of MaxLocatorBytes -- a different field's bound,
// chosen for credential/target locators, with no connection to this
// identifier's own actual encoded-size constraints -- with this dedicated,
// derived constant.
//
// Derivation: a revoke's own store.CommandResult always carries exactly 4
// ResourceChange entries (see internal/control/membership.go's
// handleMembershipRevoke), each with ID set to the same conversation
// identifier -- so the identifier appears 4 times in every encoded revoke
// receipt: the mutation response, its durable operation_results/
// command_audit row, and every later operation.get replay of that row.
// Because this identifier is exempt from bridgetext.ValidateMetadata's
// printable-ASCII rule (unlike every other identity field in this
// codebase), it may contain '"', '\', '<', '>', '&' or a raw control byte --
// each of which encoding/json.Marshal's default HTML-safe escaping expands
// to a 6-byte "\uXXXX" sequence, the worst case for any single UTF-8 byte.
// A receipt carrying 4 copies of a MaxLegacyLocatorBytes-sized, maximally
// adversarial identifier therefore encodes to at most
// 4 * MaxLegacyLocatorBytes * 6 = 983,040 bytes for the identifier text
// alone at the value below, leaving 65,536 bytes inside
// internal/control.MaxFrameBytes (1 MiB, the wire profile's single-frame
// bound every mutation response and operation.get reply must fit within)
// for the rest of the envelope (audit_id, commit_view, other resource
// fields, JSON-RPC framing), which is a few hundred bytes. The request side
// is strictly looser: it carries the identifier once (at most 6x expanded)
// in its own 1 MiB frame, and NewCommandRequest's 1 MiB digest budget counts
// it once, unescaped. internal/control's
// TestLegacyConversationBoundStaysWithinFrameLimit constructs exactly this
// adversarial receipt against a real Response.Encode() and asserts it
// fits, so a future change to the resource count, the escaping assumption
// or MaxFrameBytes itself fails that test rather than silently
// invalidating this comment.
//
// Owner decision 2026-09-19 (review 5257641949): this value is the
// frame-derived ceiling itself, not a smaller policy cutoff. An earlier
// 4096 was a deliberately conservative choice ~10x under it, which stranded
// any adopted legacy grant whose key fell between the two with no exact-key
// revocation path at all, for no encoding reason -- the historical schema
// imposes no length limit. An identifier past this bound still cannot be
// revoked through this path, but only because its worst-case receipt
// genuinely cannot be carried in one frame.
const MaxLegacyLocatorBytes = 40960

// Code is safe for diagnostics. Never replace it with a transport/SQLite error string.
type Code string

const (
	InvalidRequest         Code = "invalid_request"
	AuthenticationFailed   Code = "authentication_failed"
	NotFound               Code = "not_found"
	Forbidden              Code = "forbidden"
	IdentityConflict       Code = "identity_conflict"
	BindingUnavailable     Code = "binding_unavailable"
	HostUnverified         Code = "host_unverified"
	NotReady               Code = "not_ready"
	AlreadyConnected       Code = "already_connected"
	GenerationConflict     Code = "generation_conflict"
	VersionConflict        Code = "version_conflict"
	RequestExpired         Code = "request_expired"
	RequestTerminal        Code = "request_terminal"
	OperationConflict      Code = "operation_conflict"
	EventConflict          Code = "event_conflict"
	SecurityHold           Code = "security_hold"
	RecoveryRequired       Code = "recovery_required"
	CapacityExceeded       Code = "capacity_exceeded"
	TemporarilyUnavailable Code = "temporarily_unavailable"
	OutcomeUnknown         Code = "outcome_unknown"

	// The five codes below back PR2's membership.* mutations. They were
	// named in internal/control/errors.go as "wire-only" additions before
	// any method that could return them was wired; unlike that package's
	// remaining wire-only codes (protocol_mismatch, operation_not_found,
	// resnapshot_required, subscription_conflict), these five describe an
	// actual store-mutation precondition failure, not a wire/session
	// concern, so they must be valid, terminalResult codes here to satisfy
	// the accepted control specification's audited/replayable terminal-
	// rejection contract (docs/specifications/control.md "Command
	// atomicity, idempotency and audit": a terminal rejection records its
	// receipt/audit, and a same-ID retry returns that receipt rather than
	// reevaluating now-stale preconditions).
	StaleGrantVersion     Code = "stale_grant_version"
	InvalidMembership     Code = "invalid_membership"
	UnsupportedMembership Code = "unsupported_membership"
	NoActiveGrant         Code = "no_active_grant"
	AlreadyActive         Code = "already_active"

	// IncompatibleIdentifier promotes the wire-only spelling
	// "incompatible_identifier" (previously control-only, per
	// internal/control/errors.go's DomainCode of the same name) into a
	// genuine durable store.Code: internal/controller's validatePeerIDs
	// (the RenewTx path, checking a conversation's already-stored,
	// historical peer IDs -- never the invalid_membership pre-admission
	// check on freshly supplied peer IDs, which stays unaudited by design)
	// needed a terminal, replayable rejection for a legacy/historical peer
	// ID that fails today's ASCII-compatibility rule, instead of degrading
	// to a plain wrapped error that store.Coordinator.Execute's own error
	// classification turns into TemporarilyUnavailable -- discarding the
	// specific, audited reason and implying a transient condition a retry
	// might resolve, which this is not.
	IncompatibleIdentifier Code = "incompatible_identifier"
)

func (c Code) Error() string { return string(c) }

// Valid reports whether c is a member of the durable domain-code
// vocabulary (including the empty/absent value). Exported so a caller
// outside this package -- internal/control's wire error.data.code
// validation -- can check a code it received from a peer against exactly
// the same vocabulary this package enforces internally, rather than
// duplicating or drifting from this switch.
func (c Code) Valid() bool { return c.valid() }

func (c Code) valid() bool {
	switch c {
	case "", InvalidRequest, AuthenticationFailed, NotFound, Forbidden, IdentityConflict, BindingUnavailable, HostUnverified, NotReady, AlreadyConnected, GenerationConflict, VersionConflict, RequestExpired, RequestTerminal, OperationConflict, EventConflict, SecurityHold, RecoveryRequired, CapacityExceeded, TemporarilyUnavailable, OutcomeUnknown,
		StaleGrantVersion, InvalidMembership, UnsupportedMembership, NoActiveGrant, AlreadyActive, IncompatibleIdentifier:
		return true
	}
	return false
}

// Only terminal domain rejections are permanent operation results. Transient,
// infrastructure and pre-principal failures must roll back the entire command.
func (c Code) terminalResult() bool {
	switch c {
	case "", InvalidRequest, NotFound, Forbidden, IdentityConflict, BindingUnavailable, HostUnverified, NotReady, AlreadyConnected, GenerationConflict, VersionConflict, RequestExpired, RequestTerminal, OperationConflict, EventConflict, SecurityHold,
		StaleGrantVersion, InvalidMembership, UnsupportedMembership, NoActiveGrant, AlreadyActive, IncompatibleIdentifier:
		return true
	}
	return false
}

func validUUID(s string) bool {
	id, err := uuid.Parse(s)
	return err == nil && id != uuid.Nil && id.String() == s
}

// NextVersion never lets SQLite promote an overflowing INTEGER to REAL.
func NextVersion(current int64) (int64, error) {
	if current < 0 || current == math.MaxInt64 {
		return 0, InvalidRequest
	}
	return current + 1, nil
}

// InstantNanos rejects the wraparound permitted by time.Time.UnixNano.
func InstantNanos(t time.Time) (int64, error) {
	n := t.UnixNano()
	if !time.Unix(0, n).Equal(t) {
		return 0, InvalidRequest
	}
	return n, nil
}
func storageCode(err error) error {
	var code Code
	if errors.As(err, &code) && code.valid() && code != "" {
		return code
	}
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) && sqliteErr.Code()&255 == sqlite3.SQLITE_FULL {
		return CapacityExceeded
	}
	return TemporarilyUnavailable
}
