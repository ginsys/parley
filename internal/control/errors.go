package control

import (
	"errors"

	"github.com/ginsys/parley/internal/store"
)

// RPCCode is a JSON-RPC 2.0 envelope-level error code: parse/profile/method/
// params/internal failures that precede any domain evaluation.
type RPCCode int

const (
	ParseError     RPCCode = -32700
	InvalidRequest RPCCode = -32600
	MethodNotFound RPCCode = -32601
	InvalidParams  RPCCode = -32602
	InternalError  RPCCode = -32603
	ServerError    RPCCode = -32000 // carries error.data.code, one of DomainCode below
)

// DomainCode is the control layer's error.data.code vocabulary. It embeds
// every store.Code value (an authenticated, admitted request can fail for
// any durable domain reason) plus a set of control-only additions that
// store.Code deliberately does not carry, because they describe wire/session
// failures with no meaning inside a store mutation: ProtocolMismatch
// (unsupported parley-control/1 version at hello), OperationNotFound
// (operation.get found no record for this admin's operation ID -- absence,
// not a rejection), and the remaining constants below, which the accepted
// control specification (docs/specifications/control.md's error-contract
// table) names but which have no store.Code counterpart. Adding them here,
// rather than to store.Code, keeps the store's durable terminal-result
// vocabulary exactly what store.Code.terminalResult already enumerates.
// Naming a spelling here is not the same as implementing the method that
// returns it -- several of the wire-only additions below are not returned
// by any method wired in this PR.
//
// StaleGrantVersion/InvalidMembership/UnsupportedMembership moved to
// store.Code in PR2 (internal/store/connection_contract.go): they describe
// an actual store-mutation precondition failure, not a wire/session
// concern, and membership.* mutations need them to be valid, terminalResult
// codes so a rejection is durably audited and replayable like any other
// domain rejection. They remain accessible as DomainCode constants below
// (constant conversions of the store.Code values) purely so control-package
// callers keep one vocabulary to write against; store.Code(d).Valid() in
// DomainCode.valid() below already accepts them without a separate case.
// IncompatibleIdentifier stays control-only: no PR2 method returns it.
type DomainCode string

const (
	ProtocolMismatch  DomainCode = "protocol_mismatch"
	OperationNotFound DomainCode = "operation_not_found"

	// Wire-only additions from the accepted control specification's
	// error-contract table; no store.Code counterpart exists or is added.
	ResnapshotRequired     DomainCode = "resnapshot_required"
	SubscriptionConflict   DomainCode = "subscription_conflict"
	IncompatibleIdentifier DomainCode = "incompatible_identifier"

	// Constant conversions of the store.Code values of the same name (see
	// the type doc comment above) -- not new wire-only spellings.
	StaleGrantVersion     = DomainCode(store.StaleGrantVersion)
	InvalidMembership     = DomainCode(store.InvalidMembership)
	UnsupportedMembership = DomainCode(store.UnsupportedMembership)
)

// valid reports whether d is a member of the accepted error.data.code
// vocabulary this type's own doc comment describes: control's wire/
// session-only additions, or any durable store.Code value. The empty string
// is store.Code's own internal absent/success marker, not a wire error
// code, and is rejected outright regardless of what store.Code("").Valid()
// would otherwise report. A peer is never trusted to introduce an arbitrary
// domain code merely by sending one that happens to be nonblank (mandate
// CP-03).
func (d DomainCode) valid() bool {
	if d == "" {
		return false
	}
	switch d {
	case ProtocolMismatch, OperationNotFound,
		ResnapshotRequired, SubscriptionConflict, IncompatibleIdentifier:
		return true
	}
	// Covers StaleGrantVersion/InvalidMembership/UnsupportedMembership
	// (now store.Code values, see the type doc comment) and every other
	// durable domain code.
	return store.Code(d).Valid()
}

// domainCode converts a store.Code into the wire's error.data.code string.
// It never leaks a raw Go error string: an unrecognized/unwrapped error
// becomes TemporarilyUnavailable's fixed spelling instead.
func domainCode(err error) DomainCode {
	var code store.Code
	if errors.As(err, &code) && code != "" {
		return DomainCode(code)
	}
	var dc DomainCode
	if errors.As(err, &dc) && dc != "" {
		return dc
	}
	return DomainCode(store.TemporarilyUnavailable)
}

func (d DomainCode) Error() string { return string(d) }
