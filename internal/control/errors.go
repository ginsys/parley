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
// any durable domain reason) plus two control-only additions that store.Code
// deliberately does not carry, because they describe wire/session failures
// with no meaning inside a store mutation: ProtocolMismatch (unsupported
// parley-control/1 version at hello) and OperationNotFound (operation.get
// found no record for this admin's operation ID -- absence, not a
// rejection). Adding them here, rather than to store.Code, keeps the
// store's durable terminal-result vocabulary exactly what
// store.Code.terminalResult already enumerates.
type DomainCode string

const (
	ProtocolMismatch  DomainCode = "protocol_mismatch"
	OperationNotFound DomainCode = "operation_not_found"
)

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
