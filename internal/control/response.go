package control

import "encoding/json"

// Response is one JSON-RPC response object: exactly one of Result or Err
// is ever set, matching the profile's "never include both result and
// error" rule. ID is nil only for the rare -32600 whose own id could not
// be safely echoed.
type Response struct {
	ID     *string
	Result any
	Err    *wireError
}

type wireError struct {
	Code    RPCCode    `json:"code"`
	Message string     `json:"message"`
	Data    *errorData `json:"data,omitempty"`
}

type errorData struct {
	Code DomainCode `json:"code"`
}

// wireResponse is Response's exact JSON-RPC envelope shape.
type wireResponse struct {
	JSONRPC string     `json:"jsonrpc"`
	ID      *string    `json:"id"`
	Result  any        `json:"result,omitempty"`
	Error   *wireError `json:"error,omitempty"`
}

// Encode renders r as the exact bytes of one response frame, without the
// terminating LF (the caller appends that when writing).
func (r Response) Encode() ([]byte, error) {
	return json.Marshal(wireResponse{JSONRPC: "2.0", ID: r.ID, Result: r.Result, Error: r.Err})
}

func successResponse(id string, result any) Response {
	return Response{ID: &id, Result: result}
}

// envelopeErrorResponse builds a JSON-RPC envelope-level error response
// (-32700/-32600/-32601/-32602/-32603). These never carry error.data:
// only domain (-32000) errors do.
func envelopeErrorResponse(code RPCCode, id *string) Response {
	return Response{ID: id, Err: &wireError{Code: code, Message: envelopeMessage(code)}}
}

// domainErrorResponse builds a -32000 domain error response with a fixed,
// safe message drawn from the domain code -- never a raw error string or
// transport detail.
func domainErrorResponse(id *string, code DomainCode) Response {
	return Response{ID: id, Err: &wireError{Code: ServerError, Message: domainMessage(code), Data: &errorData{Code: code}}}
}

// responseForViolation converts an envelope Violation (see classifyEnvelope)
// into its wire response.
func responseForViolation(v *Violation) Response {
	return envelopeErrorResponse(v.RPC, v.ID)
}

func envelopeMessage(code RPCCode) string {
	switch code {
	case ParseError:
		return "Parse error."
	case InvalidRequest:
		return "Invalid request."
	case MethodNotFound:
		return "Method not found."
	case InvalidParams:
		return "Invalid params."
	default:
		return "Internal error."
	}
}

// domainMessage returns a fixed, safe human-readable summary for a domain
// code, mirroring the "Consequence" column of
// docs/specifications/control.md's error contract table. It never
// includes caller input, paths or raw storage errors.
func domainMessage(code DomainCode) string {
	switch code {
	case ProtocolMismatch:
		return "Unsupported protocol version."
	case OperationNotFound:
		return "No record exists for that operation ID."
	case DomainCode("forbidden"):
		return "Not permitted."
	case DomainCode("capacity_exceeded"):
		return "Capacity exceeded."
	case DomainCode("temporarily_unavailable"):
		return "Temporarily unavailable."
	case DomainCode("recovery_required"):
		return "Recovery is required before this operation."
	case DomainCode("outcome_unknown"):
		// store.Coordinator.Execute's own OutcomeUnknown terminal result
		// (internal/store/coordinator.go): the server itself could not
		// determine whether its commit took effect. "Request rejected." is
		// actively wrong here -- it asserts a proven failure the server
		// never established -- so this needs its own fixed, safe summary
		// distinct from every other domain rejection below, without adding
		// a new field to the terminal-error envelope schema.
		return "Outcome unknown: the server could not confirm whether this request took effect."
	default:
		return "Request rejected."
	}
}
