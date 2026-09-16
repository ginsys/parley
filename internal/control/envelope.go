package control

import "strings"

// Request is one classified, envelope-valid parley-control/1 request. Params
// is the named-parameter object; method-specific field validation (the
// -32602 invalid-params case) is each method handler's responsibility, not
// envelope classification.
type Request struct {
	ID     string
	Method string
	Params map[string]any
}

// Violation is an envelope/profile-level rejection: a JSON-RPC envelope
// error, never a domain (-32000) error. ID is the id to echo in the error
// response, nil meaning JSON null -- either because the request's own id
// could not be safely echoed, or because there was no ID-bearing envelope
// to begin with (parse error, top-level array/batch).
type Violation struct {
	RPC RPCCode
	ID  *string
}

func (v *Violation) Error() string { return "control: envelope violation" }

// classifyEnvelope applies control.md's precedence rule: an ID-less object
// is classified first and always follows the no-response close rule, even
// if every other envelope field is invalid; only an ID-bearing object
// proceeds to full envelope validation. The three return shapes are:
//
//   - (req, nil, false):   a valid ID-bearing envelope; dispatch req.
//   - (_, nil, true):      an ID-less top-level object; send no response,
//     close per the profile's bounded-diagnostic rule.
//   - (_, violation, false): a parse error, a non-object top level (batch),
//     or an ID-bearing envelope that fails validation; respond with the
//     JSON-RPC error the Violation describes.
func classifyEnvelope(frame []byte) (Request, *Violation, bool) {
	value, err := parseJSON(frame)
	if err != nil {
		return Request{}, &Violation{RPC: ParseError}, false
	}
	obj, ok := value.(map[string]any)
	if !ok {
		// A top-level array, string, number, bool or null is never the
		// ID-less-object exception (that requires an actual object); it is
		// a batch/profile violation instead.
		return Request{}, &Violation{RPC: InvalidRequest}, false
	}
	if _, hasID := obj["id"]; !hasID {
		return Request{}, nil, true
	}
	// From here the envelope is ID-bearing: every failure is -32600, echoing
	// the id only when it is itself a validly-shaped, safely-echoable string.
	echoable := func() *string {
		if s, ok := obj["id"].(string); ok && validCorrelationID(s) {
			return &s
		}
		return nil
	}
	const jsonrpcKey, idKey, methodKey, paramsKey = "jsonrpc", "id", "method", "params"
	allowed := map[string]bool{jsonrpcKey: true, idKey: true, methodKey: true, paramsKey: true}
	for k := range obj {
		if !allowed[k] {
			return Request{}, &Violation{RPC: InvalidRequest, ID: echoable()}, false
		}
	}
	for _, k := range []string{jsonrpcKey, methodKey, paramsKey} {
		if _, ok := obj[k]; !ok {
			return Request{}, &Violation{RPC: InvalidRequest, ID: echoable()}, false
		}
	}
	idStr, ok := obj[idKey].(string)
	if !ok || !validCorrelationID(idStr) {
		// The id itself is the malformed field: it cannot be echoed.
		return Request{}, &Violation{RPC: InvalidRequest}, false
	}
	if jsonrpc, ok := obj[jsonrpcKey].(string); !ok || jsonrpc != "2.0" {
		return Request{}, &Violation{RPC: InvalidRequest, ID: &idStr}, false
	}
	method, ok := obj[methodKey].(string)
	if !ok || !validMethodSyntax(method) {
		return Request{}, &Violation{RPC: InvalidRequest, ID: &idStr}, false
	}
	params, ok := obj[paramsKey].(map[string]any)
	if !ok {
		// Missing, non-object (positional-array) or scalar params.
		return Request{}, &Violation{RPC: InvalidRequest, ID: &idStr}, false
	}
	return Request{ID: idStr, Method: method, Params: params}, nil, false
}

// validCorrelationID: 1-64 printable ASCII bytes (0x20-0x7E), not entirely
// whitespace. Also used for id echoability.
func validCorrelationID(s string) bool {
	if len(s) < 1 || len(s) > 64 {
		return false
	}
	blank := true
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c > 0x7E {
			return false
		}
		if c != ' ' {
			blank = false
		}
	}
	return !blank
}

// validMethodSyntax: 1-64 ASCII letters/digits/dots/underscores,
// case-sensitive, never starting with the reserved "rpc." prefix.
func validMethodSyntax(m string) bool {
	if len(m) < 1 || len(m) > 64 {
		return false
	}
	for i := 0; i < len(m); i++ {
		c := m[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '.', c == '_':
		default:
			return false
		}
	}
	return !strings.HasPrefix(m, "rpc.")
}
