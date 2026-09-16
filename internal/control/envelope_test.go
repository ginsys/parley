package control

import "testing"

func mustEchoID(t *testing.T, v *Violation, want string) {
	t.Helper()
	if v.ID == nil || *v.ID != want {
		t.Fatalf("expected echoed id %q, got %v", want, v.ID)
	}
}

func mustNoEcho(t *testing.T, v *Violation) {
	t.Helper()
	if v.ID != nil {
		t.Fatalf("expected no echoed id, got %q", *v.ID)
	}
}

func TestClassifyEnvelopeAcceptsValidRequest(t *testing.T) {
	req, v, idless := classifyEnvelope([]byte(`{"jsonrpc":"2.0","id":"abc","method":"server.hello","params":{}}`))
	if v != nil || idless {
		t.Fatalf("unexpected rejection: violation=%v idless=%v", v, idless)
	}
	if req.ID != "abc" || req.Method != "server.hello" || req.Params == nil {
		t.Fatalf("%#v", req)
	}
}

func TestClassifyEnvelopeIDLessObjectPrecedesAllOtherErrors(t *testing.T) {
	// Missing id AND an otherwise-invalid method AND wrong jsonrpc: still the
	// ID-less no-response case, not a -32600.
	_, v, idless := classifyEnvelope([]byte(`{"jsonrpc":"1.0","method":"rpc.bad","params":[1]}`))
	if !idless || v != nil {
		t.Fatalf("expected idless with no violation, got idless=%v violation=%v", idless, v)
	}
}

func TestClassifyEnvelopeParseErrorIsRPCParseError(t *testing.T) {
	_, v, idless := classifyEnvelope([]byte(`{not json`))
	if idless || v == nil || v.RPC != ParseError || v.ID != nil {
		t.Fatalf("got violation=%v idless=%v", v, idless)
	}
}

func TestClassifyEnvelopeTopLevelArrayIsBatchViolationNotIDLess(t *testing.T) {
	_, v, idless := classifyEnvelope([]byte(`[{"jsonrpc":"2.0","id":"x","method":"server.hello","params":{}}]`))
	if idless || v == nil || v.RPC != InvalidRequest || v.ID != nil {
		t.Fatalf("got violation=%v idless=%v", v, idless)
	}
}

func TestClassifyEnvelopeExtraFieldRejectedEchoingID(t *testing.T) {
	_, v, idless := classifyEnvelope([]byte(`{"jsonrpc":"2.0","id":"abc","method":"server.hello","params":{},"extra":1}`))
	if idless || v == nil || v.RPC != InvalidRequest {
		t.Fatalf("got violation=%v idless=%v", v, idless)
	}
	mustEchoID(t, v, "abc")
}

func TestClassifyEnvelopeMissingRequiredFieldRejectedEchoingID(t *testing.T) {
	for _, frame := range []string{
		`{"id":"abc","method":"server.hello","params":{}}`,     // missing jsonrpc
		`{"jsonrpc":"2.0","id":"abc","params":{}}`,             // missing method
		`{"jsonrpc":"2.0","id":"abc","method":"server.hello"}`, // missing params
	} {
		_, v, idless := classifyEnvelope([]byte(frame))
		if idless || v == nil || v.RPC != InvalidRequest {
			t.Fatalf("frame %s: got violation=%v idless=%v", frame, v, idless)
		}
		mustEchoID(t, v, "abc")
	}
}

func TestClassifyEnvelopeBadJSONRPCVersionRejectedEchoingID(t *testing.T) {
	_, v, idless := classifyEnvelope([]byte(`{"jsonrpc":"1.0","id":"abc","method":"server.hello","params":{}}`))
	if idless || v == nil || v.RPC != InvalidRequest {
		t.Fatalf("got violation=%v idless=%v", v, idless)
	}
	mustEchoID(t, v, "abc")
}

func TestClassifyEnvelopeInvalidIDNeverEchoed(t *testing.T) {
	for _, frame := range []string{
		`{"jsonrpc":"2.0","id":null,"method":"server.hello","params":{}}`,
		`{"jsonrpc":"2.0","id":5,"method":"server.hello","params":{}}`,
		`{"jsonrpc":"2.0","id":"","method":"server.hello","params":{}}`,
		`{"jsonrpc":"2.0","id":"   ","method":"server.hello","params":{}}`,
	} {
		_, v, idless := classifyEnvelope([]byte(frame))
		if idless || v == nil || v.RPC != InvalidRequest {
			t.Fatalf("frame %s: got violation=%v idless=%v", frame, v, idless)
		}
		mustNoEcho(t, v)
	}
}

func TestClassifyEnvelopeInvalidMethodSyntaxRejectedEchoingID(t *testing.T) {
	over64 := ""
	for range 65 {
		over64 += "a"
	}
	for _, method := range []string{"rpc.internal", "bad method", "", over64} {
		frame := `{"jsonrpc":"2.0","id":"abc","method":"` + method + `","params":{}}`
		_, v, idless := classifyEnvelope([]byte(frame))
		if idless || v == nil || v.RPC != InvalidRequest {
			t.Fatalf("method %q: got violation=%v idless=%v", method, v, idless)
		}
		mustEchoID(t, v, "abc")
	}
}

func TestClassifyEnvelopePositionalOrMissingParamsRejectedEchoingID(t *testing.T) {
	for _, frame := range []string{
		`{"jsonrpc":"2.0","id":"abc","method":"server.hello","params":[1,2]}`,
		`{"jsonrpc":"2.0","id":"abc","method":"server.hello","params":"x"}`,
		`{"jsonrpc":"2.0","id":"abc","method":"server.hello","params":null}`,
	} {
		_, v, idless := classifyEnvelope([]byte(frame))
		if idless || v == nil || v.RPC != InvalidRequest {
			t.Fatalf("frame %s: got violation=%v idless=%v", frame, v, idless)
		}
		mustEchoID(t, v, "abc")
	}
}

func TestValidCorrelationIDBounds(t *testing.T) {
	if validCorrelationID("") {
		t.Fatal("empty accepted")
	}
	if validCorrelationID("   ") {
		t.Fatal("blank accepted")
	}
	if !validCorrelationID("a") {
		t.Fatal("single char rejected")
	}
	if validCorrelationID(string(make([]byte, 65))) {
		t.Fatal("65 zero bytes accepted (also not printable ASCII)")
	}
	long := ""
	for range 64 {
		long += "a"
	}
	if !validCorrelationID(long) {
		t.Fatal("64 chars rejected")
	}
	if validCorrelationID(long + "a") {
		t.Fatal("65 chars accepted")
	}
}

func TestValidMethodSyntaxRejectsReservedPrefix(t *testing.T) {
	if validMethodSyntax("rpc.anything") {
		t.Fatal("rpc. prefix accepted")
	}
	if !validMethodSyntax("server.hello") {
		t.Fatal("valid method rejected")
	}
	if validMethodSyntax("has space") {
		t.Fatal("space accepted")
	}
}
