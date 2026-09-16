package control

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/ginsys/parley/internal/store"
)

const testHelloAdmin = "40000000-0000-4000-8000-000000000001"
const testHelloOtherAdmin = "40000000-0000-4000-8000-000000000002"
const testHandleOperation = "50000000-0000-4000-8000-000000000001"

// controlTestDB opens a private, migrated store.DB for one test, mirroring
// internal/store's own test fixtures but built only from control's public
// dependency surface (control cannot reach store's unexported test
// helpers across the package boundary).
func controlTestDB(t *testing.T) *store.DB {
	t.Helper()
	db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func allowedCommand(context.Context, *sql.Tx) error { return nil }

func insertSyntheticCommand(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
	_, err := tx.ExecContext(ctx, "INSERT INTO conversations(id,name,created_at) VALUES('synthetic','synthetic','2026-01-01T00:00:00Z')")
	return store.CommandResult{}, err
}

func testServer(t *testing.T) *Server {
	t.Helper()
	cfg, err := NewConfig("/run/parley/admin.sock", 1000, map[string]uint32{
		testHelloAdmin:      1001,
		testHelloOtherAdmin: 1002,
	})
	if err != nil {
		t.Fatal(err)
	}
	return NewServer(cfg, store.Queries{}, "server-id-fixture", "epoch-fixture", StateRunning)
}

func decodeResult[T any](t *testing.T, resp Response) T {
	t.Helper()
	raw, err := json.Marshal(resp.Result)
	if err != nil {
		t.Fatal(err)
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestServerIdentifyPeerResolvesConfiguredUID(t *testing.T) {
	srv := testServer(t)
	id, ok := srv.IdentifyPeer(1001)
	if !ok || id.PrincipalID != testHelloAdmin {
		t.Fatalf("id=%#v ok=%v", id, ok)
	}
	if _, ok := srv.IdentifyPeer(9999); ok {
		t.Fatal("unconfigured UID resolved")
	}
}

func TestHandleRejectsAnyMethodBeforeHello(t *testing.T) {
	srv := testServer(t)
	sess := srv.NewSession(Identity{PrincipalID: testHelloAdmin, UID: 1001})
	req := Request{ID: "1", Method: "operation.get", Params: map[string]any{"operation_id": "40000000-0000-4000-8000-000000000099"}}
	resp, closeAfter := sess.Handle(context.Background(), req)
	if closeAfter {
		t.Fatal("unexpected close")
	}
	if resp.Err == nil || resp.Err.Code != ServerError || resp.Err.Data == nil || resp.Err.Data.Code != DomainCode(store.Forbidden) {
		t.Fatalf("%#v", resp.Err)
	}
}

func TestHandleHelloSucceedsAndNegotiates(t *testing.T) {
	srv := testServer(t)
	sess := srv.NewSession(Identity{PrincipalID: testHelloAdmin, UID: 1001})
	req := Request{ID: "1", Method: "server.hello", Params: map[string]any{"protocol": ProtocolVersion}}
	resp, closeAfter := sess.Handle(context.Background(), req)
	if closeAfter || resp.Err != nil {
		t.Fatalf("err=%#v close=%v", resp.Err, closeAfter)
	}
	result := decodeResult[HelloResult](t, resp)
	if result.Protocol != ProtocolVersion || result.AdministratorID != testHelloAdmin || result.State != string(StateRunning) {
		t.Fatalf("%#v", result)
	}
	if len(result.Methods) != len(ImplementedMethods) {
		t.Fatalf("%#v", result.Methods)
	}
	if !sess.negotiated {
		t.Fatal("session not marked negotiated")
	}
	// Now that hello succeeded, a post-negotiation unimplemented method is
	// method-not-found, not the pre-negotiation forbidden case.
	resp, _ = sess.Handle(context.Background(), Request{ID: "2", Method: "state.snapshot", Params: map[string]any{}})
	if resp.Err == nil || resp.Err.Code != MethodNotFound {
		t.Fatalf("%#v", resp.Err)
	}
}

func TestHandleHelloRejectsUnsupportedProtocolAndSignalsClose(t *testing.T) {
	srv := testServer(t)
	sess := srv.NewSession(Identity{PrincipalID: testHelloAdmin, UID: 1001})
	req := Request{ID: "1", Method: "server.hello", Params: map[string]any{"protocol": "parley-control/99"}}
	resp, closeAfter := sess.Handle(context.Background(), req)
	if !closeAfter {
		t.Fatal("expected close after protocol_mismatch")
	}
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != ProtocolMismatch {
		t.Fatalf("%#v", resp.Err)
	}
	if sess.negotiated {
		t.Fatal("session negotiated on a rejected hello")
	}
}

func TestHandleHelloRejectsMalformedParams(t *testing.T) {
	srv := testServer(t)
	sess := srv.NewSession(Identity{PrincipalID: testHelloAdmin, UID: 1001})
	for _, params := range []map[string]any{
		{},
		{"protocol": ProtocolVersion, "extra": 1},
		{"protocol": 5},
		{"wrong_key": ProtocolVersion},
	} {
		resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "server.hello", Params: params})
		if resp.Err == nil || resp.Err.Code != InvalidParams {
			t.Fatalf("params %#v: %#v", params, resp.Err)
		}
	}
}

func TestHandleOperationGetRejectsMalformedParams(t *testing.T) {
	srv := testServer(t)
	sess := srv.NewSession(Identity{PrincipalID: testHelloAdmin, UID: 1001})
	sess.negotiated = true
	for _, params := range []map[string]any{
		{},
		{"operation_id": "not-a-uuid"},
		{"operation_id": "40000000-0000-4000-8000-000000000099", "extra": 1},
	} {
		resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "operation.get", Params: params})
		if resp.Err == nil || resp.Err.Code != InvalidParams {
			t.Fatalf("params %#v: %#v", params, resp.Err)
		}
	}
}

func TestHandleOperationGetEndToEndAndScoping(t *testing.T) {
	db := controlTestDB(t)
	ctx := context.Background()
	req, err := store.NewCommandRequest("binding.register", testHandleOperation, store.Field{Name: "peer_id", Value: "exact "})
	if err != nil {
		t.Fatal(err)
	}
	principal := store.CommandPrincipal{ID: testHelloAdmin, ConnectorUID: 1001}
	receipt, err := db.Coordinator().Execute(ctx, principal, req, allowedCommand, insertSyntheticCommand, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.OpenReaders(ctx); err != nil {
		t.Fatal(err)
	}
	cfg, err := NewConfig("/run/parley/admin.sock", 1000, map[string]uint32{testHelloAdmin: 1001, testHelloOtherAdmin: 1002})
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(cfg, db.Queries(), "server-id-fixture", "epoch-fixture", StateRunning)

	owner := srv.NewSession(Identity{PrincipalID: testHelloAdmin, UID: 1001})
	owner.negotiated = true
	resp, _ := owner.Handle(ctx, Request{ID: "1", Method: "operation.get", Params: map[string]any{"operation_id": testHandleOperation}})
	if resp.Err != nil {
		t.Fatalf("%#v", resp.Err)
	}
	result := decodeResult[OperationGetResult](t, resp)
	if result.OperationID != testHandleOperation || result.AuditID != receipt.AuditID || result.OperationKind != "binding.register" {
		t.Fatalf("%#v vs receipt %#v", result, receipt)
	}

	other := srv.NewSession(Identity{PrincipalID: testHelloOtherAdmin, UID: 1002})
	other.negotiated = true
	resp, _ = other.Handle(ctx, Request{ID: "2", Method: "operation.get", Params: map[string]any{"operation_id": testHandleOperation}})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != OperationNotFound {
		t.Fatalf("a different administrator must not see this operation record: %#v", resp.Err)
	}
}

func TestHandleOperationGetNotFound(t *testing.T) {
	srv := testServer(t)
	// A reader pool is required for this call; use a real empty DB.
	db := controlTestDB(t)
	if err := db.OpenReaders(context.Background()); err != nil {
		t.Fatal(err)
	}
	srv.Queries = db.Queries()
	sess := srv.NewSession(Identity{PrincipalID: testHelloAdmin, UID: 1001})
	sess.negotiated = true
	resp, _ := sess.Handle(context.Background(), Request{ID: "1", Method: "operation.get", Params: map[string]any{"operation_id": testHandleOperation}})
	if resp.Err == nil || resp.Err.Data == nil || resp.Err.Data.Code != OperationNotFound {
		t.Fatalf("%#v", resp.Err)
	}
}

func TestResponseEncodeNeverIncludesBothResultAndError(t *testing.T) {
	success := successResponse("1", map[string]string{"a": "b"})
	raw, err := success.Encode()
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, hasErr := decoded["error"]; hasErr {
		t.Fatal("success response carries an error field")
	}
	if _, hasResult := decoded["result"]; !hasResult {
		t.Fatal("success response missing result field")
	}

	failure := envelopeErrorResponse(InvalidParams, nil)
	raw, err = failure.Encode()
	if err != nil {
		t.Fatal(err)
	}
	decoded = nil
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, hasResult := decoded["result"]; hasResult {
		t.Fatal("error response carries a result field")
	}
	if string(decoded["id"]) != "null" {
		t.Fatalf("expected null id, got %s", decoded["id"])
	}
}

func TestEnvelopeErrorResponseNeverCarriesDomainData(t *testing.T) {
	for _, code := range []RPCCode{ParseError, InvalidRequest, MethodNotFound, InvalidParams, InternalError} {
		resp := envelopeErrorResponse(code, nil)
		if resp.Err.Data != nil {
			t.Fatalf("code %d carries data: %#v", code, resp.Err.Data)
		}
	}
}

func TestDomainErrorResponseAlwaysCarriesServerErrorCode(t *testing.T) {
	resp := domainErrorResponse(nil, ProtocolMismatch)
	if resp.Err.Code != ServerError || resp.Err.Data.Code != ProtocolMismatch {
		t.Fatalf("%#v", resp.Err)
	}
}
