//go:build linux

package control

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/store"
)

// dialHelloAndCall dials a fresh connection, completes server.hello, then
// sends exactly one further request on the SAME connection and returns its
// decoded response -- the two-round-trip shape operation.get and every
// membership mutation actually require (TestListenerServiceRejectsRequestsBeforeHello),
// which dialAndRoundTrip's single-request-per-connection helper cannot
// exercise.
func dialHelloAndCall(t *testing.T, path string, serverUID uint32, jsonID, method string, params any) map[string]any {
	t.Helper()
	conn, err := connection.DialTrustedServer(context.Background(), path, serverUID)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(restartTeardownWait)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	hello := `{"jsonrpc":"2.0","id":"hello","method":"server.hello","params":{"protocol":"parley-control/1"}}`
	if _, err := conn.Write([]byte(hello + "\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadBytes('\n'); err != nil {
		t.Fatalf("hello round trip: %v", err)
	}
	line := buildRequestLine(t, jsonID, method, params)
	if _, err := conn.Write([]byte(line + "\n")); err != nil {
		t.Fatal(err)
	}
	respLine, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("%s round trip: %v", method, err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(respLine, &decoded); err != nil {
		t.Fatalf("response %q: %v", respLine, err)
	}
	return decoded
}

// dialHelloSendAndAbandon dials, completes server.hello, sends one further
// request, then closes the connection WITHOUT reading its response --
// simulating an administrator client that loses its connection after
// issuing a mutation, before ever learning whether it committed. This is
// what internal/control's client.Client already treats as an unknown
// outcome (see *control.TimeoutError's doc comment): the caller's only safe
// recourse is a same-operation-id retry, which is exactly what this fixture
// goes on to prove is safe.
func dialHelloSendAndAbandon(t *testing.T, path string, serverUID uint32, jsonID, method string, params any) {
	t.Helper()
	conn, err := connection.DialTrustedServer(context.Background(), path, serverUID)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(restartTeardownWait)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	hello := `{"jsonrpc":"2.0","id":"hello","method":"server.hello","params":{"protocol":"parley-control/1"}}`
	if _, err := conn.Write([]byte(hello + "\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadBytes('\n'); err != nil {
		t.Fatalf("hello round trip: %v", err)
	}
	line := buildRequestLine(t, jsonID, method, params)
	if _, err := conn.Write([]byte(line + "\n")); err != nil {
		t.Fatal(err)
	}
	// Deliberately no read here: the deferred Close severs the connection
	// before any response byte is consumed.
}

func buildRequestLine(t *testing.T, id, method string, params any) string {
	t.Helper()
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":%q,"params":%s}`, id, method, paramsJSON)
}

// pollUntilOperationRecorded is the explicit commit/delivery barrier this
// fixture needs after a simulated response loss: never a blind sleep, a
// bounded poll against the coordinator's own durable operation.get read
// path (the real evidence that the mutation actually committed), each
// attempt a fresh authenticated connection since operation.get is
// per-connection post-hello state. Fails the test if the operation is never
// recorded within restartTeardownWait, rather than hanging indefinitely.
func pollUntilOperationRecorded(t *testing.T, fx *listenerFixture, operationID string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(restartTeardownWait)
	for {
		resp := dialHelloAndCall(t, fx.socketPath, fx.serverUID, "poll", "operation.get", map[string]any{"operation_id": operationID})
		if resp["result"] != nil {
			return resp
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation %s was never durably recorded within %s: last response %#v", operationID, restartTeardownWait, resp)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestMembershipEnrollSurvivesResponseLossAndReplaysOnReconnect is the
// mandate's required real response-loss/reconnect evidence: a genuine Unix
// socket (internal/control's own Listener, not an in-process Session.Handle
// call), a mutation whose response is deliberately never read by the
// client, a bounded poll against operation.get as the actual commit
// evidence (not an assumption), then a fresh connection reusing the same
// operation_id to prove the coordinator's replay/conflict guarantees
// survive a real disconnect -- exactly the scenario a client that loses its
// connection after Coordinator.Execute commits, but before the response
// reaches it, actually faces.
func TestMembershipEnrollSurvivesResponseLossAndReplaysOnReconnect(t *testing.T) {
	fx := newListenerFixture(t)
	seedEnabledBinding(t, fx.db, 1, "peer-a")
	seedEnabledBinding(t, fx.db, 2, "peer-b")

	operationID := "90000000-0000-4000-8000-000000000001"
	params := map[string]any{
		"operation_id":           operationID,
		"conversation":           "conv-reconnect",
		"expected_grant_version": "0",
		"max_exchanges":          "5",
		"members": []any{
			map[string]any{"peer_id": "peer-a", "role": "member"},
			map[string]any{"peer_id": "peer-b", "role": "member"},
		},
		"policy": map[string]any{"kind": "open"},
	}

	// Step 1: send the mutation, then lose the connection before reading
	// any response -- an unknown outcome from the client's point of view.
	dialHelloSendAndAbandon(t, fx.socketPath, fx.serverUID, "1", "membership.enroll", params)

	// Step 2: the explicit barrier. Only once operation.get confirms the
	// operation is durably recorded do we know the mutation actually
	// committed (rather than merely having been sent).
	recorded := pollUntilOperationRecorded(t, fx, operationID)
	recordedResult, ok := recorded["result"].(map[string]any)
	if !ok {
		t.Fatalf("operation.get result shape: %#v", recorded)
	}
	if recordedResult["operation_kind"] != "membership.enroll" {
		t.Fatalf("operation.get recorded the wrong kind: %#v", recordedResult)
	}
	recordedAuditID, _ := recordedResult["audit_id"].(string)
	if recordedAuditID == "" {
		t.Fatalf("operation.get did not report an audit_id: %#v", recordedResult)
	}

	// Step 3: reconnect with a fresh connection, a different top-level
	// JSON-RPC id (proving the operation_id -- not the JSON-RPC id -- is
	// what identifies the operation), same operation_id and identical
	// fields. This must replay the exact recorded receipt, not re-run
	// GrantTx a second time (which would fail anyway: the conversation now
	// already has an active grant).
	replay := dialHelloAndCall(t, fx.socketPath, fx.serverUID, "reconnect-replay", "membership.enroll", params)
	if replay["error"] != nil {
		t.Fatalf("replay after reconnect failed: %#v", replay)
	}
	replayResult, ok := replay["result"].(map[string]any)
	if !ok {
		t.Fatalf("replay result shape: %#v", replay)
	}
	if replayResult["operation_id"] != operationID {
		t.Fatalf("replay echoed the wrong operation_id: %#v", replayResult)
	}
	if replayResult["audit_id"] != recordedAuditID {
		t.Fatalf("replay audit_id=%v, want the originally recorded %q (a second, distinct mutation ran)", replayResult["audit_id"], recordedAuditID)
	}

	// Step 4: a changed-input conflict on reconnect: the same operation_id
	// with a different field (budget) must be rejected as a conflicting
	// retry, not silently accepted or reprocessed as a fresh mutation.
	conflictParams := map[string]any{}
	for k, v := range params {
		conflictParams[k] = v
	}
	conflictParams["max_exchanges"] = "9"
	conflict := dialHelloAndCall(t, fx.socketPath, fx.serverUID, "reconnect-conflict", "membership.enroll", conflictParams)
	errObj, ok := conflict["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected a conflict error on reconnect with changed fields, got %#v", conflict)
	}
	data, _ := errObj["data"].(map[string]any)
	if data == nil || data["code"] != string(DomainCode(store.OperationConflict)) {
		t.Fatalf("expected operation_conflict, got %#v", errObj)
	}

	// Principal scoping for operation.get's retrieval path is proven
	// in-process, not here: SO_PEERCRED ties an administrator identity to a
	// real process UID, and every connection opened by this test process
	// shares that one real UID, so a second, genuinely distinct
	// administrator cannot be represented over a real socket within one
	// test process. TestHandleOperationGetEndToEndAndScoping (server_test.go)
	// is the actual evidence for that property, using synthetic
	// store.CommandPrincipal values directly.
}
