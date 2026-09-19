package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/control"
	"github.com/ginsys/parley/internal/membership"
	"github.com/ginsys/parley/internal/runtime"
	"github.com/ginsys/parley/internal/store"
)

// noEnv is passed to run in every test not exercising hello's or a
// membership subcommand's environment resolution, so an ambient
// PARLEY_ENDPOINT/PARLEY_SERVER_UID in the actual process environment can
// never leak into these tests.
func noEnv(string) string { return "" }

// fakeClient is the injected membershipClient every non-network membership
// test dispatches against -- the replacement for the removed fakeController
// seam from the legacy direct-database grant/revoke/renew path.
type fakeClient struct {
	method        string
	params        map[string]any
	err           error
	calls, closed int
	// emptyResult makes Call succeed (nil error, a real RPC round trip) but
	// leave *out at its zero value -- MC-01.D's regression fixture: a
	// structurally decoded but zero-valued/malformed receipt, distinct from
	// a transport-level err.
	emptyResult bool
}

func (f *fakeClient) Call(_ context.Context, method string, params map[string]any, out any) error {
	f.calls++
	f.method = method
	f.params = params
	if f.err != nil {
		return f.err
	}
	if f.emptyResult {
		return nil
	}
	if result, ok := out.(*control.CommandReceiptResult); ok {
		*result = control.CommandReceiptResult{
			AuditID:     "audit-1",
			OperationID: "operation-1",
			CommitView:  control.CommitView{Epoch: "epoch-1", Revision: "1"},
		}
	}
	return nil
}
func (f *fakeClient) Close() error { f.closed++; return nil }

func fakeDial(client *fakeClient) dialFunc {
	return func(context.Context, control.ClientConfig) (membershipClient, error) { return client, nil }
}

// fatalIfDialed is a dialFunc that fails the test if a membership subcommand
// ever dials before its own argument validation completes.
func fatalIfDialed(t *testing.T) dialFunc {
	return func(context.Context, control.ClientConfig) (membershipClient, error) {
		t.Fatal("dialed the control endpoint before argument validation")
		return nil, nil
	}
}

func TestHelpAndInvalidArgumentsNeverDial(t *testing.T) {
	tests := []struct {
		args []string
		code int
	}{
		{nil, 0}, {[]string{"help"}, 0}, {[]string{"-h"}, 0}, {[]string{"--help"}, 0},
		{[]string{"membership", "help"}, 0}, {[]string{"membership", "-h"}, 0},
		{[]string{"membership", "enroll", "--help"}, 0}, {[]string{"membership", "renew", "-h"}, 0},
		{[]string{"membership", "replace", "-h"}, 0}, {[]string{"membership", "revoke", "--help"}, 0},
		{[]string{"serve"}, 2}, {[]string{"help", "extra"}, 2},
		{[]string{"membership"}, 2}, {[]string{"membership", "unknown"}, 2},
		{[]string{"membership", "enroll"}, 2}, {[]string{"membership", "renew"}, 2},
		{[]string{"membership", "replace"}, 2}, {[]string{"membership", "revoke"}, 2},
		{[]string{"membership", "revoke", "-conversation", " "}, 2},
		{[]string{"membership", "revoke", "-conversation", "c", "-expected-grant-version", "1", "extra"}, 2},
		{[]string{"membership", "renew", "-conversation", "c", "-expected-grant-version", "1", "-max-exchanges", "-1"}, 2},
		{[]string{"membership", "renew", "-conversation", "c", "-expected-grant-version", "1", "-expires-in", "-1s"}, 2},
		{[]string{"membership", "renew", "-conversation", "c", "-expected-grant-version", "1", "-expires-in", "oops"}, 2},
		{[]string{"membership", "renew", "-conversation", "c", "-expected-grant-version", "1", "--unknown"}, 2},
		{[]string{"membership", "renew", "-conversation", "c", "-expected-grant-version", "0"}, 2},
		{[]string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "0"}, 2},
		{[]string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "a", "-max-exchanges", "1"}, 2},
		{[]string{"membership", "enroll", "-conversation", "c", "-peer-a", " ", "-peer-b", "b", "-max-exchanges", "1"}, 2},
		{[]string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-direction", "wrong"}, 2},
		{[]string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-expires-in", "-1s"}, 2},
		{[]string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-expected-grant-version", "-1"}, 2},
		{[]string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-operation-id", "not-a-uuid"}, 2},
		// Syntactically valid but no endpoint/server-uid configured anywhere:
		// ResolveClientConfig fails before dial is ever reached.
		{[]string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1"}, 2},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(tt.args, &stdout, &stderr, fatalIfDialed(t), noEnv); code != tt.code {
				t.Fatalf("exit=%d: %s %s", code, &stdout, &stderr)
			}
			if tt.code == 0 && (stdout.Len() == 0 || stderr.Len() != 0) {
				t.Fatalf("help streams: %q %q", &stdout, &stderr)
			}
			if tt.code == 2 && (stderr.Len() == 0 || stdout.Len() != 0) {
				t.Fatalf("error streams: %q %q", &stdout, &stderr)
			}
		})
	}
}

func TestUnsafePeerIdentifiersRejectedBeforeDialing(t *testing.T) {
	// The non-ASCII/invisible/directional-override fixtures use explicit
	// \uXXXX escapes rather than the raw characters themselves (a hosted
	// AI Code Review finding on an earlier PR2 candidate): a literal
	// U+202E RIGHT-TO-LEFT OVERRIDE or U+200B ZERO WIDTH SPACE in tracked
	// source is Trojan-Source-class -- it can reorder how the rest of the
	// line renders in an editor or review tool, and is silently
	// corruptible by any tool that normalizes or strips invisible
	// codepoints. "café" is the one intentionally-visible non-ASCII case
	// (a readable non-ASCII-byte rejection, not an invisible-character
	// one) and stays literal.
	for _, id := range []string{"a\xff", "a\xfe", "café", "a\ufffd", "peer\x7f", "peer\n", "peer\r", "peer\t", "peer\x00", "peer\u0085", "peer\u2028", "peer\u2029", "peer\u200b", "peer\u202e"} {
		for _, flag := range []string{"-conversation", "-peer-a", "-peer-b"} {
			t.Run(flag+id, func(t *testing.T) {
				args := []string{"membership", "enroll", "-conversation", "fixture", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "2", flag, id}
				var out, errOut bytes.Buffer
				if code := run(args, &out, &errOut, fatalIfDialed(t), noEnv); code != 2 {
					t.Fatalf("exit=%d: %s", code, &errOut)
				}
			})
		}
	}
}

// membershipEndpointArgs are appended to every test that must pass argument
// validation and reach dispatch -- the fake dialer never actually opens a
// socket, but ResolveClientConfig still requires syntactically valid values.
var membershipEndpointArgs = []string{"-endpoint", "/tmp/parleyctl-test.sock", "-server-uid", "1000"}

func TestMembershipRoutingUsesValidatedParametersAndClosesClient(t *testing.T) {
	for _, op := range []string{"enroll", "renew", "replace", "revoke"} {
		for _, fails := range []bool{false, true} {
			t.Run(op+map[bool]string{false: "", true: "_failure"}[fails], func(t *testing.T) {
				args := []string{"membership", op, "-conversation", "fixture"}
				switch op {
				case "enroll":
					args = append(args, "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "3", "-direction", "b_to_a", "-expires-in", "1h")
				case "renew":
					args = append(args, "-expected-grant-version", "2", "-cancel-pending-replies")
				case "replace":
					args = append(args, "-expected-grant-version", "3", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "5")
				case "revoke":
					args = append(args, "-expected-grant-version", "4")
				}
				args = append(args, membershipEndpointArgs...)

				fake := &fakeClient{}
				if fails {
					fake.err = errors.New("synthetic operation failure")
				}
				var out, errOut bytes.Buffer
				before := time.Now()
				code := run(args, &out, &errOut, fakeDial(fake), noEnv)
				expected := 0
				if fails {
					expected = 1
				}
				if code != expected || fake.calls != 1 || fake.closed != 1 {
					t.Fatalf("exit/call/close=%d/%d/%d", code, fake.calls, fake.closed)
				}
				if fails {
					if out.Len() != 0 || !strings.Contains(errOut.String(), "synthetic operation failure") {
						t.Fatalf("failure output: %q %q", &out, &errOut)
					}
					return
				}
				if out.Len() == 0 || errOut.Len() != 0 {
					t.Fatalf("success output: %q %q", &out, &errOut)
				}
				if fake.method != "membership."+op {
					t.Fatalf("method=%q", fake.method)
				}
				if fake.params["conversation"] != "fixture" {
					t.Fatalf("conversation=%v", fake.params["conversation"])
				}
				switch op {
				case "enroll":
					if fake.params["expected_grant_version"] != "0" || fake.params["max_exchanges"] != "3" {
						t.Fatalf("enroll params=%+v", fake.params)
					}
					checkMembersAndDirectedPolicy(t, fake.params, "b", "a")
					// expires_at loses sub-second precision through RFC3339's
					// seconds-only rendering (parseCommand's -expires-in
					// resolution), so the lower bound needs a one-second
					// slack against `before`.
					checkExpiresAt(t, fake.params, before.Add(time.Hour-time.Second), time.Now().Add(time.Hour))
				case "renew":
					if fake.params["expected_grant_version"] != "2" || fake.params["cancel_pending_replies"] != true {
						t.Fatalf("renew params=%+v", fake.params)
					}
					if _, present := fake.params["max_exchanges"]; present {
						t.Fatalf("renew sent max_exchanges despite the default 'keep current' value: %+v", fake.params)
					}
				case "replace":
					if fake.params["expected_grant_version"] != "3" || fake.params["max_exchanges"] != "5" || fake.params["cancel_pending_replies"] != false {
						t.Fatalf("replace params=%+v", fake.params)
					}
					checkMembersAndDirectedPolicy(t, fake.params, "", "") // open policy: no edge check needed
				case "revoke":
					if fake.params["expected_grant_version"] != "4" || len(fake.params) != 3 {
						t.Fatalf("revoke params=%+v", fake.params)
					}
				}
			})
		}
	}
}

// checkMembersAndDirectedPolicy asserts the canonical two-member list is
// present; if wantFrom/wantTo are nonempty it also asserts a one-edge
// directed policy with that exact edge (membership.FromGrant's translation).
func checkMembersAndDirectedPolicy(t *testing.T, params map[string]any, wantFrom, wantTo string) {
	t.Helper()
	members, ok := params["members"].([]any)
	if !ok || len(members) != 2 {
		t.Fatalf("members=%+v", params["members"])
	}
	ids := make([]string, 2)
	for i, m := range members {
		obj := m.(map[string]any)
		if obj["role"] != "member" {
			t.Fatalf("member role=%v", obj["role"])
		}
		ids[i] = obj["peer_id"].(string)
	}
	if ids[0] != "a" || ids[1] != "b" {
		t.Fatalf("members not canonically ordered: %v", ids)
	}
	if wantFrom == "" {
		return
	}
	policy := params["policy"].(map[string]any)
	if policy["kind"] != "directed" {
		t.Fatalf("policy=%+v", policy)
	}
	edges := policy["edges"].([]any)
	if len(edges) != 1 {
		t.Fatalf("edges=%+v", edges)
	}
	edge := edges[0].(map[string]any)
	if edge["from"] != wantFrom || edge["to"] != wantTo {
		t.Fatalf("edge=%+v, want %s->%s", edge, wantFrom, wantTo)
	}
}

func checkExpiresAt(t *testing.T, params map[string]any, lower, upper time.Time) {
	t.Helper()
	raw, ok := params["expires_at"].(string)
	if !ok {
		t.Fatalf("expires_at missing: %+v", params)
	}
	got, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("expires_at=%q: %v", raw, err)
	}
	if got.Before(lower) || got.After(upper) {
		t.Fatalf("expires_at=%v outside [%v,%v]", got, lower, upper)
	}
}

// TestMembershipExpiresAtValidation asserts the specific rejection reason
// for -expires-in/-expires-at misuse, not merely "some exit-2 error
// occurred": every case supplies membershipEndpointArgs so a config-
// resolution failure (also exit 2, via ResolveClientConfig) cannot mask a
// broken or missing parseCommand check -- without an endpoint configured,
// a test asserting only the exit code would still pass even if the
// mutual-exclusion/RFC3339 checks below were deleted entirely.
func TestMembershipExpiresAtValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"mutually_exclusive", []string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-expires-in", "1h", "-expires-at", "2030-01-01T00:00:00Z"}, "-expires-in and -expires-at are mutually exclusive"},
		{"bad_rfc3339", []string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-expires-at", "not-rfc3339"}, "-expires-at must be RFC3339"},
		{"renew_mutually_exclusive", []string{"membership", "renew", "-conversation", "c", "-expected-grant-version", "1", "-expires-in", "1h", "-expires-at", "2030-01-01T00:00:00Z"}, "-expires-in and -expires-at are mutually exclusive"},
		{"renew_bad_rfc3339", []string{"membership", "renew", "-conversation", "c", "-expected-grant-version", "1", "-expires-at", "not-rfc3339"}, "-expires-at must be RFC3339"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append(append([]string{}, tt.args...), membershipEndpointArgs...)
			var out, errOut bytes.Buffer
			if code := run(args, &out, &errOut, fatalIfDialed(t), noEnv); code != 2 {
				t.Fatalf("exit=%d: %s", code, &errOut)
			}
			if !strings.Contains(errOut.String(), tt.want) {
				t.Fatalf("stderr=%q, want substring %q", &errOut, tt.want)
			}
		})
	}
}

// TestMembershipExpiresAtUsedVerbatim exercises the retry-safe -expires-at
// path (as opposed to -expires-in's relative-to-now resolution, covered by
// TestMembershipRoutingUsesValidatedParametersAndClosesClient): the wire
// expires_at must equal the given absolute value exactly, not merely fall
// within a tolerance window, since a retry must reproduce the identical
// digest store.NewCommandRequest computed for the original attempt.
func TestMembershipExpiresAtUsedVerbatim(t *testing.T) {
	// The fractional-second case guards a real defect a Codex review found
	// in the first repair batch: reformatting the parsed value with
	// time.RFC3339 (no fractional spec) silently truncated sub-second
	// precision instead of reproducing the given instant exactly. The
	// fix reformats with time.RFC3339Nano.
	for _, want := range []string{"2030-06-15T12:00:00Z", "2030-06-15T12:00:00.123456789Z"} {
		for _, op := range []string{"enroll", "renew", "replace"} {
			t.Run(want+"/"+op, func(t *testing.T) {
				args := []string{"membership", op, "-conversation", "fixture", "-expires-at", want}
				switch op {
				case "enroll":
					args = append(args, "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1")
				case "renew":
					args = append(args, "-expected-grant-version", "1")
				case "replace":
					args = append(args, "-expected-grant-version", "1", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1")
				}
				args = append(args, membershipEndpointArgs...)
				fake := &fakeClient{}
				var out, errOut bytes.Buffer
				if code := run(args, &out, &errOut, fakeDial(fake), noEnv); code != 0 {
					t.Fatalf("exit=%d: %s", code, &errOut)
				}
				if fake.params["expires_at"] != want {
					t.Fatalf("expires_at=%v, want %q", fake.params["expires_at"], want)
				}
			})
		}
	}
}

func TestMembershipOperationIDDefaultsToFreshUUIDButExplicitValueUsedVerbatim(t *testing.T) {
	args := func(extra ...string) []string {
		a := []string{"membership", "revoke", "-conversation", "fixture", "-expected-grant-version", "1"}
		a = append(a, membershipEndpointArgs...)
		return append(a, extra...)
	}
	fake1, fake2 := &fakeClient{}, &fakeClient{}
	var out1, out2, errOut bytes.Buffer
	if code := run(args(), &out1, &errOut, fakeDial(fake1), noEnv); code != 0 {
		t.Fatalf("exit=%d: %s", code, &errOut)
	}
	if code := run(args(), &out2, &errOut, fakeDial(fake2), noEnv); code != 0 {
		t.Fatalf("exit=%d: %s", code, &errOut)
	}
	id1, id2 := fake1.params["operation_id"], fake2.params["operation_id"]
	if id1 == "" || id2 == "" || id1 == id2 {
		t.Fatalf("expected two distinct fresh operation IDs, got %v and %v", id1, id2)
	}

	explicit := "80000000-0000-4000-8000-000000000099"
	fake3 := &fakeClient{}
	var out3 bytes.Buffer
	if code := run(args("-operation-id", explicit), &out3, &errOut, fakeDial(fake3), noEnv); code != 0 {
		t.Fatalf("exit=%d: %s", code, &errOut)
	}
	if fake3.params["operation_id"] != explicit {
		t.Fatalf("operation_id=%v, want %s", fake3.params["operation_id"], explicit)
	}
	if !strings.Contains(out3.String(), explicit) {
		t.Fatalf("operation id not visible in output: %s", &out3)
	}
}

func TestCLIIdentifiersRemainExactAndVisible(t *testing.T) {
	for _, op := range []string{"enroll", "renew", "replace", "revoke"} {
		t.Run(op, func(t *testing.T) {
			args := []string{"membership", op, "-conversation", " x"}
			switch op {
			case "enroll":
				args = append(args, "-peer-a", "a", "-peer-b", "a ", "-max-exchanges", "1")
			case "renew":
				args = append(args, "-expected-grant-version", "1")
			case "replace":
				args = append(args, "-expected-grant-version", "1", "-peer-a", "a", "-peer-b", "a ")
			case "revoke":
				args = append(args, "-expected-grant-version", "1")
			}
			args = append(args, membershipEndpointArgs...)
			fake := &fakeClient{}
			var out, errOut bytes.Buffer
			if code := run(args, &out, &errOut, fakeDial(fake), noEnv); code != 0 {
				t.Fatalf("exit %d: %s", code, &errOut)
			}
			if !strings.Contains(out.String(), `" x"`) {
				t.Fatalf("identifier whitespace hidden: %s", &out)
			}
			if fake.params["conversation"] != " x" {
				t.Fatalf("conversation identity changed: %+v", fake.params)
			}
		})
	}
}

// TestMembershipRevokeKeepsExactMalformedKeyButOthersValidateFirst mirrors
// the legacy CLI's exact-key revocation guarantee (AGENTS.md: "Do not apply
// new-enrollment validation to that revocation path"): revoke never applies
// bridgetext.ValidateMetadata to -conversation, so a byte-malformed
// historical key is still dispatched unchanged, while every other
// subcommand -- which enrolls or requires an existing well-formed identity
// -- rejects the same key before ever dialing.
func TestMembershipRevokeKeepsExactMalformedKeyButOthersValidateFirst(t *testing.T) {
	// \ufffd is an explicit escape, not the literal replacement character --
	// see the identical rationale on TestUnsafePeerIdentifiersRejectedBeforeDialing
	// (a hosted AI Code Review finding on an earlier PR2 candidate applied
	// there; this second fixture list was missed by that same fix).
	for _, name := range []string{"café", "a\xff", "a\xfe", "a\ufffd"} {
		fake := &fakeClient{}
		var out, errOut bytes.Buffer
		args := append([]string{"membership", "renew", "-conversation", name, "-expected-grant-version", "1"}, membershipEndpointArgs...)
		if code := run(args, &out, &errOut, fatalIfDialed(t), noEnv); code != 2 {
			t.Fatalf("renew dialed for %x: exit=%d %s", name, code, &errOut)
		}

		args = append([]string{"membership", "revoke", "-conversation", name, "-expected-grant-version", "1"}, membershipEndpointArgs...)
		if code := run(args, &out, &errOut, fakeDial(fake), noEnv); code != 0 || fake.params["conversation"] != name {
			t.Fatalf("revoke changed key %x: %+v, exit=%d", name, fake.params, code)
		}
	}
}

// TestMembershipDialFailureReportsOperationalErrorNotArgumentError exercises
// the production dialControlClient against a nonexistent socket path: a
// real, deterministic dial failure (ENOENT), never a fake, mirroring
// TestHelloDialFailureReportsOperationalErrorNotArgumentError below.
func TestMembershipDialFailureReportsOperationalErrorNotArgumentError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "admin.sock")
	args := []string{"membership", "revoke", "-conversation", "fixture", "-expected-grant-version", "1", "-endpoint", path, "-server-uid", "1000"}
	var out, errOut bytes.Buffer
	if code := run(args, &out, &errOut, dialControlClient, noEnv); code != 1 || out.Len() != 0 || errOut.Len() == 0 {
		t.Fatalf("exit=%d out=%q err=%q", code, out.String(), errOut.String())
	}
}

// fatalIfOpened is retained under its old name for the hello tests below,
// which never open a database or dial a control client through the legacy
// controller factory this file used to define; it is simply fatalIfDialed
// under the name those tests were written against pre-conversion.
func fatalIfOpened(t *testing.T) dialFunc { return fatalIfDialed(t) }

func TestHelloRequiresEndpointConfiguration(t *testing.T) {
	var out, errOut bytes.Buffer
	code := run([]string{"hello"}, &out, &errOut, fatalIfOpened(t), noEnv)
	if code != 2 || !strings.Contains(errOut.String(), "no endpoint configured") {
		t.Fatalf("exit=%d err=%q", code, errOut.String())
	}
}

func TestHelloRefusesPARLEYDBAsClientSource(t *testing.T) {
	getenv := func(key string) string {
		if key == "PARLEY_DB" {
			return "parley.db"
		}
		return ""
	}
	var out, errOut bytes.Buffer
	code := run([]string{"hello", "-endpoint", "/tmp/x", "-server-uid", "1000"}, &out, &errOut, fatalIfOpened(t), getenv)
	if code != 2 || !strings.Contains(errOut.String(), "PARLEY_DB") {
		t.Fatalf("exit=%d err=%q", code, errOut.String())
	}
}

func TestHelloHelpTouchesNoDatabase(t *testing.T) {
	var out, errOut bytes.Buffer
	code := run([]string{"hello", "-h"}, &out, &errOut, fatalIfOpened(t), noEnv)
	if code != 0 || out.Len() == 0 {
		t.Fatalf("exit=%d out=%q err=%q", code, out.String(), errOut.String())
	}
}

func TestHelloDialFailureReportsOperationalErrorNotArgumentError(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "parleyctl-hello-nodial-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "admin.sock")
	var out, errOut bytes.Buffer
	code := run([]string{"hello", "-endpoint", path, "-server-uid", "1000"}, &out, &errOut, fatalIfOpened(t), noEnv)
	if code != 1 || out.Len() != 0 || errOut.Len() == 0 {
		t.Fatalf("exit=%d out=%q err=%q", code, out.String(), errOut.String())
	}
}

// startHelloTestServer wires a real control.Listen socket, control.Listener
// service and store.DB (control's own exported surface, mirroring what
// cmd/parleyd assembles) and returns the -endpoint/-server-uid arguments a
// real parleyctl hello invocation can dial against, plus the admin ID hello
// should report back.
func startHelloTestServer(t *testing.T) (endpoint, serverUID, adminID string) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "parleyctl-hello-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socketPath := filepath.Join(dir, "admin.sock")
	dbPath := filepath.Join(dir, "parley.db")
	if err := os.WriteFile(dbPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	uid := uint32(os.Getuid())
	adminID = "80000000-0000-4000-8000-000000000001"
	controlCfg, err := control.NewConfig(socketPath, uid, map[string]uint32{adminID: uid})
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.OpenReaders(context.Background()); err != nil {
		t.Fatal(err)
	}
	service := control.NewListenerService(controlCfg, 0600)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := service.Start(ctx, runtime.Resources{WorkerContext: ctx, Writer: db, Queries: db.Queries(), Mode: runtime.Normal}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { service.StopAdmission(); cancel(); service.Wait() })
	return socketPath, strconv.FormatUint(uint64(uid), 10), adminID
}

// TestHelloEndToEndRoundTripNeverOpensDatabase runs the actual parleyctl
// hello command against a real server end to end, proving both the rendered
// output and that the legacy controllerFactory is never invoked. This is the
// healthy-output control for the write-failure tests below.
func TestHelloEndToEndRoundTripNeverOpensDatabase(t *testing.T) {
	endpoint, uid, adminID := startHelloTestServer(t)
	var out, errOut bytes.Buffer
	code := run([]string{"hello", "-endpoint", endpoint, "-server-uid", uid}, &out, &errOut, fatalIfOpened(t), noEnv)
	if code != 0 {
		t.Fatalf("exit=%d out=%q err=%q", code, out.String(), errOut.String())
	}
	if !strings.Contains(out.String(), "administrator_id:") || !strings.Contains(out.String(), adminID) {
		t.Fatalf("out=%q", out.String())
	}
	if !strings.Contains(out.String(), "protocol:") || !strings.Contains(out.String(), control.ProtocolVersion) {
		t.Fatalf("out=%q", out.String())
	}
}

// TestMembershipEndToEndEnrollSucceedsAgainstARealServer is the one
// membership fixture that dials a genuine parleyd-style server end to end
// (real control.Listener, real store.DB, real Coordinator), proving the
// production dialControlClient/run wiring -- not just the fakeClient seam
// above -- actually performs a working membership.enroll round trip.
func TestMembershipEndToEndEnrollSucceedsAgainstARealServer(t *testing.T) {
	endpoint, uid, _ := startHelloTestServer(t)
	args := []string{"membership", "enroll", "-conversation", "fixture", "-peer-a", "peer-a", "-peer-b", "peer-b", "-max-exchanges", "3", "-endpoint", endpoint, "-server-uid", uid}
	var out, errOut bytes.Buffer
	code := run(args, &out, &errOut, dialControlClient, noEnv)
	// No enabled binding exists for either synthetic peer against this real
	// server, so the coordinator rejects with a domain error (BindingUnavailable)
	// rather than succeeding -- but that is still a genuine, real end-to-end
	// RPC round trip through dialControlClient/control.Client.Call, exercising
	// exactly the code path a real enrollment failure takes, distinct from
	// every dial/argument-validation failure covered elsewhere in this file.
	if code != 1 || out.Len() != 0 || errOut.Len() == 0 {
		t.Fatalf("exit=%d out=%q err=%q", code, out.String(), errOut.String())
	}
	if strings.Contains(errOut.String(), "outcome unknown") {
		t.Fatalf("a well-formed domain rejection must not be reported as an unresolved timeout: %s", &errOut)
	}
}

// errSyntheticWrite is returned by failingWriter once its allowance of
// successful writes is exhausted.
var errSyntheticWrite = errors.New("synthetic write failure")

// failingWriter succeeds its first failAfter Write calls (buffering them),
// then fails every call after that -- letting tests exercise both a writer
// that fails before any output and one that fails after partial output.
type failingWriter struct {
	failAfter int
	calls     int
	buf       bytes.Buffer
}

func (w *failingWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls > w.failAfter {
		return 0, errSyntheticWrite
	}
	return w.buf.Write(p)
}

// TestHelloReportsWriteFailureBeforeAnyOutput covers a diagnostic delivery
// failure on the very first output line, through the actual runHello path
// (a live successful hello RPC, only its output write fails). The hello RPC
// itself succeeded here -- this must be reported as a distinct operational
// failure, never conflated with a dial/RPC error message, and must not exit
// 0 having silently dropped the diagnostic.
func TestHelloReportsWriteFailureBeforeAnyOutput(t *testing.T) {
	endpoint, uid, _ := startHelloTestServer(t)
	fw := &failingWriter{failAfter: 0}
	var errOut bytes.Buffer
	code := run([]string{"hello", "-endpoint", endpoint, "-server-uid", uid}, fw, &errOut, fatalIfOpened(t), noEnv)
	if code != 1 {
		t.Fatalf("exit=%d err=%q, want 1", code, errOut.String())
	}
	if fw.buf.Len() != 0 {
		t.Fatalf("wrote output despite a failing first write: %q", fw.buf.String())
	}
	if errOut.Len() == 0 {
		t.Fatal("no diagnostic message on stderr")
	}
	if strings.Contains(errOut.String(), "dial") || strings.Contains(errOut.String(), "protocol_mismatch") {
		t.Fatalf("write failure message conflated with a dial/RPC failure: %q", errOut.String())
	}
}

// TestHelloReportsWriteFailureAfterPartialOutput covers a diagnostic
// delivery failure after some lines already wrote successfully: writing
// must stop at the first failure (not attempt every remaining line, which
// would only obscure the original error) and still report a nonzero exit.
func TestHelloReportsWriteFailureAfterPartialOutput(t *testing.T) {
	endpoint, uid, _ := startHelloTestServer(t)
	fw := &failingWriter{failAfter: 3}
	var errOut bytes.Buffer
	code := run([]string{"hello", "-endpoint", endpoint, "-server-uid", uid}, fw, &errOut, fatalIfOpened(t), noEnv)
	if code != 1 {
		t.Fatalf("exit=%d err=%q, want 1", code, errOut.String())
	}
	if got := strings.Count(fw.buf.String(), "\n"); got != 3 {
		t.Fatalf("wrote %d lines before stopping, want exactly 3: %q", got, fw.buf.String())
	}
	if errOut.Len() == 0 {
		t.Fatal("no diagnostic message on stderr")
	}
}

// TestHelloWriteFailureExitCodeSurvivesAnUnusableStderr proves the nonzero
// exit does not depend on the error-reporting stderr write itself
// succeeding: an unusable stderr must not restore a zero exit status.
func TestHelloWriteFailureExitCodeSurvivesAnUnusableStderr(t *testing.T) {
	endpoint, uid, _ := startHelloTestServer(t)
	fw := &failingWriter{failAfter: 0}
	errFw := &failingWriter{failAfter: 0}
	code := run([]string{"hello", "-endpoint", endpoint, "-server-uid", uid}, fw, errFw, fatalIfOpened(t), noEnv)
	if code != 1 {
		t.Fatalf("exit=%d, want 1 even though the diagnostic write to stderr itself failed", code)
	}
}

// TestPolicyWireOmitsEdgesForOpenAndLeadOnlyButIncludesForDirected is
// MC-02's encode-side regression: membership.md's tagged union requires the
// edges key to be present only for a directed policy -- an earlier version
// of policyWire always emitted it, even as [] for open/lead_only, which the
// server's own decodePolicy did not previously reject either (the two sides
// silently agreed on a wire shape violation). Confirms the fix from the
// wire-building side; internal/control/membership_test.go's decodePolicy
// tests confirm the corresponding decode-side enforcement.
func TestPolicyWireOmitsEdgesForOpenAndLeadOnlyButIncludesForDirected(t *testing.T) {
	for _, p := range []membership.Policy{
		{Kind: membership.PolicyOpen},
		{Kind: membership.PolicyLeadOnly},
	} {
		wire := policyWire(p)
		if _, present := wire["edges"]; present {
			t.Fatalf("%s policy must omit edges entirely, got %#v", p.Kind, wire)
		}
	}
	directed := policyWire(membership.Policy{Kind: membership.PolicyDirected, Edges: []membership.Edge{{From: "a", To: "b"}}})
	edges, present := directed["edges"]
	if !present {
		t.Fatalf("directed policy must include edges, got %#v", directed)
	}
	if arr, ok := edges.([]any); !ok || len(arr) != 1 {
		t.Fatalf("directed policy edges mismatch: %#v", edges)
	}
}

// TestMembershipOutcomeUnknownRemoteErrorGetsSameRetryGuidanceAsTimeout is
// MC-01.C's regression: store.Coordinator.Execute's own OutcomeUnknown
// commit path surfaces to the client as a *control.RemoteError whose Domain
// is outcome_unknown -- a genuinely unresolved outcome distinct from every
// other *RemoteError (a proven domain rejection), and must get the same
// retry-safe guidance as a client-side *control.TimeoutError, not fall into
// the generic no-retry-guidance error branch.
func TestMembershipOutcomeUnknownRemoteErrorGetsSameRetryGuidanceAsTimeout(t *testing.T) {
	fake := &fakeClient{err: &control.RemoteError{Domain: control.DomainCode(store.OutcomeUnknown), Message: "commit outcome unresolved"}}
	args := append([]string{"membership", "revoke", "-conversation", "fixture", "-expected-grant-version", "1"}, membershipEndpointArgs...)
	var out, errOut bytes.Buffer
	code := run(args, &out, &errOut, fakeDial(fake), noEnv)
	if code != 1 {
		t.Fatalf("exit=%d, want 1", code)
	}
	msg := errOut.String()
	if !strings.Contains(msg, "outcome unknown, not a proven failure") {
		t.Fatalf("missing outcome-unknown qualifier: %s", msg)
	}
	if !strings.Contains(msg, "-operation-id") {
		t.Fatalf("missing retry guidance: %s", msg)
	}
}

// TestMembershipUnusableReceiptIsNotReportedAsSuccess is MC-01.D's
// regression: a structurally decoded but zero-valued receipt (missing
// audit_id/operation_id -- e.g. a caller's out pointer that a broken/
// malicious peer never actually populated) must never be printed and
// exited 0 as a proven completion.
func TestMembershipUnusableReceiptIsNotReportedAsSuccess(t *testing.T) {
	fake := &fakeClient{emptyResult: true}
	args := append([]string{"membership", "revoke", "-conversation", "fixture", "-expected-grant-version", "1"}, membershipEndpointArgs...)
	var out, errOut bytes.Buffer
	code := run(args, &out, &errOut, fakeDial(fake), noEnv)
	if code != 1 {
		t.Fatalf("exit=%d, want 1", code)
	}
	if out.Len() != 0 {
		t.Fatalf("an unusable receipt must not be printed as a result: %s", &out)
	}
	msg := errOut.String()
	if !strings.Contains(msg, "unusable receipt") || !strings.Contains(msg, "-operation-id") {
		t.Fatalf("missing unusable-receipt retry guidance: %s", msg)
	}
}
