package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
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
		// OperationID echoes params["operation_id"] -- exactly what a real
		// server's mutationResponse does (internal/control/membership.go) --
		// rather than a fixed placeholder: Usable now requires this to equal
		// the caller's own requested operation ID, so a fixture hardcoding
		// an unrelated constant here would make every fakeClient-backed
		// success path spuriously "unusable" (MC-01, correcting an earlier
		// audit-1/operation-1 placeholder receipt this fixture used). Usable
		// also now validates AuditID/CommitView.Epoch as canonical UUIDs,
		// CommitView.Revision as a canonical decimal string, and requires a
		// nonempty Result.Resources with valid Kind/ID/Before/After -- a
		// second lead-reviewed residual on the same fixture (a "fixed" wire
		// operation_id alone was not itself contract-valid).
		//
		// CommandReceiptResult's Result field has an unexported concrete
		// type (server.go's wireCommandResult), so this fixture cannot build
		// one via a Go composite literal from outside the package -- it
		// round-trips through encoding/json instead, exactly the path a
		// real wire response takes through Client.Call's own
		// json.Unmarshal(resp.Result, out). This receipt is shaped exactly
		// like a real successful membership.enroll result.
		opID, _ := params["operation_id"].(string)
		wire, err := json.Marshal(map[string]any{
			"audit_id":     "60000000-0000-4000-8000-000000000001",
			"operation_id": opID,
			"commit_view":  map[string]any{"epoch": "70000000-0000-4000-8000-000000000001", "revision": "1"},
			"result": map[string]any{
				"code": "",
				"resources": []map[string]any{
					{"kind": "grant", "id": params["conversation"], "before": "0", "after": "1"},
				},
			},
		})
		if err != nil {
			return err
		}
		if err := json.Unmarshal(wire, result); err != nil {
			return err
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

// TestHelpAndInvalidArgumentsNeverDial's exit-2 cases each name the exact
// diagnostic parseCommand/runMembership is expected to produce, and the
// cases that test a parseCommand-level validation (not the deliberate
// missing-endpoint case at the end, and not the flag-set-level cases that
// fail inside fs.Parse itself before any custom validation runs) append
// membershipEndpointArgs so control.ResolveClientConfig would succeed if
// dial were ever reached. Without this, deleting the validation under test
// (e.g. the negative--max-exchanges guard) would still exit 2 -- from the
// unrelated missing-endpoint error -- and this test would not notice (a
// lead-reviewed regression against an earlier version of this test that
// checked only the exit code and stream shape, never the actual diagnostic
// or reason for reaching it).
func TestHelpAndInvalidArgumentsNeverDial(t *testing.T) {
	tests := []struct {
		args    []string
		code    int
		wantErr string // substring required in stderr when code == 2; ignored when code == 0
	}{
		{nil, 0, ""}, {[]string{"help"}, 0, ""}, {[]string{"-h"}, 0, ""}, {[]string{"--help"}, 0, ""},
		{[]string{"membership", "help"}, 0, ""}, {[]string{"membership", "-h"}, 0, ""},
		{[]string{"membership", "enroll", "--help"}, 0, ""}, {[]string{"membership", "renew", "-h"}, 0, ""},
		{[]string{"membership", "replace", "-h"}, 0, ""}, {[]string{"membership", "revoke", "--help"}, 0, ""},
		{[]string{"serve"}, 2, `unknown command "serve"`},
		{[]string{"help", "extra"}, 2, `unknown command "help"`},
		{[]string{"membership"}, 2, "membership requires a subcommand"},
		{[]string{"membership", "unknown"}, 2, `unknown membership subcommand "unknown"`},
		{[]string{"membership", "enroll"}, 2, "requires -conversation"},
		{[]string{"membership", "renew"}, 2, "requires -conversation"},
		{[]string{"membership", "replace"}, 2, "requires -conversation"},
		{[]string{"membership", "revoke"}, 2, "requires -conversation"},
		// A supplied space-only value is a representable legacy exact key on
		// revoke (see TestMembershipRevokeAcceptsSpaceOnlyLegacyConversation
		// below, batch-9 fix for comment 4053366764) -- it must clear the
		// -conversation check and fail on the next missing argument instead,
		// never dialing either way.
		{[]string{"membership", "revoke", "-conversation", " "}, 2, "requires -expected-grant-version"},
		{[]string{"membership", "revoke", "-conversation", "c", "-expected-grant-version", "1", "extra"}, 2, "unexpected positional arguments"},
		{append([]string{"membership", "renew", "-conversation", "c", "-expected-grant-version", "1", "-max-exchanges", "-1"}, membershipEndpointArgs...), 2, "must not be negative"},
		{append([]string{"membership", "renew", "-conversation", "c", "-expected-grant-version", "1", "-expires-in", "-1s"}, membershipEndpointArgs...), 2, "must not be negative"},
		{[]string{"membership", "renew", "-conversation", "c", "-expected-grant-version", "1", "-expires-in", "oops"}, 2, "invalid value"},
		{[]string{"membership", "renew", "-conversation", "c", "-expected-grant-version", "1", "--unknown"}, 2, "flag provided but not defined"},
		{append([]string{"membership", "renew", "-conversation", "c", "-expected-grant-version", "0"}, membershipEndpointArgs...), 2, "requires -expected-grant-version >= 1"},
		{append([]string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "0"}, membershipEndpointArgs...), 2, "requires -max-exchanges > 0"},
		{append([]string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "a", "-max-exchanges", "1"}, membershipEndpointArgs...), 2, "requires distinct nonempty peers"},
		{append([]string{"membership", "enroll", "-conversation", "c", "-peer-a", " ", "-peer-b", "b", "-max-exchanges", "1"}, membershipEndpointArgs...), 2, "requires distinct nonempty peers"},
		{append([]string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-direction", "wrong"}, membershipEndpointArgs...), 2, `invalid direction "wrong"`},
		{append([]string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-expires-in", "-1s"}, membershipEndpointArgs...), 2, "must not be negative"},
		{append([]string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-expected-grant-version", "-1"}, membershipEndpointArgs...), 2, "-expected-grant-version must not be negative"},
		{append([]string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-operation-id", "not-a-uuid"}, membershipEndpointArgs...), 2, "-operation-id must be a canonical UUID"},
		// Syntactically valid but no endpoint/server-uid configured anywhere:
		// ResolveClientConfig fails before dial is ever reached. Deliberately
		// omits membershipEndpointArgs -- this case exists to prove that
		// specific failure, not to isolate a parseCommand validation.
		{[]string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1"}, 2, "no endpoint configured"},
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
			if tt.code == 2 {
				if stderr.Len() == 0 || stdout.Len() != 0 {
					t.Fatalf("error streams: %q %q", &stdout, &stderr)
				}
				if !strings.Contains(stderr.String(), tt.wantErr) {
					t.Fatalf("stderr %q does not contain expected diagnostic %q", &stderr, tt.wantErr)
				}
			}
		})
	}
}

func TestUnsafePeerIdentifiersRejectedBeforeDialing(t *testing.T) {
	// wantErr and membershipEndpointArgs (appended to args below) close the
	// same isolation gap as TestHelpAndInvalidArgumentsNeverDial: without a
	// resolvable endpoint, deleting the identifier check under test would
	// still exit 2 from the unrelated missing-endpoint error, and this test
	// would not notice.
	wantErr := map[string]string{"-conversation": "conversation identifier:", "-peer-a": "peer identifier:", "-peer-b": "peer identifier:"}
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
				args := append([]string{"membership", "enroll", "-conversation", "fixture", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "2", flag, id}, membershipEndpointArgs...)
				var out, errOut bytes.Buffer
				if code := run(args, &out, &errOut, fatalIfDialed(t), noEnv); code != 2 {
					t.Fatalf("exit=%d: %s", code, &errOut)
				}
				if !strings.Contains(errOut.String(), wantErr[flag]) {
					t.Fatalf("stderr %q does not contain expected diagnostic %q", &errOut, wantErr[flag])
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
		{"bad_rfc3339", []string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-expires-at", "not-rfc3339"}, "-expires-at must match RFC3339 UTC"},
		{"renew_mutually_exclusive", []string{"membership", "renew", "-conversation", "c", "-expected-grant-version", "1", "-expires-in", "1h", "-expires-at", "2030-01-01T00:00:00Z"}, "-expires-in and -expires-at are mutually exclusive"},
		{"renew_bad_rfc3339", []string{"membership", "renew", "-conversation", "c", "-expected-grant-version", "1", "-expires-at", "not-rfc3339"}, "-expires-at must match RFC3339 UTC"},
		// MC-03 regression: time.Parse(time.RFC3339, ...) alone silently
		// normalized each of these three instead of rejecting them --
		// excess fractional precision (truncated to 9 digits), a comma
		// fraction separator (a legal ISO 8601 alternative RFC3339 itself
		// doesn't document rejecting), and a single-digit hour. Each must
		// now fail the CLI's own grammar check before any parse/reformat
		// happens, matching the server's expiresAtGrammar exactly.
		{"excess_fraction_precision", []string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-expires-at", "2030-06-15T12:00:00.1234567891Z"}, "-expires-at must match RFC3339 UTC"},
		{"comma_fraction_separator", []string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-expires-at", "2030-06-15T12:00:00,5Z"}, "-expires-at must match RFC3339 UTC"},
		{"single_digit_hour", []string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-expires-at", "2030-06-15T1:00:00Z"}, "-expires-at must match RFC3339 UTC"},
		{"numeric_offset_not_normalized", []string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-expires-at", "2030-06-15T12:00:00+02:00"}, "-expires-at must match RFC3339 UTC"},
		// Review 5257748895 (comment 4054786969): the maximum valid
		// time.Duration resolves past the store's Unix-nanosecond range;
		// fatalIfDialed below proves it now fails locally, before any dial.
		{"expires_in_out_of_range", []string{"membership", "enroll", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-expires-in", "2562047h47m16.854775807s"}, "-expires-in resolves to an expiry outside the representable range"},
		{"renew_expires_in_out_of_range", []string{"membership", "renew", "-conversation", "c", "-expected-grant-version", "1", "-expires-in", "2562047h47m16.854775807s"}, "-expires-in resolves to an expiry outside the representable range"},
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
	// MC-03: ".750Z" is a trailing-zero fraction time.Format(RFC3339Nano)
	// would itself compact to ".75Z" on a round trip. The original string
	// must survive verbatim -- reformatting instead of sending the
	// validated input as-is would silently change this digest, defeating
	// -expires-at's own reason for existing.
	for _, want := range []string{"2030-06-15T12:00:00Z", "2030-06-15T12:00:00.123456789Z", "2030-06-15T12:00:00.750Z"} {
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
// bridgetext.ValidateMetadata to -conversation, so a byte-malformed-but-
// valid-UTF-8 historical key is still dispatched unchanged, while every
// other subcommand -- which enrolls or requires an existing well-formed
// identity -- rejects the same key before ever dialing. See
// TestMembershipRevokeRejectsInvalidUTF8BeforeDialing below for the
// genuinely-invalid-UTF-8 case, which MC-02 changed: those bytes can no
// longer reach the wire unchanged, since JSON cannot transmit them
// byte-exact at all.
func TestMembershipRevokeKeepsExactMalformedKeyButOthersValidateFirst(t *testing.T) {
	// \ufffd is an explicit escape, not the literal replacement character --
	// see the identical rationale on TestUnsafePeerIdentifiersRejectedBeforeDialing
	// (a hosted AI Code Review finding on an earlier PR2 candidate applied
	// there; this second fixture list was missed by that same fix). Every
	// value below is valid UTF-8 (the accented letter and \ufffd's rune both
	// encode validly), unlike the invalid-UTF-8 byte sequences moved out to
	// TestMembershipRevokeRejectsInvalidUTF8BeforeDialing.
	for _, name := range []string{"café", "a\ufffd"} {
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

// TestMembershipRevokeRejectsInvalidUTF8BeforeDialing is MC-02's regression:
// an invalid UTF-8 byte sequence cannot be transmitted byte-exact over this
// JSON-RPC wire protocol at all -- encoding/json.Marshal silently replaces
// each invalid byte with U+FFFD rather than preserving or rejecting it, so
// sending one for revoke would silently target a different key than the one
// on disk, defeating the exact-key revocation guarantee this path exists
// for. Revoke must refuse these locally, before ever dialing -- narrower
// than every other subcommand's full bridgetext.ValidateMetadata check
// (only genuine UTF-8 invalidity, not every ASCII-incompatible byte), and
// needed only for revoke, since every other subcommand already rejects
// these bytes via ValidateMetadata regardless.
func TestMembershipRevokeRejectsInvalidUTF8BeforeDialing(t *testing.T) {
	for _, name := range []string{"a\xff", "a\xfe", "\xc0\xaf"} {
		var out, errOut bytes.Buffer
		args := append([]string{"membership", "revoke", "-conversation", name, "-expected-grant-version", "1"}, membershipEndpointArgs...)
		if code := run(args, &out, &errOut, fatalIfDialed(t), noEnv); code != 2 {
			t.Fatalf("revoke dialed for invalid UTF-8 %x: exit=%d %s", name, code, &errOut)
		}
	}
}

// TestMembershipEnrollRenewReplaceRejectOversizedConversationBeforeDialing
// and TestMembershipEnrollAcceptsConversationAtTheMaxIdentityBytesBoundary
// are the client-side half of MC-02/review-5255666571's length finding
// (internal/control/membership.go's incompatibleConversation): an ASCII
// conversation identifier longer than store.MaxIdentityBytes must be
// refused locally, before ever dialing, exactly like the invalid-UTF-8 case
// above -- not merely eventually rejected server-side after a round trip.
// Revoke keeps its exact-key escape and is deliberately excluded (see the
// bound's own comment in parseCommand).
func TestMembershipEnrollRenewReplaceRejectOversizedConversationBeforeDialing(t *testing.T) {
	oversized := strings.Repeat("x", store.MaxIdentityBytes+1)
	for _, op := range []string{"enroll", "renew", "replace"} {
		t.Run(op, func(t *testing.T) {
			args := []string{"membership", op, "-conversation", oversized}
			switch op {
			case "enroll":
				args = append(args, "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "2")
			case "renew":
				args = append(args, "-expected-grant-version", "1")
			case "replace":
				args = append(args, "-expected-grant-version", "1", "-peer-a", "a", "-peer-b", "b")
			}
			args = append(args, membershipEndpointArgs...)
			var out, errOut bytes.Buffer
			if code := run(args, &out, &errOut, fatalIfDialed(t), noEnv); code != 2 {
				t.Fatalf("%s dialed for a %d-byte conversation: exit=%d %s", op, len(oversized), code, &errOut)
			}
		})
	}
}

func TestMembershipEnrollAcceptsConversationAtTheMaxIdentityBytesBoundary(t *testing.T) {
	boundary := strings.Repeat("x", store.MaxIdentityBytes)
	fake := &fakeClient{}
	var out, errOut bytes.Buffer
	args := append([]string{"membership", "enroll", "-conversation", boundary, "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "2"}, membershipEndpointArgs...)
	if code := run(args, &out, &errOut, fakeDial(fake), noEnv); code != 0 {
		t.Fatalf("expected a %d-byte conversation to be accepted: exit=%d %s", len(boundary), code, &errOut)
	}
	if fake.params["conversation"] != boundary {
		t.Fatalf("conversation identity changed: %+v", fake.params)
	}
}

// TestMembershipRevokeAcceptsSpaceOnlyLegacyConversation and
// TestMembershipEnrollRenewReplaceRejectSpaceOnlyConversation are the batch-9
// fix for review d89c4e6's post-push finding (comment 4053366764): revoke's
// exact-key legacy escape must not apply the "at least one non-space byte"
// AGENTS.md rule new enrollment requires, since a historical space-only key
// is representable on the wire and the server's revoke path intentionally
// accepts identifiers that fail new-enrollment validation. Before this fix,
// -conversation "   " on revoke was rejected client-side by the same
// TrimSpace check that (correctly) still applies to enroll/renew/replace.
func TestMembershipRevokeAcceptsSpaceOnlyLegacyConversation(t *testing.T) {
	fake := &fakeClient{}
	var out, errOut bytes.Buffer
	args := append([]string{"membership", "revoke", "-conversation", "   ", "-expected-grant-version", "1"}, membershipEndpointArgs...)
	if code := run(args, &out, &errOut, fakeDial(fake), noEnv); code != 0 {
		t.Fatalf("expected a space-only legacy conversation to dial and revoke: exit=%d %s", code, &errOut)
	}
	if fake.params["conversation"] != "   " {
		t.Fatalf("conversation identity changed: %+v", fake.params)
	}
}

func TestMembershipEnrollRenewReplaceRejectSpaceOnlyConversation(t *testing.T) {
	for _, op := range []string{"enroll", "renew", "replace"} {
		t.Run(op, func(t *testing.T) {
			args := []string{"membership", op, "-conversation", "   "}
			switch op {
			case "enroll":
				args = append(args, "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "2")
			case "renew":
				args = append(args, "-expected-grant-version", "1")
			case "replace":
				args = append(args, "-expected-grant-version", "1", "-peer-a", "a", "-peer-b", "b")
			}
			args = append(args, membershipEndpointArgs...)
			var out, errOut bytes.Buffer
			if code := run(args, &out, &errOut, fatalIfDialed(t), noEnv); code != 2 {
				t.Fatalf("%s dialed for a space-only conversation: exit=%d %s", op, code, &errOut)
			}
		})
	}
}

// TestMembershipRevokeAcceptsExplicitEmptyLegacyConversation and
// TestMembershipEnrollRenewReplaceRejectExplicitEmptyConversation are EC-03
// (2026-09-19 review): the historical schema permits an empty TEXT
// conversation key, so revoke's exact-key legacy escape must be able to
// target one -- but comparing the parsed -conversation value against "" (as
// this used to) cannot distinguish an explicitly supplied empty string from
// an omitted flag, since both parse to the same Go zero value. parseCommand
// now tracks flag *presence* via fs.Visit instead, so an explicit empty
// -conversation value reaches the server exactly as typed on revoke, while a
// truly omitted flag is still rejected (see
// TestMembershipEnrollMissingConversationFlagStillRejected below, unchanged)
// and enroll/renew/replace still reject an explicit empty string on their
// own separate TrimSpace nonemptiness rule.
func TestMembershipRevokeAcceptsExplicitEmptyLegacyConversation(t *testing.T) {
	fake := &fakeClient{}
	var out, errOut bytes.Buffer
	args := append([]string{"membership", "revoke", "-conversation", "", "-expected-grant-version", "1"}, membershipEndpointArgs...)
	if code := run(args, &out, &errOut, fakeDial(fake), noEnv); code != 0 {
		t.Fatalf("expected an explicit empty legacy conversation to dial and revoke: exit=%d %s", code, &errOut)
	}
	if fake.params["conversation"] != "" {
		t.Fatalf("conversation identity changed: %+v", fake.params)
	}
}

func TestMembershipEnrollRenewReplaceRejectExplicitEmptyConversation(t *testing.T) {
	for _, op := range []string{"enroll", "renew", "replace"} {
		t.Run(op, func(t *testing.T) {
			args := []string{"membership", op, "-conversation", ""}
			switch op {
			case "enroll":
				args = append(args, "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "2")
			case "renew":
				args = append(args, "-expected-grant-version", "1")
			case "replace":
				args = append(args, "-expected-grant-version", "1", "-peer-a", "a", "-peer-b", "b")
			}
			args = append(args, membershipEndpointArgs...)
			var out, errOut bytes.Buffer
			if code := run(args, &out, &errOut, fatalIfDialed(t), noEnv); code != 2 {
				t.Fatalf("%s dialed for an explicit empty conversation: exit=%d %s", op, code, &errOut)
			}
		})
	}
}

func TestMembershipEnrollMissingConversationFlagStillRejected(t *testing.T) {
	// Guards the revoke fix above: an actually-omitted -conversation flag
	// (the untrimmed default "") must still be rejected on every op,
	// including revoke -- only a *supplied* space-only value is exempt.
	for _, op := range []string{"enroll", "renew", "replace", "revoke"} {
		t.Run(op, func(t *testing.T) {
			args := []string{"membership", op}
			switch op {
			case "enroll":
				args = append(args, "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "2")
			case "renew", "revoke":
				args = append(args, "-expected-grant-version", "1")
			case "replace":
				args = append(args, "-expected-grant-version", "1", "-peer-a", "a", "-peer-b", "b")
			}
			args = append(args, membershipEndpointArgs...)
			var out, errOut bytes.Buffer
			if code := run(args, &out, &errOut, fatalIfDialed(t), noEnv); code != 2 {
				t.Fatalf("%s dialed with no -conversation at all: exit=%d %s", op, code, &errOut)
			}
		})
	}
}

// TestMembershipEnrollReplaceRejectOversizedPeerBeforeDialing and
// TestMembershipEnrollAcceptsPeerAtTheMaxIdentityBytesBoundary are the
// batch-9 fix for review d89c4e6's post-push finding (comment 4053366765):
// an ASCII peer identifier longer than store.MaxIdentityBytes passed
// bridgetext.ValidateMetadata's shape check, dialed, and only then hit
// store.EnabledPeer's identical length bound inside Coordinator.Execute --
// a durable but generic invalid_request consuming an operation ID and audit
// history for a boundary this client can already reject deterministically,
// mirroring the conversation-length check already covered above.
func TestMembershipEnrollReplaceRejectOversizedPeerBeforeDialing(t *testing.T) {
	oversized := strings.Repeat("x", store.MaxIdentityBytes+1)
	for _, op := range []string{"enroll", "replace"} {
		t.Run(op, func(t *testing.T) {
			args := []string{"membership", op, "-conversation", "conv", "-peer-a", oversized, "-peer-b", "b"}
			if op == "enroll" {
				args = append(args, "-max-exchanges", "2")
			} else {
				args = append(args, "-expected-grant-version", "1")
			}
			args = append(args, membershipEndpointArgs...)
			var out, errOut bytes.Buffer
			if code := run(args, &out, &errOut, fatalIfDialed(t), noEnv); code != 2 {
				t.Fatalf("%s dialed for a %d-byte peer: exit=%d %s", op, len(oversized), code, &errOut)
			}
		})
	}
}

func TestMembershipEnrollAcceptsPeerAtTheMaxIdentityBytesBoundary(t *testing.T) {
	boundary := strings.Repeat("x", store.MaxIdentityBytes)
	fake := &fakeClient{}
	var out, errOut bytes.Buffer
	args := append([]string{"membership", "enroll", "-conversation", "conv", "-peer-a", boundary, "-peer-b", "b", "-max-exchanges", "2"}, membershipEndpointArgs...)
	if code := run(args, &out, &errOut, fakeDial(fake), noEnv); code != 0 {
		t.Fatalf("expected a %d-byte peer to be accepted: exit=%d %s", len(boundary), code, &errOut)
	}
	members, ok := fake.params["members"].([]any)
	if !ok || len(members) != 2 {
		t.Fatalf("members=%+v", fake.params["members"])
	}
	var found bool
	for _, m := range members {
		if m.(map[string]any)["peer_id"] == boundary {
			found = true
		}
	}
	if !found {
		t.Fatalf("boundary peer identity not present in members: %+v", members)
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
// startHelloTestServer starts a real parleyd-style server (real
// control.Listener, real store.DB, real Coordinator) against a fresh
// on-disk database, seeding it with seed (if given) before the listener
// admits any connection. Every existing caller passes no seed and continues
// to get an empty database exactly as before.
func startHelloTestServer(t *testing.T, seed ...func(t *testing.T, db *store.DB)) (endpoint, serverUID, adminID string) {
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
	for _, s := range seed {
		s(t, db)
	}
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

// seedEnabledBindingForTest mirrors internal/control's own
// seedEnabledBinding fixture (there is no shared exported helper between the
// two packages; both hand-construct the same minimal enabled binding +
// current credential a real membership.enroll needs to pass BindingUnavailable).
func seedEnabledBindingForTest(t *testing.T, db *store.DB, index int, peer string) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	b := store.BindingRecord{
		ID: fmt.Sprintf("60000000-0000-4000-8000-%012d", index), PeerID: peer,
		HostKind: "codex_cli", NamespaceID: "synthetic", SessionID: peer,
		ConnectorUID: 1001, Status: "enabled", Version: 1,
	}
	var secret [32]byte
	secret[0] = byte(index)
	c := store.CredentialRecord{
		ID: fmt.Sprintf("70000000-0000-4000-8000-%012d", index), BindingID: b.ID, Version: 1,
		Status: "current", ExpiresAtNS: time.Now().Add(24 * time.Hour).UnixNano(), Verifier: sha256.Sum256(secret[:]),
	}
	if err := store.InsertBindingCredential(ctx, tx, b, c); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
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

// TestMembershipEndToEndEnrollRejectsWithoutEnabledBindingAgainstARealServer
// is the one membership fixture that dials a genuine parleyd-style server
// end to end (real control.Listener, real store.DB, real Coordinator),
// proving the production dialControlClient/run wiring -- not just the
// fakeClient seam above -- actually performs a real membership.enroll round
// trip. Despite the name of an earlier version of this test, this is a
// rejection fixture, not a success one: no enabled binding exists for
// either synthetic peer against this real server, so the coordinator
// rejects with a domain error (BindingUnavailable) rather than succeeding.
// It is still a genuine, real end-to-end RPC round trip through
// dialControlClient/control.Client.Call, exercising exactly the code path a
// real enrollment failure takes, distinct from every dial/argument-
// validation failure covered elsewhere in this file. See
// TestMembershipEndToEndEnrollSucceedsAgainstARealServer below for the
// actual success case this name previously (and wrongly) claimed to cover.
func TestMembershipEndToEndEnrollRejectsWithoutEnabledBindingAgainstARealServer(t *testing.T) {
	endpoint, uid, _ := startHelloTestServer(t)
	args := []string{"membership", "enroll", "-conversation", "fixture", "-peer-a", "peer-a", "-peer-b", "peer-b", "-max-exchanges", "3", "-endpoint", endpoint, "-server-uid", uid}
	var out, errOut bytes.Buffer
	code := run(args, &out, &errOut, dialControlClient, noEnv)
	if code != 1 || out.Len() != 0 || errOut.Len() == 0 {
		t.Fatalf("exit=%d out=%q err=%q", code, out.String(), errOut.String())
	}
	if strings.Contains(errOut.String(), "outcome unknown") {
		t.Fatalf("a well-formed domain rejection must not be reported as an unresolved timeout: %s", &errOut)
	}
}

// TestMembershipEndToEndEnrollSucceedsAgainstARealServer is the actual
// success counterpart the name above previously claimed but did not cover:
// both synthetic peers get a real enabled binding + current credential
// seeded into the server's database before the listener admits any
// connection, so this membership.enroll genuinely succeeds end to end --
// real socket, real frame/profile encoding, real Coordinator.Execute, real
// commit -- and parleyctl's production output rendering is exercised
// against a real, non-empty CommandReceiptResult (real audit_id/operation_id/
// epoch/revision), not the fakeClient seam's synthetic values.
func TestMembershipEndToEndEnrollSucceedsAgainstARealServer(t *testing.T) {
	seed := func(t *testing.T, db *store.DB) {
		seedEnabledBindingForTest(t, db, 1, "peer-a")
		seedEnabledBindingForTest(t, db, 2, "peer-b")
	}
	endpoint, uid, _ := startHelloTestServer(t, seed)
	opID := "80000000-0000-4000-8000-000000000099"
	args := []string{
		"membership", "enroll", "-conversation", "fixture", "-peer-a", "peer-a", "-peer-b", "peer-b",
		"-max-exchanges", "3", "-operation-id", opID, "-endpoint", endpoint, "-server-uid", uid,
	}
	var out, errOut bytes.Buffer
	code := run(args, &out, &errOut, dialControlClient, noEnv)
	if code != 0 {
		t.Fatalf("exit=%d out=%q err=%q", code, out.String(), errOut.String())
	}
	if errOut.Len() != 0 {
		t.Fatalf("unexpected stderr on success: %q", errOut.String())
	}
	if !strings.Contains(out.String(), "operation_id "+opID) {
		t.Fatalf("out=%q missing echoed operation_id %q", out.String(), opID)
	}
	if !strings.Contains(out.String(), `grant "fixture"`) {
		t.Fatalf("out=%q missing the enrolled conversation's resource change", out.String())
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
