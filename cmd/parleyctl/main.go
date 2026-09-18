// Command parleyctl is the human-operated grant administrator, never an
// agent tool or a long-running bridge. See docs/architecture.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ginsys/parley/internal/bridgetext"
	"github.com/ginsys/parley/internal/control"
	"github.com/ginsys/parley/internal/membership"
	"github.com/ginsys/parley/internal/store"
	"github.com/google/uuid"
)

// membershipClient is the subset of *control.Client every membership
// subcommand dispatch needs. It exists so a test can inject a fake client
// that never opens a real socket -- the replacement for the removed
// controllerFactory seam from the legacy direct-database grant/revoke/renew
// path (PR1). Every membership.* method is a mutation dispatched through
// Call; nothing here opens a database or a lock of any kind.
type membershipClient interface {
	Call(ctx context.Context, method string, params map[string]any, out any) error
	Close() error
}

// dialFunc is the injectable seam production wires to dialControlClient and
// tests substitute with a fake dialer.
type dialFunc func(ctx context.Context, cfg control.ClientConfig) (membershipClient, error)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, dialControlClient, os.Getenv))
}

// dialControlClient is dialFunc's production body: a real control.Dial,
// discarding its HelloResult -- membership dispatch only needs Call/Close,
// runHello below performs its own separate Dial when it needs the full
// handshake result to print.
func dialControlClient(ctx context.Context, cfg control.ClientConfig) (membershipClient, error) {
	client, _, err := control.Dial(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// command is one parsed `membership <op>` invocation. endpoint/serverUID
// mirror hello's own flags; resolution precedence (flag > env > refused
// PARLEY_DB) is applied once, later, by control.ResolveClientConfig -- never
// duplicated here.
type command struct {
	op                  string // enroll | renew | replace | revoke
	conversation        string
	peerA, peerB        string
	direction           store.Direction
	maxExchanges        int64
	expiresIn           time.Duration
	cancelReplies       bool
	expectedVersion     int64
	operationID         string
	endpoint, serverUID string
}

func parseCommand(args []string, output io.Writer) (command, error) {
	c := command{op: args[0]}
	switch c.op {
	case "enroll", "renew", "replace", "revoke":
	default:
		return c, fmt.Errorf("unknown membership subcommand %q", c.op)
	}
	fs := flag.NewFlagSet("membership "+c.op, flag.ContinueOnError)
	fs.SetOutput(output)
	fs.StringVar(&c.conversation, "conversation", "", "conversation name (required)")
	fs.StringVar(&c.operationID, "operation-id", "", "explicit operation ID (a UUID) for retrying a lost response; default: a fresh random UUID")
	fs.StringVar(&c.endpoint, "endpoint", "", "administration socket path (or $PARLEY_ENDPOINT)")
	fs.StringVar(&c.serverUID, "server-uid", "", "the server process's UID (or $PARLEY_SERVER_UID)")
	var direction string
	switch c.op {
	case "enroll", "replace":
		fs.StringVar(&c.peerA, "peer-a", "", "peer A identifier (required)")
		fs.StringVar(&c.peerB, "peer-b", "", "peer B identifier (required)")
		fs.StringVar(&direction, "direction", "bidirectional", "bidirectional | a_to_b | b_to_a")
	}
	switch c.op {
	case "enroll":
		fs.Int64Var(&c.maxExchanges, "max-exchanges", 0, "positive exchange budget (required)")
		fs.DurationVar(&c.expiresIn, "expires-in", 0, "TTL; 0 means no expiry")
		fs.Int64Var(&c.expectedVersion, "expected-grant-version", 0, "expected latest historical grant version; 0 if the conversation has never been enrolled")
	case "renew", "replace":
		fs.Int64Var(&c.maxExchanges, "max-exchanges", 0, "new budget; 0 keeps the current value")
		fs.DurationVar(&c.expiresIn, "expires-in", 0, "new TTL; 0 keeps the current expiry")
		fs.BoolVar(&c.cancelReplies, "cancel-pending-replies", false, "cancel pending trusted replies instead of carrying them forward")
		fs.Int64Var(&c.expectedVersion, "expected-grant-version", -1, "expected current active grant version (required)")
	case "revoke":
		fs.Int64Var(&c.expectedVersion, "expected-grant-version", -1, "expected current active grant version (required)")
	}
	if err := fs.Parse(args[1:]); err != nil {
		return c, err
	}
	if fs.NArg() != 0 {
		return c, fmt.Errorf("unexpected positional arguments")
	}
	if c.op == "enroll" || c.op == "replace" {
		c.direction = store.Direction(direction)
	}
	// Identifiers are opaque exact keys. TrimSpace checks emptiness only;
	// normalization could retarget existing grants. Revoke keeps the exact
	// key even if it is byte-malformed -- new-enrollment validation does not
	// apply to that path (AGENTS.md).
	if strings.TrimSpace(c.conversation) == "" {
		return c, fmt.Errorf("membership %s requires -conversation", c.op)
	}
	if c.op != "revoke" {
		if err := bridgetext.ValidateMetadata(c.conversation); err != nil {
			return c, fmt.Errorf("conversation identifier: %w", err)
		}
	}
	if c.maxExchanges < 0 || c.expiresIn < 0 {
		return c, fmt.Errorf("budget and expiry must not be negative")
	}
	if c.op == "enroll" || c.op == "replace" {
		if strings.TrimSpace(c.peerA) == "" || strings.TrimSpace(c.peerB) == "" || c.peerA == c.peerB {
			return c, fmt.Errorf("%s requires distinct nonempty peers", c.op)
		}
		for _, id := range []string{c.peerA, c.peerB} {
			if err := bridgetext.ValidateMetadata(id); err != nil {
				return c, fmt.Errorf("peer identifier: %w", err)
			}
		}
		switch c.direction {
		case store.Bidirectional, store.AToB, store.BToA:
		default:
			return c, fmt.Errorf("invalid direction %q", c.direction)
		}
	}
	if c.op == "enroll" {
		if c.expectedVersion < 0 {
			return c, fmt.Errorf("-expected-grant-version must not be negative")
		}
		if c.maxExchanges == 0 {
			return c, fmt.Errorf("enroll requires -max-exchanges > 0")
		}
	} else if c.expectedVersion < 1 {
		return c, fmt.Errorf("membership %s requires -expected-grant-version >= 1", c.op)
	}
	if c.operationID != "" && !validOperationID(c.operationID) {
		return c, fmt.Errorf("-operation-id must be a canonical UUID")
	}
	return c, nil
}

// validOperationID mirrors internal/control's own canonicalUUID check (the
// wire profile's identifier form) so a locally rejected -operation-id fails
// before dialing, never as a server round trip.
func validOperationID(s string) bool {
	id, err := uuid.Parse(s)
	return err == nil && id != uuid.Nil && id.String() == s
}

// run validates everything and resolves client configuration before ever
// dialing. Tests inject a fake dialer; they never reach a real control.Dial.
func run(args []string, stdout, stderr io.Writer, dial dialFunc, getenv func(string) string) int {
	if len(args) == 0 || (len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help")) {
		usage(stdout)
		return 0
	}
	// hello is a pure client diagnostic: it dials but performs no mutation.
	if args[0] == "hello" {
		return runHello(args[1:], stdout, stderr, getenv)
	}
	if args[0] == "membership" {
		return runMembership(args[1:], stdout, stderr, dial, getenv)
	}
	fmt.Fprintf(stderr, "parleyctl: unknown command %q\n", args[0])
	usage(stderr)
	return 2
}

// runMembership dispatches one membership.enroll/renew/replace/revoke call.
func runMembership(args []string, stdout, stderr io.Writer, dial dialFunc, getenv func(string) string) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "parleyctl: membership requires a subcommand")
		membershipUsage(stderr)
		return 2
	}
	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		membershipUsage(stdout)
		return 0
	}
	// FlagSet sends help to the chosen writer and does not exit the process.
	var parseOutput strings.Builder
	c, err := parseCommand(args, &parseOutput)
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprint(stdout, parseOutput.String())
		return 0
	}
	if err != nil {
		fmt.Fprintf(stderr, "parleyctl: %v\n", err)
		membershipUsage(stderr)
		return 2
	}
	cfg, err := control.ResolveClientConfig(control.ClientOptions{EndpointFlag: c.endpoint, ServerUIDFlag: c.serverUID, Getenv: getenv})
	if err != nil {
		fmt.Fprintf(stderr, "parleyctl membership %s: %v\n", c.op, err)
		return 2
	}
	dialCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := dial(dialCtx, cfg)
	if err != nil {
		fmt.Fprintf(stderr, "parleyctl membership %s: %v\n", c.op, describeDialFailure(err))
		return 1
	}
	defer client.Close()

	operationID := c.operationID
	if operationID == "" {
		operationID = uuid.NewString()
	}
	params := membershipParams(c, operationID)

	callCtx, cancelCall := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelCall()
	var result control.CommandReceiptResult
	if err := client.Call(callCtx, "membership."+c.op, params, &result); err != nil {
		var timeout *control.TimeoutError
		if errors.As(err, &timeout) {
			fmt.Fprintf(stderr, "parleyctl membership %s: %v -- retry with -operation-id %s\n", c.op, err, operationID)
		} else {
			fmt.Fprintf(stderr, "parleyctl membership %s: %v\n", c.op, err)
		}
		return 1
	}
	printMembershipResult(stdout, c, operationID, result)
	return 0
}

// describeDialFailure adds the "outcome unknown" qualifier hello already
// uses whenever a dial failure is a *control.TimeoutError -- an ambiguous
// pre-mutation outcome, never a proven refusal.
func describeDialFailure(err error) string {
	var timeout *control.TimeoutError
	if errors.As(err, &timeout) {
		return fmt.Sprintf("%v (outcome unknown, not a proven failure)", err)
	}
	return err.Error()
}

// membershipParams builds the wire params object for c.op, per
// docs/specifications/control.md's decimal-string 64-bit codec: every
// counter/version/budget is sent as a canonical decimal string, never a
// bare JSON number.
func membershipParams(c command, operationID string) map[string]any {
	params := map[string]any{
		"operation_id":           operationID,
		"conversation":           c.conversation,
		"expected_grant_version": strconv.FormatInt(c.expectedVersion, 10),
	}
	switch c.op {
	case "enroll":
		model := membership.FromGrant(c.peerA, c.peerB, c.direction)
		params["members"] = membersWire(model.Members)
		params["policy"] = policyWire(model.Policy)
		params["max_exchanges"] = strconv.FormatInt(c.maxExchanges, 10)
		if expiresAt := expiresAtFromDuration(c.expiresIn); expiresAt != "" {
			params["expires_at"] = expiresAt
		}
	case "renew":
		params["cancel_pending_replies"] = c.cancelReplies
		if c.maxExchanges != 0 {
			params["max_exchanges"] = strconv.FormatInt(c.maxExchanges, 10)
		}
		if expiresAt := expiresAtFromDuration(c.expiresIn); expiresAt != "" {
			params["expires_at"] = expiresAt
		}
	case "replace":
		model := membership.FromGrant(c.peerA, c.peerB, c.direction)
		params["members"] = membersWire(model.Members)
		params["policy"] = policyWire(model.Policy)
		params["cancel_pending_replies"] = c.cancelReplies
		if c.maxExchanges != 0 {
			params["max_exchanges"] = strconv.FormatInt(c.maxExchanges, 10)
		}
		if expiresAt := expiresAtFromDuration(c.expiresIn); expiresAt != "" {
			params["expires_at"] = expiresAt
		}
	case "revoke":
		// conversation + expected_grant_version already set above; revoke
		// carries nothing else.
	}
	return params
}

func membersWire(members []membership.Member) []any {
	out := make([]any, len(members))
	for i, m := range members {
		out[i] = map[string]any{"peer_id": m.PeerID, "role": string(m.Role)}
	}
	return out
}

func policyWire(p membership.Policy) map[string]any {
	edges := make([]any, len(p.Edges))
	for i, e := range p.Edges {
		edges[i] = map[string]any{"from": e.From, "to": e.To}
	}
	return map[string]any{"kind": string(p.Kind), "edges": edges}
}

// expiresAtFromDuration renders a TTL as an absolute RFC3339 UTC timestamp,
// or "" for "no explicit expiry" (enroll) / "keep the current expiry"
// (renew, replace) -- the wire's own optionality for expires_at.
func expiresAtFromDuration(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return time.Now().Add(d).UTC().Format(time.RFC3339)
}

// printMembershipResult renders a command receipt deterministically -- one
// field per line, in a fixed order, never a dumped map (matching hello's
// own printHello convention).
func printMembershipResult(w io.Writer, c command, operationID string, result control.CommandReceiptResult) {
	fmt.Fprintf(w, "membership %s %q: operation_id %s\n", c.op, c.conversation, operationID)
	fmt.Fprintf(w, "  audit_id %s, commit_epoch %s, commit_revision %s\n", result.AuditID, result.CommitView.Epoch, result.CommitView.Revision)
	for _, r := range result.Result.Resources {
		fmt.Fprintf(w, "  %s %s: %s -> %s\n", r.Kind, r.ID, r.Before, r.After)
	}
}

// runHello is the client-side diagnostic added in mandate PR1 §4.D: it
// resolves an endpoint/server-uid from -endpoint/-server-uid flags or the
// PARLEY_ENDPOINT/PARLEY_SERVER_UID environment (control.ResolveClientConfig
// refuses PARLEY_DB outright), dials, performs the required server.hello
// handshake, and renders the result deterministically -- one field per
// line, in a fixed order, never a dumped map. It opens no database and
// takes no lock.
func runHello(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	var output strings.Builder
	fs := flag.NewFlagSet("hello", flag.ContinueOnError)
	fs.SetOutput(&output)
	endpoint := fs.String("endpoint", "", "administration socket path (or $PARLEY_ENDPOINT)")
	serverUID := fs.String("server-uid", "", "the server process's UID (or $PARLEY_SERVER_UID)")
	if err := fs.Parse(args); errors.Is(err, flag.ErrHelp) {
		fmt.Fprint(stdout, output.String())
		return 0
	} else if err != nil {
		fmt.Fprint(stderr, output.String())
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "parleyctl hello: unexpected positional arguments")
		return 2
	}
	cfg, err := control.ResolveClientConfig(control.ClientOptions{EndpointFlag: *endpoint, ServerUIDFlag: *serverUID, Getenv: getenv})
	if err != nil {
		fmt.Fprintf(stderr, "parleyctl hello: %v\n", err)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, hello, err := control.Dial(ctx, cfg)
	if err != nil {
		var timeout *control.TimeoutError
		if errors.As(err, &timeout) {
			fmt.Fprintf(stderr, "parleyctl hello: %v (outcome unknown, not a proven failure)\n", err)
		} else {
			fmt.Fprintf(stderr, "parleyctl hello: %v\n", err)
		}
		return 1
	}
	defer client.Close()
	// The hello RPC itself already succeeded by this point -- a failure
	// writing its output is a distinct, later failure and must not be
	// reported as a dial/handshake/RPC outcome (mandate S15-1): the hello
	// may have succeeded; only publishing its diagnostic failed.
	if err := printHello(stdout, hello); err != nil {
		fmt.Fprintf(stderr, "parleyctl hello: writing diagnostic output: %v\n", err)
		return 1
	}
	return 0
}

// printHello writes the hello diagnostic and returns the first write error
// encountered, if any (mandate S15-1): a caller piped into a closed or
// otherwise failing writer must be told delivery failed rather than exiting
// 0 having silently dropped the diagnostic. Lines are rendered up front so
// only the actual io.WriteString calls below can fail, and writing stops at
// the first failure rather than attempting every remaining line (whose
// errors would only obscure the original one).
func printHello(w io.Writer, h control.HelloResult) error {
	lines := []string{
		fmt.Sprintf("protocol:                      %s\n", h.Protocol),
		fmt.Sprintf("server_id:                     %s\n", h.ServerID),
		fmt.Sprintf("server_epoch:                  %s\n", h.ServerEpoch),
		fmt.Sprintf("administrator_id:              %s\n", h.AdministratorID),
		fmt.Sprintf("state:                         %s\n", h.State),
		fmt.Sprintf("max_frame_bytes:               %d\n", h.Limits.MaxFrameBytes),
		fmt.Sprintf("max_nesting_depth:             %d\n", h.Limits.MaxNestingDepth),
		fmt.Sprintf("max_sockets_per_administrator: %d\n", h.Limits.MaxSocketsPerAdministrator),
		fmt.Sprintf("max_sockets_total:             %d\n", h.Limits.MaxSocketsTotal),
		fmt.Sprintf("max_executing_per_socket:      %d\n", h.Limits.MaxExecutingPerSocket),
		fmt.Sprintf("max_queued_per_socket:         %d\n", h.Limits.MaxQueuedPerSocket),
		fmt.Sprintf("methods:                       %s\n", strings.Join(h.Methods, ", ")),
	}
	for _, line := range lines {
		if _, err := io.WriteString(w, line); err != nil {
			return err
		}
	}
	return nil
}

func membershipUsage(output io.Writer) {
	fmt.Fprintln(output, `parleyctl membership: authenticated membership mutations against parleyd.

Usage:
  parleyctl membership enroll  -conversation NAME -peer-a ID -peer-b ID -max-exchanges N [-direction bidirectional|a_to_b|b_to_a] [-expires-in DURATION] [-expected-grant-version N] [-operation-id UUID] -endpoint PATH -server-uid UID
  parleyctl membership renew   -conversation NAME -expected-grant-version N [-max-exchanges N] [-expires-in DURATION] [-cancel-pending-replies] [-operation-id UUID] -endpoint PATH -server-uid UID
  parleyctl membership replace -conversation NAME -expected-grant-version N -peer-a ID -peer-b ID [-direction bidirectional|a_to_b|b_to_a] [-max-exchanges N] [-expires-in DURATION] [-cancel-pending-replies] [-operation-id UUID] -endpoint PATH -server-uid UID
  parleyctl membership revoke  -conversation NAME -expected-grant-version N [-operation-id UUID] -endpoint PATH -server-uid UID

-expected-grant-version pins optimistic concurrency: 0 for enroll of a
conversation with no prior history, else the exact current active grant
version. A stale value is rejected rather than silently overwritten.
-operation-id defaults to a fresh random UUID; pass the same value again to
retry a call whose response was lost without risking a second mutation.
Endpoint/server UID: -endpoint/-server-uid flags, else
$PARLEY_ENDPOINT/$PARLEY_SERVER_UID; $PARLEY_DB is refused as a client source.`)
}

func usage(output io.Writer) {
	fmt.Fprintln(output, `parleyctl: the Parley grant administration client.
Run directly in your own shell, never through an agent tool call.
This command does not start a running bridge; no-argument invocation shows help.
It is a pure client of parleyd's administration socket -- it never opens the
database directly and $PARLEY_DB is refused wherever it is set.

Usage:
  parleyctl membership enroll|renew|replace|revoke ...  (see 'membership help')
  parleyctl hello  [-endpoint PATH] [-server-uid UID]
  parleyctl [help|-h|--help]

Endpoint/server UID: -endpoint/-server-uid flags, else
$PARLEY_ENDPOINT/$PARLEY_SERVER_UID; $PARLEY_DB is refused as a client source.
Help exits 0, invalid arguments exit 2, operational failures exit 1.`)
}
