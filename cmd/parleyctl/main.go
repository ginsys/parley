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
	"strings"
	"time"

	"github.com/ginsys/parley/internal/bridgetext"
	"github.com/ginsys/parley/internal/control"
	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/runtime"
	"github.com/ginsys/parley/internal/store"
)

type controllerAPI interface {
	Grant(context.Context, controller.GrantParams) (*store.Grant, error)
	Revoke(context.Context, string) (*controller.RevokeResult, error)
	Renew(context.Context, controller.RenewParams) (*store.Grant, error)
}
type controllerFactory func(context.Context, string) (controllerAPI, io.Closer, error)

func main() {
	os.Exit(run(os.Args[1:], os.Getenv("PARLEY_DB"), os.Stdout, os.Stderr, openController, os.Getenv))
}

// openController is the legacy direct-database writer path (transitional:
// PR2 removes it once membership.* wire commands replace grant/revoke/renew).
// It joins the same canonical ownership exclusion the server uses so a
// running parleyd cannot have its database opened out from under it by this
// client.
func openController(ctx context.Context, path string) (controllerAPI, io.Closer, error) {
	return openControllerWith(ctx, path, runtime.Acquire, store.Open)
}

// openControllerWith is openController's actual body, with the underlying
// database-open operation as the only injectable seam. This lets a test
// exercise the real ownership-lock-then-open sequence -- refusal before the
// store is ever opened, and the lock retained for the returned controller's
// full lifetime, not just at acquisition -- without invoking the production
// CLI or reimplementing the locking logic in the test. openStore is given
// owner.Path(), not the caller's raw path: acquire resolves symlinks before
// taking the lock (runtime.Acquire's canonical path), while store.Open only
// lexically cleans its input, so a path reaching the database through a
// symlinked ancestor could otherwise name a different file to each of them.
// Passing the already-resolved canonical path to both guarantees they can
// never diverge.
func openControllerWith(ctx context.Context, path string, acquire func(string) (*runtime.Ownership, error), openStore func(context.Context, string) (*store.DB, error)) (controllerAPI, io.Closer, error) {
	owner, err := acquire(path)
	if err != nil {
		return nil, nil, err
	}
	db, err := openStore(ctx, owner.Path())
	if err != nil {
		return nil, nil, errors.Join(err, owner.Close())
	}
	return controller.New(db), &ownedController{db: db, owner: owner}, nil
}

// ownedController closes the store before releasing ownership: the lock
// protects the database for the controller's entire lifetime, not merely
// until the factory returns.
type ownedController struct {
	db    *store.DB
	owner *runtime.Ownership
}

func (c *ownedController) Close() error {
	return errors.Join(c.db.Close(), c.owner.Close())
}

type command struct {
	name, conversation, peerA, peerB string
	direction                        store.Direction
	budget                           int64
	expiresIn                        time.Duration
	cancelReplies                    bool
}

func parseCommand(args []string, output io.Writer) (command, error) {
	c := command{name: args[0]}
	if c.name != "grant" && c.name != "revoke" && c.name != "renew" {
		return c, fmt.Errorf("unknown command %q", c.name)
	}
	fs := flag.NewFlagSet(c.name, flag.ContinueOnError)
	fs.SetOutput(output)
	fs.StringVar(&c.conversation, "conversation", "", "conversation name (required)")
	var direction string
	if c.name == "grant" {
		fs.StringVar(&c.peerA, "peer-a", "", "peer A identifier (required)")
		fs.StringVar(&c.peerB, "peer-b", "", "peer B identifier (required)")
		fs.StringVar(&direction, "direction", "bidirectional", "bidirectional | a_to_b | b_to_a")
		fs.Int64Var(&c.budget, "max-exchanges", 0, "positive exchange budget (required)")
		fs.DurationVar(&c.expiresIn, "expires-in", 0, "TTL; 0 means no expiry")
	} else if c.name == "renew" {
		fs.Int64Var(&c.budget, "max-exchanges", 0, "new budget; 0 keeps the current value")
		fs.DurationVar(&c.expiresIn, "expires-in", 0, "new TTL; 0 keeps the current expiry")
		fs.BoolVar(&c.cancelReplies, "cancel-pending-replies", false, "cancel pending trusted replies instead of carrying them forward")
	}
	if err := fs.Parse(args[1:]); err != nil {
		return c, err
	}
	c.direction = store.Direction(direction)
	if fs.NArg() != 0 {
		return c, fmt.Errorf("unexpected positional arguments")
	}
	// Identifiers are opaque exact keys. TrimSpace checks emptiness only;
	// normalization could retarget existing grants.
	if strings.TrimSpace(c.conversation) == "" {
		return c, fmt.Errorf("%s requires -conversation", c.name)
	}
	if c.name != "revoke" {
		if err := bridgetext.ValidateMetadata(c.conversation); err != nil {
			return c, fmt.Errorf("conversation identifier: %w", err)
		}
	}
	if c.budget < 0 || c.expiresIn < 0 {
		return c, fmt.Errorf("budget and expiry must not be negative")
	}
	if c.name == "grant" {
		if strings.TrimSpace(c.peerA) == "" || strings.TrimSpace(c.peerB) == "" || c.peerA == c.peerB || c.budget == 0 {
			return c, fmt.Errorf("grant requires distinct nonempty peers and -max-exchanges > 0")
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
	return c, nil
}

// run validates everything before opening storage. Tests inject a fake controller;
// they never execute the protected CLI or write grants through its production factory.
func run(args []string, dbPath string, stdout, stderr io.Writer, factory controllerFactory, getenv func(string) string) int {
	if len(args) == 0 || (len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help")) {
		usage(stdout)
		return 0
	}
	// hello is a pure client diagnostic: it never opens a database and does
	// not go through parseCommand/factory at all.
	if args[0] == "hello" {
		return runHello(args[1:], stdout, stderr, getenv)
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
		usage(stderr)
		return 2
	}
	if dbPath == "" {
		dbPath = "parley.db"
	}
	ctx := context.Background()
	ctrl, closer, err := factory(ctx, dbPath)
	if err != nil {
		fmt.Fprintf(stderr, "parleyctl: open database: %v\n", err)
		return 1
	}
	// Close before returning to main's os.Exit, on success and operational failure.
	defer closer.Close()
	var expiresAt *time.Time
	if c.expiresIn > 0 {
		value := time.Now().Add(c.expiresIn)
		expiresAt = &value
	}
	switch c.name {
	case "grant":
		g, opErr := ctrl.Grant(ctx, controller.GrantParams{Conversation: c.conversation, PeerAID: c.peerA, PeerBID: c.peerB, Direction: c.direction, MaxExchanges: c.budget, ExpiresAt: expiresAt})
		err = opErr
		if err == nil {
			fmt.Fprintf(stdout, "granted %q v%d: %q <-> %q, %s, budget %d, expires %s\n", g.Conversation, g.GrantVersion, g.PeerAID, g.PeerBID, g.Direction, g.MaxExchanges, orNever(g.ExpiresAt))
		}
	case "renew":
		g, opErr := ctrl.Renew(ctx, controller.RenewParams{Conversation: c.conversation, MaxExchanges: c.budget, ExpiresAt: expiresAt, CancelPendingReplies: c.cancelReplies})
		err = opErr
		if err == nil {
			fmt.Fprintf(stdout, "renewed %q to v%d: budget %d, expires %s\n", g.Conversation, g.GrantVersion, g.MaxExchanges, orNever(g.ExpiresAt))
		}
	case "revoke":
		result, opErr := ctrl.Revoke(ctx, c.conversation)
		err = opErr
		if err == nil {
			fmt.Fprintf(stdout, "revoked %q: %d cancelled, %d already dispatching, %d already handed off\n", c.conversation, result.Cancelled, result.AlreadyDispatching, result.AlreadyHandedOff)
			fmt.Fprintln(stdout, "This stops Parley's own delivery only; other communication paths remain possible.")
		}
	}
	if err != nil {
		fmt.Fprintf(stderr, "parleyctl: %s: %v\n", c.name, err)
		return 1
	}
	return 0
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
	printHello(stdout, hello)
	return 0
}

func printHello(w io.Writer, h control.HelloResult) {
	fmt.Fprintf(w, "protocol:                      %s\n", h.Protocol)
	fmt.Fprintf(w, "server_id:                     %s\n", h.ServerID)
	fmt.Fprintf(w, "server_epoch:                  %s\n", h.ServerEpoch)
	fmt.Fprintf(w, "administrator_id:              %s\n", h.AdministratorID)
	fmt.Fprintf(w, "state:                         %s\n", h.State)
	fmt.Fprintf(w, "max_frame_bytes:               %d\n", h.Limits.MaxFrameBytes)
	fmt.Fprintf(w, "max_nesting_depth:             %d\n", h.Limits.MaxNestingDepth)
	fmt.Fprintf(w, "max_sockets_per_administrator: %d\n", h.Limits.MaxSocketsPerAdministrator)
	fmt.Fprintf(w, "max_sockets_total:             %d\n", h.Limits.MaxSocketsTotal)
	fmt.Fprintf(w, "max_executing_per_socket:      %d\n", h.Limits.MaxExecutingPerSocket)
	fmt.Fprintf(w, "max_queued_per_socket:         %d\n", h.Limits.MaxQueuedPerSocket)
	fmt.Fprintf(w, "methods:                       %s\n", strings.Join(h.Methods, ", "))
}

func orNever(value *string) string {
	if value == nil {
		return "never"
	}
	return *value
}

func usage(output io.Writer) {
	fmt.Fprintln(output, `parleyctl: the protected Parley grant administrator.
Run directly in your own shell, never through an agent tool call.
This command does not start a running bridge; no-argument invocation shows help.

Usage:
  parleyctl grant  -conversation NAME -peer-a ID -peer-b ID -max-exchanges N [-direction bidirectional|a_to_b|b_to_a] [-expires-in DURATION]
  parleyctl revoke -conversation NAME
  parleyctl renew  -conversation NAME [-max-exchanges N] [-expires-in DURATION] [-cancel-pending-replies]
  parleyctl hello  [-endpoint PATH] [-server-uid UID]
  parleyctl [help|-h|--help]

Use a subcommand's -h for flag details. Grant budget must be positive.
Renewal uses 0 to keep budget/expiry; negative values are invalid.
Renewal carries eligible trusted replies forward unless -cancel-pending-replies is set.
grant/revoke/renew open the database directly (transitional; see AGENTS.md) --
database path: $PARLEY_DB (default ./parley.db).
hello is a pure client diagnostic against parleyd's administration socket; it
never opens a database. Endpoint/server UID: -endpoint/-server-uid flags, else
$PARLEY_ENDPOINT/$PARLEY_SERVER_UID; $PARLEY_DB is refused as a client source.
Help exits 0, invalid arguments exit 2, operational failures exit 1.`)
}
