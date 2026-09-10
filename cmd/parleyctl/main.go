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
	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/store"
)

type controllerAPI interface {
	Grant(context.Context, controller.GrantParams) (*store.Grant, error)
	Revoke(context.Context, string) (*controller.RevokeResult, error)
	Renew(context.Context, controller.RenewParams) (*store.Grant, error)
}
type controllerFactory func(context.Context, string) (controllerAPI, io.Closer, error)

func main() { os.Exit(run(os.Args[1:], os.Getenv("PARLEY_DB"), os.Stdout, os.Stderr, openController)) }

func openController(ctx context.Context, path string) (controllerAPI, io.Closer, error) {
	db, err := store.Open(ctx, path)
	if err != nil {
		return nil, nil, err
	}
	return controller.New(db), db, nil
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
func run(args []string, dbPath string, stdout, stderr io.Writer, factory controllerFactory) int {
	if len(args) == 0 || (len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help")) {
		usage(stdout)
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
  parleyctl [help|-h|--help]

Use a subcommand's -h for flag details. Grant budget must be positive.
Renewal uses 0 to keep budget/expiry; negative values are invalid.
Renewal carries eligible trusted replies forward unless -cancel-pending-replies is set.
Database path: $PARLEY_DB (default ./parley.db)
Help exits 0, invalid arguments exit 2, operational failures exit 1.`)
}
