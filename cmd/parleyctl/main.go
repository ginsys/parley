// Command parleyctl is Parley's protected controller CLI. It is the only
// code path that ever writes a grant, revocation, or renewal — per the
// design plan, it is meant to be run directly by a human in their own
// shell, never invoked as a tool by either peer session.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	dbPath := os.Getenv("PARLEY_DB")
	if dbPath == "" {
		dbPath = "parley.db"
	}

	ctx := context.Background()
	db, err := store.Open(ctx, dbPath)
	if err != nil {
		fatalf("open %s: %v", dbPath, err)
	}
	defer db.Close()
	ctrl := controller.New(db)

	switch os.Args[1] {
	case "grant":
		runGrant(ctx, ctrl, os.Args[2:])
	case "revoke":
		runRevoke(ctx, ctrl, os.Args[2:])
	case "renew":
		runRenew(ctx, ctrl, os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func runGrant(ctx context.Context, ctrl *controller.Controller, args []string) {
	fs := flag.NewFlagSet("grant", flag.ExitOnError)
	conversation := fs.String("conversation", "", "conversation name (required)")
	peerA := fs.String("peer-a", "", "peer A identifier, e.g. a Claude session id (required)")
	peerB := fs.String("peer-b", "", "peer B identifier, e.g. a Codex thread UUID (required)")
	direction := fs.String("direction", "bidirectional", "bidirectional | a_to_b | b_to_a")
	maxExchanges := fs.Int64("max-exchanges", 0, "exchange budget for this grant (required, >0)")
	expiresIn := fs.Duration("expires-in", 0, "optional TTL, e.g. 24h")
	fs.Parse(args)
	if *expiresIn < 0 {
		fatalf("expires-in must not be negative")
	}

	if *conversation == "" || *peerA == "" || *peerB == "" || *maxExchanges <= 0 {
		fatalf("grant requires -conversation, -peer-a, -peer-b, and -max-exchanges > 0")
	}
	var expiresAt *time.Time
	if *expiresIn > 0 {
		t := time.Now().Add(*expiresIn)
		expiresAt = &t
	}

	g, err := ctrl.Grant(ctx, controller.GrantParams{
		Conversation: *conversation,
		PeerAID:      *peerA,
		PeerBID:      *peerB,
		Direction:    store.Direction(*direction),
		MaxExchanges: *maxExchanges,
		ExpiresAt:    expiresAt,
	})
	if err != nil {
		fatalf("grant: %v", err)
	}
	fmt.Printf("granted %s v%d: %s <-> %s, %s, budget %d, expires %s\n",
		g.Conversation, g.GrantVersion, g.PeerAID, g.PeerBID, g.Direction, g.MaxExchanges, orNever(g.ExpiresAt))
}

func runRevoke(ctx context.Context, ctrl *controller.Controller, args []string) {
	fs := flag.NewFlagSet("revoke", flag.ExitOnError)
	conversation := fs.String("conversation", "", "conversation name (required)")
	fs.Parse(args)
	if *conversation == "" {
		fatalf("revoke requires -conversation")
	}

	result, err := ctrl.Revoke(ctx, *conversation)
	if err != nil {
		fatalf("revoke: %v", err)
	}
	fmt.Printf("revoked %s: %d cancelled, %d already dispatching, %d already handed off\n",
		*conversation, result.Cancelled, result.AlreadyDispatching, result.AlreadyHandedOff)
	fmt.Println("note: this stops Parley's own delivery only — it does not prevent either peer from messaging the other through a different path outright.")
}

func runRenew(ctx context.Context, ctrl *controller.Controller, args []string) {
	fs := flag.NewFlagSet("renew", flag.ExitOnError)
	conversation := fs.String("conversation", "", "conversation name (required)")
	maxExchanges := fs.Int64("max-exchanges", 0, "new exchange budget (0 keeps the current value)")
	expiresIn := fs.Duration("expires-in", 0, "new TTL from now (0 keeps the current expiry)")
	cancelReplies := fs.Bool("cancel-pending-replies", false, "cancel queued replies instead of carrying them forward")
	fs.Parse(args)
	if *expiresIn < 0 || *maxExchanges < 0 {
		fatalf("expiry and budget must not be negative")
	}
	if *conversation == "" {
		fatalf("renew requires -conversation")
	}
	var expiresAt *time.Time
	if *expiresIn > 0 {
		t := time.Now().Add(*expiresIn)
		expiresAt = &t
	}

	g, err := ctrl.Renew(ctx, controller.RenewParams{
		Conversation:         *conversation,
		CancelPendingReplies: *cancelReplies,
		MaxExchanges:         *maxExchanges,
		ExpiresAt:            expiresAt,
	})
	if err != nil {
		fatalf("renew: %v", err)
	}
	fmt.Printf("renewed %s to v%d: budget %d, expires %s\n", g.Conversation, g.GrantVersion, g.MaxExchanges, orNever(g.ExpiresAt))
}

func orNever(s *string) string {
	if s == nil {
		return "never"
	}
	return *s
}

func usage() {
	fmt.Fprintln(os.Stderr, `parleyctl: the protected Parley controller. Run this directly yourself — never through an agent tool call.

Usage:
  parleyctl grant  -conversation NAME -peer-a ID -peer-b ID -max-exchanges N [-direction bidirectional|a_to_b|b_to_a] [-expires-in DURATION]
  parleyctl revoke -conversation NAME
  parleyctl renew  -conversation NAME [-max-exchanges N] [-expires-in DURATION] [-cancel-pending-replies]

Database path: $PARLEY_DB (default ./parley.db)`)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "parleyctl: "+format+"\n", args...)
	os.Exit(1)
}
