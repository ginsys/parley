// Command parleyd is the standing server that owns SQLite and serves the
// authenticated parley-control/1 administration endpoint. It is a separate
// binary from parleyctl: production account and filesystem controls can
// permit running parleyd without ever granting the direct-database-open code
// path to whichever account runs the client. See AGENTS.md and
// docs/specifications/control.md.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/ginsys/parley/internal/control"
	"github.com/ginsys/parley/internal/recovery"
	"github.com/ginsys/parley/internal/runtime"
	"github.com/ginsys/parley/internal/store"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelp(args[0]) {
		usage(stdout)
		return 0
	}
	switch args[0] {
	case "init":
		return runInit(args[1:], stdout, stderr)
	case "serve":
		return runServe(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "parleyd: unknown command %q\n", args[0])
		usage(stderr)
		return 2
	}
}

func isHelp(s string) bool { return s == "help" || s == "-h" || s == "--help" }

func usage(output io.Writer) {
	fmt.Fprintln(output, `parleyd: the Parley administration server.
Owns the database exclusively. parleyctl is a pure client of the control
endpoint (-endpoint, -server-uid; hello and membership enroll|renew|replace|
revoke) and never opens the database.

Usage:
  parleyd init  -database PATH
  parleyd serve -database PATH -admin-socket PATH -administrator ID=UID [-administrator ID=UID ...]
                [-server-uid UID] [-socket-mode MODE] -recovery-markers-dir PATH [-recovery-markers-capacity N]
  parleyd [help|-h|--help]

init creates a new, empty database at PATH and never overwrites an existing
file there; run it exactly once per deployment before the first serve.
serve refuses to substitute an empty database for a missing PATH -- run init
first. Help and invalid arguments open neither a database nor a socket.
Help exits 0, invalid arguments exit 2, operational failures exit 1.`)
}

// helpOrError renders a FlagSet's own captured -h/usage output on stdout
// (exit 0) or its parse error on stderr (exit 2). FlagSet already writes
// argument-usage text into output for both cases; this only chooses the
// destination and exit code, matching the flag.ErrHelp convention.
func helpOrError(err error, captured string, stdout, stderr io.Writer) int {
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprint(stdout, captured)
		return 0
	}
	fmt.Fprint(stderr, captured)
	return 2
}

// runInit parses "init"'s arguments only; no filesystem or database action
// happens before parsing succeeds.
func runInit(args []string, stdout, stderr io.Writer) int {
	var output strings.Builder
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(&output)
	dbPath := fs.String("database", "", "absolute path for the new database file (required; must not already exist)")
	if err := fs.Parse(args); err != nil {
		return helpOrError(err, output.String(), stdout, stderr)
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "parleyd init: unexpected positional arguments")
		return 2
	}
	if strings.TrimSpace(*dbPath) == "" {
		fmt.Fprintln(stderr, "parleyd init: -database is required")
		return 2
	}
	if !filepath.IsAbs(*dbPath) || filepath.Clean(*dbPath) != *dbPath {
		fmt.Fprintln(stderr, "parleyd init: -database must be an absolute, clean path")
		return 2
	}
	return initDatabase(context.Background(), *dbPath, stdout, stderr)
}

// initDeps lets tests inject a deterministic failure at each phase of
// initDatabaseWith without faking OS-level errors. realInitDeps wires the
// true production functions; initDatabase always uses it -- production
// behavior is exactly initDatabaseWith(realInitDeps()).
//
// closeStore/closeOwner are their own seam, distinct from openStore/acquire:
// store.DB.Close and Ownership.Close are both idempotent, so a test cannot
// force a *second*, distinct close failure through the public API by
// repeating a call the production code already made once successfully.
// Injecting the close functions themselves lets a test make one specific
// close fail deterministically without OS-level fault injection (e.g.
// closing the underlying fd out from under the driver), while
// realInitDeps's versions are exactly db.Close()/owner.Close() -- there is
// no behavior difference in the production path, only a place for a test to
// intercept it.
type initDeps struct {
	acquire    func(path string) (*runtime.Ownership, error)
	openStore  func(ctx context.Context, path string) (*store.DB, error)
	readID     func(ctx context.Context, db *store.DB) (string, error)
	closeStore func(db *store.DB) error
	closeOwner func(owner *runtime.Ownership) error
}

func realInitDeps() initDeps {
	return initDeps{
		acquire:    runtime.Acquire,
		openStore:  store.Open,
		readID:     readServerID,
		closeStore: func(db *store.DB) error { return db.Close() },
		closeOwner: func(owner *runtime.Ownership) error { return owner.Close() },
	}
}

// initDatabase performs the actual initialization; see initDatabaseWith.
func initDatabase(ctx context.Context, path string, stdout, stderr io.Writer) int {
	return initDatabaseWith(ctx, path, stdout, stderr, realInitDeps())
}

// initDatabaseWith is explicit and non-overwriting (O_EXCL refuses any
// existing entry, of any kind, at path), establishes the file at the
// private mode runtime.Acquire requires before Acquire or store.Open ever
// run (so no looser default mode is briefly in effect), then joins the same
// ownership lock cmd/parleyctl's EP-02 path uses before opening the store,
// and finally reads back the installation identity it just created so the
// operator can record it (see docs/operations.md).
//
// It never deletes the file it created, on any failure path (mandate R1):
// runtime.Acquire requires a pre-existing target, so ownership structurally
// cannot be proven before creation. store.Open's migration runs as a single
// atomic transaction (internal/store/migrations.go's migrate: one BEGIN,
// every step, one final Commit, with defer tx.Rollback() covering every
// early return) -- a store.Open failure here means nothing was committed,
// not a partial schema, but this phase still never deletes the file: the
// file's exact on-disk state after a failed migration is not otherwise
// independently re-verified here, and no phase in this function treats
// "nothing should have committed" as license to act on the file without
// inspection. Every failure message instead names exactly which phase
// failed, states plainly that the file is retained, and gives safe,
// actionable next steps; it never claims the file is empty or unusable when
// that has not been established, and never instructs blind deletion. A
// later store-close or identity-read failure after a successful commit is
// reported as exactly that -- a diagnostic problem, not a lost database --
// since store.Open succeeding at all means the installation row is
// guaranteed durably committed (internal/store/registry.go's
// addConnectionRegistry migration step).
func initDatabaseWith(ctx context.Context, path string, stdout, stderr io.Writer, deps initDeps) int {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			fmt.Fprintf(stderr, "parleyd init: %s already exists; init never overwrites an existing database\n", path)
		} else {
			fmt.Fprintf(stderr, "parleyd init: create %s: %v\n", path, err)
		}
		return 1
	}
	if err := f.Close(); err != nil {
		fmt.Fprintf(stderr, "parleyd init: created %s but failed to finalize it (close failed): %v\n", path, err)
		fmt.Fprintln(stderr, "parleyd init: the file exists and is retained; it may be incomplete. Inspect it manually -- init will refuse to overwrite it on retry.")
		return 1
	}

	owner, err := deps.acquire(path)
	if err != nil {
		if errors.Is(err, runtime.ErrAlreadyRunning) {
			fmt.Fprintf(stderr, "parleyd init: created %s, but its ownership is already held by another process\n", path)
			fmt.Fprintln(stderr, "parleyd init: the file exists and is retained. This attempt did not initialize it; its current contents are unverified -- another owner may already be using or have replaced it. Stop the other process before retrying, or investigate a stale lock manually (see docs/operations.md) -- init will refuse to overwrite this file.")
		} else {
			fmt.Fprintf(stderr, "parleyd init: created %s, but ownership could not be established: %v\n", path, err)
			fmt.Fprintln(stderr, "parleyd init: the file exists and is retained. This attempt did not initialize it; its current contents are unverified. Do not delete it blindly -- inspect it manually before deciding how to proceed.")
		}
		return 1
	}

	// owner.Path() is the canonical, trust-walked target runtime.Acquire
	// just resolved -- the same rule cmd/parleyctl's EP-02 openControllerWith
	// applies (acquire and the store opener must target the identical
	// resolved path, not merely the original input string). This is not a
	// claim of protection against a malicious same-UID writer relocating or
	// replacing the target between acquire and open; it only keeps the lock
	// and the opened database pointed at the same entry this attempt itself
	// established, per the deployment's no-replacement/no-relocation
	// assumption.
	db, err := deps.openStore(ctx, owner.Path())
	if err != nil {
		ownerCloseErr := deps.closeOwner(owner)
		fmt.Fprintf(stderr, "parleyd init: created %s, but schema initialization failed: %v\n", path, err)
		fmt.Fprintln(stderr, "parleyd init: the file is retained. Migration runs as a single transaction, so this failure normally means no schema was committed, but do not delete it blindly -- inspect it manually; init will refuse to overwrite this file on retry.")
		if ownerCloseErr != nil {
			fmt.Fprintf(stderr, "parleyd init: additionally failed to release ownership cleanly: %v\n", ownerCloseErr)
		}
		return 1
	}

	serverID, err := deps.readID(ctx, db)
	if err != nil {
		closeErr := errors.Join(deps.closeStore(db), deps.closeOwner(owner))
		fmt.Fprintf(stderr, "parleyd init: created %s: schema initialization (store.Open) reported success, but the subsequent installation-identity read failed: %v\n", path, err)
		if closeErr != nil {
			fmt.Fprintf(stderr, "parleyd init: additionally failed to close cleanly: %v\n", closeErr)
		}
		fmt.Fprintln(stderr, "parleyd init: a failed read does not by itself establish whether the database is corrupt or intact -- verify independently with `parleyctl hello` against a `parleyd serve` on this file, or inspect the installation table directly.")
		return 1
	}

	// Explicit close-and-check on the success path, distinct from any
	// deferred cleanup a failure path above used: both Ownership.Close and
	// store.DB.Close are safe to call more than once, so this cannot
	// conflict with anything else, and it lets a post-success close
	// failure be reported truthfully instead of silently swallowed by a
	// bare defer.
	if err := deps.closeStore(db); err != nil {
		ownerCloseErr := deps.closeOwner(owner)
		fmt.Fprintf(stderr, "parleyd init: %s was initialized (server_id: %s) but failed to close cleanly: %v\n", path, serverID, err)
		fmt.Fprintln(stderr, "parleyd init: the database was written successfully; this failure is limited to closing this handle. Verify before relying on it.")
		if ownerCloseErr != nil {
			fmt.Fprintf(stderr, "parleyd init: additionally failed to release ownership cleanly: %v\n", ownerCloseErr)
		}
		return 1
	}
	if err := deps.closeOwner(owner); err != nil {
		fmt.Fprintf(stderr, "parleyd init: %s was initialized (server_id: %s) but failed to release ownership cleanly: %v\n", path, serverID, err)
		fmt.Fprintln(stderr, "parleyd init: the database was written successfully; this failure is limited to releasing the ownership lock. Verify no stale lock blocks a subsequent `parleyd serve` before relying on it.")
		return 1
	}

	// The database is fully initialized and closed at this point; a failure
	// from here on is confined to reporting that fact, and must never be
	// answered by reinitializing, reopening or deleting anything.
	if _, err := fmt.Fprintf(stdout, "initialized %s\ninstallation server_id: %s\n", path, serverID); err != nil {
		fmt.Fprintf(stderr, "parleyd init: %s was initialized (server_id: %s), but the success message could not be written: %v\n", path, serverID, err)
		fmt.Fprintln(stderr, "parleyd init: the database itself was written and closed successfully; only this status output failed. Do not reinitialize or delete it -- verify with `parleyctl hello`.")
		return 1
	}
	if _, err := fmt.Fprintln(stdout, "Record this server_id. Back up this file only while parleyd is stopped, preserving its -wal/-shm sidecars and ownership (see docs/operations.md)."); err != nil {
		fmt.Fprintf(stderr, "parleyd init: %s was initialized (server_id: %s), but the follow-up guidance could not be written: %v\n", path, serverID, err)
		fmt.Fprintln(stderr, "parleyd init: the database itself was written and closed successfully; only this status output failed. Do not reinitialize or delete it -- verify with `parleyctl hello`.")
		return 1
	}
	return 0
}

// readServerID is the one raw-SQL read every internal caller of this row
// already performs (e.g. internal/recovery/service.go); there is no
// Queries-based accessor for it, by design -- it is read once at startup,
// not on the ordinary reader-pool path.
func readServerID(ctx context.Context, db *store.DB) (string, error) {
	var serverID string
	err := db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT server_id FROM installation WHERE singleton=1").Scan(&serverID)
	})
	return serverID, err
}

// administrators collects repeated -administrator ID=UID flags into an
// immutable map, later validated as a whole by control.NewConfig.
type administrators map[string]uint32

func (a administrators) String() string {
	parts := make([]string, 0, len(a))
	for id, uid := range a {
		parts = append(parts, fmt.Sprintf("%s=%d", id, uid))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func (a administrators) Set(value string) error {
	id, uidText, ok := strings.Cut(value, "=")
	if !ok || id == "" || uidText == "" {
		return fmt.Errorf("expected ADMINISTRATOR_ID=UID, got %q", value)
	}
	if _, exists := a[id]; exists {
		return fmt.Errorf("administrator %q repeated", id)
	}
	// control.ParseUID rejects anything that does not fit a 32-bit
	// platform UID (mandate R2) -- the shared validator every UID-bearing
	// input in this binary and parleyctl reuses, rather than each caller
	// narrowing an unbounded value on its own.
	uid, err := control.ParseUID(uidText)
	if err != nil {
		return fmt.Errorf("invalid UID in %q: %w", value, err)
	}
	a[id] = uid
	return nil
}

// runServe parses "serve"'s arguments and validates the resulting
// control.Config before any filesystem or socket action; no database or
// socket is touched by a help or invalid-argument outcome.
func runServe(args []string, stdout, stderr io.Writer) int {
	var output strings.Builder
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(&output)
	dbPath := fs.String("database", "", "absolute path to an already-initialized database (required; see parleyd init)")
	adminSocket := fs.String("admin-socket", "", "absolute path for the administration Unix socket (required)")
	// Default to the effective UID, not the real UID: internal/connection's
	// established socket/peer-credential identity convention (publication_linux.go)
	// trusts os.Geteuid(), and this flag's value is what a peer's SO_PEERCRED
	// check must actually match (mandate CP-05).
	serverUIDText := fs.String("server-uid", strconv.Itoa(os.Geteuid()), "this process's own effective UID, as administrators/clients verify it via SO_PEERCRED")
	socketMode := fs.String("socket-mode", "0600", "administration socket file mode: 0600, or 0660 with an explicitly provisioned administrator-only group")
	markersDir := fs.String("recovery-markers-dir", "", "absolute path to a private (mode 0700), server-owned directory for durable recovery markers (required)")
	markersCapacity := fs.Int("recovery-markers-capacity", 64, "bounded materialization capacity for recovery marker listing")
	admins := make(administrators)
	fs.Var(admins, "administrator", "administrator UUID=UID; repeatable, at least one required")
	if err := fs.Parse(args); err != nil {
		return helpOrError(err, output.String(), stdout, stderr)
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "parleyd serve: unexpected positional arguments")
		return 2
	}
	for _, required := range []struct{ name, value string }{
		{"-database", *dbPath},
		{"-admin-socket", *adminSocket},
		{"-recovery-markers-dir", *markersDir},
	} {
		if strings.TrimSpace(required.value) == "" {
			fmt.Fprintf(stderr, "parleyd serve: %s is required\n", required.name)
			return 2
		}
	}
	if len(admins) == 0 {
		fmt.Fprintln(stderr, "parleyd serve: at least one -administrator is required")
		return 2
	}
	mode, err := strconv.ParseUint(*socketMode, 8, 32)
	if err != nil {
		fmt.Fprintf(stderr, "parleyd serve: invalid -socket-mode %q: %v\n", *socketMode, err)
		return 2
	}
	// Reject any mode outside the two the listener actually supports, here
	// -- before recovery.NewDirectory or database access run -- rather than
	// deep inside control.Listen where the same check exists today but only
	// after other startup I/O has already occurred (mandate CP-01). An
	// invalid local argument must be rejected before it has operational
	// effects, not merely before the process exits.
	if mode != 0600 && mode != 0660 {
		fmt.Fprintf(stderr, "parleyd serve: -socket-mode must be 0600 or 0660, got %q\n", *socketMode)
		return 2
	}
	// recovery.NewDirectory rejects capacity <= 0 too, but only once
	// serve() is already running -- surfacing there misreports an invalid
	// local argument as an operational failure (mandate CP-01).
	if *markersCapacity <= 0 {
		fmt.Fprintf(stderr, "parleyd serve: -recovery-markers-capacity must be positive, got %d\n", *markersCapacity)
		return 2
	}
	// control.ParseUID validates the full value before any narrowing
	// (mandate R2): fs.Uint's unchecked uint32(*serverUID) conversion let
	// 2^32 silently become 0 (root) on a 64-bit host.
	serverUID, err := control.ParseUID(*serverUIDText)
	if err != nil {
		fmt.Fprintf(stderr, "parleyd serve: invalid -server-uid %q: %v\n", *serverUIDText, err)
		return 2
	}
	// The configured server UID must match the identity socket/peer
	// authentication actually uses (os.Geteuid(), the same convention
	// internal/connection/publication_linux.go establishes) -- an
	// explicitly supplied value that names a different account would make
	// every hello/response claim an identity this process cannot prove and
	// SO_PEERCRED will never actually match (mandate CP-04).
	if serverUID != uint32(os.Geteuid()) {
		fmt.Fprintf(stderr, "parleyd serve: -server-uid %d does not match this process's effective UID %d\n", serverUID, os.Geteuid())
		return 2
	}
	// -admin-socket's absoluteness is enforced by control.NewConfig below.
	// -database and -recovery-markers-dir are checked here so behavior does
	// not depend on the server's working directory across restarts.
	if !filepath.IsAbs(*dbPath) || filepath.Clean(*dbPath) != *dbPath {
		fmt.Fprintln(stderr, "parleyd serve: -database must be an absolute, clean path")
		return 2
	}
	if !filepath.IsAbs(*markersDir) || filepath.Clean(*markersDir) != *markersDir {
		fmt.Fprintln(stderr, "parleyd serve: -recovery-markers-dir must be an absolute, clean path")
		return 2
	}
	controlCfg, err := control.NewConfig(*adminSocket, serverUID, admins)
	if err != nil {
		fmt.Fprintf(stderr, "parleyd serve: %v\n", err)
		return 2
	}

	return serve(context.Background(), serveConfig{
		databasePath:    *dbPath,
		control:         controlCfg,
		socketMode:      os.FileMode(mode),
		markersDir:      *markersDir,
		markersUID:      serverUID,
		markersCapacity: *markersCapacity,
	}, stdout, stderr)
}

type serveConfig struct {
	databasePath    string
	control         control.Config
	socketMode      os.FileMode
	markersDir      string
	markersUID      uint32
	markersCapacity int
}

// serve performs the actual startup: bind the admin socket and open the
// recovery marker directory (both fail closed with no partial state left
// running), then hand ownership acquisition, the writer open, migration,
// recovery inspection and reader startup entirely to runtime.Start, per
// docs/runtime.md's sequencing. It blocks until an interrupt/TERM signal or
// a fail-stop condition, then performs runtime's own ordered shutdown.
func serve(ctx context.Context, cfg serveConfig, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// listenerService does not bind cfg.control's socket yet -- Start does,
	// once runtime.Start has already acquired exclusive database ownership.
	// A process that loses that race must never create, probe or replace
	// the admin socket at all (docs/runtime.md's startup ordering). It
	// resolves its own server/epoch identity from the writer runtime.Start
	// supplies (mandate R6) rather than being handed a separately minted one.
	listenerService := control.NewListenerService(cfg.control, cfg.socketMode)

	markers, err := recovery.NewDirectory(cfg.markersDir, cfg.markersUID, cfg.markersCapacity)
	if err != nil {
		fmt.Fprintf(stderr, "parleyd serve: open recovery marker directory: %v\n", err)
		return 1
	}

	// stopSignal generalizes the previous failStop-only signal (mandate
	// R4) to carry an optional message and to be triggerable from either
	// of two independent sources -- recovery's own fail-stop callback, or
	// the listener's accept loop giving up on an unexpected error -- while
	// remaining exactly one "wake main and stop" mechanism, not two. Both
	// callbacks must stay nonblocking, perform no I/O and never reenter the
	// store; the actual shutdown/exit-code reaction happens in the select
	// loop below, outside either callback.
	stopSignal := make(chan struct{})
	var stopOnce sync.Once
	var stopMessage string
	triggerStop := func(message string) {
		stopOnce.Do(func() {
			stopMessage = message
			close(stopSignal)
		})
	}
	failStop := func() {
		triggerStop("recovery evidence could not be made durable; stopping for supervised recovery")
	}
	listenerService.OnAcceptFailure(func(err error) {
		triggerStop(fmt.Sprintf("administration listener stopped accepting connections: %v", err))
	})

	var recoveryService *recovery.Service
	runtimeCfg := runtime.Config{
		DatabasePath: cfg.databasePath,
		InspectRecovery: func(ctx context.Context, writer *store.DB) (runtime.RecoveryMode, error) {
			recoveryService, err = recovery.New(ctx, recovery.Config{Store: writer, Markers: markers, FailStop: failStop})
			if err != nil {
				return 0, err
			}
			return recoveryService.InspectRecovery(ctx, writer)
		},
		// RecoveryOnly: the administration endpoint must still answer
		// server.hello/operation.get under a recovery hold -- this is the
		// limited human-recovery surface docs/architecture.md's D3 note
		// describes -- rather than runtime.Start silently skipping this
		// registration the way it does for ordinary (non-RecoveryOnly)
		// services while held.
		Services: []runtime.Registration{{Service: listenerService, RecoveryOnly: true}},
	}

	running, err := runtime.Start(ctx, runtimeCfg)
	if err != nil {
		fmt.Fprintf(stderr, "parleyd serve: %v\n", err)
		return 1
	}

	select {
	case <-ctx.Done():
	case <-stopSignal:
		fmt.Fprintf(stderr, "parleyd serve: %s\n", stopMessage)
	}
	if err := running.Stop(context.Background()); err != nil {
		fmt.Fprintf(stderr, "parleyd serve: shutdown: %v\n", err)
		return 1
	}
	select {
	case <-stopSignal:
		return 1
	default:
		return 0
	}
}
