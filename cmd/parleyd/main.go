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
	"github.com/google/uuid"
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
Owns the database exclusively; parleyctl never opens it directly.

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

// initDatabase performs the actual initialization: it is explicit and
// non-overwriting (O_EXCL refuses any existing entry, of any kind, at path),
// establishes the file at the private mode runtime.Acquire requires before
// Acquire or store.Open ever run (so no looser default mode is briefly in
// effect), then joins the same ownership lock cmd/parleyctl's EP-02 path
// uses before opening the store, and finally reads back the installation
// identity it just created so the operator can record it (see
// docs/operations.md).
func initDatabase(ctx context.Context, path string, stdout, stderr io.Writer) int {
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
		fmt.Fprintf(stderr, "parleyd init: %v\n", err)
		return 1
	}

	owner, err := runtime.Acquire(path)
	if err != nil {
		fmt.Fprintf(stderr, "parleyd init: acquire ownership: %v\n", err)
		return 1
	}
	defer owner.Close()

	db, err := store.Open(ctx, path)
	if err != nil {
		fmt.Fprintf(stderr, "parleyd init: initialize schema: %v\n", err)
		return 1
	}
	defer db.Close()

	serverID, err := readServerID(ctx, db)
	if err != nil {
		fmt.Fprintf(stderr, "parleyd init: read installation identity: %v\n", err)
		return 1
	}

	fmt.Fprintf(stdout, "initialized %s\ninstallation server_id: %s\n", path, serverID)
	fmt.Fprintln(stdout, "Record this server_id. Back up this file only while parleyd is stopped, preserving its -wal/-shm sidecars and ownership (see docs/operations.md).")
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
	uid, err := strconv.ParseUint(uidText, 10, 32)
	if err != nil {
		return fmt.Errorf("invalid UID in %q: %w", value, err)
	}
	a[id] = uint32(uid)
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
	serverUID := fs.Uint("server-uid", uint(os.Getuid()), "this process's own UID, as administrators/clients verify it")
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
	controlCfg, err := control.NewConfig(*adminSocket, uint32(*serverUID), admins)
	if err != nil {
		fmt.Fprintf(stderr, "parleyd serve: %v\n", err)
		return 2
	}

	return serve(context.Background(), serveConfig{
		databasePath:    *dbPath,
		control:         controlCfg,
		socketMode:      os.FileMode(mode),
		markersDir:      *markersDir,
		markersUID:      uint32(*serverUID),
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

	listener, err := control.Listen(cfg.control, cfg.socketMode)
	if err != nil {
		fmt.Fprintf(stderr, "parleyd serve: bind admin socket: %v\n", err)
		return 1
	}
	listenerService := control.NewListenerService(listener, cfg.control, uuid.NewString())

	markers, err := recovery.NewDirectory(cfg.markersDir, cfg.markersUID, cfg.markersCapacity)
	if err != nil {
		listener.Close()
		fmt.Fprintf(stderr, "parleyd serve: open recovery marker directory: %v\n", err)
		return 1
	}

	// failStop must be nonblocking, perform no I/O and never reenter the
	// store (internal/recovery/service.go's Config.FailStop contract). It
	// only signals; the actual shutdown/exit-code reaction happens in the
	// select loop below, outside this callback.
	failStopSignal := make(chan struct{})
	var failStopOnce sync.Once
	failStop := func() { failStopOnce.Do(func() { close(failStopSignal) }) }

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
		Services: []runtime.Registration{{Service: listenerService}},
	}

	running, err := runtime.Start(ctx, runtimeCfg)
	if err != nil {
		listener.Close()
		fmt.Fprintf(stderr, "parleyd serve: %v\n", err)
		return 1
	}

	select {
	case <-ctx.Done():
	case <-failStopSignal:
		fmt.Fprintln(stderr, "parleyd serve: recovery evidence could not be made durable; stopping for supervised recovery")
	}
	if err := running.Stop(context.Background()); err != nil {
		fmt.Fprintf(stderr, "parleyd serve: shutdown: %v\n", err)
		return 1
	}
	select {
	case <-failStopSignal:
		return 1
	default:
		return 0
	}
}
