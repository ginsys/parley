package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/control"
	"github.com/ginsys/parley/internal/runtime"
	"github.com/ginsys/parley/internal/store"
)

// privateDir mirrors internal/runtime/ownership_linux_test.go's disposable
// private-directory pattern: runtime.Acquire, connection.TrustedDirectory
// and recovery.NewDirectory all require a non-group/other-writable parent,
// which t.TempDir()'s shared per-package work directory does not guarantee
// under this host's umask.
func privateDir(t *testing.T, prefix string) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestRunHelpAndNoArgsShowUsageWithoutTouchingAnything(t *testing.T) {
	for _, args := range [][]string{nil, {"help"}, {"-h"}, {"--help"}} {
		var out, errOut bytes.Buffer
		code := run(args, &out, &errOut)
		if code != 0 || !strings.Contains(out.String(), "parleyd:") {
			t.Fatalf("args=%v code=%d out=%q", args, code, out.String())
		}
	}
}

func TestRunUnknownCommandExitsTwo(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"bogus"}, &out, &errOut); code != 2 {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
}

func TestInitRequiresDatabaseFlag(t *testing.T) {
	var out, errOut bytes.Buffer
	code := runInit(nil, &out, &errOut)
	if code != 2 || !strings.Contains(errOut.String(), "-database is required") {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
}

func TestInitRejectsRelativePath(t *testing.T) {
	var out, errOut bytes.Buffer
	code := runInit([]string{"-database", "relative.db"}, &out, &errOut)
	if code != 2 {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
}

// TestInitHelpTouchesNoFile passes a real, disposable -database path before
// -h (rather than omitting the flag entirely, as an earlier version of this
// test did): omitting it proved only that init works with no -database at
// all, never that a help request ignores one that was actually supplied.
// The lock file is checked too, not just the database file itself, since
// runtime.Acquire creates a separate <path>.lock entry.
func TestInitHelpTouchesNoFile(t *testing.T) {
	dir := privateDir(t, "parleyd-init-help-")
	path := filepath.Join(dir, "parley.db")
	var out, errOut bytes.Buffer
	if code := runInit([]string{"-database", path, "-h"}, &out, &errOut); code != 0 {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("help touched the database path")
	}
	if _, err := os.Stat(path + ".lock"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("help touched the ownership lock path")
	}
}

func TestInitCreatesDatabaseAndRefusesOverwrite(t *testing.T) {
	dir := privateDir(t, "parleyd-init-")
	path := filepath.Join(dir, "parley.db")

	var out, errOut bytes.Buffer
	if code := runInit([]string{"-database", path}, &out, &errOut); code != 0 {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
	if !strings.Contains(out.String(), "installation server_id:") {
		t.Fatalf("out=%q", out.String())
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode=%v", info.Mode())
	}

	out.Reset()
	errOut.Reset()
	code := runInit([]string{"-database", path}, &out, &errOut)
	if code != 1 || !strings.Contains(errOut.String(), "already exists") {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
}

func TestAdministratorsFlagParsesAndRejectsMalformed(t *testing.T) {
	a := make(administrators)
	if err := a.Set("70000000-0000-4000-8000-000000000001=1000"); err != nil {
		t.Fatal(err)
	}
	if a["70000000-0000-4000-8000-000000000001"] != 1000 {
		t.Fatalf("%#v", a)
	}
	if err := a.Set("70000000-0000-4000-8000-000000000001=1000"); err == nil {
		t.Fatal("expected duplicate rejection")
	}
	for _, bad := range []string{"", "no-equals", "id=", "=1000", "id=notanumber"} {
		b := make(administrators)
		if err := b.Set(bad); err == nil {
			t.Fatalf("expected rejection for %q", bad)
		}
	}
}

// TestAdministratorsFlagRejectsUIDOverflowAndReservedSentinel exercises
// mandate R2 at the -administrator flag: fs.Uint's own unchecked
// uint32(*serverUID) narrowing let 2^32 silently become 0 (root) on a
// 64-bit host; administrators.Set now goes through control.ParseUID, which
// checks the full value before any conversion.
func TestAdministratorsFlagRejectsUIDOverflowAndReservedSentinel(t *testing.T) {
	const id = "70000000-0000-4000-8000-000000000001"
	for _, bad := range []string{
		"4294967296",          // 2^32: the exact overflow this mandate names
		"8589934592",          // 2^33
		"4294967296000000001", // 2^32 + a valid-looking UID, still overflow
		"4294967295",          // reserved (uid_t)-1 sentinel
		"-1",                  // negative
	} {
		b := make(administrators)
		if err := b.Set(id + "=" + bad); err == nil {
			t.Fatalf("uid=%q: expected rejection, got %#v", bad, b)
		}
	}
	// The supported boundary values must still work: zero (root, a
	// legitimately supported administrator identity) and the largest valid
	// 32-bit UID.
	for _, ok := range []string{"0", "4294967294"} {
		b := make(administrators)
		if err := b.Set(id + "=" + ok); err != nil {
			t.Fatalf("uid=%q: unexpected rejection: %v", ok, err)
		}
	}
}

// TestServeRejectsOverflowAndReservedServerUID is R2's sweep onto
// -server-uid: an invalid value must be rejected as an argument error
// before serve ever touches a database or socket, exactly like the other
// TestServeRequiresFlagsBeforeAnyIO cases.
func TestServeRejectsOverflowAndReservedServerUID(t *testing.T) {
	const admin = "-administrator=70000000-0000-4000-8000-000000000001=1000"
	for _, bad := range []string{"4294967296", "4294967295", "-1", "notanumber"} {
		t.Run(bad, func(t *testing.T) {
			var out, errOut bytes.Buffer
			args := []string{
				"-database=/x/parley.db", "-admin-socket=/y/admin.sock", admin,
				"-recovery-markers-dir=/z", "-server-uid=" + bad,
			}
			code := runServe(args, &out, &errOut)
			if code != 2 || !strings.Contains(errOut.String(), "invalid -server-uid") {
				t.Fatalf("uid=%q: code=%d err=%q", bad, code, errOut.String())
			}
		})
	}
}

// initTestDeps builds initDeps with every function overridden by an
// explicit failure marker, so a test that expects a given phase never to
// be reached fails loudly (not silently) if that assumption is wrong. A
// test exercising a phase past acquire/openStore must explicitly override
// closeStore/closeOwner with the real thing (or its own injected failure),
// since those are reached on paths this default deliberately refuses.
func initTestDeps(t *testing.T) initDeps {
	t.Helper()
	unreachable := func(name string) func() { return func() { t.Fatalf("%s should not have been called", name) } }
	return initDeps{
		acquire: func(string) (*runtime.Ownership, error) {
			unreachable("acquire")()
			return nil, nil
		},
		openStore: func(context.Context, string) (*store.DB, error) {
			unreachable("openStore")()
			return nil, nil
		},
		readID: func(context.Context, *store.DB) (string, error) {
			unreachable("readID")()
			return "", nil
		},
		closeStore: func(*store.DB) error {
			unreachable("closeStore")()
			return nil
		},
		closeOwner: func(*runtime.Ownership) error {
			unreachable("closeOwner")()
			return nil
		},
	}
}

// TestInitDatabaseWithRejectsPreExistingTargetWithoutDeletingIt exercises
// R1's non-overwrite behavior directly through initDatabaseWith (rather
// than only via the already-existing runInit-level coverage), confirming
// the pre-existing entry -- of any kind, here a directory -- is left
// completely untouched.
func TestInitDatabaseWithRejectsPreExistingTargetWithoutDeletingIt(t *testing.T) {
	dir := privateDir(t, "parleyd-initdeps-preexisting-")
	path := filepath.Join(dir, "parley.db")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := initDatabaseWith(context.Background(), path, &out, &errOut, initTestDeps(t))
	if code != 1 || !strings.Contains(errOut.String(), "already exists") {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		t.Fatalf("pre-existing target was altered: err=%v info=%v", err, info)
	}
}

// TestInitDatabaseWithOwnershipContentionPreservesFileAndNamesThePhase
// injects runtime.ErrAlreadyRunning at the acquire phase (mandate R1's
// "ownership contention" case): the created file must be retained, never
// deleted, and the message must name contention specifically rather than a
// generic failure or an empty/unusable database.
func TestInitDatabaseWithOwnershipContentionPreservesFileAndNamesThePhase(t *testing.T) {
	dir := privateDir(t, "parleyd-initdeps-contention-")
	path := filepath.Join(dir, "parley.db")
	deps := initTestDeps(t)
	deps.acquire = func(string) (*runtime.Ownership, error) { return nil, runtime.ErrAlreadyRunning }

	var out, errOut bytes.Buffer
	code := initDatabaseWith(context.Background(), path, &out, &errOut, deps)
	if code != 1 {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
	msg := errOut.String()
	if !strings.Contains(msg, "already held by another process") {
		t.Fatalf("err=%q", msg)
	}
	if strings.Contains(msg, "empty") || strings.Contains(msg, "delete it") && !strings.Contains(msg, "not delete it blindly") {
		t.Fatalf("message must not instruct blind deletion or claim an empty database: %q", msg)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("created file was not retained: %v", err)
	}

	// Retry/preserved-state outcome: a second attempt at the same path
	// still refuses, via the ordinary non-overwrite check, never silently
	// reopening or overwriting what the failed attempt left behind.
	out.Reset()
	errOut.Reset()
	retryDeps := initTestDeps(t)
	code = initDatabaseWith(context.Background(), path, &out, &errOut, retryDeps)
	if code != 1 || !strings.Contains(errOut.String(), "already exists") {
		t.Fatalf("retry: code=%d err=%q", code, errOut.String())
	}
}

// TestInitDatabaseWithForeignOwnershipFailurePreservesFile covers R1's
// "target replaced/not owned by the attempt" case: a non-ErrAlreadyRunning
// acquire failure (e.g. the trust walk rejecting an entry that is no
// longer the file this attempt created) must be reported honestly as an
// ownership failure, distinct from contention, without deleting anything.
func TestInitDatabaseWithForeignOwnershipFailurePreservesFile(t *testing.T) {
	dir := privateDir(t, "parleyd-initdeps-foreign-")
	path := filepath.Join(dir, "parley.db")
	sentinel := errors.New("synthetic: target is not the file this attempt created")
	deps := initTestDeps(t)
	deps.acquire = func(string) (*runtime.Ownership, error) { return nil, sentinel }

	var out, errOut bytes.Buffer
	code := initDatabaseWith(context.Background(), path, &out, &errOut, deps)
	if code != 1 {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
	msg := errOut.String()
	if !strings.Contains(msg, "ownership could not be established") || strings.Contains(msg, "already held by another process") {
		t.Fatalf("err=%q", msg)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("created file was not retained: %v", err)
	}
}

// TestInitDatabaseWithSchemaFailurePreservesFile covers a named
// post-create failure phase (store.Open/schema migration): the file must
// be retained and the message must not claim the database is definitely
// empty or unusable -- store.Open's migration runs as a single atomic
// transaction (internal/store/migrations.go's migrate: one BEGIN, every
// step, one final Commit, with defer tx.Rollback() covering every early
// return), so a failure here normally means nothing at all was committed,
// not a partial schema -- but this phase still never asserts that as fact
// on the file's own behalf, only reports the phase that failed.
func TestInitDatabaseWithSchemaFailurePreservesFile(t *testing.T) {
	dir := privateDir(t, "parleyd-initdeps-schema-")
	path := filepath.Join(dir, "parley.db")
	sentinel := errors.New("synthetic: schema migration failed")
	deps := initTestDeps(t)
	deps.acquire = runtime.Acquire
	deps.openStore = func(context.Context, string) (*store.DB, error) { return nil, sentinel }
	deps.closeOwner = func(o *runtime.Ownership) error { return o.Close() }

	var out, errOut bytes.Buffer
	code := initDatabaseWith(context.Background(), path, &out, &errOut, deps)
	if code != 1 {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
	msg := errOut.String()
	if !strings.Contains(msg, "schema initialization failed") {
		t.Fatalf("err=%q", msg)
	}
	if strings.Contains(msg, "empty database") {
		t.Fatalf("message must not assert the database is empty: %q", msg)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("created file was not retained: %v", err)
	}
	// The ownership lock must have been released on this failure path (not
	// leaked), so a fresh attempt at a different path is unaffected and,
	// per the non-overwrite rule, a retry at the same path still refuses.
	out.Reset()
	errOut.Reset()
	code = initDatabaseWith(context.Background(), path, &out, &errOut, initTestDeps(t))
	if code != 1 || !strings.Contains(errOut.String(), "already exists") {
		t.Fatalf("retry: code=%d err=%q", code, errOut.String())
	}
}

// TestInitDatabaseWithIdentityReadFailureAfterRealCommitIsHonest covers
// R1's "successful init followed by identity/output/close failure" case
// using a real store.Open (so the schema and the installation row are
// genuinely committed), with only the final readID phase injected to
// fail. The message must say the database was actually initialized, not
// imply corruption or data loss, and the database must still be usable
// (verified by reopening it with the real deps afterward).
func TestInitDatabaseWithIdentityReadFailureAfterRealCommitIsHonest(t *testing.T) {
	dir := privateDir(t, "parleyd-initdeps-identity-")
	path := filepath.Join(dir, "parley.db")
	sentinel := errors.New("synthetic: identity readback failed")
	deps := initDeps{
		acquire:    runtime.Acquire,
		openStore:  store.Open,
		readID:     func(context.Context, *store.DB) (string, error) { return "", sentinel },
		closeStore: func(db *store.DB) error { return db.Close() },
		closeOwner: func(o *runtime.Ownership) error { return o.Close() },
	}

	var out, errOut bytes.Buffer
	code := initDatabaseWith(context.Background(), path, &out, &errOut, deps)
	if code != 1 {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
	msg := errOut.String()
	// The message must name which earlier phase succeeded (store.Open) and
	// which later, distinct observation failed (the identity read) -- and
	// must not claim a failed read establishes either corruption or its
	// absence.
	if !strings.Contains(msg, "store.Open") || !strings.Contains(msg, "reported success") {
		t.Fatalf("err=%q", msg)
	}
	if !strings.Contains(msg, "does not by itself establish whether the database is corrupt") {
		t.Fatalf("err=%q", msg)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("created file was not retained: %v", err)
	}

	// The database is genuinely usable: reopen it for real and confirm a
	// real installation.server_id exists, proving the commit reported as
	// successful actually was.
	ctx := context.Background()
	owner, err := runtime.Acquire(path)
	if err != nil {
		t.Fatalf("reacquire after reported success: %v", err)
	}
	defer owner.Close()
	db, err := store.OpenExisting(ctx, path)
	if err != nil {
		t.Fatalf("reopen after reported success: %v", err)
	}
	defer db.Close()
	var serverID string
	if err := db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT server_id FROM installation WHERE singleton=1").Scan(&serverID)
	}); err != nil || serverID == "" {
		t.Fatalf("installation row missing after reported success: err=%v serverID=%q", err, serverID)
	}
}

// TestInitDatabaseWithStoreCloseFailureIsReportedAndOwnershipStillReleased
// covers R1's close-failure requirement through the closeStore/closeOwner
// seam: store.DB.Close and Ownership.Close are both idempotent, so a real
// second close after a genuinely successful first one cannot be made to
// fail through the public API -- there is no OS-level fault to inject here
// without faking the filesystem. Injecting closeStore directly forces this
// specific, distinct failure deterministically. The failure must be
// reported (not swallowed), and ownership must still be released
// afterward: initDatabaseWith's own code always calls closeOwner next
// regardless of closeStore's outcome, so the fixture confirms that
// wiring, not just that closeStore ran.
func TestInitDatabaseWithStoreCloseFailureIsReportedAndOwnershipStillReleased(t *testing.T) {
	dir := privateDir(t, "parleyd-initdeps-storeclose-")
	path := filepath.Join(dir, "parley.db")
	sentinel := errors.New("synthetic: store close failed")
	ownerClosed := false
	deps := initDeps{
		acquire:    runtime.Acquire,
		openStore:  store.Open,
		readID:     readServerID,
		closeStore: func(*store.DB) error { return sentinel },
		closeOwner: func(o *runtime.Ownership) error { ownerClosed = true; return o.Close() },
	}

	var out, errOut bytes.Buffer
	code := initDatabaseWith(context.Background(), path, &out, &errOut, deps)
	if code != 1 {
		t.Fatalf("code=%d out=%q err=%q", code, out.String(), errOut.String())
	}
	msg := errOut.String()
	if !strings.Contains(msg, "failed to close cleanly") || !strings.Contains(msg, sentinel.Error()) {
		t.Fatalf("err=%q", msg)
	}
	if !ownerClosed {
		t.Fatal("ownership was not released after a store-close failure")
	}
	// Ownership must actually be free (not merely reported as released): a
	// fresh acquire at the same path must succeed.
	owner, err := runtime.Acquire(path)
	if err != nil {
		t.Fatalf("ownership was not actually released: %v", err)
	}
	owner.Close()
}

// TestInitDatabaseWithOwnerCloseFailureAfterStoreCloseSucceedsIsReported
// covers the second, independent close-failure path: a successful
// store.DB.Close followed by a failing Ownership.Close. Distinguishing
// this from the previous test proves both close calls are independently
// checked and reported, not folded into one combined check that could mask
// which one actually failed.
func TestInitDatabaseWithOwnerCloseFailureAfterStoreCloseSucceedsIsReported(t *testing.T) {
	dir := privateDir(t, "parleyd-initdeps-ownerclose-")
	path := filepath.Join(dir, "parley.db")
	sentinel := errors.New("synthetic: ownership close failed")
	storeClosed := false
	deps := initDeps{
		acquire:    runtime.Acquire,
		openStore:  store.Open,
		readID:     readServerID,
		closeStore: func(db *store.DB) error { storeClosed = true; return db.Close() },
		closeOwner: func(*runtime.Ownership) error { return sentinel },
	}

	var out, errOut bytes.Buffer
	code := initDatabaseWith(context.Background(), path, &out, &errOut, deps)
	if code != 1 {
		t.Fatalf("code=%d out=%q err=%q", code, out.String(), errOut.String())
	}
	if !storeClosed {
		t.Fatal("store was not closed before the ownership-close failure was reported")
	}
	msg := errOut.String()
	if !strings.Contains(msg, "failed to release ownership cleanly") || !strings.Contains(msg, sentinel.Error()) {
		t.Fatalf("err=%q", msg)
	}
}

// TestInitDatabaseWithOpensTheCanonicalOwnershipPathNotTheRawInput covers
// R1's canonical-path rule directly: deps.openStore must receive
// owner.Path() (the resolved path runtime.Acquire actually locked), not
// initDatabaseWith's raw path argument. A symlinked parent directory makes
// the two differ textually while still naming the same on-disk file, so a
// regression back to passing the raw path is caught even though both
// strings would happen to open the identical database today.
func TestInitDatabaseWithOpensTheCanonicalOwnershipPathNotTheRawInput(t *testing.T) {
	realDir := privateDir(t, "parleyd-initdeps-canonical-real-")
	linkParent := privateDir(t, "parleyd-initdeps-canonical-link-")
	aliasDir := filepath.Join(linkParent, "alias")
	if err := os.Symlink(realDir, aliasDir); err != nil {
		t.Fatal(err)
	}
	rawPath := filepath.Join(aliasDir, "parley.db")

	var openedPath string
	deps := initTestDeps(t)
	deps.acquire = runtime.Acquire
	deps.openStore = func(_ context.Context, p string) (*store.DB, error) {
		openedPath = p
		return nil, errors.New("synthetic: stop before a real store.Open")
	}
	deps.closeOwner = func(o *runtime.Ownership) error { return o.Close() }

	var out, errOut bytes.Buffer
	code := initDatabaseWith(context.Background(), rawPath, &out, &errOut, deps)
	if code != 1 {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
	if openedPath == "" {
		t.Fatal("openStore was never called")
	}
	if openedPath == rawPath {
		t.Fatalf("openStore received the raw symlinked path %q instead of the resolved canonical path", rawPath)
	}
	if want := filepath.Join(realDir, "parley.db"); openedPath != want {
		t.Fatalf("openStore path=%q, want the canonical resolved path %q", openedPath, want)
	}
}

// failAfterWriter fails on its failAt'th call to Write (1-indexed),
// succeeding on every earlier call by delegating to an underlying buffer.
// This lets a test force a stdout failure either on the very first status
// line or only on a later one, distinguishing "nothing was reported" from
// "the success line already got out before a later write failed".
type failAfterWriter struct {
	buf    bytes.Buffer
	failAt int
	calls  int
}

func (w *failAfterWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		return 0, errors.New("synthetic: stdout write failed")
	}
	return w.buf.Write(p)
}

// TestInitDatabaseWithStdoutFailurePreservesDatabaseAndReportsFailure covers
// R1's output/publication requirement directly: initDatabaseWith previously
// ignored the final fmt.Fprintf/Fprintln errors and always returned 0, so a
// failing output sink could be reported as successful completion. Both the
// first write (the success/server_id line) and the second (the follow-up
// backup guidance) are exercised, proving the check is not limited to only
// the very first write. In every case the database itself, already
// initialized and closed before either write runs, must remain usable --
// this is a status-output failure, never a reason to reinitialize or delete
// anything.
func TestInitDatabaseWithStdoutFailurePreservesDatabaseAndReportsFailure(t *testing.T) {
	cases := []struct {
		name    string
		failAt  int
		wantMsg string
	}{
		{"first write (success/server_id line)", 1, "success message could not be written"},
		{"second write (follow-up backup guidance)", 2, "follow-up guidance could not be written"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := privateDir(t, "parleyd-initdeps-stdoutfail-")
			path := filepath.Join(dir, "parley.db")
			stdout := &failAfterWriter{failAt: c.failAt}
			var errOut bytes.Buffer
			code := initDatabaseWith(context.Background(), path, stdout, &errOut, realInitDeps())
			if code != 1 {
				t.Fatalf("code=%d err=%q", code, errOut.String())
			}
			if !strings.Contains(errOut.String(), c.wantMsg) {
				t.Fatalf("err=%q, want substring %q", errOut.String(), c.wantMsg)
			}
			if !strings.Contains(errOut.String(), "Do not reinitialize or delete it") {
				t.Fatalf("err=%q", errOut.String())
			}

			// The database is genuinely usable despite the reported failure.
			owner, err := runtime.Acquire(path)
			if err != nil {
				t.Fatalf("reacquire after reported output failure: %v", err)
			}
			defer owner.Close()
			db, err := store.OpenExisting(context.Background(), path)
			if err != nil {
				t.Fatalf("reopen after reported output failure: %v", err)
			}
			defer db.Close()
			var serverID string
			if err := db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
				return tx.QueryRowContext(ctx, "SELECT server_id FROM installation WHERE singleton=1").Scan(&serverID)
			}); err != nil || serverID == "" {
				t.Fatalf("installation row missing after reported output failure: err=%v serverID=%q", err, serverID)
			}
		})
	}
}

func TestServeRequiresFlagsBeforeAnyIO(t *testing.T) {
	const admin = "-administrator=70000000-0000-4000-8000-000000000001=1000"
	cases := []struct {
		name     string
		args     []string
		wantText string
	}{
		{"missing database", []string{"-admin-socket=/x/admin.sock", admin, "-recovery-markers-dir=/y"}, "-database is required"},
		{"missing socket", []string{"-database=/x/parley.db", admin, "-recovery-markers-dir=/y"}, "-admin-socket is required"},
		{"missing markers dir", []string{"-database=/x/parley.db", "-admin-socket=/y/admin.sock", admin}, "-recovery-markers-dir is required"},
		{"missing administrator", []string{"-database=/x/parley.db", "-admin-socket=/y/admin.sock", "-recovery-markers-dir=/z"}, "at least one -administrator is required"},
		{"bad socket mode", []string{"-database=/x/parley.db", "-admin-socket=/y/admin.sock", admin, "-recovery-markers-dir=/z", "-socket-mode=bogus"}, "invalid -socket-mode"},
		{"relative database", []string{"-database=relative.db", "-admin-socket=/y/admin.sock", admin, "-recovery-markers-dir=/z"}, "-database must be an absolute"},
		{"relative markers dir", []string{"-database=/x/parley.db", "-admin-socket=/y/admin.sock", admin, "-recovery-markers-dir=relative"}, "-recovery-markers-dir must be an absolute"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			code := runServe(c.args, &out, &errOut)
			if code != 2 || !strings.Contains(errOut.String(), c.wantText) {
				t.Fatalf("code=%d err=%q", code, errOut.String())
			}
			// None of these are I/O failures; -database and -admin-socket
			// name paths that do not exist, so any file/socket touch would
			// surface as a different, later error instead of exit code 2.
		})
	}
}

// TestServeHelpTouchesNoFileOrSocket supplies every other required flag with
// real, disposable, otherwise-valid paths before -h -- an earlier version of
// this test passed only "-h" alone, which proves nothing about a help
// request that arrives alongside real arguments, since runServe never
// reaches any I/O until after flag parsing regardless of what those
// arguments are.
func TestServeHelpTouchesNoFileOrSocket(t *testing.T) {
	dir := privateDir(t, "parleyd-serve-help-")
	dbPath := filepath.Join(dir, "parley.db")
	socketPath := filepath.Join(dir, "admin.sock")
	markersDir := filepath.Join(dir, "markers")
	const admin = "-administrator=70000000-0000-4000-8000-000000000001=1000"
	args := []string{
		"-database=" + dbPath, "-admin-socket=" + socketPath, admin,
		"-recovery-markers-dir=" + markersDir, "-h",
	}
	var out, errOut bytes.Buffer
	if code := runServe(args, &out, &errOut); code != 0 {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
	if _, err := os.Stat(dbPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("help touched the database path")
	}
	if _, err := os.Stat(dbPath + ".lock"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("help touched the ownership lock path")
	}
	if _, err := os.Stat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("help touched the socket path")
	}
	if _, err := os.Stat(markersDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("help touched the recovery markers directory")
	}
}

// TestServeRefusesMissingDatabaseWithoutSubstitutingEmptyOne exercises
// runtime.Start's actual store.OpenExisting path (via serve, with no init
// having run) end to end: a missing deployment database must fail startup,
// never be silently created.
func TestServeRefusesMissingDatabaseWithoutSubstitutingEmptyOne(t *testing.T) {
	dbPath := filepath.Join(privateDir(t, "parleyd-serve-missing-db-"), "parley.db")
	socketPath := filepath.Join(privateDir(t, "parleyd-serve-missing-sock-"), "admin.sock")
	markersDir := filepath.Join(privateDir(t, "parleyd-serve-missing-mk-"), "markers")
	if err := os.Mkdir(markersDir, 0700); err != nil {
		t.Fatal(err)
	}
	uid := uint32(os.Getuid())
	adminID := "70000000-0000-4000-8000-000000000002"
	controlCfg, err := control.NewConfig(socketPath, uid, map[string]uint32{adminID: uid})
	if err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := serve(context.Background(), serveConfig{
		databasePath: dbPath, control: controlCfg, socketMode: 0600,
		markersDir: markersDir, markersUID: uid, markersCapacity: 8,
	}, &out, &errOut)
	if code != 1 {
		t.Fatalf("code=%d out=%q err=%q", code, out.String(), errOut.String())
	}
	if _, err := os.Stat(dbPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("serve created a database file for a missing deployment path")
	}
}

// shutdownWait bounds both the normal-path and the cleanup-path wait for
// serve to actually return after cancellation, and the raw handshake I/O in
// TestServeStartsServesHelloAndShutsDownOnCancellation -- so a stuck server
// cannot hang either the test or its own cleanup indefinitely (mandate T1).
const shutdownWait = 5 * time.Second

// TestServeStartsServesHelloAndShutsDownOnCancellation is the end-to-end
// acceptance path: init a real database, bind a real socket and marker
// directory, start serve in the background, prove a real authenticated
// server.hello round trip through the actual listener/session/store stack,
// then cancel the context and confirm ordered shutdown returns success.
func TestServeStartsServesHelloAndShutsDownOnCancellation(t *testing.T) {
	dbPath := filepath.Join(privateDir(t, "parleyd-serve-db-"), "parley.db")
	var initOut, initErr bytes.Buffer
	if code := runInit([]string{"-database", dbPath}, &initOut, &initErr); code != 0 {
		t.Fatalf("init failed: code=%d err=%q", code, initErr.String())
	}

	socketPath := filepath.Join(privateDir(t, "parleyd-serve-sock-"), "admin.sock")
	markersDir := filepath.Join(privateDir(t, "parleyd-serve-mk-"), "markers")
	if err := os.Mkdir(markersDir, 0700); err != nil {
		t.Fatal(err)
	}
	uid := uint32(os.Getuid())
	adminID := "70000000-0000-4000-8000-000000000001"
	controlCfg, err := control.NewConfig(socketPath, uid, map[string]uint32{adminID: uid})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// finished is closed exactly once by the goroutine below, after storing
	// its result -- unlike a single-value buffered channel, a closed
	// channel can be observed repeatedly, so both the normal-path
	// assertion below and the cleanup guard can each read it without
	// blocking on a value the other side already drained (mandate T1: a
	// cleanup that must not "consume an already-consumed single result and
	// then hang").
	finished := make(chan struct{})
	var resultMu sync.Mutex
	var resultCode int
	var out, errOut bytes.Buffer
	go func() {
		code := serve(ctx, serveConfig{
			databasePath: dbPath, control: controlCfg, socketMode: 0600,
			markersDir: markersDir, markersUID: uid, markersCapacity: 8,
		}, &out, &errOut)
		resultMu.Lock()
		resultCode = code
		resultMu.Unlock()
		close(finished)
	}()

	// Registered right after launch, before any assertion below can fail:
	// t.Cleanup runs on every exit path, including an early t.Fatal, and
	// runs LIFO, so this fires before privateDir's own directory-removal
	// cleanups (registered earlier, above) even though this line runs
	// later in the function body. A bare cancel() is not evidence that
	// serve actually finished (mandate T1) -- this waits for it, bounded,
	// before conceding those directories to removal.
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(shutdownWait):
			t.Errorf("cleanup: serve did not shut down within %s of cancellation", shutdownWait)
		}
	})

	var conn *net.UnixConn
	deadline := time.Now().Add(3 * time.Second)
	for conn == nil && time.Now().Before(deadline) {
		c, dialErr := connection.DialTrustedServer(context.Background(), socketPath, uid)
		if dialErr == nil {
			conn = c
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if conn == nil {
		t.Fatal("server never accepted an authenticated connection")
	}
	defer conn.Close()
	// Bounds the raw handshake I/O below so a server that never responds
	// cannot hang this test (and so delay reaching the cleanup above)
	// indefinitely (mandate T1).
	if err := conn.SetDeadline(time.Now().Add(shutdownWait)); err != nil {
		t.Fatal(err)
	}

	if _, err := conn.Write([]byte(`{"jsonrpc":"2.0","id":"1","method":"server.hello","params":{"protocol":"parley-control/1"}}` + "\n")); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var resp map[string]any
	if err := json.Unmarshal(line, &resp); err != nil {
		t.Fatalf("response %q: %v", line, err)
	}
	if resp["error"] != nil {
		t.Fatalf("%#v", resp)
	}
	result, ok := resp["result"].(map[string]any)
	if !ok || result["administrator_id"] != adminID {
		t.Fatalf("%#v", resp)
	}
	serverID, _ := result["server_id"].(string)
	if serverID == "" {
		t.Fatalf("missing server_id: %#v", result)
	}
	if !strings.Contains(initOut.String(), serverID) {
		t.Fatalf("hello's server_id %q does not match init's installation record %q", serverID, initOut.String())
	}

	cancel()
	select {
	case <-finished:
		resultMu.Lock()
		code := resultCode
		resultMu.Unlock()
		if code != 0 {
			t.Fatalf("code=%d err=%q", code, errOut.String())
		}
	case <-time.After(shutdownWait):
		t.Fatal("serve did not shut down after context cancellation")
	}
}

// TestServeCleanupWaitsForCompletionEvenOnEarlyReturn is T1's direct
// regression. It deliberately does *not* do the hello round trip or call
// cancel() itself -- exactly mirroring what would remain if an assertion
// between launch and the acceptance test's own explicit cancel()/select
// had failed early -- and relies solely on t.Cleanup to tear serve down.
// testing.T.Cleanup runs on every exit path, including a plain return, not
// only after t.FailNow(), so this proves the same thing an early t.Fatal
// would without leaving an intentionally-failing test in the suite (a
// failing subtest would mark this package permanently red, defeating its
// own purpose as a regression).
//
// Two cleanups are registered, in the order that makes t.Cleanup's LIFO
// firing check the property that matters: the ownership re-acquisition
// check is registered *first* (so it fires *last*), the cancel-and-wait is
// registered *second* (so it fires *first*) -- meaning the re-acquisition
// only ever runs after the wait has already blocked until serve() genuinely
// returned. A cleanup that only called cancel() without waiting could let
// this re-acquisition race the still-exiting goroutine and intermittently
// find the lock still held.
func TestServeCleanupWaitsForCompletionEvenOnEarlyReturn(t *testing.T) {
	dbPath := filepath.Join(privateDir(t, "parleyd-serve-earlyret-db-"), "parley.db")
	var initOut, initErr bytes.Buffer
	if code := runInit([]string{"-database", dbPath}, &initOut, &initErr); code != 0 {
		t.Fatalf("init failed: code=%d err=%q", code, initErr.String())
	}
	socketPath := filepath.Join(privateDir(t, "parleyd-serve-earlyret-sock-"), "admin.sock")
	markersDir := filepath.Join(privateDir(t, "parleyd-serve-earlyret-mk-"), "markers")
	if err := os.Mkdir(markersDir, 0700); err != nil {
		t.Fatal(err)
	}
	uid := uint32(os.Getuid())
	adminID := "70000000-0000-4000-8000-000000000001"
	controlCfg, err := control.NewConfig(socketPath, uid, map[string]uint32{adminID: uid})
	if err != nil {
		t.Fatal(err)
	}

	// Registered first, so LIFO runs it *last*: by then, the cancel-and-wait
	// cleanup below has already completed, so a fresh Acquire here proves
	// ownership was actually released, not merely that cancellation was
	// requested.
	t.Cleanup(func() {
		owner, err := runtime.Acquire(dbPath)
		if err != nil {
			t.Errorf("ownership was not released by the time the wait-for-completion cleanup returned: %v", err)
			return
		}
		if err := owner.Close(); err != nil {
			t.Errorf("close after re-acquiring released ownership: %v", err)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	var out, errOut bytes.Buffer
	go func() {
		serve(ctx, serveConfig{
			databasePath: dbPath, control: controlCfg, socketMode: 0600,
			markersDir: markersDir, markersUID: uid, markersCapacity: 8,
		}, &out, &errOut)
		close(finished)
	}()
	// Registered second, so LIFO runs it *first* -- the same cancel/wait
	// shape as the acceptance test's own t.Cleanup.
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(shutdownWait):
			t.Errorf("cleanup: serve did not shut down within %s of cancellation", shutdownWait)
		}
	})

	// Deliberately nothing else here: no hello round trip, no explicit
	// cancel(). Reaching the end of the test function with only the two
	// t.Cleanup registrations above is the "early exit after launch" this
	// regression demonstrates recovers cleanly.
}
