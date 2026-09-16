package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/control"
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

func TestInitHelpTouchesNoFile(t *testing.T) {
	dir := privateDir(t, "parleyd-init-help-")
	path := filepath.Join(dir, "parley.db")
	var out, errOut bytes.Buffer
	if code := runInit([]string{"-h"}, &out, &errOut); code != 0 {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("help touched the database path")
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

func TestServeHelpTouchesNoFileOrSocket(t *testing.T) {
	dir := privateDir(t, "parleyd-serve-help-")
	dbPath := filepath.Join(dir, "parley.db")
	socketPath := filepath.Join(dir, "admin.sock")
	var out, errOut bytes.Buffer
	if code := runServe([]string{"-h"}, &out, &errOut); code != 0 {
		t.Fatalf("code=%d err=%q", code, errOut.String())
	}
	if _, err := os.Stat(dbPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("help touched the database path")
	}
	if _, err := os.Stat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("help touched the socket path")
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
	done := make(chan int, 1)
	var out, errOut bytes.Buffer
	go func() {
		done <- serve(ctx, serveConfig{
			databasePath: dbPath, control: controlCfg, socketMode: 0600,
			markersDir: markersDir, markersUID: uid, markersCapacity: 8,
		}, &out, &errOut)
	}()

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
	case code := <-done:
		if code != 0 {
			t.Fatalf("code=%d err=%q", code, errOut.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not shut down after context cancellation")
	}
}
