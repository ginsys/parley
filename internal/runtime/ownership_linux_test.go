//go:build linux

package runtime

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func privateDB(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "parley-ownership-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "parley.db")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestOwnershipPaths(t *testing.T) {
	path := privateDB(t)
	first, err := Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := Acquire(path); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("contender: %v", err)
	}
	alias := filepath.Join(filepath.Dir(path), "alias")
	if err := os.Symlink(path, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(alias); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("alias: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".lock"); err != nil {
		t.Fatal("lock removed:", err)
	}
	for _, name := range []string{"", ":memory:", "file:" + path, path + "missing"} {
		if o, err := Acquire(name); err == nil {
			o.Close()
			t.Fatalf("accepted %q", name)
		}
	}
	if err := os.Link(path, path+"hard"); err != nil {
		t.Fatal(err)
	}
	if o, err := Acquire(path); err == nil {
		o.Close()
		t.Fatal("accepted hard link")
	}
}

func TestOwnershipRejectsUnsafeFiles(t *testing.T) {
	for _, kind := range []string{"db-mode", "parent-mode", "lock-symlink", "lock-mode", "wal-symlink", "fifo"} {
		t.Run(kind, func(t *testing.T) {
			path := privateDB(t)
			var err error
			switch kind {
			case "db-mode":
				err = os.Chmod(path, 0644)
			case "parent-mode":
				err = os.Chmod(filepath.Dir(path), 0777)
			case "lock-symlink":
				err = os.Symlink(path, path+".lock")
			case "lock-mode":
				err = os.WriteFile(path+".lock", nil, 0666)
			case "wal-symlink":
				err = os.Symlink(path, path+"-wal")
			case "fifo":
				err = unix.Mkfifo(path+".lock", 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if o, err := Acquire(path); err == nil {
				o.Close()
				t.Fatal("unsafe ownership accepted")
			}
		})
	}
}

// The test binary is the only child executable; no installed host CLI is used.
func TestOwnershipChild(t *testing.T) {
	path := os.Getenv("PARLEY_TEST_OWNERSHIP")
	if path == "" {
		return
	}
	if os.Getenv("PARLEY_TEST_EXEC") == "1" {
		if _, err := Acquire(path); !errors.Is(err, ErrAlreadyRunning) {
			t.Fatal(err)
		}
		os.Stdout.WriteString("exec-ready\n")
		bufio.NewReader(os.Stdin).ReadString('\n')
		o, err := Acquire(path)
		if err != nil {
			t.Fatal("inherited ownership:", err)
		}
		o.Close()
		return
	}
	o, err := Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	os.Stdout.WriteString("locked\n")
	bufio.NewReader(os.Stdin).ReadString('\n')
}

func TestOwnershipCrashAndExec(t *testing.T) {
	for _, scenario := range []string{"crash", "exec"} {
		t.Run(scenario, func(t *testing.T) {
			path := privateDB(t)
			var owner *Ownership
			var err error
			if scenario == "exec" {
				owner, err = Acquire(path)
				if err != nil {
					t.Fatal(err)
				}
				defer owner.Close()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOwnershipChild$")
			cmd.Env = append(os.Environ(), "PARLEY_TEST_OWNERSHIP="+path)
			if scenario == "exec" {
				cmd.Env = append(cmd.Env, "PARLEY_TEST_EXEC=1")
			}
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			in, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			cmd.Stderr = os.Stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { cmd.Process.Kill(); in.Close() })
			line, err := bufio.NewReader(out).ReadString('\n')
			if err != nil {
				t.Fatalf("child readiness %q: %v", line, err)
			}
			if scenario == "crash" {
				if _, err := Acquire(path); !errors.Is(err, ErrAlreadyRunning) {
					t.Fatal(err)
				}
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				if err := cmd.Wait(); err == nil {
					t.Fatal("killed child succeeded")
				}
				next, err := Acquire(path)
				if err != nil {
					t.Fatal("crash retained lock:", err)
				}
				next.Close()
			} else {
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := in.Write([]byte("continue\n")); err != nil {
					t.Fatal(err)
				}
				if err := cmd.Wait(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestOwnershipRejectsUntrustedAliasChain(t *testing.T) {
	path := privateDB(t)
	other := privateDB(t)
	unsafe := filepath.Dir(other)
	if err := os.Chmod(unsafe, 0777); err != nil {
		t.Fatal(err)
	}
	inner := filepath.Join(unsafe, "inner")
	if err := os.Symlink(path, inner); err != nil {
		t.Fatal(err)
	}
	outer := filepath.Join(filepath.Dir(path), "outer")
	if err := os.Symlink(inner, outer); err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{inner, outer} {
		if o, err := Acquire(alias); err == nil {
			o.Close()
			t.Fatalf("accepted mutable alias %s", alias)
		}
	}
}

func TestOwnershipResolvesParentAfterSymlink(t *testing.T) {
	first, second := privateDB(t), privateDB(t)
	sub := filepath.Join(filepath.Dir(second), "sub")
	if err := os.Mkdir(sub, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(filepath.Dir(first), "alias")
	if err := os.Symlink(sub, alias); err != nil {
		t.Fatal(err)
	}
	o, err := Acquire(alias + "/../parley.db")
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if o.Path() != second {
		t.Fatalf("canonical=%s, want %s", o.Path(), second)
	}
	if _, err := Acquire(second); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatal("alias did not share lock:", err)
	}
}
