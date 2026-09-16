package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/runtime"
	"github.com/ginsys/parley/internal/store"
)

type fakeController struct {
	grant         controller.GrantParams
	renew         controller.RenewParams
	revoke        string
	err           error
	calls, closed int
}

func (f *fakeController) Grant(_ context.Context, p controller.GrantParams) (*store.Grant, error) {
	f.calls++
	f.grant = p
	return &store.Grant{Conversation: p.Conversation, GrantVersion: 1, PeerAID: p.PeerAID, PeerBID: p.PeerBID, Direction: p.Direction, MaxExchanges: p.MaxExchanges}, f.err
}
func (f *fakeController) Renew(_ context.Context, p controller.RenewParams) (*store.Grant, error) {
	f.calls++
	f.renew = p
	return &store.Grant{Conversation: p.Conversation, GrantVersion: 2, MaxExchanges: 3}, f.err
}
func (f *fakeController) Revoke(_ context.Context, c string) (*controller.RevokeResult, error) {
	f.calls++
	f.revoke = c
	return &controller.RevokeResult{Cancelled: 1}, f.err
}
func (f *fakeController) Close() error { f.closed++; return nil }

func TestHelpAndInvalidArgumentsNeverOpenDatabase(t *testing.T) {
	tests := []struct {
		args []string
		code int
	}{
		{nil, 0}, {[]string{"help"}, 0}, {[]string{"-h"}, 0}, {[]string{"--help"}, 0},
		{[]string{"grant", "--help"}, 0}, {[]string{"renew", "-h"}, 0}, {[]string{"revoke", "--help"}, 0},
		{[]string{"serve"}, 2}, {[]string{"help", "extra"}, 2},
		{[]string{"grant"}, 2}, {[]string{"renew"}, 2}, {[]string{"revoke"}, 2},
		{[]string{"revoke", "-conversation", " "}, 2}, {[]string{"revoke", "-conversation", "c", "extra"}, 2},
		{[]string{"renew", "-conversation", "c", "-max-exchanges", "-1"}, 2},
		{[]string{"renew", "-conversation", "c", "-expires-in", "-1s"}, 2},
		{[]string{"renew", "-conversation", "c", "-expires-in", "oops"}, 2},
		{[]string{"renew", "-conversation", "c", "--unknown"}, 2},
		{[]string{"grant", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "0"}, 2},
		{[]string{"grant", "-conversation", "c", "-peer-a", "a", "-peer-b", "a", "-max-exchanges", "1"}, 2},
		{[]string{"grant", "-conversation", "c", "-peer-a", " ", "-peer-b", "b", "-max-exchanges", "1"}, 2},
		{[]string{"grant", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-direction", "wrong"}, 2},
		{[]string{"grant", "-conversation", "c", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "1", "-expires-in", "-1s"}, 2},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			factory := func(context.Context, string) (controllerAPI, io.Closer, error) {
				t.Fatal("opened database before argument validation")
				return nil, nil, nil
			}
			if code := run(tt.args, "unused.db", &stdout, &stderr, factory); code != tt.code {
				t.Fatalf("exit=%d: %s %s", code, &stdout, &stderr)
			}
			if tt.code == 0 && (stdout.Len() == 0 || stderr.Len() != 0) {
				t.Fatalf("help streams: %q %q", &stdout, &stderr)
			}
			if tt.code == 2 && (stderr.Len() == 0 || stdout.Len() != 0) {
				t.Fatalf("error streams: %q %q", &stdout, &stderr)
			}
		})
	}
}

func TestUnsafePeerIdentifiersRejectedBeforeStorage(t *testing.T) {
	for _, id := range []string{"a\xff", "a\xfe", "café", "a\ufffd", "peer\x7f", "peer\n", "peer\r", "peer\t", "peer\x00", "peer\u0085", "peer\u2028", "peer\u2029", "peer\u200b", "peer\u202e"} {
		for _, flag := range []string{"-conversation", "-peer-a", "-peer-b"} {
			t.Run(flag+id, func(t *testing.T) {
				args := []string{"grant", "-conversation", "fixture", "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "2", flag, id}
				var out, errOut bytes.Buffer
				factory := func(context.Context, string) (controllerAPI, io.Closer, error) {
					t.Fatal("opened storage for unsafe peer identifier")
					return nil, nil, nil
				}
				if code := run(args, "unused.db", &out, &errOut, factory); code != 2 {
					t.Fatalf("exit=%d: %s", code, &errOut)
				}
			})
		}
	}
}

func TestRoutingUsesValidatedParametersAndClosesStorage(t *testing.T) {
	for _, operation := range []string{"grant", "renew", "revoke"} {
		for _, fails := range []bool{false, true} {
			t.Run(operation+map[bool]string{false: "", true: "_failure"}[fails], func(t *testing.T) {
				args := []string{operation, "-conversation", "fixture"}
				switch operation {
				case "grant":
					args = append(args, "-peer-a", "a", "-peer-b", "b", "-max-exchanges", "3", "-direction", "b_to_a", "-expires-in", "1h")
				case "renew":
					args = append(args, "-cancel-pending-replies")
				}
				fake := &fakeController{}
				if fails {
					fake.err = errors.New("synthetic operation failure")
				}
				opened := 0
				factory := func(_ context.Context, path string) (controllerAPI, io.Closer, error) {
					opened++
					if path != "selected.db" {
						t.Fatalf("path=%q", path)
					}
					return fake, fake, nil
				}
				var out, errOut bytes.Buffer
				before := time.Now()
				code := run(args, "selected.db", &out, &errOut, factory)
				expected := 0
				if fails {
					expected = 1
				}
				if code != expected || opened != 1 || fake.calls != 1 || fake.closed != 1 {
					t.Fatalf("exit/open/call/close=%d/%d/%d/%d", code, opened, fake.calls, fake.closed)
				}
				if fails {
					if out.Len() != 0 || !strings.Contains(errOut.String(), "synthetic operation failure") {
						t.Fatalf("failure output: %q %q", &out, &errOut)
					}
				} else if out.Len() == 0 || errOut.Len() != 0 {
					t.Fatalf("success output: %q %q", &out, &errOut)
				}
				switch operation {
				case "grant":
					p := fake.grant
					if p.Conversation != "fixture" || p.PeerAID != "a" || p.PeerBID != "b" || p.Direction != store.BToA || p.MaxExchanges != 3 || p.ExpiresAt == nil || p.ExpiresAt.Before(before.Add(time.Hour)) || p.ExpiresAt.After(time.Now().Add(time.Hour)) {
						t.Fatalf("grant=%+v", p)
					}
				case "renew":
					p := fake.renew
					if p.Conversation != "fixture" || !p.CancelPendingReplies || p.MaxExchanges != 0 || p.ExpiresAt != nil {
						t.Fatalf("renew=%+v", p)
					}
				case "revoke":
					if fake.revoke != "fixture" {
						t.Fatal(fake.revoke)
					}
				}
			})
		}
	}
}

func TestDatabaseOpenFailureAndDefaultPath(t *testing.T) {
	var out, errOut bytes.Buffer
	factory := func(_ context.Context, path string) (controllerAPI, io.Closer, error) {
		if path != "parley.db" {
			t.Fatal(path)
		}
		return nil, nil, errors.New("synthetic open failure")
	}
	if code := run([]string{"revoke", "-conversation", "fixture"}, "", &out, &errOut, factory); code != 1 || out.Len() != 0 || !strings.Contains(errOut.String(), "synthetic open failure") {
		t.Fatalf("exit=%d: %s %s", code, &out, &errOut)
	}
}

func TestCLIIdentifiersRemainExactAndVisible(t *testing.T) {
	for _, operation := range []string{"grant", "renew", "revoke"} {
		t.Run(operation, func(t *testing.T) {
			fake := &fakeController{}
			factory := func(context.Context, string) (controllerAPI, io.Closer, error) { return fake, fake, nil }
			args := []string{operation, "-conversation", " x"}
			if operation == "grant" {
				args = append(args, "-peer-a", "a", "-peer-b", "a ", "-max-exchanges", "1")
			}
			var out, errOut bytes.Buffer
			if code := run(args, "unused", &out, &errOut, factory); code != 0 {
				t.Fatalf("exit %d: %s", code, &errOut)
			}
			if !strings.Contains(out.String(), `" x"`) {
				t.Fatalf("identifier whitespace hidden: %s", &out)
			}
			switch operation {
			case "grant":
				if fake.grant.Conversation != " x" || fake.grant.PeerAID != "a" || fake.grant.PeerBID != "a " || !strings.Contains(out.String(), `"a" <-> "a "`) {
					t.Fatalf("grant identity changed: %+v %s", fake.grant, &out)
				}
			case "renew":
				if fake.renew.Conversation != " x" {
					t.Fatalf("renew identity changed: %+v", fake.renew)
				}
			case "revoke":
				if fake.revoke != " x" {
					t.Fatalf("revoke identity changed: %q", fake.revoke)
				}
			}
		})
	}
}

// TestOpenControllerAcquiresCanonicalLockForItsFullLifetime is the EP-02
// acceptance test: it exercises openController's actual production body
// (openControllerWith) with the real runtime.Acquire and a real on-disk
// database, injecting only the underlying store-open call -- never the whole
// lock-bearing function, and never a reimplementation of the lock itself.
func TestOpenControllerAcquiresCanonicalLockForItsFullLifetime(t *testing.T) {
	// runtime.Acquire requires a private, non-group/other-writable parent
	// directory (internal/runtime/ownership_linux.go trustedParents).
	// t.TempDir()'s shared per-package work directory can be group-writable
	// under this host's umask; use the same disposable-private-directory
	// pattern as internal/runtime/ownership_linux_test.go's privateDB instead.
	dir, err := os.MkdirTemp("/tmp", "parleyctl-ownership-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "parley.db")
	// Seed a real, already-initialized database (mirrors `parleyd init`).
	// runtime.Acquire requires the private (owner-only, 0600) mode
	// internal/runtime/ownership_linux.go's openPrivate enforces; pre-create
	// the file with that mode so store.Open's migration writes into it
	// without SQLite's own (looser) create-mode ever taking effect.
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	seed, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("refuses_before_store_open_when_already_owned", func(t *testing.T) {
		owner, err := runtime.Acquire(path)
		if err != nil {
			t.Fatal(err)
		}
		defer owner.Close()
		opened := false
		_, _, err = openControllerWith(context.Background(), path, runtime.Acquire, func(ctx context.Context, p string) (*store.DB, error) {
			opened = true
			return store.Open(ctx, p)
		})
		if !errors.Is(err, runtime.ErrAlreadyRunning) {
			t.Fatalf("err=%v, want ErrAlreadyRunning", err)
		}
		if opened {
			t.Fatal("store opener called despite a contended lock")
		}
	})

	t.Run("lock_and_opener_target_the_same_canonical_path", func(t *testing.T) {
		var openedWith string
		_, closer, err := openControllerWith(context.Background(), path, func(p string) (*runtime.Ownership, error) {
			owner, err := runtime.Acquire(p)
			if err == nil && owner.Path() != canonical {
				t.Fatalf("lock path=%q, want %q", owner.Path(), canonical)
			}
			return owner, err
		}, func(ctx context.Context, p string) (*store.DB, error) {
			openedWith = p
			return store.Open(ctx, p)
		})
		if err != nil {
			t.Fatal(err)
		}
		defer closer.Close()
		if openedWith != path {
			t.Fatalf("store opener path=%q, want %q", openedWith, path)
		}
	})

	t.Run("ownership_retained_until_close_not_just_acquisition", func(t *testing.T) {
		_, closer, err := openControllerWith(context.Background(), path, runtime.Acquire, store.Open)
		if err != nil {
			t.Fatal(err)
		}
		// The resource lifetime, not just acquisition order, is the acceptance
		// condition: a contender must still be refused after the factory has
		// already returned a live controller.
		if _, err := runtime.Acquire(path); !errors.Is(err, runtime.ErrAlreadyRunning) {
			t.Fatalf("contender acquired the lock before close: %v", err)
		}
		if err := closer.Close(); err != nil {
			t.Fatal(err)
		}
		released, err := runtime.Acquire(path)
		if err != nil {
			t.Fatalf("lock not released after close: %v", err)
		}
		released.Close()
	})

	t.Run("store_open_failure_unwinds_the_lock_without_deleting_it", func(t *testing.T) {
		synthetic := errors.New("synthetic store open failure")
		_, _, err := openControllerWith(context.Background(), path, runtime.Acquire, func(context.Context, string) (*store.DB, error) {
			return nil, synthetic
		})
		if !errors.Is(err, synthetic) {
			t.Fatalf("err=%v, want synthetic store open failure", err)
		}
		if _, statErr := os.Stat(canonical + ".lock"); statErr != nil {
			t.Fatalf("lock file removed on unwind: %v", statErr)
		}
		released, err := runtime.Acquire(path)
		if err != nil {
			t.Fatalf("lock not released after store-open failure: %v", err)
		}
		released.Close()
	})
}

func TestCLIRenewRejectsIncompatibleNamesButRevokeKeepsExactKey(t *testing.T) {
	for _, name := range []string{"café", "a\xff", "a\xfe", "a\ufffd"} {
		for _, operation := range []string{"renew", "revoke"} {
			fake := &fakeController{}
			opened := false
			factory := func(context.Context, string) (controllerAPI, io.Closer, error) {
				opened = true
				return fake, fake, nil
			}
			var out, errOut bytes.Buffer
			code := run([]string{operation, "-conversation", name}, "unused", &out, &errOut, factory)
			if operation == "renew" {
				if code != 2 || opened {
					t.Fatalf("renew opened storage for %x: exit=%d", name, code)
				}
			} else if code != 0 || !opened || fake.revoke != name {
				t.Fatalf("revoke changed key %x: %+v, exit=%d", name, fake, code)
			}
		}
	}
}
