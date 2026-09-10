package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/controller"
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
	for _, id := range []string{"peer\n", "peer\r", "peer\t", "peer\x00", "peer\u0085", "peer\u2028", "peer\u2029", "peer\u200b", "peer\u202e"} {
		for _, flag := range []string{"-peer-a", "-peer-b"} {
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
