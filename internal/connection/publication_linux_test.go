//go:build linux

package connection

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/ginsys/parley/internal/store"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func credentialState(t *testing.T) string {
	t.Helper()
	// As in runtime ownership fixtures, use a root-owned sticky ancestor;
	// the host TMPDIR may intentionally be group-writable. Under 10 KiB of
	// synthetic credential data, removed by this test's cleanup.
	state, err := os.MkdirTemp("/tmp", "parley-publication-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(state) })
	if err := os.MkdirAll(filepath.Join(state, "parley", "credentials"), 0700); err != nil {
		t.Fatal(err)
	}
	return state
}
func syntheticFile() CredentialFile {
	return CredentialFile{ServerID: adminID, BindingID: targetID, CredentialID: registerID, CredentialVersion: 1, secret: [32]byte{1, 2, 3}}
}
func TestPrivatePublicationNeverOverwrites(t *testing.T) {
	state := credentialState(t)
	publisher, err := NewPrivatePublisher(state, uint32(os.Geteuid()))
	if err != nil {
		t.Fatal(err)
	}
	file := syntheticFile()
	if err := publisher.Publish(context.Background(), file); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(state, "parley", "credentials", file.CredentialID)
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("private file permissions: %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(before, &parsed); err != nil || len(parsed) != 5 {
		t.Fatal("credential file malformed")
	}
	file.secret[0] = 99
	if err := publisher.Publish(context.Background(), file); err == nil {
		t.Fatal("overwrote existing credential")
	}
	after, err := os.ReadFile(path)
	if err != nil || string(before) != string(after) {
		t.Fatal("existing credential changed")
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(entries) != 1 {
		t.Fatal("temporary publication file leaked")
	}
}
func TestPrivatePublicationRejectsUnsafeTargets(t *testing.T) {
	for _, kind := range []string{"permissions", "symlink", "wrong_uid"} {
		t.Run(kind, func(t *testing.T) {
			state := credentialState(t)
			path := filepath.Join(state, "parley", "credentials")
			uid := uint32(os.Geteuid())
			switch kind {
			case "permissions":
				if err := os.Chmod(path, 0755); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Rename(path, path+"-real"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+"-real", path); err != nil {
					t.Fatal(err)
				}
			case "wrong_uid":
				uid++
			}
			if _, err := NewPrivatePublisher(state, uid); err == nil {
				t.Fatal("accepted unsafe target")
			}
		})
	}
}
func TestPrivatePublicationFailureAfterRenameIsAmbiguous(t *testing.T) {
	state := credentialState(t)
	publisher, err := NewPrivatePublisher(state, uint32(os.Geteuid()))
	if err != nil {
		t.Fatal(err)
	}
	publisher.syncDirectory = func(int) error { return os.ErrInvalid }
	if err := publisher.Publish(context.Background(), syntheticFile()); err == nil {
		t.Fatal("claimed durable publication after failed directory sync")
	}
	if _, err := os.Stat(filepath.Join(state, "parley", "credentials", registerID)); err != nil {
		t.Fatal("fixture never reached rename")
	}
}

func TestProvisioningCrashRecovery(t *testing.T) {
	const childFlag = "PARLEY_TEST_PROVISIONING_CRASH"
	if stage := os.Getenv(childFlag); stage != "" {
		publisher, err := NewPrivatePublisher(os.Getenv("PARLEY_TEST_PROVISIONING_STATE"), uint32(os.Geteuid()))
		if err != nil {
			t.Fatal(err)
		}
		p, _ := testProvisioner(t, PublisherFunc(func(ctx context.Context, file CredentialFile) error {
			if stage == "before_file" {
				os.Exit(71)
			}
			if err := publisher.Publish(ctx, file); err != nil {
				return err
			}
			if stage == "after_file" {
				os.Exit(71)
			}
			return nil
		}))
		db, err := store.Open(context.Background(), os.Getenv("PARLEY_TEST_PROVISIONING_DB"))
		if err != nil {
			t.Fatal(err)
		}
		p.config.Store = db
		result, err := p.Register(context.Background(), store.CommandPrincipal{ID: adminID, ConnectorUID: uint32(os.Geteuid())}, testRegistration())
		if err != nil || result.Publication != "published" {
			t.Fatal("child did not commit publication evidence")
		}
		os.Exit(71)
	}
	for _, stage := range []string{"before_file", "after_file", "after_evidence"} {
		t.Run(stage, func(t *testing.T) {
			state := credentialState(t)
			path := filepath.Join(state, "database.sqlite")
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, executable, "-test.run=^TestProvisioningCrashRecovery$")
			child.Env = append(os.Environ(), childFlag+"="+stage, "PARLEY_TEST_PROVISIONING_STATE="+state, "PARLEY_TEST_PROVISIONING_DB="+path)
			output, err := child.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 71 {
				t.Fatalf("controlled child did not exit at crash point: %v (%d diagnostic bytes)", err, len(output))
			}
			db, err := store.Open(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			p, _ := testProvisioner(t, PublisherFunc(func(context.Context, CredentialFile) error { t.Error("crash replay republished secret"); return nil }))
			p.config.Store = db
			result, err := p.Register(context.Background(), store.CommandPrincipal{ID: adminID, ConnectorUID: uint32(os.Geteuid())}, testRegistration())
			if err != nil || !result.Receipt.Replayed {
				t.Fatalf("crash receipt replay: %v", err)
			}
			want := "pending"
			if stage == "after_evidence" {
				want = "published"
			}
			if result.Publication != want {
				t.Fatalf("publication=%s want=%s", result.Publication, want)
			}
			entries, err := os.ReadDir(filepath.Join(state, "parley", "credentials"))
			if err != nil {
				t.Fatal(err)
			}
			expected := 1
			if stage == "before_file" {
				expected = 0
			}
			if len(entries) != expected {
				t.Fatalf("published files=%d want=%d", len(entries), expected)
			}
		})
	}
}
