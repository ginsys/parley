package connection

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/store"
)

const adminID = "10000000-0000-4000-8000-000000000001"
const registerID = "20000000-0000-4000-8000-000000000001"
const targetID = "30000000-0000-4000-8000-000000000001"

func testProvisioner(t *testing.T, publisher Publisher) (*Provisioner, *store.DB) {
	t.Helper()
	db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "provision.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	p, err := NewProvisioner(ProvisioningConfig{
		Store: db, Now: func() time.Time { return time.Unix(100, 0) },
		Authorize:         func(context.Context, *sql.Tx, store.CommandPrincipal) error { return nil },
		Guard:             func(context.Context, *sql.Tx, string) error { return nil },
		Verify:            func(context.Context, NativeTuple) error { return nil },
		LegacyEligibility: func(context.Context, *sql.Tx, string) error { return nil },
		Target: func(ref string, uid uint32) (Publisher, error) {
			if ref != targetID {
				return nil, store.Forbidden
			}
			return publisher, nil
		},
		Invalidate: func(string) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	return p, db
}
func testRegistration() RegisterRequest {
	return RegisterRequest{OperationID: registerID, PeerID: "peer", Native: NativeTuple{Kind: "codex_cli", Namespace: "synthetic", Session: "test"}, ConnectorUID: uint32(os.Geteuid()), ExpiresAt: time.Unix(200, 0), TargetRef: targetID}
}
func TestRegistrationPublicationAndReplay(t *testing.T) {
	calls := 0
	var credential CredentialFile
	var db *store.DB
	var p *Provisioner
	p, db = testProvisioner(t, PublisherFunc(func(ctx context.Context, file CredentialFile) error {
		calls++
		credential = file
		// The verifier must already be committed, and publication must not hold the coordinator gate.
		return db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
			_, err := store.ReadCredential(ctx, tx, file.CredentialID)
			return err
		})
	}))
	actor := store.CommandPrincipal{ID: adminID, ConnectorUID: uint32(os.Geteuid())}
	result, err := p.Register(context.Background(), actor, testRegistration())
	if err != nil || result.Publication != "published" || calls != 1 {
		t.Fatalf("publication=%s calls=%d err=%v", result.Publication, calls, err)
	}
	replay, err := p.Register(context.Background(), actor, testRegistration())
	if err != nil || !replay.Receipt.Replayed || calls != 1 || replay.CredentialID != result.CredentialID {
		t.Fatalf("replay failed: calls=%d err=%v", calls, err)
	}
	var stored store.CredentialRecord
	if err := db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		var err error
		stored, err = store.ReadCredential(ctx, tx, result.CredentialID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !stored.Matches(credential.secret) {
		t.Fatal("published secret does not match committed verifier")
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if bytesContainSecret(data, credential.secret) {
		t.Fatal("ordinary result disclosed secret")
	}
}
func TestPublicationAmbiguityAndRotationNeverRepublishes(t *testing.T) {
	calls := 0
	p, db := testProvisioner(t, PublisherFunc(func(context.Context, CredentialFile) error {
		calls++
		return errors.New("synthetic private path must not escape")
	}))
	actor := store.CommandPrincipal{ID: adminID, ConnectorUID: uint32(os.Geteuid())}
	first, err := p.Register(context.Background(), actor, testRegistration())
	if err != nil || first.Publication != "unknown" {
		t.Fatalf("publication=%s err=%v", first.Publication, err)
	}
	if _, err := p.Register(context.Background(), actor, testRegistration()); err != nil || calls != 1 {
		t.Fatalf("replayed publication calls=%d err=%v", calls, err)
	}
	rotated, err := p.Rotate(context.Background(), actor, RotateRequest{OperationID: "20000000-0000-4000-8000-000000000002", BindingID: first.BindingID, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1, ExpiresAt: time.Unix(300, 0), TargetRef: targetID})
	if err != nil || rotated.Receipt.Result.Code != "" || rotated.CredentialID == first.CredentialID || calls != 2 {
		t.Fatalf("rotation calls=%d err=%v code=%s", calls, err, rotated.Receipt.Result.Code)
	}
	if err := db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		old, err := store.ReadCredential(ctx, tx, first.CredentialID)
		if err == nil && old.Status != "superseded" {
			t.Error("old credential revived")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func bytesContainSecret(data []byte, secret [32]byte) bool {
	return bytes.Contains(data, []byte(base64.StdEncoding.EncodeToString(secret[:])))
}

func TestReplaySkipsEvidenceAndRejectionDoesNotInvalidate(t *testing.T) {
	p, _ := testProvisioner(t, PublisherFunc(func(context.Context, CredentialFile) error { return nil }))
	actor := store.CommandPrincipal{ID: adminID, ConnectorUID: uint32(os.Geteuid())}
	first, err := p.Register(context.Background(), actor, testRegistration())
	if err != nil {
		t.Fatal(err)
	}
	p.config.Verify = func(context.Context, NativeTuple) error {
		t.Error("replay requested host evidence")
		return store.HostUnverified
	}
	p.config.Target = func(string, uint32) (Publisher, error) {
		t.Error("replay resolved provisioning target")
		return nil, store.Forbidden
	}
	p.config.Guard = func(context.Context, *sql.Tx, string) error {
		t.Error("replay reevaluated mutation preconditions")
		return store.RecoveryRequired
	}
	if _, err := p.Register(context.Background(), actor, testRegistration()); err != nil {
		t.Fatal(err)
	}
	p.config.Target = func(string, uint32) (Publisher, error) {
		return PublisherFunc(func(context.Context, CredentialFile) error { t.Error("rejected rotation published"); return nil }), nil
	}
	p.config.Guard = func(context.Context, *sql.Tx, string) error { return nil }
	p.config.Invalidate = func(string) { t.Error("rejected rotation invalidated connection") }
	denied, err := p.Rotate(context.Background(), actor, RotateRequest{OperationID: "20000000-0000-4000-8000-000000000002", BindingID: first.BindingID, ExpectedBindingVersion: 2, ExpectedCredentialVersion: 1, ExpiresAt: time.Unix(300, 0), TargetRef: targetID})
	if err != nil || denied.Receipt.Result.Code != store.VersionConflict {
		t.Fatalf("code=%s err=%v", denied.Receipt.Result.Code, err)
	}
}
func TestUnauthorizedRegistrationDoesNotConsultHostOrPublisher(t *testing.T) {
	p, _ := testProvisioner(t, PublisherFunc(func(context.Context, CredentialFile) error { t.Error("unauthorized publication"); return nil }))
	p.config.Authorize = func(context.Context, *sql.Tx, store.CommandPrincipal) error { return store.Forbidden }
	p.config.Verify = func(context.Context, NativeTuple) error { t.Error("unauthorized host evidence"); return nil }
	p.config.Target = func(string, uint32) (Publisher, error) { t.Error("unauthorized target lookup"); return nil, nil }
	_, err := p.Register(context.Background(), store.CommandPrincipal{ID: adminID}, testRegistration())
	if err != store.Forbidden {
		t.Fatalf("error=%v", err)
	}
}

func TestProvisionerRequiresEveryTrustedCapability(t *testing.T) {
	p, _ := testProvisioner(t, PublisherFunc(func(context.Context, CredentialFile) error { return nil }))
	for name, remove := range map[string]func(*ProvisioningConfig){
		"authority": func(c *ProvisioningConfig) { c.Authorize = nil }, "recovery": func(c *ProvisioningConfig) { c.Guard = nil },
		"host": func(c *ProvisioningConfig) { c.Verify = nil }, "legacy": func(c *ProvisioningConfig) { c.LegacyEligibility = nil },
		"target": func(c *ProvisioningConfig) { c.Target = nil }, "invalidation": func(c *ProvisioningConfig) { c.Invalidate = nil },
	} {
		config := p.config
		remove(&config)
		if _, err := NewProvisioner(config); err != store.InvalidRequest {
			t.Fatalf("missing %s accepted: %v", name, err)
		}
	}
}

func TestRegistrationExpiryAndLegacyDenialHaveNoEnrollment(t *testing.T) {
	for _, reason := range []string{"expiry", "legacy"} {
		t.Run(reason, func(t *testing.T) {
			p, db := testProvisioner(t, PublisherFunc(func(context.Context, CredentialFile) error { t.Error("rejected enrollment published"); return nil }))
			request := testRegistration()
			want := store.InvalidRequest
			if reason == "expiry" {
				request.ExpiresAt = p.config.Now()
			} else {
				p.config.LegacyEligibility = func(context.Context, *sql.Tx, string) error { return store.SecurityHold }
				want = store.SecurityHold
			}
			result, err := p.Register(context.Background(), store.CommandPrincipal{ID: adminID}, request)
			if err != nil || result.Receipt.Result.Code != want {
				t.Fatalf("result=%s err=%v", result.Receipt.Result.Code, err)
			}
			if err := db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
				for _, table := range []string{"bindings", "credentials", "credential_publications", "grants"} {
					var n int
					if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
						return err
					}
					if n != 0 {
						t.Errorf("%s mutated", table)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
