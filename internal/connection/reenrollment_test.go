package connection

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/store"
)

func revokedProvisioner(t *testing.T) (*Provisioner, ReenrollRequest) {
	t.Helper()
	p, db := testProvisioner(t, PublisherFunc(func(context.Context, CredentialFile) error { return nil }))
	ctx := context.Background()
	actor := store.CommandPrincipal{ID: adminID}
	registered, err := p.Register(ctx, actor, testRegistration())
	if err != nil || registered.Receipt.Result.Code != "" {
		t.Fatalf("register=%+v err=%v", registered, err)
	}
	lifecycle, err := NewLifecycle(LifecycleConfig{
		Store: db, Now: p.config.Now, Authorize: p.config.Authorize, Guard: p.config.Guard,
		PendingWork:        func(context.Context, *sql.Tx, string) ([]store.WorkRef, error) { return nil, nil },
		PendingDisposition: func(context.Context, *sql.Tx, store.WorkRef, string) error { return store.Forbidden },
		LegacyEvidence:     func(context.Context, LegacyDispositionRequest) error { return nil },
		Invalidate:         p.config.Invalidate,
	})
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := lifecycle.Revoke(ctx, actor, BindingLifecycleRequest{OperationID: targetID, BindingID: registered.BindingID, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1})
	if err != nil || revoked.Result.Code != "" {
		t.Fatalf("revoke=%+v err=%v", revoked, err)
	}
	return p, ReenrollRequest{RotateRequest: RotateRequest{OperationID: "80000000-0000-4000-8000-000000000001", BindingID: registered.BindingID, ExpectedBindingVersion: 2, ExpectedCredentialVersion: 1, ExpiresAt: time.Unix(250, 0), TargetRef: targetID}, HostEvidenceRef: adminID}
}

func TestReenrollmentRetriesUnavailableHostEvidence(t *testing.T) {
	for name, failure := range map[string]error{
		"unavailable":         store.TemporarilyUnavailable,
		"wrapped unavailable": fmt.Errorf("synthetic verifier: %w", store.TemporarilyUnavailable),
		"canceled":            context.Canceled,
		"deadline":            context.DeadlineExceeded,
		"provider failure":    errors.New("synthetic private evidence source unavailable"),
	} {
		t.Run(name, func(t *testing.T) {
			p, request := revokedProvisioner(t)
			ctx := context.Background()
			actor := store.CommandPrincipal{ID: adminID}
			publications, invalidations, verifications := 0, 0, 0
			p.config.Target = func(string, uint32) (Publisher, error) {
				return PublisherFunc(func(context.Context, CredentialFile) error { publications++; return nil }), nil
			}
			p.config.Invalidate = func(string) { invalidations++ }
			p.config.ReenrollEvidence = func(context.Context, string, NativeTuple) error {
				verifications++
				if verifications == 1 {
					return failure
				}
				return nil
			}
			// Registration/publication/revocation already have durable evidence.
			// A failed reenrollment must preserve it without appending new rows.
			counts := func() [7]int {
				t.Helper()
				var counts [7]int
				if err := p.config.Store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
					for i, table := range []string{"bindings", "credentials", "credential_publications", "operation_results", "command_audit", "revocation_incidents", "ingestion_barriers"} {
						if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&counts[i]); err != nil {
							return err
						}
					}
					binding, err := store.ReadBinding(ctx, tx, request.BindingID)
					if err != nil {
						return err
					}
					credential, err := store.LatestCredential(ctx, tx, binding.ID)
					if err != nil {
						return err
					}
					var status string
					var version int64
					if err := tx.QueryRowContext(ctx, "SELECT status,barrier_version FROM ingestion_barriers WHERE binding_id=?", binding.ID).Scan(&status, &version); err != nil {
						return err
					}
					if binding.Status != "revoked" || binding.Version != 2 || credential.Status != "revoked" || credential.Version != 1 || status != "held" || version != 1 {
						t.Error("failed reenrollment changed revoked binding, credential or ingestion barrier")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				return counts
			}
			before := counts()
			result, err := p.Reenroll(ctx, actor, request)
			if err != store.TemporarilyUnavailable || result.Receipt.Result.Code != "" || publications != 0 || invalidations != 0 {
				t.Errorf("unavailable result=%+v err=%v publications=%d invalidations=%d", result, err, publications, invalidations)
			}
			if after := counts(); after != before {
				t.Errorf("failed reenrollment persisted effects: before=%v after=%v", before, after)
			}
			result, err = p.Reenroll(ctx, actor, request)
			if err != nil || result.Receipt.Result.Code != "" || result.Receipt.Replayed || result.Publication != "published" || result.BindingVersion != 3 || result.CredentialVersion != 2 || publications != 1 || invalidations != 1 || verifications != 2 {
				t.Fatalf("retry result=%+v err=%v publications=%d invalidations=%d verifications=%d", result, err, publications, invalidations, verifications)
			}
			replay, err := p.Reenroll(ctx, actor, request)
			if err != nil || !replay.Receipt.Replayed || replay.CredentialID != result.CredentialID || publications != 1 || invalidations != 1 || verifications != 2 {
				t.Fatalf("replay result=%+v err=%v publications=%d invalidations=%d verifications=%d", replay, err, publications, invalidations, verifications)
			}
		})
	}
}

func TestReenrollmentRetainsHostMismatchRejection(t *testing.T) {
	for name, failure := range map[string]error{
		"missing provider": nil,
		"mismatch":         store.HostUnverified,
		"wrapped mismatch": fmt.Errorf("synthetic mismatch: %w", store.HostUnverified),
	} {
		t.Run(name, func(t *testing.T) {
			p, request := revokedProvisioner(t)
			if failure != nil {
				p.config.ReenrollEvidence = func(context.Context, string, NativeTuple) error { return failure }
			}
			p.config.Target = func(string, uint32) (Publisher, error) {
				t.Error("unverified host consulted publisher")
				return nil, nil
			}
			p.config.Invalidate = func(string) { t.Error("unverified host invalidated binding") }
			actor := store.CommandPrincipal{ID: adminID}
			result, err := p.Reenroll(context.Background(), actor, request)
			if err != nil || result.Receipt.Result.Code != store.HostUnverified || result.Receipt.Replayed {
				t.Fatalf("mismatch result=%+v err=%v", result, err)
			}
			p.config.ReenrollEvidence = func(context.Context, string, NativeTuple) error {
				t.Error("terminal rejection replay consulted host verifier")
				return nil
			}
			replay, err := p.Reenroll(context.Background(), actor, request)
			if err != nil || replay.Receipt.Result.Code != store.HostUnverified || !replay.Receipt.Replayed {
				t.Fatalf("replay result=%+v err=%v", replay, err)
			}
		})
	}
}
