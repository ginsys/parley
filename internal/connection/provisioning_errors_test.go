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

func provisioningCounts(t *testing.T, db *store.DB) [5]int {
	t.Helper()
	var counts [5]int
	if err := db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		for i, table := range []string{"bindings", "credentials", "credential_publications", "operation_results", "command_audit"} {
			if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&counts[i]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return counts
}

func TestTargetResolutionFailureSemantics(t *testing.T) {
	for _, kind := range []string{"register", "rotate", "reenroll"} {
		for name, tc := range map[string]struct {
			failure  error
			code     store.Code
			terminal bool
		}{
			"unavailable":         {store.TemporarilyUnavailable, store.TemporarilyUnavailable, false},
			"wrapped unavailable": {fmt.Errorf("synthetic target: %w", store.TemporarilyUnavailable), store.TemporarilyUnavailable, false},
			"canceled":            {context.Canceled, store.TemporarilyUnavailable, false},
			"provider error":      {errors.New("synthetic private target unavailable"), store.TemporarilyUnavailable, false},
			"forbidden":           {store.Forbidden, store.Forbidden, true},
			"wrapped forbidden":   {fmt.Errorf("synthetic target: %w", store.Forbidden), store.Forbidden, true},
			"missing publisher":   {nil, store.Forbidden, true},
		} {
			t.Run(kind+"/"+name, func(t *testing.T) {
				publications, invalidations, lookups := 0, 0, 0
				publisher := PublisherFunc(func(context.Context, CredentialFile) error { publications++; return nil })
				p, db := testProvisioner(t, publisher)
				ctx := context.Background()
				actor := store.CommandPrincipal{ID: adminID}
				invoke := func() (ProvisioningResult, error) { return p.Register(ctx, actor, testRegistration()) }
				if kind == "rotate" {
					registered, err := invoke()
					if err != nil || registered.Receipt.Result.Code != "" {
						t.Fatalf("register=%+v err=%v", registered, err)
					}
					request := RotateRequest{OperationID: targetID, BindingID: registered.BindingID, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1, ExpiresAt: time.Unix(250, 0), TargetRef: targetID}
					invoke = func() (ProvisioningResult, error) { return p.Rotate(ctx, actor, request) }
					publications = 0
				}
				if kind == "reenroll" {
					var request ReenrollRequest
					p, request = revokedProvisioner(t)
					db = p.config.Store
					p.config.ReenrollEvidence = func(context.Context, string, NativeTuple) error { return nil }
					invoke = func() (ProvisioningResult, error) { return p.Reenroll(ctx, actor, request) }
				}
				p.config.Invalidate = func(string) { invalidations++ }
				p.config.Target = func(string, uint32) (Publisher, error) { lookups++; return nil, tc.failure }
				before := provisioningCounts(t, db)
				result, err := invoke()
				if tc.terminal {
					if err != nil || result.Receipt.Result.Code != tc.code || result.Receipt.Replayed {
						t.Errorf("terminal result=%+v err=%v", result, err)
					}
					before[3]++
					before[4]++
				} else if err != tc.code || result.Receipt.AuditID != "" {
					t.Errorf("transient result=%+v err=%v", result, err)
				}
				if after := provisioningCounts(t, db); after != before || publications != 0 || invalidations != 0 {
					t.Errorf("failure effects: before=%v after=%v publications=%d invalidations=%d", before, after, publications, invalidations)
				}
				p.config.Target = func(string, uint32) (Publisher, error) { lookups++; return publisher, nil }
				retry, err := invoke()
				if tc.terminal {
					if err != nil || !retry.Receipt.Replayed || retry.Receipt.Result.Code != tc.code || lookups != 1 || publications != 0 || invalidations != 0 {
						t.Fatalf("terminal replay=%+v err=%v lookups=%d publications=%d invalidations=%d", retry, err, lookups, publications, invalidations)
					}
					return
				}
				wantInvalidations := 0
				if kind != "register" {
					wantInvalidations = 1
				}
				if err != nil || retry.Receipt.Result.Code != "" || retry.Receipt.Replayed || retry.Publication != "published" || lookups != 2 || publications != 1 || invalidations != wantInvalidations {
					t.Fatalf("retry=%+v err=%v lookups=%d publications=%d invalidations=%d", retry, err, lookups, publications, invalidations)
				}
				replay, err := invoke()
				if err != nil || !replay.Receipt.Replayed || replay.CredentialID != retry.CredentialID || lookups != 2 || publications != 1 || invalidations != wantInvalidations {
					t.Fatalf("replay=%+v err=%v lookups=%d publications=%d invalidations=%d", replay, err, lookups, publications, invalidations)
				}
			})
		}
	}
}

func TestWrappedProviderRejectionsRetainTerminalSemantics(t *testing.T) {
	for _, provider := range []string{"guard", "legacy"} {
		for _, code := range []store.Code{store.SecurityHold, store.TemporarilyUnavailable} {
			t.Run(provider+"/"+string(code), func(t *testing.T) {
				p, db := testProvisioner(t, PublisherFunc(func(context.Context, CredentialFile) error { return nil }))
				calls := 0
				failure := fmt.Errorf("synthetic provider: %w", code)
				check := func(context.Context, *sql.Tx, string) error { calls++; return failure }
				if provider == "guard" {
					p.config.Guard = check
				} else {
					p.config.LegacyEligibility = check
				}
				actor := store.CommandPrincipal{ID: adminID}
				result, err := p.Register(context.Background(), actor, testRegistration())
				terminal := code == store.SecurityHold
				want := [5]int{}
				if terminal {
					want[3], want[4] = 1, 1
					if err != nil || result.Receipt.Result.Code != code {
						t.Errorf("terminal result=%+v err=%v", result, err)
					}
				} else if err != code || result.Receipt.AuditID != "" {
					t.Errorf("transient result=%+v err=%v", result, err)
				}
				if got := provisioningCounts(t, db); got != want {
					t.Errorf("rows=%v want=%v", got, want)
				}
				failure = nil
				retry, err := p.Register(context.Background(), actor, testRegistration())
				if terminal {
					if err != nil || !retry.Receipt.Replayed || retry.Receipt.Result.Code != code || calls != 1 {
						t.Fatalf("terminal replay=%+v err=%v calls=%d", retry, err, calls)
					}
				} else if err != nil || retry.Receipt.Replayed || retry.Publication != "published" || calls != 2 {
					t.Fatalf("retry=%+v err=%v calls=%d", retry, err, calls)
				}
			})
		}
	}
}

func TestMissingBindingRotationRetainsRejection(t *testing.T) {
	p, db := testProvisioner(t, PublisherFunc(func(context.Context, CredentialFile) error { t.Error("missing binding published"); return nil }))
	p.config.Target = func(string, uint32) (Publisher, error) { t.Error("missing binding resolved target"); return nil, nil }
	request := RotateRequest{OperationID: registerID, BindingID: adminID, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1, ExpiresAt: time.Unix(250, 0), TargetRef: targetID}
	actor := store.CommandPrincipal{ID: adminID}
	result, err := p.Rotate(context.Background(), actor, request)
	if err != nil || result.Receipt.Result.Code != store.BindingUnavailable || result.Receipt.Replayed {
		t.Errorf("missing result=%+v err=%v", result, err)
	}
	if got := provisioningCounts(t, db); got != [5]int{0, 0, 0, 1, 1} {
		t.Errorf("rows=%v", got)
	}
	p.config.Guard = func(context.Context, *sql.Tx, string) error {
		t.Error("missing binding replay rechecked guard")
		return nil
	}
	replay, err := p.Rotate(context.Background(), actor, request)
	if err != nil || replay.Receipt.Result.Code != store.BindingUnavailable || !replay.Receipt.Replayed {
		t.Errorf("missing replay=%+v err=%v", replay, err)
	}
	p.config.Authorize = func(context.Context, *sql.Tx, store.CommandPrincipal) error { return store.Forbidden }
	if _, err := p.Rotate(context.Background(), actor, request); err != store.Forbidden {
		t.Fatalf("unauthorized replay err=%v", err)
	}
}

func TestEmptyProviderErrorCannotCommitSuccess(t *testing.T) {
	for _, failure := range []error{store.Code(""), fmt.Errorf("synthetic provider: %w", store.Code(""))} {
		p, db := testProvisioner(t, PublisherFunc(func(context.Context, CredentialFile) error { t.Error("failed precondition published"); return nil }))
		p.config.Guard = func(context.Context, *sql.Tx, string) error { return failure }
		result, err := p.Register(context.Background(), store.CommandPrincipal{ID: adminID}, testRegistration())
		if err != store.TemporarilyUnavailable || result.Receipt.AuditID != "" {
			t.Errorf("empty error result=%+v err=%v", result, err)
		}
		if got := provisioningCounts(t, db); got != [5]int{} {
			t.Errorf("empty error persisted rows=%v", got)
		}
	}
}
