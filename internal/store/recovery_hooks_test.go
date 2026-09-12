package store

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestRecoveryHooksFreezeAuthorityTimeAndCoverReplay(t *testing.T) {
	db := commandDB(t)
	ctx := context.Background()
	var maintenance *RecoveryMaintenance
	held := false
	before, after := 0, 0
	instant := time.Unix(110, 0)
	var err error
	maintenance, err = db.Coordinator().InstallRecovery(RecoveryHooks{
		Before: func(ctx context.Context, kind string) error {
			before++
			if held && kind != "human_inspection" {
				return RecoveryRequired
			}
			// Reentering through maintenance proves no writer gate is held here.
			return maintenance.Inspect(ctx, func(context.Context, *sql.Tx) error { return nil })
		},
		Time: func(context.Context, *sql.Tx, string) (time.Time, error) { return instant, nil },
		After: func() error {
			after++
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			return maintenance.Inspect(ctx, func(context.Context, *sql.Tx) error { return nil })
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest(t, testOperation)
	mutate := func(ctx context.Context, tx *sql.Tx) (CommandResult, error) {
		first := AuthorityTime(ctx, func() time.Time { t.Fatal("resampled authority clock"); return time.Time{} })
		instant = time.Unix(50, 0)
		second := AuthorityTime(ctx, func() time.Time { t.Fatal("resampled authority clock"); return time.Time{} })
		if !first.Equal(time.Unix(110, 0)) || !second.Equal(first) {
			t.Fatalf("authority times=%v / %v", first, second)
		}
		return CommandResult{}, nil
	}
	receipt, err := db.Coordinator().Execute(ctx, CommandPrincipal{ID: testPrincipal}, request, allowed, mutate, nil)
	if err != nil || receipt.Result.Code != "" {
		t.Fatalf("command=%+v %v", receipt, err)
	}
	held = true
	if _, err := db.Coordinator().Execute(ctx, CommandPrincipal{ID: testPrincipal}, request, allowed, mutate, nil); err != RecoveryRequired {
		t.Fatalf("ordinary replay bypassed hold=%v", err)
	}
	replay, found, err := db.Coordinator().LookupCommand(ctx, CommandPrincipal{ID: testPrincipal}, request, allowed)
	if err != nil || !found || !replay.Replayed || replay.AuditID != receipt.AuditID {
		t.Fatalf("human recovery lookup=%+v %v %v", replay, found, err)
	}
	if before != 3 || after != 3 {
		t.Fatalf("hook phases=%d/%d", before, after)
	}
}
func TestRecoveryHooksCannotBeInstalledAfterAdmission(t *testing.T) {
	db := commandDB(t)
	if err := db.Coordinator().ClaimConnections(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := db.Coordinator().InstallRecovery(RecoveryHooks{Before: func(context.Context, string) error { return nil }, Time: func(context.Context, *sql.Tx, string) (time.Time, error) { return time.Now(), nil }, After: func() error { return nil }})
	if err != InvalidRequest {
		t.Fatalf("late recovery installation=%v", err)
	}
}
