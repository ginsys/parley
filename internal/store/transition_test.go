package store

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"testing"
)

func TestConnectionTransitionCommitBeforePublication(t *testing.T) {
	db := commandDB(t)
	ctx := context.Background()
	published := false
	code, err := db.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, view CommitView) (TransitionResult, error) {
		if view.Epoch == "" {
			t.Fatal("missing startup epoch")
		}
		_, err := insertSynthetic(ctx, tx)
		return TransitionResult{Changed: true, Code: AuthenticationFailed}, err
	}, func(view CommitView) {
		var n int
		if err := db.sql.QueryRow("SELECT count(*) FROM conversations").Scan(&n); err != nil || n != 1 {
			t.Errorf("publication before commit: n=%d err=%v", n, err)
		}
		if view.Revision != 1 {
			t.Errorf("revision=%d", view.Revision)
		}
		published = true
	})
	if err != nil || code != AuthenticationFailed || !published {
		t.Fatalf("code=%s err=%v published=%v", code, err, published)
	}
	for _, table := range []string{"operation_results", "command_audit"} {
		var n int
		if err := db.sql.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("%s: %d %v", table, n, err)
		}
	}
}

func TestConnectionTransitionDiscardsUnpublishedEffects(t *testing.T) {
	for _, tc := range []string{"read", "error", "overflow", "cancel"} {
		t.Run(tc, func(t *testing.T) {
			db := commandDB(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc == "overflow" {
				db.Coordinator().revision = math.MaxInt64
			}
			published := false
			_, err := db.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ CommitView) (TransitionResult, error) {
				_, err := insertSynthetic(ctx, tx)
				if err != nil {
					return TransitionResult{}, err
				}
				if tc == "error" {
					return TransitionResult{}, TemporarilyUnavailable
				}
				if tc == "cancel" {
					cancel()
				}
				return TransitionResult{Changed: tc != "read"}, nil
			}, func(CommitView) { published = true })
			if tc != "read" && err == nil {
				t.Fatal("expected failure")
			}
			if tc == "overflow" && !errors.Is(err, InvalidRequest) {
				t.Fatalf("overflow=%v", err)
			}
			if published {
				t.Fatal("published uncommitted state")
			}
			var n int
			if err := db.sql.QueryRow("SELECT count(*) FROM conversations").Scan(&n); err != nil || n != 0 {
				t.Fatalf("effects=%d err=%v", n, err)
			}
			if db.Coordinator().failed != (tc == "overflow") {
				t.Fatal("unexpected coordinator failure state")
			}
		})
	}
}

func TestConnectionManagerOwnershipIsExclusive(t *testing.T) {
	c := commandDB(t).Coordinator()
	if err := c.ClaimConnections(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.ClaimConnections(context.Background()); err != InvalidRequest {
		t.Fatalf("duplicate owner=%v", err)
	}
}

func TestRevisionOverflowStopsOrdinaryTransitions(t *testing.T) {
	c := commandDB(t).Coordinator()
	c.revision = math.MaxInt64
	if _, err := c.Transition(context.Background(), func(context.Context, *sql.Tx, CommitView) (TransitionResult, error) {
		return TransitionResult{Changed: true}, nil
	}, nil); err != InvalidRequest {
		t.Fatalf("overflow=%v", err)
	}
	if _, err := c.Transition(context.Background(), func(context.Context, *sql.Tx, CommitView) (TransitionResult, error) {
		t.Fatal("ordinary operation admitted after overflow")
		return TransitionResult{}, nil
	}, nil); err != RecoveryRequired {
		t.Fatalf("after overflow=%v", err)
	}
}
