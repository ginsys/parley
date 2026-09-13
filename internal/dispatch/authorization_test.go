package dispatch_test

import (
	"context"
	"errors"
	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/dispatch"
	"github.com/ginsys/parley/internal/store"
	bridgefixture "github.com/ginsys/parley/internal/testfixture/bridge"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSendEnforcesGrantPairAndDirection(t *testing.T) {
	for _, pair := range [][2]string{{"codex", "claude"}, {"stranger", "codex"}, {"claude", "stranger"}} {
		t.Run(pair[0]+"-"+pair[1], func(t *testing.T) {
			ctx := context.Background()
			db := openTestDB(t)
			_, err := controller.New(db).Grant(ctx, controller.GrantParams{Conversation: "c", PeerAID: "claude", PeerBID: "codex", Direction: store.AToB, MaxExchanges: 2})
			if err != nil {
				t.Fatal(err)
			}
			tr := newFakeTransport()
			bridge := bridgefixture.New(t, db, tr)
			if _, err := bridge.Send(ctx, "c", pair[0], pair[1], "unauthorized", nil); err == nil {
				t.Fatal("accepted unauthorized message")
			}
			tx, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			var count int
			if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM envelopes").Scan(&count); err != nil || count != 0 {
				t.Fatalf("rows=%d: %v", count, err)
			}
			g, err := store.CurrentGrant(ctx, tx, "c")
			if err != nil || g.ExchangesUsed != 0 {
				t.Fatalf("budget mutated: %v, %v", g, err)
			}
		})
	}
}

func TestClaimRejectsStaleVersionWithoutCallingItExhaustion(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	ctrl := controller.New(db)
	grantOne(t, ctrl, "c", 10)
	if _, err := ctrl.Renew(ctx, controller.RenewParams{Conversation: "c"}); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	e := store.Envelope{ID: "old", Conversation: "c", FromPeer: "claude-session-a", ToPeer: "codex-thread-b", Text: "old", GrantVersion: 1, State: store.Queued, CreatedAt: now, UpdatedAt: now}
	if err := store.InsertQueued(ctx, tx, e); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tr := newFakeTransport()
	state, err := bridgefixture.New(t, db, tr).Dispatch(ctx, e.ID)
	if !errors.Is(err, dispatch.ErrStaleGrantVersion) || state != store.Cancelled || len(tr.delivered) != 0 {
		t.Fatalf("state=%s err=%v delivered=%v", state, err, tr.delivered)
	}
}

func TestRenewalCancellationChoiceIncludesLateSettlement(t *testing.T) {
	for _, mode := range []string{"queued-default", "queued-cancel", "late-default", "late-cancel", "late-cancel-then-renew", "late-revoke-reenroll"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := openTestDB(t)
			ctrl := controller.New(db)
			grantOne(t, ctrl, "c", 10)
			ackEnvelope(t, db, "c", "original", 1)
			reply := sendTrustedReply(t, db, "c", "codex-thread-b", "claude-session-a", "answer", "original", 1)
			cancelReplies := mode == "queued-cancel" || mode == "late-cancel" || mode == "late-cancel-then-renew"
			renew := func() {
				if _, err := ctrl.Renew(ctx, controller.RenewParams{Conversation: "c", CancelPendingReplies: cancelReplies}); err != nil {
					t.Fatal(err)
				}
				if mode == "late-cancel-then-renew" {
					if _, err := ctrl.Renew(ctx, controller.RenewParams{Conversation: "c"}); err != nil {
						t.Fatal(err)
					}
				}
			}
			late := mode != "queued-default" && mode != "queued-cancel"
			tr := transportFunc(func(context.Context, store.Envelope) error {
				if late {
					if mode == "late-revoke-reenroll" {
						if _, err := ctrl.Revoke(ctx, "c"); err != nil {
							t.Fatal(err)
						}
						grantOne(t, ctrl, "c", 10)
					} else {
						renew()
					}
					return dispatch.ErrNoAttempt
				}
				return nil
			})
			if !late {
				renew()
			}
			state, err := bridgefixture.New(t, db, tr).Dispatch(ctx, reply.ID)
			if err != nil {
				t.Fatal(err)
			}
			want := store.HandedOff
			if late {
				want = store.Queued
			}
			if cancelReplies || mode == "late-revoke-reenroll" {
				want = store.Cancelled
			}
			if state != want {
				t.Fatalf("state=%s want=%s", state, want)
			}
		})
	}
}

func TestBudgetRaceAcrossIndependentConnections(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "budget.db")
	a, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.OpenReaders(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.OpenReaders(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	grantOne(t, controller.New(a), "c", 1)
	bridge := bridgefixture.New(t, a, newFakeTransport())
	e, err := bridge.Send(ctx, "c", "claude-session-a", "codex-thread-b", "message", nil)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	out := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); <-start; _, err := bridge.Dispatch(ctx, e.ID); out <- err }()
	// A separate SQLite writer competes for the same remaining budget. It is
	// deliberately not a second connection manager for one live installation.
	go func() {
		defer wg.Done()
		<-start
		tx, err := b.Begin(ctx)
		if err != nil {
			out <- err
			return
		}
		defer tx.Rollback()
		ok, err := store.ClaimExchange(ctx, tx, "c", 1)
		if err != nil {
			out <- err
			return
		}
		if !ok {
			out <- dispatch.ErrBudgetExhausted
			return
		}
		out <- tx.Commit()
	}()
	close(start)
	wg.Wait()
	close(out)
	successes, exhausted := 0, 0
	for err := range out {
		if err == nil {
			successes++
		} else if errors.Is(err, dispatch.ErrBudgetExhausted) {
			exhausted++
		} else {
			t.Fatal(err)
		}
	}
	if successes != 1 || exhausted != 1 {
		t.Fatalf("success=%d exhausted=%d", successes, exhausted)
	}
}

func TestRevokeWhileIndependentDispatchIsInFlight(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "revoke.db")
	a, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.OpenReaders(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.OpenReaders(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctrl := controller.New(b)
	grantOne(t, ctrl, "c", 3)
	entered, release := make(chan struct{}), make(chan struct{})
	bridge := bridgefixture.New(t, a, transportFunc(func(context.Context, store.Envelope) error { close(entered); <-release; return nil }))
	first, err := bridge.Send(ctx, "c", "claude-session-a", "codex-thread-b", "first", nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := bridge.Send(ctx, "c", "claude-session-a", "codex-thread-b", "second", nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		state, err := bridge.Dispatch(ctx, first.ID)
		if err == nil && state != store.HandedOff {
			err = errors.New("first not handed off")
		}
		done <- err
	}()
	<-entered
	result, err := ctrl.Revoke(ctx, "c")
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	if result.AlreadyDispatching != 1 || result.Cancelled != 1 {
		t.Fatalf("revoke=%+v", result)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	state, err := bridge.Dispatch(ctx, second.ID)
	if err != nil || state != store.Cancelled {
		t.Fatalf("second=%s: %v", state, err)
	}
}

func TestPermittedDirectionsAndClaimAuthorization(t *testing.T) {
	for _, direction := range []store.Direction{store.AToB, store.BToA, store.Bidirectional} {
		t.Run(string(direction), func(t *testing.T) {
			ctx := context.Background()
			db := openTestDB(t)
			_, err := controller.New(db).Grant(ctx, controller.GrantParams{Conversation: "c", PeerAID: "a", PeerBID: "b", Direction: direction, MaxExchanges: 10})
			if err != nil {
				t.Fatal(err)
			}
			tr := newFakeTransport()
			bridge := bridgefixture.New(t, db, tr)
			for i, pair := range [][2]string{{"a", "b"}, {"b", "a"}, {"other", "b"}} {
				allowed := i == 0 && (direction == store.AToB || direction == store.Bidirectional) || i == 1 && (direction == store.BToA || direction == store.Bidirectional)
				e, err := bridge.Send(ctx, "c", pair[0], pair[1], "message", nil)
				if allowed {
					if err != nil {
						t.Fatal(err)
					}
					if state, err := bridge.Dispatch(ctx, e.ID); err != nil || state != store.HandedOff {
						t.Fatalf("state=%s: %v", state, err)
					}
					continue
				}
				if !errors.Is(err, store.Forbidden) {
					t.Fatalf("accept error=%v", err)
				}
				// Direct store insertion represents an invalid row reaching the shared claim boundary.
				tx, err := db.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				now := time.Now().UTC().Format(time.RFC3339Nano)
				injected := store.Envelope{ID: pair[0], Conversation: "c", FromPeer: pair[0], ToPeer: pair[1], Text: "forged", GrantVersion: 1, State: store.Queued, CreatedAt: now, UpdatedAt: now}
				if err := store.InsertQueued(ctx, tx, injected); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				before := len(tr.delivered)
				state, err := bridge.Dispatch(ctx, injected.ID)
				if !errors.Is(err, dispatch.ErrNotPermitted) || state != store.Cancelled || len(tr.delivered) != before {
					t.Fatalf("claim state=%s err=%v", state, err)
				}
			}
			expireGrant(t, db, "c")
			if _, err := bridge.Send(ctx, "c", "a", "b", "expired", nil); !errors.Is(err, store.Forbidden) {
				t.Fatalf("expired acceptance=%v", err)
			}
		})
	}
}

func TestReaderCandidateDoesNotAuthorizeAfterAdministration(t *testing.T) {
	for _, action := range []string{"revoke", "renew"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			db := openTestDB(t)
			ctrl := controller.New(db)
			grantOne(t, ctrl, "c", 5)
			tr := newFakeTransport()
			bridge := bridgefixture.New(t, db, tr)
			e, err := bridge.Send(ctx, "c", "claude-session-a", "codex-thread-b", "synthetic", nil)
			if err != nil {
				t.Fatal(err)
			}
			ids, _, err := db.Queries().QueueBatch(ctx, "c", "codex-thread-b", 100, nil)
			if err != nil || len(ids) != 1 || ids[0].ID != e.ID {
				t.Fatalf("selection: %v %v", ids, err)
			}
			if action == "revoke" {
				_, err = ctrl.Revoke(ctx, "c")
			} else {
				_, err = ctrl.Renew(ctx, controller.RenewParams{Conversation: "c", MaxExchanges: 5})
			}
			if err != nil {
				t.Fatal(err)
			}
			out, err := bridge.DispatchOutcome(ctx, ids[0].ID)
			if err != nil || out.Attempted || out.State != store.Cancelled {
				t.Fatalf("stale selection delivered: %+v %v", out, err)
			}
			if len(tr.delivered) != 0 {
				t.Fatal("host called after authority changed")
			}
			tx, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			var used int64
			if err := tx.QueryRowContext(ctx, "SELECT sum(exchanges_used) FROM grants WHERE conversation='c'").Scan(&used); err != nil || used != 0 {
				t.Fatalf("budget changed: %d %v", used, err)
			}
		})
	}
}
