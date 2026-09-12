//go:build linux

package runtime

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/store"
)

type fixtureService struct {
	start func(context.Context, Resources) error
	stop  func() error
	wait  func() error
}

func (s fixtureService) Start(ctx context.Context, r Resources) error {
	if s.start != nil {
		return s.start(ctx, r)
	}
	return nil
}
func (s fixtureService) StopAdmission() error {
	if s.stop != nil {
		return s.stop()
	}
	return nil
}
func (s fixtureService) Wait() error {
	if s.wait != nil {
		return s.wait()
	}
	return nil
}
func normal(context.Context, *store.DB) (RecoveryMode, error) { return Normal, nil }

func seededRuntimeDB(t *testing.T) string {
	t.Helper()
	path := privateDB(t)
	db, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	statements := []string{
		`INSERT INTO conversations(id,name,created_at) VALUES('c','c','2026-01-01T00:00:00Z')`,
		`INSERT INTO grants(conversation,grant_version,peer_a_id,peer_b_id,direction,max_exchanges,exchanges_used,granted_at,status) VALUES('c',1,'a','b','bidirectional',5,1,'2026-01-01T00:00:00Z','active')`,
		`INSERT INTO envelopes(id,conversation,from_peer,to_peer,text,grant_version,state,created_at,updated_at,dispatch_attempt,created_at_ns) VALUES('e','c','a','b','synthetic',1,'dispatching','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z',7,1)`,
	}
	for _, s := range statements {
		if _, err := tx.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLifecycleRecoveryBeforeAdmission(t *testing.T) {
	path := seededRuntimeDB(t)
	for round := range 2 {
		started := false
		service := fixtureService{start: func(ctx context.Context, r Resources) error {
			started = true
			out, err := r.Queries.Outcome(ctx, "e")
			if err != nil {
				return err
			}
			if out.State != store.Uncertain || out.ErrorCode != "interrupted" {
				t.Fatalf("recovery missing: %+v", out)
			}
			tx, err := r.Writer.Begin(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			e, err := store.GetByID(ctx, tx, "e")
			if err != nil {
				return err
			}
			g, err := store.CurrentGrant(ctx, tx, "c")
			if err != nil {
				return err
			}
			if e.DispatchAttempt != 7 || g.ExchangesUsed != 1 {
				t.Fatalf("recovery replay/refund: %+v %+v", e, g)
			}
			return nil
		}}
		r, err := Start(context.Background(), Config{DatabasePath: path, InspectRecovery: normal, Services: []Registration{{Service: service}}})
		if err != nil {
			t.Fatal(round, err)
		}
		if !started {
			t.Fatal("service not started")
		}
		if err := r.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHeldOnlyStartsRecoveryServices(t *testing.T) {
	path := seededRuntimeDB(t)
	recoveryStarted := false
	inspect := func(ctx context.Context, db *store.DB) (RecoveryMode, error) {
		if _, err := db.Queries().Outcome(ctx, "e"); !errors.Is(err, store.ErrReadersNotReady) {
			t.Fatal("readers before inspection:", err)
		}
		return Held, nil
	}
	ordinary := fixtureService{start: func(context.Context, Resources) error { t.Fatal("ordinary admission during hold"); return nil }}
	recovery := fixtureService{start: func(ctx context.Context, r Resources) error {
		recoveryStarted = true
		out, err := r.Queries.Outcome(ctx, "e")
		if err == nil && out.State != store.Dispatching {
			t.Fatal("held startup mutated envelope")
		}
		return err
	}}
	r, err := Start(context.Background(), Config{DatabasePath: path, InspectRecovery: inspect, Services: []Registration{{Service: ordinary}, {Service: recovery, RecoveryOnly: true}}})
	if err != nil {
		t.Fatal(err)
	}
	if !recoveryStarted {
		t.Fatal("recovery inspection unavailable")
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStartupFailuresReleaseOwnership(t *testing.T) {
	for _, phase := range []string{"inspector-missing", "writer", "migration", "recovery-inspection", "recovery-mode", "recovery-write", "readers", "service"} {
		t.Run(phase, func(t *testing.T) {
			path := privateDB(t)
			inspector := normal
			open := store.OpenExisting
			stopped, waited := false, false
			service := fixtureService{start: func(context.Context, Resources) error {
				if phase != "service" {
					t.Fatal("early admission")
				}
				return errors.New("synthetic service failure")
			}, stop: func() error { stopped = true; return nil }, wait: func() error { waited = true; return nil }}
			switch phase {
			case "inspector-missing":
				inspector = nil
			case "writer":
				open = func(context.Context, string) (*store.DB, error) { return nil, errors.New("synthetic open failure") }
			case "migration":
				raw, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := raw.Exec("PRAGMA user_version=999"); err != nil {
					t.Fatal(err)
				}
				raw.Close()
			case "recovery-inspection":
				inspector = func(context.Context, *store.DB) (RecoveryMode, error) { return 0, errors.New("inspection failure") }
			case "recovery-mode":
				inspector = func(context.Context, *store.DB) (RecoveryMode, error) { return 0, nil }
			case "recovery-write":
				inspector = func(_ context.Context, db *store.DB) (RecoveryMode, error) { db.Close(); return Normal, nil }
			case "readers":
				inspector = func(_ context.Context, db *store.DB) (RecoveryMode, error) { db.Close(); return Held, nil }
			}
			if r, err := start(context.Background(), Config{DatabasePath: path, InspectRecovery: inspector, Services: []Registration{{Service: service, RecoveryOnly: true}}}, open); err == nil {
				r.Stop(context.Background())
				t.Fatal("failure admitted runtime")
			}
			if phase == "service" && (!stopped || !waited) {
				t.Fatal("partially started service leaked")
			}
			o, err := Acquire(path)
			if err != nil {
				t.Fatal("failure leaked ownership:", err)
			}
			o.Close()
		})
	}
}

func TestShutdownOrderAndCancelledWait(t *testing.T) {
	path := privateDB(t)
	var mu sync.Mutex
	var events []string
	add := func(s string) { mu.Lock(); events = append(events, s); mu.Unlock() }
	blocked := make(chan struct{})
	waiting := make(chan struct{})
	var db *store.DB
	service := func(name string) fixtureService {
		var workerCtx context.Context
		return fixtureService{start: func(ctx context.Context, r Resources) error {
			workerCtx = ctx
			db = r.Writer
			add("start-" + name)
			return nil
		}, stop: func() error {
			if workerCtx.Err() != nil {
				t.Error("workers cancelled before admission stopped")
			}
			add("stop-" + name)
			return nil
		}, wait: func() error {
			if workerCtx.Err() == nil {
				t.Error("workers not cancelled")
			}
			add("wait-" + name)
			if name == "b" {
				close(waiting)
				<-blocked
			}
			// Simulate independent settlement: writer must still be alive.
			tx, err := db.Begin(context.Background())
			if err != nil {
				return err
			}
			return tx.Rollback()
		}}
	}
	r, err := Start(context.Background(), Config{DatabasePath: path, InspectRecovery: normal, Services: []Registration{{Service: service("a")}, {Service: service("b")}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Stop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	<-waiting
	if _, err := Acquire(path); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatal("cancelled wait released lock:", err)
	}
	close(blocked)
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Begin(context.Background()); err == nil {
		t.Fatal("writer left open")
	}
	if _, err := db.Queries().Outcome(context.Background(), "missing"); !errors.Is(err, store.ErrReadersNotReady) {
		t.Fatal("readers left open:", err)
	}
	o, err := Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	o.Close()
	want := []string{"start-a", "start-b", "stop-b", "stop-a", "wait-b", "wait-a"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("order=%v", events)
	}
}

func TestParentCancellationStopsRuntime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r, err := Start(ctx, Config{DatabasePath: privateDB(t), InspectRecovery: normal})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-r.done:
	case <-time.After(time.Second):
		t.Fatal("parent cancellation leaked runtime")
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// A second process proves the contender cannot call even the injected opener.
func TestRuntimeContenderChild(t *testing.T) {
	path := os.Getenv("PARLEY_TEST_CONTENDER")
	if path == "" {
		return
	}
	_, err := start(context.Background(), Config{DatabasePath: path, InspectRecovery: normal}, func(context.Context, string) (*store.DB, error) {
		t.Fatal("contender reached store.Open")
		return nil, nil
	})
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Fatal(err)
	}
}

func TestContenderBeforeAndDuringMigration(t *testing.T) {
	for _, phase := range []string{"before", "during"} {
		t.Run(phase, func(t *testing.T) {
			path := privateDB(t)
			paused, release := make(chan struct{}), make(chan struct{})
			var blocker *sql.Tx
			var external *sql.DB
			if phase == "during" {
				seed, err := store.Open(context.Background(), path)
				if err != nil {
					t.Fatal(err)
				}
				seed.Close()
				external, err = sql.Open("sqlite", path+"?_txlock=immediate")
				if err != nil {
					t.Fatal(err)
				}
				defer external.Close()
				blocker, err = external.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer blocker.Rollback()
			}
			result := make(chan error, 1)
			go func() {
				r, err := start(context.Background(), Config{DatabasePath: path, InspectRecovery: normal}, func(ctx context.Context, p string) (*store.DB, error) {
					close(paused)
					if phase == "before" {
						<-release
					}
					return store.OpenExisting(ctx, p)
				})
				if err == nil {
					err = r.Stop(context.Background())
				}
				result <- err
			}()
			<-paused
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRuntimeContenderChild$")
			cmd.Env = append(os.Environ(), "PARLEY_TEST_CONTENDER="+path)
			output, err := cmd.CombinedOutput()
			close(release)
			if blocker != nil {
				blocker.Rollback()
			}
			startErr := <-result
			if err != nil {
				t.Fatalf("contender: %s %v", output, err)
			}
			if startErr != nil {
				t.Fatal(startErr)
			}
		})
	}
}

func TestCancelledPartialStartupCleansEveryService(t *testing.T) {
	path := privateDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	var events []string
	service := func(name string, block bool) fixtureService {
		return fixtureService{
			start: func(ctx context.Context, _ Resources) error {
				events = append(events, "start-"+name)
				if block {
					close(entered)
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}, stop: func() error { events = append(events, "stop-"+name); return nil }, wait: func() error { events = append(events, "wait-"+name); return nil },
		}
	}
	result := make(chan error, 1)
	go func() {
		_, err := Start(ctx, Config{DatabasePath: path, InspectRecovery: normal, Services: []Registration{{Service: service("a", false)}, {Service: service("b", true)}}})
		result <- err
	}()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	want := []string{"start-a", "start-b", "stop-b", "stop-a", "wait-b", "wait-a"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("partial cleanup order=%v", events)
	}
	o, err := Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	o.Close()
}
