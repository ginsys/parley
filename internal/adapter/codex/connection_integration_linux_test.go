package codex

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/dispatch"
	"github.com/ginsys/parley/internal/recovery"
	runtimeowner "github.com/ginsys/parley/internal/runtime"
	"github.com/ginsys/parley/internal/store"
	bridgefixture "github.com/ginsys/parley/internal/testfixture/bridge"
	"github.com/google/uuid"
)

// This is controlled composition, not a shipped daemon/service or host verifier.
type connectionRuntimeService struct {
	t         *testing.T
	transport dispatch.Transport
	resources runtimeowner.Resources
	bridge    *bridgefixture.Fixture
	started   bool
	stopped   atomic.Bool
	workers   sync.WaitGroup
}

func (s *connectionRuntimeService) Start(_ context.Context, r runtimeowner.Resources) error {
	s.resources = r
	s.bridge = bridgefixture.New(s.t, r.Writer, s.transport)
	s.started = true
	return nil
}
func (s *connectionRuntimeService) StopAdmission() error { s.stopped.Store(true); return nil }
func (s *connectionRuntimeService) Wait() error          { s.workers.Wait(); return nil }

type integratedResult struct {
	out dispatch.Outcome
	err error
}

func (s *connectionRuntimeService) dispatch(id string) <-chan integratedResult {
	result := make(chan integratedResult, 1)
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		out, err := s.bridge.DispatchOutcome(s.resources.WorkerContext, id)
		result <- integratedResult{out, err}
	}()
	return result
}

type integrationQueue func(context.Context, string, string) error

func (f integrationQueue) QueueMessage(ctx context.Context, thread, text string) error {
	return f(ctx, thread, text)
}

type integrationFixture struct {
	writer       *store.DB
	path, server string
	markers      *recovery.Directory
	now          atomic.Int64
}

func newIntegrationFixture(t *testing.T) *integrationFixture {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "parley-connection-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	markerPath := filepath.Join(directory, "recovery")
	if err := os.Mkdir(markerPath, 0700); err != nil {
		t.Fatal(err)
	}
	markers, err := recovery.NewDirectory(markerPath, uint32(os.Geteuid()), 20)
	if err != nil {
		t.Fatal(err)
	}
	f := &integrationFixture{path: filepath.Join(directory, "bridge.db"), markers: markers}
	f.now.Store(time.Now().UnixNano())
	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(context.Background(), f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, name := range []string{"first", "second"} {
		if _, err := controller.New(db).Grant(context.Background(), controller.GrantParams{Conversation: name, PeerAID: "author", PeerBID: "recipient", Direction: store.Bidirectional, MaxExchanges: 5}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT server_id FROM installation").Scan(&f.server)
	}); err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *integrationFixture) start(t *testing.T, service *connectionRuntimeService, markers recovery.Markers, failStop func()) (*runtimeowner.Runtime, *recovery.Service) {
	t.Helper()
	var recoveryService *recovery.Service
	r, err := runtimeowner.Start(context.Background(), runtimeowner.Config{DatabasePath: f.path, InspectRecovery: func(ctx context.Context, db *store.DB) (runtimeowner.RecoveryMode, error) {
		var err error
		f.writer = db
		recoveryService, err = recovery.New(ctx, recovery.Config{Store: db, Markers: markers, Now: func() time.Time { return time.Unix(0, f.now.Load()) }, FailStop: failStop})
		if err != nil {
			return 0, err
		}
		return recoveryService.InspectRecovery(ctx, db)
	}, Services: []runtimeowner.Registration{{Service: service}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := r.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return r, recoveryService
}
func awaitIntegration(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("controlled child synchronization timed out")
	}
}

// C09/C10: one binding participates in independent conversations; the real
// runtime installs recovery before private-session work and retained ingestion.
func TestConnectionRuntimeMultipleConversationsAndAtomicReply(t *testing.T) {
	f := newIntegrationFixture(t)
	service := &connectionRuntimeService{t: t, transport: NewTransport(integrationQueue(func(context.Context, string, string) error { return nil }), "synthetic-native", "author", "recipient")}
	_, _ = f.start(t, service, f.markers, func() { t.Error("unexpected fail-stop") })
	ctx := context.Background()
	var originals []string
	for _, conversation := range []string{"first", "second"} {
		e, err := service.bridge.Send(ctx, conversation, "author", "recipient", "synthetic", nil)
		if err != nil {
			t.Fatal(err)
		}
		out, err := service.bridge.DispatchOutcome(ctx, e.ID)
		if err != nil || out.State != store.HandedOff || !out.Attempted {
			t.Fatalf("handoff=%+v %v", out, err)
		}
		originals = append(originals, e.ID)
	}
	recipient, err := service.bridge.Identity.Session("recipient")
	if err != nil {
		t.Fatal(err)
	}
	ingestor, err := connection.NewIngestor(connection.IngestorConfig{Manager: service.bridge.Identity.Manager, Verify: func(context.Context, connection.NativeTuple, connection.Token, connection.IngestRequest) error {
		return nil
	}, Origin: func(context.Context, connection.NativeTuple, connection.Token, string, string) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	source := uuid.NewString()
	if err := ingestor.Initialize(ctx, recipient, source, "0"); err != nil {
		t.Fatal(err)
	}
	request := connection.IngestRequest{Event: connection.NativeEvent{ID: "native-event", SourceID: source, Revision: "1", Before: "0", After: "1"}, Conversation: "first", Recipient: "author", Text: fmt.Sprintf("```BRIDGE-REPLY\n{\"in_reply_to\":%q,\"to\":\"author\",\"text\":\"reply\"}\n```", originals[0])}
	accepted, err := ingestor.Ingest(ctx, recipient, request)
	if err != nil || accepted.Classification != "accepted" {
		t.Fatalf("ingestion=%+v %v", accepted, err)
	}
	replay, err := ingestor.Ingest(ctx, recipient, request)
	if err != nil || !replay.Replayed || replay.EnvelopeID != accepted.EnvelopeID {
		t.Fatalf("replay=%+v %v", replay, err)
	}
	if err := service.resources.Writer.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		for index, id := range originals {
			e, err := store.GetByID(ctx, tx, id)
			if err != nil {
				return err
			}
			want := store.HandedOff
			if index == 0 {
				want = store.Acked
			}
			if e.State != want {
				t.Errorf("cross-conversation ACK=%+v", e)
			}
		}
		var bindings, events int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM bindings").Scan(&bindings); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM ingestion_evidence").Scan(&events); err != nil {
			return err
		}
		if bindings != 2 || events != 1 {
			t.Errorf("bindings=%d events=%d", bindings, events)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

type integrationReadyWriter struct {
	once  sync.Once
	ready chan struct{}
}

func (w *integrationReadyWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		w.once.Do(func() { close(w.ready) })
	}
	return len(p), nil
}
func TestConnectionRuntimeChild(t *testing.T) {
	if os.Getenv("PARLEY_CONNECTION_CHILD") != "1" {
		return
	}
	if _, err := os.Stdout.Write([]byte("ready")); err != nil {
		os.Exit(2)
	}
	for {
		time.Sleep(time.Hour)
	}
}

// C12: combine private recipient cancellation and real controlled subprocess
// startup with independent settlement and runtime writer/lease drain ordering.
func TestConnectionRuntimeCancellationAfterChildStartup(t *testing.T) {
	for _, mode := range []string{"recipient", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			f := newIntegrationFixture(t)
			ready := make(chan struct{})
			hostExited := make(chan struct{})
			allowSettlement := make(chan struct{})
			var release sync.Once
			writer := &integrationReadyWriter{ready: ready}
			sender := ExecSender{command: func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestConnectionRuntimeChild$")
				cmd.Env = append(os.Environ(), "PARLEY_CONNECTION_CHILD=1")
				cmd.Stdout = writer
				return cmd
			}}
			transport := NewTransport(integrationQueue(func(ctx context.Context, thread, text string) error {
				err := sender.QueueMessage(ctx, thread, text)
				close(hostExited)
				<-allowSettlement
				return err
			}), "synthetic-native", "author", "recipient")
			service := &connectionRuntimeService{t: t, transport: transport}
			r, _ := f.start(t, service, f.markers, func() { t.Error("unexpected fail-stop") })
			t.Cleanup(func() { release.Do(func() { close(allowSettlement) }) })
			e, err := service.bridge.Send(context.Background(), "first", "author", "recipient", "synthetic", nil)
			if err != nil {
				t.Fatal(err)
			}
			result := service.dispatch(e.ID)
			awaitIntegration(t, ready)
			stopped := make(chan error, 1)
			if mode == "shutdown" {
				go func() { stopped <- r.Stop(context.Background()) }()
			} else {
				// Trusted committed disconnect publication cancels only this bound recipient.
				var binding string
				_, err := service.resources.Writer.Coordinator().Transition(context.Background(), func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
					err := tx.QueryRowContext(ctx, "SELECT binding_id FROM bindings WHERE peer_id='recipient'").Scan(&binding)
					return store.TransitionResult{Changed: true}, err
				}, func(store.CommitView) { service.bridge.Identity.Manager.Invalidate(binding) })
				if err != nil {
					t.Fatal(err)
				}
				author, err := service.bridge.Identity.Session("author")
				if err != nil {
					t.Fatal(err)
				}
				recipient, err := service.bridge.Identity.Session("recipient")
				if err != nil {
					t.Fatal(err)
				}
				if recipient.Context().Err() == nil {
					t.Fatal("recipient cancellation did not reach its exact capability")
				}
				if author.Context().Err() != nil {
					t.Fatal("recipient cancellation invalidated unrelated author")
				}
				if err := service.bridge.Identity.Manager.RequireReady(context.Background(), author); err != nil {
					t.Fatalf("unaffected author lost readiness: %v", err)
				}
			}
			awaitIntegration(t, hostExited)
			if lease, err := runtimeowner.Acquire(f.path); !errors.Is(err, runtimeowner.ErrAlreadyRunning) {
				if lease != nil {
					lease.Close()
				}
				t.Fatalf("writer lease released before settlement: %v", err)
			}
			if mode == "shutdown" {
				select {
				case err := <-stopped:
					t.Fatalf("shutdown bypassed pending settlement: %v", err)
				default:
				}
			}
			release.Do(func() { close(allowSettlement) })
			select {
			case result := <-result:
				if result.err != nil || result.out.State != store.Uncertain || !result.out.Attempted {
					t.Fatalf("post-start=%+v %v", result.out, result.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("settlement did not finish")
			}
			if mode == "shutdown" {
				select {
				case err := <-stopped:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("runtime did not drain")
				}
			} else {
				if err := r.Stop(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			db, err := store.OpenExisting(context.Background(), f.path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
				saved, err := store.GetByID(ctx, tx, e.ID)
				if err != nil {
					return err
				}
				grant, err := store.CurrentGrant(ctx, tx, "first")
				if err != nil {
					return err
				}
				if saved.State != store.Uncertain || saved.DispatchAttempt != 1 || grant.ExchangesUsed != 1 {
					t.Errorf("lost attempt/budget evidence=%+v %+v", saved, grant)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConnectionRuntimeRestoreMarkerSkipsOrdinaryAdmission(t *testing.T) {
	f := newIntegrationFixture(t)
	ctx := context.Background()
	if err := f.markers.Put(ctx, recovery.Marker{IncidentID: uuid.NewString(), ServerID: f.server, Kind: "restore"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		service := &connectionRuntimeService{t: t}
		r, _ := f.start(t, service, f.markers, func() { t.Error("unexpected fail-stop") })
		if service.started {
			t.Fatal("restore admitted ordinary manager/workers")
		}
		if err := r.Stop(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

type unavailableIntegrationMarkers struct{ recovery.Markers }

func (unavailableIntegrationMarkers) Put(context.Context, recovery.Marker) error {
	return store.TemporarilyUnavailable
}
func TestConnectionRuntimeClockPersistenceFailureSignalsSupervisedStop(t *testing.T) {
	f := newIntegrationFixture(t)
	service := &connectionRuntimeService{t: t}
	stops := make(chan struct{}, 1)
	r, _ := f.start(t, service, unavailableIntegrationMarkers{f.markers}, func() {
		select {
		case stops <- struct{}{}:
		default:
		}
	})
	f.now.Add(-int64(time.Second))
	if _, err := service.bridge.Send(context.Background(), "first", "author", "recipient", "held", nil); err != store.RecoveryRequired {
		t.Fatalf("failed clock evidence admitted work=%v", err)
	}
	awaitIntegration(t, stops)
	// The synthetic supervisor acts outside the writer callback. Real deployment
	// must additionally prevent unattended restart, which belongs to its operator.
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !service.stopped.Load() {
		t.Fatal("supervisor did not stop admission")
	}
	db, err := store.OpenExisting(context.Background(), f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM envelopes").Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			t.Errorf("failed hold queued %d envelopes", count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// C16: restore an actual stopped SQLite snapshot from before delivery, ACK,
// revocation and budget use. The restored history must never authorize replay.
func TestConnectionRuntimeOldSnapshotCannotReplayLostEffects(t *testing.T) {
	f := newIntegrationFixture(t)
	ctx := context.Background()
	transport := NewTransport(integrationQueue(func(context.Context, string, string) error { return nil }), "synthetic-native", "author", "recipient")
	first := &connectionRuntimeService{t: t, transport: transport}
	r, _ := f.start(t, first, f.markers, func() { t.Error("unexpected stop") })
	e, err := first.bridge.Send(ctx, "first", "author", "recipient", "snapshot-original", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(f.path + "-wal"); err == nil && info.Size() != 0 {
		t.Fatal("fixture snapshot requires checkpointed closed storage")
	} else if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	snapshot, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	second := &connectionRuntimeService{t: t, transport: transport}
	r, _ = f.start(t, second, f.markers, func() { t.Error("unexpected stop") })
	if out, err := second.bridge.DispatchOutcome(ctx, e.ID); err != nil || out.State != store.HandedOff {
		t.Fatalf("post-snapshot delivery=%+v %v", out, err)
	}
	marker := fmt.Sprintf("```BRIDGE-REPLY\n{\"in_reply_to\":%q,\"to\":\"author\",\"text\":\"reply\"}\n```", e.ID)
	if _, err := bridgefixture.IngestTurn(t, ctx, second.resources.Writer, "first", "recipient", "author", marker); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.New(second.resources.Writer).Revoke(ctx, "first"); err != nil {
		t.Fatal(err)
	}
	if err := second.resources.Writer.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		saved, err := store.GetByID(ctx, tx, e.ID)
		if err != nil {
			return err
		}
		var used int64
		var status string
		if err := tx.QueryRowContext(ctx, "SELECT exchanges_used,status FROM grants WHERE conversation='first' AND grant_version=1").Scan(&used, &status); err != nil {
			return err
		}
		if saved.State != store.Acked || used != 1 || status != "revoked" {
			t.Errorf("post-snapshot evidence missing: state=%s used=%d status=%s", saved.State, used, status)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	incident := uuid.NewString()
	if err := f.markers.Put(ctx, recovery.Marker{IncidentID: incident, ServerID: f.server, Kind: "restore"}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.path, snapshot, 0600); err != nil {
		t.Fatal(err)
	}
	restored := &connectionRuntimeService{t: t, transport: transport}
	r, _ = f.start(t, restored, f.markers, func() { t.Error("unexpected stop") })
	if restored.started {
		t.Fatal("old snapshot admitted replay-capable services")
	}
	if err := f.writer.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		saved, err := store.GetByID(ctx, tx, e.ID)
		if err != nil {
			return err
		}
		record, err := store.ReadRecovery(ctx, tx, incident)
		if err != nil {
			return err
		}
		grant, err := store.CurrentGrant(ctx, tx, "first")
		if err != nil {
			return err
		}
		if saved.State != store.Queued || saved.DispatchAttempt != 0 || grant.ExchangesUsed != 0 || record.Status != "held" {
			t.Errorf("restored evidence was silently reconciled: %+v %+v %+v", saved, grant, record)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.writer.Coordinator().Transition(ctx, func(context.Context, *sql.Tx, store.CommitView) (store.TransitionResult, error) {
		t.Fatal("restored snapshot authorized new work")
		return store.TransitionResult{}, nil
	}, nil); err != store.RecoveryRequired {
		t.Fatalf("restore gate=%v", err)
	}
}
