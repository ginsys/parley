package recovery

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	runtimeowner "github.com/ginsys/parley/internal/runtime"
	"github.com/ginsys/parley/internal/store"
)

func recoveryFixture(t *testing.T) (*Service, *time.Time, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "recovery.db")
	db, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	now := time.Unix(110, 0)
	s, err := New(context.Background(), Config{Store: db, Markers: markerDirectory(t), Now: func() time.Time { return now }, FailStop: func() { t.Error("unexpected fail-stop") }})
	if err != nil {
		t.Fatal(err)
	}
	return s, &now, path
}
func rejectOrdinary(t *testing.T, s *Service) error {
	t.Helper()
	_, err := s.config.Store.Coordinator().Transition(context.Background(), func(context.Context, *sql.Tx, store.CommitView) (store.TransitionResult, error) {
		t.Fatal("held runtime authorized ordinary work")
		return store.TransitionResult{}, nil
	}, nil)
	return err
}
func TestClockRollbackPersistsAndSurvivesCorrectedTimeRestart(t *testing.T) {
	s, now, path := recoveryFixture(t)
	ctx := context.Background()
	*now = time.Unix(100, 0)
	if err := rejectOrdinary(t, s); err != store.RecoveryRequired {
		t.Fatalf("rollback=%v", err)
	}
	markers, err := s.config.Markers.List(ctx)
	if err != nil || len(markers) != 1 || markers[0].Kind != "clock" || *markers[0].Floor != time.Unix(110, 0).UnixNano() {
		t.Fatalf("clock markers=%+v %v", markers, err)
	}
	if err := s.config.Store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	restarted, err := New(ctx, Config{Store: db, Markers: s.config.Markers, Now: func() time.Time { return time.Unix(200, 0) }, FailStop: func() { t.Fatal("unexpected fail-stop") }})
	if err != nil {
		t.Fatal(err)
	}
	if mode, err := restarted.InspectRecovery(ctx, db); err != nil || mode != runtimeowner.Held {
		t.Fatalf("restart lost hold: mode=%v %v", mode, err)
	}
	if err := rejectOrdinary(t, restarted); err != store.RecoveryRequired {
		t.Fatalf("corrected wall bypassed incident: %v", err)
	}
	again, err := s.config.Markers.List(ctx)
	if err != nil || len(again) != 1 || !sameMarker(again[0], markers[0]) {
		t.Fatalf("restart replaced incident=%+v %v", again, err)
	}
}
func TestClockMarkerSurvivesFailedDatabaseRecording(t *testing.T) {
	s, now, path := recoveryFixture(t)
	ctx := context.Background()
	_, err := s.maintenance.Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		_, err := tx.ExecContext(ctx, "CREATE TRIGGER fail_recovery BEFORE INSERT ON recovery_incidents BEGIN SELECT RAISE(ABORT,'synthetic'); END")
		return store.TransitionResult{Changed: true}, err
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	*now = time.Unix(100, 0)
	if err := rejectOrdinary(t, s); err != store.TemporarilyUnavailable {
		t.Fatalf("recording failure=%v", err)
	}
	markers, err := s.config.Markers.List(ctx)
	if err != nil || len(markers) != 1 {
		t.Fatalf("lost external evidence=%+v %v", markers, err)
	}
	_, err = s.maintenance.Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		_, err := tx.ExecContext(ctx, "DROP TRIGGER fail_recovery")
		return store.TransitionResult{Changed: true}, err
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.config.Store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	restarted, err := New(ctx, Config{Store: db, Markers: s.config.Markers, Now: func() time.Time { return time.Unix(200, 0) }, FailStop: func() { t.Fatal("unexpected fail-stop") }})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		r, err := store.ReadRecovery(ctx, tx, markers[0].IncidentID)
		if err == nil && r.Status != "held" {
			t.Errorf("reconstructed=%+v", r)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

type unavailableMarkers struct{ Markers }

func (unavailableMarkers) Put(context.Context, Marker) error { return store.TemporarilyUnavailable }
func TestFailedExternalClockPersistenceRequiresSupervisorStop(t *testing.T) {
	s, now, _ := recoveryFixture(t)
	stops := 0
	s.config.FailStop = func() { stops++ }
	s.config.Markers = unavailableMarkers{s.config.Markers}
	*now = time.Unix(100, 0)
	if err := rejectOrdinary(t, s); err != store.RecoveryRequired {
		t.Fatalf("failed persistence=%v", err)
	}
	if stops == 0 {
		t.Fatal("missing supervisor stop obligation")
	}
	if err := rejectOrdinary(t, s); err != store.RecoveryRequired {
		t.Fatalf("failed marker reported recovered=%v", err)
	}
}
func TestRestoreAndClockIncidentsRemainIndependent(t *testing.T) {
	s, now, _ := recoveryFixture(t)
	ctx := context.Background()
	m := Marker{IncidentID: "60000000-0000-4000-8000-000000000001", ServerID: s.serverID, Kind: "restore"}
	if err := s.config.Markers.Put(ctx, m); err != nil {
		t.Fatal(err)
	}
	if mode, err := s.InspectRecovery(ctx, s.config.Store); err != nil || mode != runtimeowner.Held {
		t.Fatalf("restore=%v %v", mode, err)
	}
	*now = time.Unix(100, 0)
	if err := rejectOrdinary(t, s); err != store.RecoveryRequired {
		t.Fatalf("clock during restore=%v", err)
	}
	markers, err := s.config.Markers.List(ctx)
	if err != nil || len(markers) != 2 {
		t.Fatalf("independent incidents=%+v %v", markers, err)
	}
	for n := 0; n < 2; n++ {
		if err := s.config.Store.Coordinator().Inspect(ctx, func(context.Context, *sql.Tx) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	markers, err = s.config.Markers.List(ctx)
	if err != nil || len(markers) != 2 {
		t.Fatalf("inspection fabricated incidents=%+v %v", markers, err)
	}
}

func TestPreparingClockObservesConcurrentWriterFloorAndRetainsIncident(t *testing.T) {
	s, now, _ := recoveryFixture(t)
	ctx := context.Background()
	// Model an already-admitted writer that advances the floor while the next
	// operation is waiting to prepare. The next sample must compare that new floor.
	entered := make(chan struct{})
	release := make(chan struct{})
	written := make(chan error, 1)
	go func() {
		_, err := s.maintenance.Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
			close(entered)
			<-release
			changed, err := store.AdvanceClockCheckpoint(ctx, tx, time.Unix(120, 0).UnixNano())
			return store.TransitionResult{Changed: changed}, err
		}, nil)
		written <- err
	}()
	<-entered
	*now = time.Unix(115, 0)
	prepared := make(chan error, 1)
	go func() { prepared <- s.prepare(ctx) }()
	close(release)
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if err := <-prepared; err != nil {
		t.Fatal(err)
	}
	markers, err := s.config.Markers.List(ctx)
	if err != nil || len(markers) != 1 || *markers[0].Floor != time.Unix(120, 0).UnixNano() {
		t.Fatalf("concurrent floor detection=%+v %v", markers, err)
	}
	*now = time.Unix(200, 0)
	if err := rejectOrdinary(t, s); err != store.RecoveryRequired {
		t.Fatalf("corrected time erased concurrent detection=%v", err)
	}
}

type listInterleaving struct {
	Markers
	beforeList func()
}

func (m *listInterleaving) List(ctx context.Context) ([]Marker, error) {
	if m.beforeList != nil {
		run := m.beforeList
		m.beforeList = nil
		run()
	}
	return m.Markers.List(ctx)
}
func TestClockPreparationIncludesRolledBackObservationWaitingForFlush(t *testing.T) {
	s, now, _ := recoveryFixture(t)
	ctx := context.Background()
	s.config.Markers = &listInterleaving{Markers: s.config.Markers, beforeList: func() {
		// prepare owns ioMu and has already flushed. An earlier admitted writer
		// observes 120 but rolls back before its independent After can acquire ioMu.
		*now = time.Unix(120, 0)
		_, err := s.maintenance.Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
			_, err := s.transactionTime(ctx, tx, "human_inspection")
			return store.TransitionResult{}, err
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		*now = time.Unix(115, 0)
	}}
	if err := s.prepare(ctx); err != nil {
		t.Fatal(err)
	}
	markers, err := s.config.Markers.List(ctx)
	if err != nil || len(markers) != 1 || *markers[0].Floor != time.Unix(120, 0).UnixNano() || *markers[0].Observed != time.Unix(115, 0).UnixNano() {
		t.Fatalf("lost pending observation=%+v %v", markers, err)
	}
	*now = time.Unix(200, 0)
	if err := rejectOrdinary(t, s); err != store.RecoveryRequired {
		t.Fatalf("correction erased observed rollback=%v", err)
	}
}
