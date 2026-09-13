package recovery

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	runtimeowner "github.com/ginsys/parley/internal/runtime"
	"github.com/ginsys/parley/internal/store"
)

type runtimeFixtureService struct {
	start func(runtimeowner.Resources) error
}

func (s runtimeFixtureService) Start(_ context.Context, r runtimeowner.Resources) error {
	return s.start(r)
}
func (runtimeFixtureService) StopAdmission() error { return nil }
func (runtimeFixtureService) Wait() error          { return nil }

func TestRuntimeConstructsRecoveryOnOwnedWriter(t *testing.T) {
	for _, mode := range []string{"normal", "held", "invalid-marker"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			markers := markerDirectory(t)
			path := filepath.Join(filepath.Dir(markers.path), "runtime.db")
			if err := os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
			seed, err := store.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			var server string
			if err := seed.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
				return tx.QueryRowContext(ctx, "SELECT server_id FROM installation").Scan(&server)
			}); err != nil {
				t.Fatal(err)
			}
			if err := seed.Close(); err != nil {
				t.Fatal(err)
			}
			if mode == "held" {
				marker := testMarker()
				marker.ServerID = server
				if err := markers.Put(ctx, marker); err != nil {
					t.Fatal(err)
				}
			} else if mode == "invalid-marker" {
				if err := os.WriteFile(filepath.Join(markers.path, "invalid"), []byte("synthetic"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var recovery *Service
			ordinaryStarts, recoveryStarts := 0, 0
			check := func(r runtimeowner.Resources) error {
				if recovery == nil || recovery.config.Store != r.Writer || !r.Writer.RecoveryControlled() {
					t.Error("recovery not installed on runtime writer before service start")
				}
				return nil
			}
			config := runtimeowner.Config{DatabasePath: path, InspectRecovery: func(ctx context.Context, writer *store.DB) (runtimeowner.RecoveryMode, error) {
				var err error
				recovery, err = New(ctx, Config{Store: writer, Markers: markers, Now: func() time.Time { return time.Unix(110, 0) }, FailStop: func() { t.Error("unexpected fail-stop") }})
				if err != nil {
					return 0, err
				}
				return recovery.InspectRecovery(ctx, writer)
			}, Services: []runtimeowner.Registration{
				{Service: runtimeFixtureService{start: func(r runtimeowner.Resources) error { ordinaryStarts++; return check(r) }}},
				{RecoveryOnly: true, Service: runtimeFixtureService{start: func(r runtimeowner.Resources) error { recoveryStarts++; return check(r) }}},
			}}
			running, err := runtimeowner.Start(ctx, config)
			if mode == "invalid-marker" {
				if err == nil || running != nil || ordinaryStarts != 0 || recoveryStarts != 0 {
					t.Fatalf("invalid marker admitted services: %v %d/%d", err, ordinaryStarts, recoveryStarts)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := running.Stop(ctx); err != nil {
					t.Error(err)
				}
			}()
			wantOrdinary := 1
			if mode == "held" {
				wantOrdinary = 0
			}
			if ordinaryStarts != wantOrdinary || recoveryStarts != 1 {
				t.Errorf("starts ordinary/recovery=%d/%d", ordinaryStarts, recoveryStarts)
			}
			if _, err := recovery.InspectRecovery(ctx, seed); err != store.InvalidRequest {
				t.Errorf("foreign writer accepted=%v", err)
			}
		})
	}
}
