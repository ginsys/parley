package recovery

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ginsys/parley/internal/store"
	"golang.org/x/sys/unix"
)

func markerDirectory(t *testing.T) *Directory {
	t.Helper()
	// The host TMPDIR may be group-writable; use a private synthetic tree
	// beneath the root-owned sticky directory, as credential publication tests do.
	root, err := os.MkdirTemp("/tmp", "parley-recovery-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	path := filepath.Join(root, "markers")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	d, err := NewDirectory(path, uint32(os.Geteuid()), 4)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func testMarker() Marker {
	return Marker{IncidentID: "60000000-0000-4000-8000-000000000001", ServerID: "70000000-0000-4000-8000-000000000001", Kind: "restore"}
}
func TestMarkerPublicationNeverReplacesIncident(t *testing.T) {
	d := markerDirectory(t)
	ctx := context.Background()
	m := testMarker()
	for attempt := 0; attempt < 2; attempt++ {
		if err := d.Put(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	changed := m
	changed.ServerID = "70000000-0000-4000-8000-000000000002"
	if err := d.Put(ctx, changed); err != store.RecoveryRequired {
		t.Fatalf("replaced marker=%v", err)
	}
	markers, err := d.List(ctx)
	if err != nil || len(markers) != 1 || !sameMarker(markers[0], m) {
		t.Fatalf("markers=%+v %v", markers, err)
	}
	if err := d.Remove(ctx, changed); err != store.RecoveryRequired {
		t.Fatalf("mismatched removal=%v", err)
	}
}
func TestAbsentMarkerRemovalStillRequiresDirectorySync(t *testing.T) {
	d := markerDirectory(t)
	ctx := context.Background()
	m := testMarker()
	if err := d.Put(ctx, m); err != nil {
		t.Fatal(err)
	}
	calls := 0
	d.syncDirectory = func(int) error { calls++; return errors.New("synthetic sync failure") }
	if err := d.Remove(ctx, m); err != store.TemporarilyUnavailable {
		t.Fatalf("unlink uncertainty=%v", err)
	}
	if err := d.Remove(ctx, m); err != store.TemporarilyUnavailable || calls != 2 {
		t.Fatalf("absent entry skipped sync: %v calls=%d", err, calls)
	}
	d.syncDirectory = unix.Fsync
	if err := d.Remove(ctx, m); err != nil {
		t.Fatal(err)
	}
}
func TestMarkerPublicationSyncFailureRetainsEvidence(t *testing.T) {
	d := markerDirectory(t)
	m := testMarker()
	d.syncDirectory = func(int) error { return errors.New("synthetic sync failure") }
	if err := d.Put(context.Background(), m); err != store.TemporarilyUnavailable {
		t.Fatalf("publication uncertainty=%v", err)
	}
	d.syncDirectory = unix.Fsync
	if err := d.Put(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	markers, err := d.List(context.Background())
	if err != nil || len(markers) != 1 {
		t.Fatalf("evidence lost=%+v %v", markers, err)
	}
}
func TestMalformedAndUnfinishedMarkersFailClosed(t *testing.T) {
	for _, kind := range []string{"symlink", "fifo", "temporary", "duplicate_json"} {
		t.Run(kind, func(t *testing.T) {
			d := markerDirectory(t)
			m := testMarker()
			path := filepath.Join(d.path, m.IncidentID)
			switch kind {
			case "symlink":
				if err := os.Symlink("missing", path); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := unix.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			case "temporary":
				path = filepath.Join(d.path, ".pending-synthetic")
				if err := os.WriteFile(path, []byte("incomplete"), 0600); err != nil {
					t.Fatal(err)
				}
			case "duplicate_json":
				data, _ := m.encode()
				data = append(data[:len(data)-1], []byte(",\"kind\":\"restore\"}")...)
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := d.List(context.Background()); err != store.RecoveryRequired {
				t.Fatalf("malformed marker=%v", err)
			}
		})
	}
}

func TestCompletePendingMarkerIsRecoveredWithoutLosingEvidence(t *testing.T) {
	for _, kind := range []string{"complete", "conflict", "truncated", "sync-failure"} {
		t.Run(kind, func(t *testing.T) {
			d := markerDirectory(t)
			m := testMarker()
			ctx := context.Background()
			if kind == "conflict" {
				other := m
				other.ServerID = "70000000-0000-4000-8000-000000000002"
				if err := d.Put(ctx, other); err != nil {
					t.Fatal(err)
				}
			}
			data, err := m.encode()
			if err != nil {
				t.Fatal(err)
			}
			if kind == "truncated" {
				data = data[:len(data)/2]
			}
			pending := filepath.Join(d.path, ".pending-90000000-0000-4000-8000-000000000001")
			file, err := os.OpenFile(pending, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.Write(data); err != nil {
				t.Fatal(err)
			}
			if err := file.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			d, err = NewDirectory(d.path, d.uid, 4)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "sync-failure" {
				d.syncDirectory = func(int) error { return errors.New("synthetic sync failure") }
			}
			markers, err := d.List(ctx)
			if kind == "conflict" || kind == "truncated" {
				if err != store.RecoveryRequired {
					t.Fatalf("unsafe pending accepted=%+v %v", markers, err)
				}
				if _, err := os.Stat(pending); err != nil {
					t.Fatalf("unsafe evidence removed=%v", err)
				}
				return
			}
			if kind == "sync-failure" {
				if !errors.Is(err, store.TemporarilyUnavailable) {
					t.Fatalf("unsynced publication=%v", err)
				}
				d.syncDirectory = unix.Fsync
				markers, err = d.List(ctx)
			}
			if err != nil || len(markers) != 1 || !sameMarker(markers[0], m) {
				t.Fatalf("pending not recovered=%+v %v", markers, err)
			}
			if _, err := os.Stat(pending); !os.IsNotExist(err) {
				t.Fatalf("pending retained after promotion=%v", err)
			}
			again, err := d.List(ctx)
			if err != nil || len(again) != 1 || !sameMarker(again[0], m) {
				t.Fatalf("repeat listing=%+v %v", again, err)
			}
		})
	}
}

func TestPendingPromotionFailureInvokesSupervisorFailStop(t *testing.T) {
	s, _, _ := recoveryFixture(t)
	d := s.config.Markers.(*Directory)
	marker := testMarker()
	marker.ServerID = s.serverID
	data, err := marker.encode()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(d.path, ".pending-90000000-0000-4000-8000-000000000002")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	stopped := 0
	s.config.FailStop = func() { stopped++ }
	d.syncDirectory = func(int) error { return errors.New("synthetic promotion sync failure") }
	if _, err := s.InspectRecovery(context.Background(), s.config.Store); err != store.RecoveryRequired {
		t.Fatalf("failed publication=%v", err)
	}
	if stopped == 0 {
		t.Fatal("promotion publication failure did not fail-stop")
	}
}

func TestSameMarkerPartialTimes(t *testing.T) {
	for _, field := range []string{"floor", "observed"} {
		t.Run(field, func(t *testing.T) {
			a, b := testMarker(), testMarker()
			x, y := int64(1), int64(1)
			if field == "floor" {
				a.Floor, b.Floor = &x, &y
			} else {
				a.Observed, b.Observed = &x, &y
			}
			if !sameMarker(a, b) {
				t.Fatal("equal partial times differ")
			}
			y = 2
			if sameMarker(a, b) {
				t.Fatal("unequal partial times compare equal")
			}
		})
	}
}

func TestListEnumeratesBeforePromoting(t *testing.T) {
	d := markerDirectory(t)
	a, b := testMarker(), testMarker()
	b.IncidentID = "60000000-0000-4000-8000-000000000002"
	pending := ".pending-80000000-0000-4000-8000-000000000001"
	data, err := a.encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d.path, pending), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := d.Put(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	calls := 0
	d.readNames = func(*os.File, int) ([]string, error) {
		calls++
		if calls == 1 {
			return []string{pending}, nil
		}
		if calls == 2 {
			// A directory mutation during enumeration may make readdir skip B.
			if _, err := os.Stat(filepath.Join(d.path, pending)); errors.Is(err, os.ErrNotExist) {
				return nil, io.EOF
			}
			return []string{b.IncidentID}, nil
		}
		return nil, io.EOF
	}
	markers, err := d.List(context.Background())
	if err != nil || len(markers) != 2 {
		t.Fatalf("lost marker during promotion: %+v %v", markers, err)
	}
}
