package recovery

import (
	"context"
	"errors"
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
