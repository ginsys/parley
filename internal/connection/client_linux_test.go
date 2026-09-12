package connection

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/store"
)

func TestTrustedDialChecksPathAndKernelBeforeReturning(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "parley-socket-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "control.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	for _, tc := range []struct {
		name, path string
		uid        uint32
		ok         bool
	}{
		{"valid", path, uint32(os.Geteuid()), true}, {"wrong_uid", path, uint32(os.Geteuid()) + 1, false}, {"abstract", "@forbidden", uint32(os.Geteuid()), false}, {"relative", "relative.sock", uint32(os.Geteuid()), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := DialTrustedServer(context.Background(), tc.path, tc.uid)
			if tc.ok {
				if err != nil {
					t.Fatal(err)
				}
				conn.Close()
			} else if err == nil {
				conn.Close()
				t.Fatal("untrusted connection returned")
			}
		})
	}
	alias := filepath.Join(dir, "alias")
	if err := os.Symlink(path, alias); err != nil {
		t.Fatal(err)
	}
	if conn, err := DialTrustedServer(context.Background(), alias, uint32(os.Geteuid())); err == nil {
		conn.Close()
		t.Fatal("followed socket symlink")
	}
	if err := os.Chmod(dir, 0777); err != nil {
		t.Fatal(err)
	}
	if conn, err := DialTrustedServer(context.Background(), path, uint32(os.Geteuid())); err == nil {
		conn.Close()
		t.Fatal("accepted replaceable socket directory")
	}
}
func TestReconnectIsBoundedAndReinspects(t *testing.T) {
	for _, wins := range []bool{false, true} {
		t.Run(map[bool]string{false: "exhausted", true: "success"}[wins], func(t *testing.T) {
			inspections, attempts, waits := 0, 0, 0
			winner := &Session{}
			got, err := reconnect(context.Background(), func(context.Context) (Snapshot, error) {
				inspections++
				return Snapshot{Generation: int64(inspections)}, nil
			}, func(_ context.Context, generation int64) (*Session, error) {
				attempts++
				if generation != int64(inspections) {
					t.Fatal("did not re-inspect")
				}
				if wins && attempts == 3 {
					return winner, nil
				}
				return nil, store.GenerationConflict
			}, func(_ context.Context, d time.Duration) error {
				waits++
				if d < time.Second {
					t.Fatal("retry too soon")
				}
				return nil
			})
			if wins {
				if err != nil || got != winner || waits != 2 {
					t.Fatalf("winner=%p waits=%d err=%v", got, waits, err)
				}
			} else if err != store.GenerationConflict || attempts != 4 || waits != 3 {
				t.Fatalf("attempts=%d waits=%d err=%v", attempts, waits, err)
			}
		})
	}
}
