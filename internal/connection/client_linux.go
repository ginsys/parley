package connection

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/ginsys/parley/internal/store"
	"golang.org/x/sys/unix"
)

// DialTrustedServer returns a connection only after pathname protection and the
// connected peer's kernel UID have been checked. No secret is sent here. Linux
// descriptor-relative dialing prevents ancestor renames from retargeting connect.
func DialTrustedServer(ctx context.Context, path string, serverUID uint32) (*net.UnixConn, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) {
		return nil, store.AuthenticationFailed
	}
	parent, err := protectedSocketDirectory(filepath.Dir(path), serverUID)
	if err != nil {
		return nil, err
	}
	defer unix.Close(parent)
	var st unix.Stat_t
	if err := unix.Fstatat(parent, filepath.Base(path), &st, unix.AT_SYMLINK_NOFOLLOW); err != nil || st.Mode&unix.S_IFMT != unix.S_IFSOCK || st.Uid != serverUID {
		return nil, store.AuthenticationFailed
	}
	ctx, cancel := context.WithTimeout(ctx, store.AuthenticationDeadline)
	defer cancel()
	dialer := net.Dialer{}
	raw, err := dialer.DialContext(ctx, "unix", fmt.Sprintf("/proc/self/fd/%d/%s", parent, filepath.Base(path)))
	if err != nil {
		return nil, store.AuthenticationFailed
	}
	conn, ok := raw.(*net.UnixConn)
	if !ok {
		raw.Close()
		return nil, store.AuthenticationFailed
	}
	uid, err := peerUID(conn)
	if err != nil || uid != serverUID {
		conn.Close()
		return nil, store.AuthenticationFailed
	}
	return conn, nil
}
func protectedSocketDirectory(path string, uid uint32) (int, error) {
	if path == "/" {
		return -1, store.AuthenticationFailed
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, store.AuthenticationFailed
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, part := range parts {
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if err != nil {
			return -1, store.AuthenticationFailed
		}
		fd = next
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			unix.Close(fd)
			return -1, store.AuthenticationFailed
		}
		stickyAncestor := i < len(parts)-1 && st.Uid == 0 && st.Mode&unix.S_ISVTX != 0
		if (st.Uid != 0 && st.Uid != uid) || (!stickyAncestor && st.Mode&0022 != 0) {
			unix.Close(fd)
			return -1, store.AuthenticationFailed
		}
	}
	return fd, nil
}

// Reconnect performs one attempt plus at most three spaced retries. It cannot
// evict another socket; each retry observes the newly committed generation.
func (m *Manager) Reconnect(ctx context.Context, s *Socket, a Authentication) (*Session, error) {
	return reconnect(ctx, func(ctx context.Context) (Snapshot, error) { return m.Inspect(ctx, s, a) }, func(ctx context.Context, generation int64) (*Session, error) { return m.Attach(ctx, s, a, generation) }, func(ctx context.Context, d time.Duration) error {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return store.TemporarilyUnavailable
		case <-s.Context().Done():
			return store.AuthenticationFailed
		case <-timer.C:
			return nil
		}
	})
}
func reconnect(ctx context.Context, inspect func(context.Context) (Snapshot, error), attach func(context.Context, int64) (*Session, error), wait func(context.Context, time.Duration) error) (*Session, error) {
	var last error
	for attempt := 0; attempt <= store.ReconnectRetries; attempt++ {
		if ctx.Err() != nil {
			return nil, store.TemporarilyUnavailable
		}
		if attempt > 0 {
			if err := wait(ctx, store.ReconnectInterval); err != nil {
				return nil, err
			}
		}
		snapshot, err := inspect(ctx)
		if err != nil {
			return nil, err
		}
		if snapshot.Active {
			last = store.AlreadyConnected
			continue
		}
		session, err := attach(ctx, snapshot.Generation)
		if err == nil {
			return session, nil
		}
		if err != store.AlreadyConnected && err != store.GenerationConflict {
			return nil, err
		}
		last = err
	}
	return nil, last
}
