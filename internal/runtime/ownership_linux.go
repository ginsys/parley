//go:build linux

package runtime

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

var ErrAlreadyRunning = errors.New("already_running")

// Ownership is a cooperative lease. The directory must remain controlled by
// trusted accounts; flock cannot stop processes that bypass this API.
// Its private descriptor must never be passed via exec.Cmd.ExtraFiles.
type Ownership struct {
	path string
	file *os.File
	once sync.Once
	err  error
}

func (o *Ownership) Path() string { return o.path }
func (o *Ownership) Close() error {
	o.once.Do(func() { o.err = o.file.Close() })
	return o.err
}

// Acquire validates an existing local database and takes its persistent adjacent
// lock without waiting. It never opens SQLite, creates a database or removes a lock.
func Acquire(path string) (*Ownership, error) {
	if path == "" || path == ":memory:" || strings.HasPrefix(path, "file:") || strings.Contains(path, "://") {
		return nil, fmt.Errorf("ordinary database path required")
	}
	absolute := path
	if !filepath.IsAbs(path) {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		absolute = cwd + string(os.PathSeparator) + path
	}
	// Do not lexically clean symlink/.. before filesystem resolution.
	canonical, err := resolveTrusted(absolute)
	if err != nil {
		return nil, fmt.Errorf("resolve database: %w", err)
	}
	if err := trustedParents(filepath.Dir(canonical)); err != nil {
		return nil, err
	}
	db, err := openPrivate(canonical, false)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	var fs unix.Statfs_t
	if err := unix.Fstatfs(int(db.Fd()), &fs); err != nil {
		return nil, err
	}
	// Explicitly supported local Linux filesystems. Unknown/FUSE/network types
	// fail closed. tmpfs and overlay permit disposable development/CI fixtures.
	switch fs.Type {
	case unix.EXT4_SUPER_MAGIC, unix.XFS_SUPER_MAGIC, unix.BTRFS_SUPER_MAGIC, unix.TMPFS_MAGIC, unix.OVERLAYFS_SUPER_MAGIC:
	default:
		return nil, fmt.Errorf("unsupported database filesystem")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		f, err := openPrivate(canonical+suffix, false)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	}
	lock, err := openPrivate(canonical+".lock", true)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrAlreadyRunning
		}
		return nil, fmt.Errorf("lock database: %w", err)
	}
	// Detect replacement during validation/acquisition; trusted owners must not
	// relocate or replace files during service lifetime.
	for _, f := range []*os.File{db, lock} {
		held, e1 := f.Stat()
		named, e2 := os.Lstat(f.Name())
		if e1 != nil || e2 != nil || !os.SameFile(held, named) {
			lock.Close()
			return nil, fmt.Errorf("ownership path changed")
		}
	}
	return &Ownership{path: canonical, file: lock}, nil
}

func openPrivate(path string, create bool) (*os.File, error) {
	flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
	if create {
		flags = unix.O_RDWR | unix.O_CREAT | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
	}
	fd, err := unix.Open(path, flags, 0600)
	if err != nil {
		return nil, fmt.Errorf("open ownership file: %w", err)
	}
	f := os.NewFile(uintptr(fd), path)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		f.Close()
		return nil, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1 || st.Uid != uint32(os.Geteuid()) || st.Mode&0077 != 0 {
		f.Close()
		return nil, fmt.Errorf("database/lock/sidecar must be private, singly linked regular files owned by the server")
	}
	return f, nil
}

func trustedParents(path string) error {
	immediate := true
	for {
		var st unix.Stat_t
		if err := unix.Lstat(path, &st); err != nil {
			return err
		}
		trusted := st.Uid == 0 || st.Uid == uint32(os.Geteuid())
		// Root-owned sticky /tmp cannot have another user's private child replaced.
		stickyRoot := st.Uid == 0 && st.Mode&unix.S_ISVTX != 0
		if st.Mode&unix.S_IFMT != unix.S_IFDIR || !trusted || (st.Mode&0022 != 0 && (immediate || !stickyRoot)) {
			return fmt.Errorf("untrusted database parent %s (uid=%d mode=%o)", path, st.Uid, st.Mode)
		}
		// The immediate parent itself must be private against replacement of DB,
		// lock and sidecars; a sticky directory alone is insufficient here.
		if path == filepath.Dir(path) {
			return nil
		}
		path = filepath.Dir(path)
		immediate = false
	}
}

// Resolve one component at a time so every symlink in a target chain is checked,
// including aliases that disappear from the final canonical path. Cleaning only
// the final target would allow an agent-writable alias to retarget configuration.
func resolveTrusted(absolute string) (string, error) {
	pending := strings.Split(absolute, string(os.PathSeparator))
	resolved := string(os.PathSeparator)
	links := 0
	for len(pending) > 0 {
		part := pending[0]
		pending = pending[1:]
		switch part {
		case "", ".":
			continue
		case "..":
			resolved = filepath.Dir(resolved)
			continue
		}
		candidate := filepath.Join(resolved, part)
		var st unix.Stat_t
		if err := unix.Lstat(candidate, &st); err != nil {
			return "", err
		}
		if st.Mode&unix.S_IFMT != unix.S_IFLNK {
			if len(pending) > 0 && st.Mode&unix.S_IFMT != unix.S_IFDIR {
				return "", fmt.Errorf("non-directory path component")
			}
			resolved = candidate
			continue
		}
		links++
		if links > 40 {
			return "", fmt.Errorf("too many database symlinks")
		}
		if st.Uid != 0 && st.Uid != uint32(os.Geteuid()) {
			return "", fmt.Errorf("untrusted database symlink")
		}
		if err := trustedParents(resolved); err != nil {
			return "", err
		}
		target, err := os.Readlink(candidate)
		if err != nil {
			return "", err
		}
		if filepath.IsAbs(target) {
			resolved = string(os.PathSeparator)
		}
		pending = append(strings.Split(target, string(os.PathSeparator)), pending...)
	}
	return resolved, nil
}
