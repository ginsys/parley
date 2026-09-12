//go:build linux

package connection

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/ginsys/parley/internal/store"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

// PrivatePublisher writes only generated credential-ID files below an existing,
// trusted account's XDG state directory. It never creates/chmods/chowns that
// directory, follows symlinks, updates an active-file pointer or overwrites a
// credential. Cross-account setup is a separate trusted capability.
type PrivatePublisher struct {
	directory     string
	uid           uint32
	syncDirectory func(int) error
}

func NewPrivatePublisher(stateDirectory string, uid uint32) (*PrivatePublisher, error) {
	if !filepath.IsAbs(stateDirectory) || filepath.Clean(stateDirectory) != stateDirectory {
		return nil, store.InvalidRequest
	}
	p := &PrivatePublisher{directory: filepath.Join(stateDirectory, "parley", "credentials"), uid: uid, syncDirectory: unix.Fsync}
	fd, err := p.openDirectory()
	if err != nil {
		return nil, err
	}
	if err := unix.Close(fd); err != nil {
		return nil, store.TemporarilyUnavailable
	}
	return p, nil
}
func (p *PrivatePublisher) openDirectory() (int, error) {
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, store.Forbidden
	}
	parts := strings.Split(strings.TrimPrefix(p.directory, "/"), "/")
	for i, part := range parts {
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if err != nil {
			return -1, store.Forbidden
		}
		fd = next
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			unix.Close(fd)
			return -1, store.Forbidden
		}
		trusted := st.Uid == 0 || st.Uid == p.uid || st.Uid == uint32(os.Geteuid())
		stickyRoot := st.Uid == 0 && st.Mode&unix.S_ISVTX != 0
		// XDG state, parley and credentials are private; higher ancestors may be
		// readable, but cannot be replaceable by an untrusted account.
		private := i >= len(parts)-3
		if !trusted || st.Mode&unix.S_IFMT != unix.S_IFDIR || (!stickyRoot && st.Mode&0022 != 0) || (private && (st.Uid != p.uid || st.Mode&0777 != 0700)) {
			unix.Close(fd)
			return -1, store.Forbidden
		}
	}
	return fd, nil
}
func (p *PrivatePublisher) Publish(ctx context.Context, file CredentialFile) error {
	defer clear(file.secret[:])
	if !canonicalID(file.ServerID) || !canonicalID(file.BindingID) || !canonicalID(file.CredentialID) || file.CredentialVersion < 1 {
		return store.InvalidRequest
	}
	if ctx.Err() != nil {
		return store.TemporarilyUnavailable
	}
	directory, err := p.openDirectory()
	if err != nil {
		return err
	}
	defer unix.Close(directory)
	temporaryID, err := uuid.NewRandom()
	if err != nil {
		return store.TemporarilyUnavailable
	}
	temporary := ".pending-" + temporaryID.String()
	fd, err := unix.Openat(directory, temporary, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return store.TemporarilyUnavailable
	}
	staged := os.NewFile(uintptr(fd), temporary)
	defer staged.Close()
	renamed := false
	defer func() {
		if !renamed {
			unix.Unlinkat(directory, temporary, 0)
		}
	}()
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0777 != 0600 || stat.Nlink != 1 || stat.Uid != p.uid {
		return store.Forbidden
	}
	data, err := file.encode()
	if err != nil {
		return store.InvalidRequest
	}
	defer clear(data)
	if _, err := staged.Write(data); err != nil {
		return store.TemporarilyUnavailable
	}
	if err := staged.Sync(); err != nil {
		return store.TemporarilyUnavailable
	}
	if err := staged.Close(); err != nil {
		return store.TemporarilyUnavailable
	}
	if ctx.Err() != nil {
		return store.TemporarilyUnavailable
	}
	// RENAME_NOREPLACE is atomic and fails if any destination already exists.
	// Unsupported kernels/filesystems fail closed; never fall back to replacement.
	if err := unix.Renameat2(directory, temporary, directory, file.CredentialID, unix.RENAME_NOREPLACE); err != nil {
		return store.TemporarilyUnavailable
	}
	renamed = true
	// File fsync alone does not make the directory entry durable. An error here
	// leaves an ambiguous publication; keep the file and require human rotation.
	if err := p.syncDirectory(directory); err != nil {
		return store.TemporarilyUnavailable
	}
	return nil
}
