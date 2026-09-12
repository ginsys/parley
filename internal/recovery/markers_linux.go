package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ginsys/parley/internal/store"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

type Directory struct {
	path          string
	uid           uint32
	capacity      int
	syncDirectory func(int) error
}

func NewDirectory(path string, uid uint32, capacity int) (*Directory, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.IndexByte(path, 0) >= 0 || capacity <= 0 {
		return nil, store.InvalidRequest
	}
	d := &Directory{path: path, uid: uid, capacity: capacity, syncDirectory: unix.Fsync}
	fd, err := d.open()
	if err != nil {
		return nil, err
	}
	unix.Close(fd)
	return d, nil
}
func (d *Directory) open() (int, error) {
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, store.RecoveryRequired
	}
	parts := strings.Split(strings.TrimPrefix(d.path, "/"), "/")
	for i, part := range parts {
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if err != nil {
			return -1, store.RecoveryRequired
		}
		fd = next
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			unix.Close(fd)
			return -1, store.RecoveryRequired
		}
		stickyRoot := st.Uid == 0 && st.Mode&unix.S_ISVTX != 0
		if (st.Uid != 0 && st.Uid != d.uid) || (!stickyRoot && st.Mode&0022 != 0) || (i == len(parts)-1 && (st.Uid != d.uid || st.Mode&0777 != 0700)) {
			unix.Close(fd)
			return -1, store.RecoveryRequired
		}
	}
	return fd, nil
}
func (d *Directory) read(fd int, id string) (Marker, error) {
	fileFD, err := unix.Openat(fd, id, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if errors.Is(err, unix.ENOENT) {
		return Marker{}, store.NotFound
	}
	if err != nil {
		return Marker{}, store.RecoveryRequired
	}
	file := os.NewFile(uintptr(fileFD), id)
	defer file.Close()
	var st unix.Stat_t
	if err := unix.Fstat(fileFD, &st); err != nil || st.Uid != d.uid || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0777 != 0600 || st.Nlink != 1 {
		return Marker{}, store.RecoveryRequired
	}
	data, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil || len(data) > 4096 {
		return Marker{}, store.RecoveryRequired
	}
	var marker Marker
	if err := json.Unmarshal(data, &marker); err != nil || !marker.valid() || marker.IncidentID != id {
		return Marker{}, store.RecoveryRequired
	}
	canonical, err := marker.encode()
	if err != nil || !bytes.Equal(canonical, data) {
		return Marker{}, store.RecoveryRequired
	}
	return marker, nil
}
func (d *Directory) List(ctx context.Context) ([]Marker, error) {
	fd, err := d.open()
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), d.path)
	defer file.Close()
	var markers []Marker
	for {
		if ctx.Err() != nil {
			return nil, store.TemporarilyUnavailable
		}
		names, err := file.Readdirnames(100)
		if err != nil && err != io.EOF {
			return nil, store.RecoveryRequired
		}
		for _, name := range names {
			if !canonicalID(name) {
				return nil, store.RecoveryRequired
			}
			if len(markers) >= d.capacity {
				return nil, store.CapacityExceeded
			}
			marker, err := d.read(fd, name)
			if err != nil {
				return nil, err
			}
			markers = append(markers, marker)
		}
		if err == io.EOF {
			break
		}
	}
	sort.Slice(markers, func(i, j int) bool { return markers[i].IncidentID < markers[j].IncidentID })
	return markers, nil
}
func (d *Directory) Put(ctx context.Context, m Marker) error {
	data, err := m.encode()
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return store.TemporarilyUnavailable
	}
	fd, err := d.open()
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	id, err := uuid.NewRandom()
	if err != nil {
		return store.TemporarilyUnavailable
	}
	temporary := ".pending-" + id.String()
	fileFD, err := unix.Openat(fd, temporary, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return store.TemporarilyUnavailable
	}
	file := os.NewFile(uintptr(fileFD), temporary)
	defer file.Close()
	var st unix.Stat_t
	if err := unix.Fstat(fileFD, &st); err != nil || st.Uid != d.uid || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0777 != 0600 || st.Nlink != 1 {
		unix.Unlinkat(fd, temporary, 0)
		return store.RecoveryRequired
	}
	renamed := false
	defer func() {
		if !renamed {
			unix.Unlinkat(fd, temporary, 0)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return store.TemporarilyUnavailable
	}
	if err := file.Sync(); err != nil {
		return store.TemporarilyUnavailable
	}
	if err := file.Close(); err != nil {
		return store.TemporarilyUnavailable
	}
	if ctx.Err() != nil {
		return store.TemporarilyUnavailable
	}
	err = unix.Renameat2(fd, temporary, fd, m.IncidentID, unix.RENAME_NOREPLACE)
	if errors.Is(err, unix.EEXIST) {
		old, readErr := d.read(fd, m.IncidentID)
		if readErr != nil || !sameMarker(old, m) {
			return store.RecoveryRequired
		}
		if err := unix.Unlinkat(fd, temporary, 0); err != nil {
			return store.TemporarilyUnavailable
		}
		renamed = true
	} else if err != nil {
		return store.TemporarilyUnavailable
	} else {
		renamed = true
	}
	if err := d.syncDirectory(fd); err != nil {
		return store.TemporarilyUnavailable
	}
	return nil
}
func (d *Directory) Remove(ctx context.Context, m Marker) error {
	if !m.valid() {
		return store.InvalidRequest
	}
	if ctx.Err() != nil {
		return store.TemporarilyUnavailable
	}
	fd, err := d.open()
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	old, err := d.read(fd, m.IncidentID)
	if err != nil && err != store.NotFound {
		return err
	}
	if err == nil {
		if !sameMarker(old, m) {
			return store.RecoveryRequired
		}
		if err := unix.Unlinkat(fd, m.IncidentID, 0); err != nil {
			return store.TemporarilyUnavailable
		}
	}
	// ENOENT is not proof that an earlier unlink was durable.
	if err := d.syncDirectory(fd); err != nil {
		return store.TemporarilyUnavailable
	}
	return nil
}
