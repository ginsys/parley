// Package identity provides synthetic private-session fixtures. It is imported
// only by tests; no live adapter or runtime may use its permissive providers.
package identity

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/bridgetext"
	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/store"
	"github.com/google/uuid"
)

// ErrInvalidPeer distinguishes fixture setup rejection from production wire errors.
var ErrInvalidPeer = errors.New("synthetic session requires a compatible peer identifier")

type Fixture struct {
	DB      *store.DB
	Manager *connection.Manager
	t       testing.TB
	mu      sync.Mutex
	peers   map[string]*connection.Session
}

var registry = struct {
	sync.Mutex
	items map[*store.DB]*Fixture
}{items: map[*store.DB]*Fixture{}}

func For(t testing.TB, db *store.DB) *Fixture {
	t.Helper()
	registry.Lock()
	defer registry.Unlock()
	if f := registry.items[db]; f != nil {
		return f
	}
	m, err := connection.NewManager(connection.ManagerConfig{Store: db, MaxNonattached: 100, Guard: func(context.Context, *sql.Tx, string) error { return nil }, Verify: func(context.Context, connection.NativeTuple, connection.Token) error { return nil }, AfterFunc: func(time.Duration, func()) func() { return func() {} }})
	if err != nil {
		t.Fatal(err)
	}
	f := &Fixture{DB: db, Manager: m, t: t, peers: map[string]*connection.Session{}}
	registry.items[db] = f
	t.Cleanup(func() { registry.Lock(); delete(registry.items, db); registry.Unlock() })
	return f
}
func (f *Fixture) Session(peer string) (*connection.Session, error) {
	if len(peer) > store.MaxIdentityBytes || bridgetext.ValidateMetadata(peer) != nil {
		return nil, ErrInvalidPeer
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if s := f.peers[peer]; s != nil {
		return s, nil
	}
	ctx := context.Background()
	var b store.BindingRecord
	var c store.CredentialRecord
	secret := [32]byte{1, 2, 3, 4}
	// Reopened crash fixtures keep the exact deterministic synthetic verifier.
	if err := f.DB.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var id string
		err := tx.QueryRowContext(ctx, "SELECT binding_id FROM bindings WHERE peer_id=?", peer).Scan(&id)
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}
		b, err = store.ReadBinding(ctx, tx, id)
		if err != nil {
			return err
		}
		c, err = store.LatestCredential(ctx, tx, id)
		return err
	}); err != nil {
		return nil, err
	}
	if b.ID == "" {
		b = store.BindingRecord{ID: uuid.NewString(), PeerID: peer, HostKind: "codex_cli", NamespaceID: "synthetic", SessionID: uuid.NewString(), ConnectorUID: uint32(os.Geteuid()), Status: "enabled", Version: 1}
		c = store.CredentialRecord{ID: uuid.NewString(), BindingID: b.ID, Version: 1, Status: "current", ExpiresAtNS: time.Now().Add(24 * time.Hour).UnixNano(), Verifier: sha256.Sum256(secret[:])}
		if _, err := f.DB.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
			return store.TransitionResult{Changed: true}, store.InsertBindingCredential(ctx, tx, b, c)
		}, nil); err != nil {
			return nil, err
		}
	}
	auth, err := connection.NewAuthentication(c.ID, secret[:], connection.NativeTuple{Kind: b.HostKind, Namespace: b.NamespaceID, Session: b.SessionID})
	if err != nil {
		return nil, err
	}
	path := filepath.Join(f.t.TempDir(), "sock")
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	defer l.Close()
	client, err := net.DialUnix("unix", nil, l.Addr().(*net.UnixAddr))
	if err != nil {
		return nil, err
	}
	f.t.Cleanup(func() { client.Close() })
	server, err := l.AcceptUnix()
	if err != nil {
		return nil, err
	}
	socket, err := f.Manager.Accept(ctx, server)
	if err != nil {
		server.Close()
		return nil, err
	}
	f.t.Cleanup(func() { socket.Close() })
	s, err := f.Manager.Attach(ctx, socket, auth, b.Generation)
	if err != nil {
		return nil, err
	}
	probe, err := f.Manager.BeginReadiness(ctx, s)
	if err != nil {
		return nil, err
	}
	if err := f.Manager.Acknowledge(ctx, s, probe); err != nil {
		return nil, err
	}
	f.peers[peer] = s
	return s, nil
}

// GrantPeers explicitly enrolls the synthetic existing grant peers for dispatch.
func (f *Fixture) GrantPeers() error {
	var peers []string
	if err := f.DB.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, "SELECT peer_a_id FROM grants UNION SELECT peer_b_id FROM grants")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var peer string
			if err := rows.Scan(&peer); err != nil {
				return err
			}
			peers = append(peers, peer)
		}
		return rows.Err()
	}); err != nil {
		return err
	}
	for _, peer := range peers {
		if bridgetext.ValidateMetadata(peer) != nil {
			continue
		} // preserved incompatible history is never enrolled
		if _, err := f.Session(peer); err != nil {
			return err
		}
	}
	return nil
}

// Record is called explicitly by hand-built envelope fixtures inside their Tx.
// It never repairs provenance at claim time.
func Record(ctx context.Context, tx *sql.Tx, e store.Envelope) error {
	var binding string
	var version int64
	if err := tx.QueryRowContext(ctx, "SELECT b.binding_id,c.credential_version FROM bindings b JOIN credentials c USING(binding_id) WHERE b.peer_id=? ORDER BY c.credential_version DESC LIMIT 1", e.FromPeer).Scan(&binding, &version); err != nil {
		return err
	}
	return store.RecordAuthenticatedEnvelope(ctx, tx, e.ID, binding, version)
}
