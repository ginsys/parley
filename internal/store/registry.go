package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"unicode/utf8"

	"github.com/ginsys/parley/internal/bridgetext"
	"github.com/google/uuid"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

//go:embed registry_schema.sql
var registrySchema string

func addConnectionRegistry(ctx context.Context, tx *sql.Tx) error {
	id, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, registrySchema); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO installation(singleton,server_id) VALUES(1,?)", id.String())
	return err
}

// BindingRecord is durable identity, not an authenticated connection principal.
type BindingRecord struct {
	ID, PeerID, HostKind, NamespaceID, SessionID string
	ConnectorUID                                 uint32
	Status                                       string
	Version, Generation                          int64
}
type CredentialRecord struct {
	BindingID, ID string
	Version       int64
	Verifier      [32]byte
	ExpiresAtNS   int64
	Status        string
}

// InsertBindingCredential is a transaction primitive for trusted registration.
// The caller validates host evidence and legacy eligibility, generates credential
// material and owns the surrounding coordinator command. It grants no membership.
func InsertBindingCredential(ctx context.Context, tx *sql.Tx, b BindingRecord, c CredentialRecord) error {
	if !validUUID(b.ID) || !validUUID(c.ID) || c.BindingID != b.ID || b.Status != "enabled" || b.Version != 1 || b.Generation != 0 || c.Version != 1 || c.Status != "current" || len(b.PeerID) > MaxIdentityBytes || bridgetext.ValidateMetadata(b.PeerID) != nil {
		return InvalidRequest
	}
	if b.HostKind != "claude_code" && b.HostKind != "codex_cli" {
		return InvalidRequest
	}
	for _, locator := range []string{b.NamespaceID, b.SessionID} {
		if len(locator) == 0 || len(locator) > MaxLocatorBytes || !utf8.ValidString(locator) {
			return InvalidRequest
		}
	}
	// Keep the pair atomic even if a caller elects to record a terminal rejection.
	if _, err := tx.ExecContext(ctx, "SAVEPOINT binding_registration"); err != nil {
		return storageCode(err)
	}
	failure := func(err error) error {
		// SQLITE_FULL can automatically roll back the whole transaction, removing
		// this savepoint. Cleanup failure must not hide the original capacity error.
		original := storageCode(err)
		if _, rollbackErr := tx.ExecContext(ctx, "ROLLBACK TO binding_registration"); rollbackErr != nil {
			if original == CapacityExceeded {
				return original
			}
			return storageCode(rollbackErr)
		}
		if _, releaseErr := tx.ExecContext(ctx, "RELEASE binding_registration"); releaseErr != nil {
			if original == CapacityExceeded {
				return original
			}
			return storageCode(releaseErr)
		}
		var se *sqlite.Error
		if errors.As(err, &se) && (se.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE || se.Code() == sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY) {
			return IdentityConflict
		}
		return storageCode(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO bindings(binding_id,peer_id,host_kind,host_namespace_id,host_session_id,connector_uid,status,binding_version,connection_generation) VALUES(?,?,?,?,?,?,?,?,?)`, b.ID, b.PeerID, b.HostKind, b.NamespaceID, b.SessionID, b.ConnectorUID, b.Status, b.Version, b.Generation); err != nil {
		return failure(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO credentials(binding_id,credential_version,credential_id,verifier,expires_at_ns,status) VALUES(?,?,?,?,?,?)`, c.BindingID, c.Version, c.ID, c.Verifier[:], c.ExpiresAtNS, c.Status); err != nil {
		return failure(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO credential_publications(credential_id) VALUES(?)", c.ID); err != nil {
		return failure(err)
	}
	_, err := tx.ExecContext(ctx, "RELEASE binding_registration")
	if err != nil {
		return storageCode(err)
	}
	return nil
}
func ReadBinding(ctx context.Context, tx *sql.Tx, id string) (BindingRecord, error) {
	var b BindingRecord
	err := tx.QueryRowContext(ctx, `SELECT binding_id,peer_id,host_kind,host_namespace_id,host_session_id,connector_uid,status,binding_version,connection_generation FROM bindings WHERE binding_id=?`, id).Scan(&b.ID, &b.PeerID, &b.HostKind, &b.NamespaceID, &b.SessionID, &b.ConnectorUID, &b.Status, &b.Version, &b.Generation)
	if errors.Is(err, sql.ErrNoRows) {
		return b, BindingUnavailable
	}
	if err != nil {
		return b, storageCode(err)
	}
	return b, nil
}
