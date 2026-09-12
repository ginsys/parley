package store

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"errors"
)

// Inspect serializes a materialized read with command publication. It always
// rolls back, so even an accidentally mutating read callback cannot commit.
func (c *Coordinator) Inspect(ctx context.Context, read func(context.Context, *sql.Tx) error) error {
	if read == nil {
		return InvalidRequest
	}
	if err := c.lock(ctx); err != nil {
		return storageCode(err)
	}
	defer func() { <-c.gate }()
	if c.failed {
		return RecoveryRequired
	}
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return storageCode(err)
	}
	defer tx.Rollback()
	if err := read(ctx, tx); err != nil {
		return storageCode(err)
	}
	return nil
}
func ReadCredential(ctx context.Context, tx *sql.Tx, id string) (CredentialRecord, error) {
	return scanCredential(tx.QueryRowContext(ctx, `SELECT binding_id,credential_id,credential_version,verifier,expires_at_ns,status FROM credentials WHERE credential_id=?`, id))
}
func LatestCredential(ctx context.Context, tx *sql.Tx, binding string) (CredentialRecord, error) {
	return scanCredential(tx.QueryRowContext(ctx, `SELECT binding_id,credential_id,credential_version,verifier,expires_at_ns,status FROM credentials WHERE binding_id=? ORDER BY credential_version DESC LIMIT 1`, binding))
}
func scanCredential(row *sql.Row) (CredentialRecord, error) {
	var c CredentialRecord
	var hash []byte
	err := row.Scan(&c.BindingID, &c.ID, &c.Version, &hash, &c.ExpiresAtNS, &c.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return c, BindingUnavailable
	}
	if err != nil || len(hash) != 32 {
		return c, TemporarilyUnavailable
	}
	copy(c.Verifier[:], hash)
	return c, nil
}

// Matches compares fixed-length hashes; credentials are uniformly random machine
// secrets, not human passwords. UID, expiry and lifecycle checks remain required.
func (c CredentialRecord) Matches(secret [32]byte) bool {
	digest := sha256.Sum256(secret[:])
	return subtle.ConstantTimeCompare(c.Verifier[:], digest[:]) == 1
}
func RotateCredential(ctx context.Context, tx *sql.Tx, binding string, bindingVersion, credentialVersion int64, next CredentialRecord) error {
	b, err := ReadBinding(ctx, tx, binding)
	if err != nil {
		return err
	}
	old, err := LatestCredential(ctx, tx, binding)
	if err != nil {
		return err
	}
	if b.Status != "enabled" {
		return BindingUnavailable
	}
	if b.Version != bindingVersion || old.Version != credentialVersion {
		return VersionConflict
	}
	bv, err := NextVersion(b.Version)
	if err != nil {
		return err
	}
	cv, err := NextVersion(old.Version)
	if err != nil {
		return err
	}
	if !validUUID(next.ID) || next.BindingID != binding || next.Version != cv || next.Status != "current" {
		return InvalidRequest
	}
	if _, err := tx.ExecContext(ctx, `UPDATE credentials SET status='superseded' WHERE binding_id=? AND status='current'`, binding); err != nil {
		return storageCode(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO credentials(binding_id,credential_version,credential_id,verifier,expires_at_ns,status) VALUES(?,?,?,?,?,'current')`, binding, cv, next.ID, next.Verifier[:], next.ExpiresAtNS); err != nil {
		return storageCode(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO credential_publications(credential_id) VALUES(?)`, next.ID); err != nil {
		return storageCode(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE bindings SET binding_version=? WHERE binding_id=?`, bv, binding); err != nil {
		return storageCode(err)
	}
	return nil
}

// LookupCommand returns only currently authorized retained metadata. A miss is
// not proof that another in-flight request will not commit; Execute rechecks it.
func (c *Coordinator) LookupCommand(ctx context.Context, p CommandPrincipal, r CommandRequest, authorize func(context.Context, *sql.Tx) error) (CommandReceipt, bool, error) {
	if !validUUID(p.ID) || !validUUID(r.id) || r.kind == "" || authorize == nil {
		return CommandReceipt{}, false, InvalidRequest
	}
	var receipt CommandReceipt
	found := false
	err := c.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := authorize(ctx, tx); err != nil {
			return err
		}
		var err error
		receipt, err = lookupReceipt(ctx, tx, p.ID, r)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		receipt.Replayed = true
		return nil
	})
	return receipt, found, err
}
