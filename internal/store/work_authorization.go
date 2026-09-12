package store

import (
	"context"
	"database/sql"
	"errors"
	"github.com/ginsys/parley/internal/bridgetext"
	"time"
)

func EnabledPeer(ctx context.Context, tx *sql.Tx, peer string, now time.Time) (BindingRecord, error) {
	if len(peer) > MaxIdentityBytes || bridgetext.ValidateMetadata(peer) != nil {
		return BindingRecord{}, InvalidRequest
	}
	var id string
	err := tx.QueryRowContext(ctx, "SELECT binding_id FROM bindings WHERE peer_id=?", peer).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return BindingRecord{}, BindingUnavailable
	}
	if err != nil {
		return BindingRecord{}, storageCode(err)
	}
	b, err := ReadBinding(ctx, tx, id)
	if err != nil {
		return b, err
	}
	c, err := LatestCredential(ctx, tx, id)
	if err != nil {
		return b, err
	}
	expired := !now.Before(time.Unix(0, c.ExpiresAtNS)) || observedExpiry(ctx, c.ID)
	if c.Status == "current" && expired {
		recordExpiry(ctx, c)
	}
	if b.Status != "enabled" || c.Status != "current" || expired {
		return b, BindingUnavailable
	}
	var retired bool
	err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM retired_namespaces WHERE principal_id=?)", b.ID).Scan(&retired)
	if err != nil {
		return b, storageCode(err)
	}
	if retired {
		return b, BindingUnavailable
	}
	return b, nil
}

// AuthorizeProvenance checks independent holds plus the immutable accepting
// author. Reviewed legacy work still needs current exact enrollment; review never
// fabricates a historical credential or makes an uncertain row retryable.
func AuthorizeProvenance(ctx context.Context, tx *sql.Tx, e *Envelope, now time.Time) error {
	held, err := WorkHeld(ctx, tx, WorkRef{Kind: "envelope", ID: e.ID})
	if err != nil {
		return err
	}
	if held {
		return SecurityHold
	}
	var tag, author string
	var binding sql.NullString
	var credential sql.NullInt64
	err = tx.QueryRowContext(ctx, "SELECT provenance,original_from_peer,binding_id,credential_version FROM work_provenance WHERE work_kind='envelope' AND work_id=?", e.ID).Scan(&tag, &author, &binding, &credential)
	if errors.Is(err, sql.ErrNoRows) {
		return SecurityHold
	}
	if err != nil {
		return storageCode(err)
	}
	if author != e.FromPeer {
		return SecurityHold
	}
	b, err := EnabledPeer(ctx, tx, e.FromPeer, now)
	if err != nil {
		return err
	}
	if tag == "authenticated" && (!binding.Valid || binding.String != b.ID || !credential.Valid) {
		return SecurityHold
	}
	return nil
}
