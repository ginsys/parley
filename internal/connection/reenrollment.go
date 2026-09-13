package connection

import (
	"context"
	"database/sql"
	"time"

	"github.com/ginsys/parley/internal/store"
)

type ReenrollRequest struct {
	RotateRequest
	HostEvidenceRef string
}

func (p *Provisioner) Reenroll(ctx context.Context, actor store.CommandPrincipal, r ReenrollRequest) (ProvisioningResult, error) {
	if !canonicalID(r.BindingID) || !canonicalID(r.TargetRef) || !canonicalID(r.HostEvidenceRef) || r.ExpectedBindingVersion < 1 || r.ExpectedCredentialVersion < 1 {
		return ProvisioningResult{}, store.InvalidRequest
	}
	expiry, err := store.InstantNanos(r.ExpiresAt)
	if err != nil {
		return ProvisioningResult{}, err
	}
	request, err := store.NewCommandRequest("binding.reenroll", r.OperationID, store.Field{Name: "binding_id", Value: r.BindingID}, store.Field{Name: "expected_binding_version", Value: r.ExpectedBindingVersion}, store.Field{Name: "expected_credential_version", Value: r.ExpectedCredentialVersion}, store.Field{Name: "expires_at", Value: r.ExpiresAt.UTC().Format(time.RFC3339Nano)}, store.Field{Name: "provisioning_target_ref", Value: r.TargetRef}, store.Field{Name: "host_evidence_ref", Value: r.HostEvidenceRef})
	if err != nil {
		return ProvisioningResult{}, err
	}
	previous, found, err := p.config.Store.Coordinator().LookupCommand(ctx, actor, request, p.authorization(actor))
	if err != nil {
		return ProvisioningResult{}, err
	}
	if found {
		return p.finish(ctx, actor, previous, CredentialFile{}, nil)
	}
	var binding store.BindingRecord
	err = p.config.Store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := p.config.Authorize(ctx, tx, actor); err != nil {
			return err
		}
		var err error
		binding, err = store.ReadBinding(ctx, tx, r.BindingID)
		return err
	})
	if err != nil {
		return ProvisioningResult{}, err
	}
	evidenceErr := error(store.HostUnverified)
	if p.config.ReenrollEvidence != nil {
		evidenceErr = p.config.ReenrollEvidence(ctx, r.HostEvidenceRef, NativeTuple{binding.HostKind, binding.NamespaceID, binding.SessionID})
	}
	var publisher Publisher
	var targetErr error
	if evidenceErr == nil {
		publisher, targetErr = p.config.Target(r.TargetRef, binding.ConnectorUID)
	}
	var file CredentialFile
	defer clear(file.secret[:])
	changed := false
	receipt, err := p.config.Store.Coordinator().Execute(ctx, actor, request, p.authorization(actor), func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
		if err := p.config.Guard(ctx, tx, "binding.reenroll"); err != nil {
			return domainRejection(err)
		}
		if evidenceErr != nil {
			return rejection(store.HostUnverified)
		}
		if targetErr != nil || publisher == nil {
			return rejection(store.Forbidden)
		}
		if !store.AuthorityTime(ctx, p.config.Now).Before(r.ExpiresAt) {
			return rejection(store.InvalidRequest)
		}
		cv, err := store.NextVersion(r.ExpectedCredentialVersion)
		if err != nil {
			return domainRejection(err)
		}
		bv, err := store.NextVersion(r.ExpectedBindingVersion)
		if err != nil {
			return domainRejection(err)
		}
		if err := newCredentialFile(ctx, tx, &file, r.BindingID, cv); err != nil {
			return store.CommandResult{}, err
		}
		if err := store.ReenrollCredential(ctx, tx, r.BindingID, r.ExpectedBindingVersion, r.ExpectedCredentialVersion, credentialRecord(file, expiry)); err != nil {
			return domainRejection(err)
		}
		changed = true
		return credentialResult(file, r.ExpectedBindingVersion, bv), nil
	}, func(store.CommitView) {
		if changed {
			p.config.Invalidate(r.BindingID)
		}
	})
	if err != nil {
		return ProvisioningResult{}, err
	}
	return p.finish(ctx, actor, receipt, file, publisher)
}
