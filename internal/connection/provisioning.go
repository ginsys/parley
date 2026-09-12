// Package connection implements internal trusted connection services. It exposes
// no human transport and does not turn request identity fields into authority.
package connection

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"github.com/ginsys/parley/internal/bridgetext"
	"time"
	"unicode/utf8"

	"github.com/ginsys/parley/internal/store"
	"github.com/google/uuid"
)

type NativeTuple struct{ Kind, Namespace, Session string }
type RegisterRequest struct {
	OperationID, PeerID string
	Native              NativeTuple
	ConnectorUID        uint32
	ExpiresAt           time.Time
	TargetRef           string
}
type RotateRequest struct {
	OperationID, BindingID                            string
	ExpectedBindingVersion, ExpectedCredentialVersion int64
	ExpiresAt                                         time.Time
	TargetRef                                         string
}
type CredentialFile struct {
	ServerID          string `json:"server_id"`
	BindingID         string `json:"binding_id"`
	CredentialID      string `json:"credential_id"`
	CredentialVersion int64  `json:"credential_version,string"`
	secret            [32]byte
}

func (CredentialFile) String() string   { return "Parley credential (redacted)" }
func (CredentialFile) GoString() string { return "Parley credential (redacted)" }
func (f CredentialFile) encode() ([]byte, error) {
	return json.Marshal(struct {
		ServerID          string `json:"server_id"`
		BindingID         string `json:"binding_id"`
		CredentialID      string `json:"credential_id"`
		CredentialVersion int64  `json:"credential_version,string"`
		Secret            string `json:"secret"`
	}{f.ServerID, f.BindingID, f.CredentialID, f.CredentialVersion, base64.StdEncoding.EncodeToString(f.secret[:])})
}

type Publisher interface {
	Publish(context.Context, CredentialFile) error
}
type PublisherFunc func(context.Context, CredentialFile) error

func (f PublisherFunc) Publish(ctx context.Context, c CredentialFile) error { return f(ctx, c) }

// All capabilities are supplied by trusted runtime configuration. Missing host,
// authority, legacy or recovery providers fail closed, including in development.
// Target resolves a configured UUID reference, never a request-supplied path.
type ProvisioningConfig struct {
	Store             *store.DB
	Now               func() time.Time
	Authorize         func(context.Context, *sql.Tx, store.CommandPrincipal) error
	Guard             func(context.Context, *sql.Tx, string) error
	Verify            func(context.Context, NativeTuple) error
	LegacyEligibility func(context.Context, *sql.Tx, string) error
	Target            func(string, uint32) (Publisher, error)
	Invalidate        func(string)
}
type Provisioner struct{ config ProvisioningConfig }

func NewProvisioner(config ProvisioningConfig) (*Provisioner, error) {
	if config.Store == nil || config.Authorize == nil || config.Guard == nil || config.Verify == nil || config.LegacyEligibility == nil || config.Target == nil || config.Invalidate == nil {
		return nil, store.InvalidRequest
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Provisioner{config}, nil
}

type ProvisioningResult struct {
	Receipt                           store.CommandReceipt
	BindingID, CredentialID           string
	BindingVersion, CredentialVersion int64
	Publication                       string
}

func canonicalID(id string) bool {
	parsed, err := uuid.Parse(id)
	return err == nil && parsed != uuid.Nil && parsed.String() == id
}
func (p *Provisioner) authorization(actor store.CommandPrincipal) func(context.Context, *sql.Tx) error {
	return func(ctx context.Context, tx *sql.Tx) error { return p.config.Authorize(ctx, tx, actor) }
}

func (p *Provisioner) Register(ctx context.Context, actor store.CommandPrincipal, r RegisterRequest) (ProvisioningResult, error) {
	if !canonicalID(r.TargetRef) || len(r.PeerID) > store.MaxIdentityBytes || bridgetext.ValidateMetadata(r.PeerID) != nil {
		return ProvisioningResult{}, store.InvalidRequest
	}
	if r.Native.Kind != "claude_code" && r.Native.Kind != "codex_cli" {
		return ProvisioningResult{}, store.InvalidRequest
	}
	for _, locator := range []string{r.Native.Namespace, r.Native.Session} {
		if len(locator) == 0 || len(locator) > store.MaxLocatorBytes || !utf8.ValidString(locator) {
			return ProvisioningResult{}, store.InvalidRequest
		}
	}
	expiry, err := store.InstantNanos(r.ExpiresAt)
	if err != nil {
		return ProvisioningResult{}, err
	}
	request, err := store.NewCommandRequest("binding.register", r.OperationID,
		store.Field{Name: "peer_id", Value: r.PeerID}, store.Field{Name: "host_kind", Value: r.Native.Kind},
		store.Field{Name: "host_namespace_id", Value: r.Native.Namespace}, store.Field{Name: "host_session_id", Value: r.Native.Session},
		store.Field{Name: "connector_uid", Value: r.ConnectorUID}, store.Field{Name: "expires_at", Value: r.ExpiresAt.UTC().Format(time.RFC3339Nano)}, store.Field{Name: "provisioning_target_ref", Value: r.TargetRef})
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
	// Slow evidence/provisioning lookup is outside the gate. Its result is used
	// only for a new mutation; an authorized replay precedes these preconditions.
	evidenceErr := p.config.Verify(ctx, r.Native)
	publisher, targetErr := p.config.Target(r.TargetRef, r.ConnectorUID)
	var file CredentialFile
	defer clear(file.secret[:])
	receipt, err := p.config.Store.Coordinator().Execute(ctx, actor, request, p.authorization(actor), func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
		if err := p.config.Guard(ctx, tx, "binding.register"); err != nil {
			return domainRejection(err)
		}
		if evidenceErr != nil {
			return rejection(store.HostUnverified)
		}
		if targetErr != nil || publisher == nil {
			return rejection(store.Forbidden)
		}
		if !p.config.Now().Before(r.ExpiresAt) {
			return rejection(store.InvalidRequest)
		}
		if err := p.config.LegacyEligibility(ctx, tx, r.PeerID); err != nil {
			return domainRejection(err)
		}
		if err := newCredentialFile(ctx, tx, &file, "", 1); err != nil {
			return store.CommandResult{}, err
		}
		b := store.BindingRecord{ID: file.BindingID, PeerID: r.PeerID, HostKind: r.Native.Kind, NamespaceID: r.Native.Namespace, SessionID: r.Native.Session, ConnectorUID: r.ConnectorUID, Status: "enabled", Version: 1}
		c := credentialRecord(file, expiry)
		if err := store.InsertBindingCredential(ctx, tx, b, c); err != nil {
			if code, ok := err.(store.Code); ok && (code == store.IdentityConflict || code == store.InvalidRequest) {
				return rejection(code)
			}
			return store.CommandResult{}, err
		}
		return credentialResult(file, 0, 1), nil
	}, nil)
	if err != nil {
		return ProvisioningResult{}, err
	}
	return p.finish(ctx, actor, receipt, file, publisher)
}
func (p *Provisioner) Rotate(ctx context.Context, actor store.CommandPrincipal, r RotateRequest) (ProvisioningResult, error) {
	if !canonicalID(r.TargetRef) || !canonicalID(r.BindingID) || r.ExpectedBindingVersion < 1 || r.ExpectedCredentialVersion < 1 {
		return ProvisioningResult{}, store.InvalidRequest
	}
	expiry, err := store.InstantNanos(r.ExpiresAt)
	if err != nil {
		return ProvisioningResult{}, err
	}
	request, err := store.NewCommandRequest("binding.rotate", r.OperationID, store.Field{Name: "binding_id", Value: r.BindingID}, store.Field{Name: "expected_binding_version", Value: r.ExpectedBindingVersion}, store.Field{Name: "expected_credential_version", Value: r.ExpectedCredentialVersion}, store.Field{Name: "expires_at", Value: r.ExpiresAt.UTC().Format(time.RFC3339Nano)}, store.Field{Name: "provisioning_target_ref", Value: r.TargetRef})
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
	// Resolve the enrolled UID under a serialized, currently authorized read. Do
	// not let an arbitrary request choose ownership for the publication target.
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
	publisher, targetErr := p.config.Target(r.TargetRef, binding.ConnectorUID)
	var file CredentialFile
	rotated := false
	defer clear(file.secret[:])
	receipt, err := p.config.Store.Coordinator().Execute(ctx, actor, request, p.authorization(actor), func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
		if err := p.config.Guard(ctx, tx, "binding.rotate"); err != nil {
			return domainRejection(err)
		}
		if targetErr != nil || publisher == nil {
			return rejection(store.Forbidden)
		}
		if !p.config.Now().Before(r.ExpiresAt) {
			return rejection(store.InvalidRequest)
		}
		cv, err := store.NextVersion(r.ExpectedCredentialVersion)
		if err != nil {
			return rejection(store.InvalidRequest)
		}
		bv, err := store.NextVersion(r.ExpectedBindingVersion)
		if err != nil {
			return rejection(store.InvalidRequest)
		}
		if err := newCredentialFile(ctx, tx, &file, r.BindingID, cv); err != nil {
			return store.CommandResult{}, err
		}
		if err := store.RotateCredential(ctx, tx, r.BindingID, r.ExpectedBindingVersion, r.ExpectedCredentialVersion, credentialRecord(file, expiry)); err != nil {
			if code, ok := err.(store.Code); ok && (code == store.VersionConflict || code == store.BindingUnavailable || code == store.InvalidRequest) {
				return rejection(code)
			}
			return store.CommandResult{}, err
		}
		rotated = true
		return credentialResult(file, r.ExpectedBindingVersion, bv), nil
	}, func(store.CommitView) {
		if rotated {
			p.config.Invalidate(r.BindingID)
		}
	})
	if err != nil {
		return ProvisioningResult{}, err
	}
	return p.finish(ctx, actor, receipt, file, publisher)
}
func rejection(code store.Code) (store.CommandResult, error) {
	return store.CommandResult{Code: code}, nil
}
func newCredentialFile(ctx context.Context, tx *sql.Tx, file *CredentialFile, binding string, version int64) error {
	id, err := uuid.NewRandom()
	if err != nil {
		return store.TemporarilyUnavailable
	}
	file.CredentialID = id.String()
	file.BindingID = binding
	file.CredentialVersion = version
	if binding == "" {
		id, err = uuid.NewRandom()
		if err != nil {
			return store.TemporarilyUnavailable
		}
		file.BindingID = id.String()
	}
	if _, err := rand.Read(file.secret[:]); err != nil {
		return store.TemporarilyUnavailable
	}
	return tx.QueryRowContext(ctx, "SELECT server_id FROM installation WHERE singleton=1").Scan(&file.ServerID)
}
func credentialRecord(file CredentialFile, expiry int64) store.CredentialRecord {
	return store.CredentialRecord{ID: file.CredentialID, BindingID: file.BindingID, Version: file.CredentialVersion, Verifier: sha256.Sum256(file.secret[:]), ExpiresAtNS: expiry, Status: "current"}
}
func credentialResult(file CredentialFile, before, after int64) store.CommandResult {
	return store.CommandResult{Resources: []store.ResourceChange{{Kind: "binding", ID: file.BindingID, Before: before, After: after}, {Kind: "credential", ID: file.CredentialID, After: file.CredentialVersion}}}
}
func (p *Provisioner) finish(ctx context.Context, actor store.CommandPrincipal, receipt store.CommandReceipt, file CredentialFile, publisher Publisher) (ProvisioningResult, error) {
	defer clear(file.secret[:])
	result := ProvisioningResult{Receipt: receipt}
	if receipt.Result.Code != "" {
		return result, nil
	}
	for _, resource := range receipt.Result.Resources {
		switch resource.Kind {
		case "binding":
			result.BindingID = resource.ID
			result.BindingVersion = resource.After
		case "credential":
			result.CredentialID = resource.ID
			result.CredentialVersion = resource.After
		}
	}
	if !receipt.Replayed {
		result.Publication = "unknown"
		if ctx.Err() == nil && publisher.Publish(ctx, file) == nil {
			result.Publication = "published"
		}
		// Like delivery settlement, publication evidence outlives request cancellation.
		evidenceCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		operation, err := store.NewCommandRequest("credential.publication", file.CredentialID, store.Field{Name: "credential_id", Value: file.CredentialID}, store.Field{Name: "status", Value: result.Publication})
		if err != nil {
			return result, store.OutcomeUnknown
		}
		_, err = p.config.Store.Coordinator().Execute(evidenceCtx, actor, operation, func(ctx context.Context, tx *sql.Tx) error { return p.config.Authorize(ctx, tx, actor) }, func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
			ns, err := store.InstantNanos(p.config.Now())
			if err != nil {
				return store.CommandResult{}, err
			}
			updated, err := tx.ExecContext(ctx, "UPDATE credential_publications SET status=?,observed_at_ns=? WHERE credential_id=? AND status='pending'", result.Publication, ns, file.CredentialID)
			if err != nil {
				return store.CommandResult{}, err
			}
			count, err := updated.RowsAffected()
			if err != nil {
				return store.CommandResult{}, err
			}
			if count != 1 {
				return store.CommandResult{}, store.VersionConflict
			}
			return store.CommandResult{Resources: []store.ResourceChange{{Kind: "credential", ID: file.CredentialID, After: file.CredentialVersion}}}, nil
		}, nil)
		if err != nil {
			result.Publication = "unknown"
			return result, store.OutcomeUnknown
		}
		return result, nil
	}
	err := p.config.Store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := p.config.Authorize(ctx, tx, actor); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, "SELECT status FROM credential_publications WHERE credential_id=?", result.CredentialID).Scan(&result.Publication)
	})
	return result, err
}

func domainRejection(err error) (store.CommandResult, error) {
	if code, ok := err.(store.Code); ok {
		return rejection(code)
	}
	return store.CommandResult{}, err
}
