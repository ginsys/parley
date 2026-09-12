package connection

import (
	"context"
	"database/sql"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ginsys/parley/internal/store"
	"github.com/google/uuid"
)

type LifecycleConfig struct {
	Store              *store.DB
	Now                func() time.Time
	Authorize          func(context.Context, *sql.Tx, store.CommandPrincipal) error
	Guard              func(context.Context, *sql.Tx, string) error
	Invalidate         func(string)
	PendingWork        func(context.Context, *sql.Tx, string) ([]store.WorkRef, error)
	PendingDisposition func(context.Context, *sql.Tx, store.WorkRef, string) error
	LegacyEvidence     func(context.Context, LegacyDispositionRequest) error
}
type Lifecycle struct{ config LifecycleConfig }

func NewLifecycle(c LifecycleConfig) (*Lifecycle, error) {
	if c.Store == nil || c.Authorize == nil || c.Guard == nil || c.Invalidate == nil || c.PendingWork == nil || c.PendingDisposition == nil || c.LegacyEvidence == nil {
		return nil, store.InvalidRequest
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return &Lifecycle{c}, nil
}

type BindingLifecycleRequest struct {
	OperationID, BindingID                            string
	ExpectedBindingVersion, ExpectedCredentialVersion int64
}

func (l *Lifecycle) authorization(p store.CommandPrincipal) func(context.Context, *sql.Tx) error {
	return func(ctx context.Context, tx *sql.Tx) error { return l.config.Authorize(ctx, tx, p) }
}
func (l *Lifecycle) Revoke(ctx context.Context, p store.CommandPrincipal, r BindingLifecycleRequest) (store.CommandReceipt, error) {
	return l.disable(ctx, p, r, false)
}
func (l *Lifecycle) Retire(ctx context.Context, p store.CommandPrincipal, r BindingLifecycleRequest) (store.CommandReceipt, error) {
	return l.disable(ctx, p, r, true)
}
func (l *Lifecycle) disable(ctx context.Context, p store.CommandPrincipal, r BindingLifecycleRequest, retire bool) (store.CommandReceipt, error) {
	if !canonicalID(r.BindingID) || r.ExpectedBindingVersion < 1 || r.ExpectedCredentialVersion < 1 {
		return store.CommandReceipt{}, store.InvalidRequest
	}
	kind := "binding.revoke"
	if retire {
		kind = "binding.retire"
	}
	request, err := store.NewCommandRequest(kind, r.OperationID, store.Field{Name: "binding_id", Value: r.BindingID}, store.Field{Name: "expected_binding_version", Value: r.ExpectedBindingVersion}, store.Field{Name: "expected_credential_version", Value: r.ExpectedCredentialVersion})
	if err != nil {
		return store.CommandReceipt{}, err
	}
	changed := false
	return l.config.Store.Coordinator().Execute(ctx, p, request, l.authorization(p), func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
		if err := l.config.Guard(ctx, tx, kind); err != nil {
			return domainRejection(err)
		}
		pending, err := l.config.PendingWork(ctx, tx, r.BindingID)
		if err != nil {
			return domainRejection(err)
		}
		now, err := store.InstantNanos(l.config.Now())
		if err != nil {
			return domainRejection(err)
		}
		incident, err := uuid.NewRandom()
		if err != nil {
			return store.CommandResult{}, store.TemporarilyUnavailable
		}
		change, err := store.RevokeBinding(ctx, tx, store.RevocationRequest{BindingID: r.BindingID, IncidentID: incident.String(), ExpectedBindingVersion: r.ExpectedBindingVersion, ExpectedCredentialVersion: r.ExpectedCredentialVersion, Retire: retire, NowNS: now}, pending)
		if err != nil {
			return domainRejection(err)
		}
		changed = true
		return store.CommandResult{Resources: []store.ResourceChange{
			{Kind: "binding", ID: r.BindingID, Before: r.ExpectedBindingVersion, After: change.BindingVersion},
			{Kind: "revocation_incident", ID: change.IncidentID, After: 1},
			{Kind: "ingestion_barrier", ID: r.BindingID, Before: change.BarrierVersion - 1, After: change.BarrierVersion},
		}}, nil
	}, func(store.CommitView) {
		if changed {
			l.config.Invalidate(r.BindingID)
		}
	})
}

type DispositionReason struct {
	Code string
	Note *string
}
type HoldDispositionRequest struct {
	OperationID         string
	Work                store.WorkRef
	IncidentID          string
	ExpectedHoldVersion int64
	Action              string
	Reason              DispositionReason
}

func (r DispositionReason) fields() (store.Fields, error) {
	switch r.Code {
	case "owner_reviewed", "compromise", "retirement", "restore_reconciled", "cancelled":
	default:
		return nil, store.InvalidRequest
	}
	fields := store.Fields{{Name: "code", Value: r.Code}}
	if r.Note != nil {
		if len(*r.Note) > 512 || !utf8.ValidString(*r.Note) {
			return nil, store.InvalidRequest
		}
		for _, c := range *r.Note {
			if unicode.IsControl(c) || unicode.Is(unicode.Cf, c) || c == '\u2028' || c == '\u2029' {
				return nil, store.InvalidRequest
			}
		}
		fields = append(fields, store.Field{Name: "note", Value: *r.Note})
	}
	return fields, nil
}
func (l *Lifecycle) HoldDisposition(ctx context.Context, p store.CommandPrincipal, r HoldDispositionRequest) (store.CommandReceipt, error) {
	reason, err := r.Reason.fields()
	if err != nil {
		return store.CommandReceipt{}, err
	}
	if !canonicalID(r.IncidentID) || r.ExpectedHoldVersion < 1 || (r.Action != "release" && r.Action != "cancel") {
		return store.CommandReceipt{}, store.InvalidRequest
	}
	request, err := store.NewCommandRequest("hold.disposition", r.OperationID, store.Field{Name: "work", Value: store.Fields{{Name: "kind", Value: r.Work.Kind}, {Name: "id", Value: r.Work.ID}}}, store.Field{Name: "incident_id", Value: r.IncidentID}, store.Field{Name: "expected_hold_version", Value: r.ExpectedHoldVersion}, store.Field{Name: "action", Value: r.Action}, store.Field{Name: "reason", Value: reason})
	if err != nil {
		return store.CommandReceipt{}, err
	}
	return l.disposition(ctx, p, request, store.DispositionRequest{Work: r.Work, IncidentID: r.IncidentID, ExpectedVersion: r.ExpectedHoldVersion, Action: r.Action, ReasonCode: r.Reason.Code, ReasonNote: r.Reason.Note, PrincipalID: p.ID, OperationID: r.OperationID}, nil)
}

type LegacyDispositionRequest struct {
	OperationID, MigrationIncidentID, WorkID string
	ExpectedQuarantineVersion                int64
	Action, DispositionRef                   string
}

func (l *Lifecycle) LegacyDisposition(ctx context.Context, p store.CommandPrincipal, r LegacyDispositionRequest) (store.CommandReceipt, error) {
	if !canonicalID(r.MigrationIncidentID) || !canonicalID(r.DispositionRef) || r.WorkID == "" || r.ExpectedQuarantineVersion < 1 || (r.Action != "release" && r.Action != "cancel") {
		return store.CommandReceipt{}, store.InvalidRequest
	}
	request, err := store.NewCommandRequest("legacy.disposition", r.OperationID, store.Field{Name: "migration_incident_id", Value: r.MigrationIncidentID}, store.Field{Name: "work_id", Value: r.WorkID}, store.Field{Name: "expected_quarantine_version", Value: r.ExpectedQuarantineVersion}, store.Field{Name: "action", Value: r.Action}, store.Field{Name: "disposition_ref", Value: r.DispositionRef})
	if err != nil {
		return store.CommandReceipt{}, err
	}
	previous, found, err := l.config.Store.Coordinator().LookupCommand(ctx, p, request, l.authorization(p))
	if err != nil {
		return store.CommandReceipt{}, err
	}
	if found {
		return previous, nil
	}
	// The trusted resolver checks an immutable reviewed manifest bound to this exact
	// incident/work/version/action. Its file I/O precedes the writer transaction.
	evidenceErr := l.config.LegacyEvidence(ctx, r)
	return l.disposition(ctx, p, request, store.DispositionRequest{Legacy: true, Work: store.WorkRef{Kind: "envelope", ID: r.WorkID}, IncidentID: r.MigrationIncidentID, ExpectedVersion: r.ExpectedQuarantineVersion, Action: r.Action, ReasonCode: "owner_reviewed", EvidenceRef: r.DispositionRef, PrincipalID: p.ID, OperationID: r.OperationID}, evidenceErr)
}
func (l *Lifecycle) disposition(ctx context.Context, p store.CommandPrincipal, request store.CommandRequest, r store.DispositionRequest, evidenceErr error) (store.CommandReceipt, error) {
	kind := "hold.disposition"
	if r.Legacy {
		kind = "legacy.disposition"
	}
	return l.config.Store.Coordinator().Execute(ctx, p, request, l.authorization(p), func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
		if err := l.config.Guard(ctx, tx, kind); err != nil {
			return domainRejection(err)
		}
		if evidenceErr != nil {
			return rejection(store.Forbidden)
		}
		now, err := store.InstantNanos(l.config.Now())
		if err != nil {
			return domainRejection(err)
		}
		r.NowNS = now
		change, err := store.ApplyWorkDisposition(ctx, tx, r, l.config.PendingDisposition)
		if err != nil {
			return domainRejection(err)
		}
		return store.CommandResult{Resources: []store.ResourceChange{change}}, nil
	}, nil)
}
