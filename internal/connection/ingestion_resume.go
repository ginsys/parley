package connection

import (
	"context"
	"database/sql"
	"github.com/ginsys/parley/internal/store"
)

type IngestionResumeRequest struct {
	OperationID, BindingID, DispositionRef         string
	ExpectedBindingVersion, ExpectedBarrierVersion int64
}
type IngestionRecoveryConfig struct {
	Store     *store.DB
	Authorize func(context.Context, *sql.Tx, store.CommandPrincipal) error
	Guard     func(context.Context, *sql.Tx, string) error
	Resolve   func(context.Context, IngestionResumeRequest) (store.ReviewedInterval, error)
}
type IngestionRecovery struct{ config IngestionRecoveryConfig }

func NewIngestionRecovery(c IngestionRecoveryConfig) (*IngestionRecovery, error) {
	if c.Store == nil || c.Authorize == nil || c.Guard == nil || c.Resolve == nil {
		return nil, store.InvalidRequest
	}
	return &IngestionRecovery{c}, nil
}
func (s *IngestionRecovery) Resume(ctx context.Context, p store.CommandPrincipal, r IngestionResumeRequest) (store.CommandReceipt, error) {
	if !canonicalID(r.BindingID) || !canonicalID(r.DispositionRef) || r.ExpectedBindingVersion < 1 || r.ExpectedBarrierVersion < 1 {
		return store.CommandReceipt{}, store.InvalidRequest
	}
	request, err := store.NewCommandRequest("ingestion.resume", r.OperationID, store.Field{Name: "binding_id", Value: r.BindingID}, store.Field{Name: "expected_binding_version", Value: r.ExpectedBindingVersion}, store.Field{Name: "expected_barrier_version", Value: r.ExpectedBarrierVersion}, store.Field{Name: "disposition_ref", Value: r.DispositionRef})
	if err != nil {
		return store.CommandReceipt{}, err
	}
	authorize := func(ctx context.Context, tx *sql.Tx) error { return s.config.Authorize(ctx, tx, p) }
	previous, found, err := s.config.Store.Coordinator().LookupCommand(ctx, p, request, authorize)
	if err != nil {
		return store.CommandReceipt{}, err
	}
	if found {
		return previous, nil
	}
	evidenceCtx, cancel := context.WithTimeout(ctx, store.ReadinessDeadline)
	interval, evidenceErr := s.config.Resolve(evidenceCtx, r)
	if evidenceCtx.Err() != nil {
		evidenceErr = store.HostUnverified
	}
	cancel()
	return s.config.Store.Coordinator().Execute(ctx, p, request, authorize, func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
		if err := s.config.Guard(ctx, tx, "ingestion.resume"); err != nil {
			return domainRejection(err)
		}
		if evidenceErr != nil {
			return rejection(store.HostUnverified)
		}
		change, err := store.ResumeIngestion(ctx, tx, store.ResumeIngestionRequest{BindingID: r.BindingID, ExpectedBindingVersion: r.ExpectedBindingVersion, ExpectedBarrierVersion: r.ExpectedBarrierVersion, EvidenceRef: r.DispositionRef, PrincipalID: p.ID, OperationID: r.OperationID, Interval: interval})
		if err != nil {
			return domainRejection(err)
		}
		return store.CommandResult{Resources: []store.ResourceChange{change}}, nil
	}, nil)
}
