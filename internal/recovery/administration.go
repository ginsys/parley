package recovery

import (
	"context"
	"database/sql"
	"time"

	"github.com/ginsys/parley/internal/store"
)

type ClockReconcileRequest struct {
	OperationID, IncidentID, TimeEvidenceRef string
	ExpectedClockVersion                     int64
}
type AdministrationConfig struct {
	Service      *Service
	Authorize    func(context.Context, *sql.Tx, store.CommandPrincipal) error
	VerifyTime   func(context.Context, ClockReconcileRequest, store.RecoveryRecord) error
	MonotonicNow func() time.Time
	Wait         func(context.Context, time.Duration) error
}
type Administration struct{ config AdministrationConfig }

func NewAdministration(c AdministrationConfig) (*Administration, error) {
	if c.Service == nil || c.Authorize == nil || c.VerifyTime == nil {
		return nil, store.InvalidRequest
	}
	if c.MonotonicNow == nil {
		c.MonotonicNow = time.Now
	}
	if c.Wait == nil {
		c.Wait = func(ctx context.Context, d time.Duration) error {
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-timer.C:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return &Administration{c}, nil
}
func (a *Administration) authorization(p store.CommandPrincipal) func(context.Context, *sql.Tx) error {
	return func(ctx context.Context, tx *sql.Tx) error { return a.config.Authorize(ctx, tx, p) }
}
func rejection(code store.Code) (store.CommandResult, error) {
	return store.CommandResult{Code: code}, nil
}
func (a *Administration) ClockReconcile(ctx context.Context, p store.CommandPrincipal, r ClockReconcileRequest) (store.CommandReceipt, error) {
	if !canonicalID(r.IncidentID) || !canonicalID(r.TimeEvidenceRef) || r.ExpectedClockVersion < 1 {
		return store.CommandReceipt{}, store.InvalidRequest
	}
	request, err := store.NewCommandRequest("clock.reconcile", r.OperationID, store.Field{Name: "incident_id", Value: r.IncidentID}, store.Field{Name: "expected_clock_version", Value: r.ExpectedClockVersion}, store.Field{Name: "time_evidence_ref", Value: r.TimeEvidenceRef})
	if err != nil {
		return store.CommandReceipt{}, err
	}
	coordinator := a.config.Service.config.Store.Coordinator()
	prior, found, err := coordinator.LookupCommand(ctx, p, request, a.authorization(p))
	if err != nil {
		return store.CommandReceipt{}, err
	}
	if found {
		if prior.Result.Code != "" {
			return prior, nil
		}
		return a.finish(prior, r.IncidentID, r.ExpectedClockVersion, r.TimeEvidenceRef)
	}
	var record store.RecoveryRecord
	if err := coordinator.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := a.config.Authorize(ctx, tx, p); err != nil {
			return err
		}
		var err error
		record, err = store.ReadRecovery(ctx, tx, r.IncidentID)
		return err
	}); err != nil {
		return store.CommandReceipt{}, err
	}
	var evidenceErr error
	var first, second int64
	if record.Kind != "clock" {
		evidenceErr = store.InvalidRequest
	} else if record.Version != r.ExpectedClockVersion || record.Status != "held" {
		evidenceErr = store.VersionConflict
	} else {
		evidenceCtx, cancel := context.WithTimeout(ctx, store.ReadinessDeadline)
		evidenceErr = a.config.VerifyTime(evidenceCtx, r, record)
		if evidenceErr == nil {
			first, evidenceErr = store.InstantNanos(a.config.Service.config.Now())
			monotonicStart := a.config.MonotonicNow()
			if evidenceErr == nil {
				evidenceErr = a.config.Wait(evidenceCtx, time.Second)
			}
			if evidenceErr == nil && a.config.MonotonicNow().Sub(monotonicStart) < time.Second {
				evidenceErr = store.HostUnverified
			}
			if evidenceErr == nil {
				second, evidenceErr = store.InstantNanos(a.config.Service.config.Now())
			}
			if evidenceErr == nil && (second < first || first < record.Floor.Int64) {
				evidenceErr = store.HostUnverified
			}
		}
		if evidenceCtx.Err() != nil {
			evidenceErr = store.HostUnverified
		}
		cancel()
	}
	receipt, err := coordinator.Execute(ctx, p, request, a.authorization(p), func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
		if err := store.CheckRetiredMutation(ctx, tx, p.ID); err != nil {
			return store.CommandResult{}, err
		}
		current, err := store.ReadRecovery(ctx, tx, r.IncidentID)
		if err != nil {
			return store.CommandResult{}, err
		}
		if current.Version != r.ExpectedClockVersion {
			return rejection(store.VersionConflict)
		}
		if current.Kind != "clock" {
			return rejection(store.InvalidRequest)
		}
		if current.Status != "held" {
			return rejection(store.RequestTerminal)
		}
		if evidenceErr != nil {
			return rejection(store.HostUnverified)
		}
		now, err := store.InstantNanos(store.AuthorityTime(ctx, a.config.Service.config.Now))
		if err != nil {
			return store.CommandResult{}, err
		}
		checkpoint, err := store.ReadClockCheckpoint(ctx, tx)
		if err != nil {
			return store.CommandResult{}, err
		}
		if now < first || now < second || now < current.Floor.Int64 || (checkpoint.Instant.Valid && now < checkpoint.Instant.Int64) {
			return store.CommandResult{}, store.RecoveryRequired
		}
		if _, err := store.AdvanceClockCheckpoint(ctx, tx, now); err != nil {
			return store.CommandResult{}, err
		}
		change, err := store.ReconcileRecovery(ctx, tx, r.IncidentID, r.ExpectedClockVersion, r.TimeEvidenceRef)
		if err != nil {
			return store.CommandResult{}, err
		}
		return store.CommandResult{Resources: []store.ResourceChange{change}}, nil
	}, nil)
	if err != nil || receipt.Result.Code != "" {
		return receipt, err
	}
	return a.finish(receipt, r.IncidentID, r.ExpectedClockVersion, r.TimeEvidenceRef)
}
func markerForRecord(r store.RecoveryRecord) Marker {
	m := Marker{IncidentID: r.ID, ServerID: r.ServerID, Kind: r.Kind}
	if r.Kind == "clock" {
		floor, observed := r.Floor.Int64, r.Observed.Int64
		m.Floor = &floor
		m.Observed = &observed
	}
	return m
}

// finish is independent of caller cancellation after the audited commit. Its
// ordering is record match -> exact marker removal+fsync -> exact durable clear.
func (a *Administration) finish(receipt store.CommandReceipt, id string, originalVersion int64, evidence string) (store.CommandReceipt, error) {
	ctx, cancel := context.WithTimeout(context.Background(), store.AuthenticationDeadline)
	defer cancel()
	s := a.config.Service
	s.ioMu.Lock()
	defer s.ioMu.Unlock()
	reconciled, err := store.NextVersion(originalVersion)
	if err != nil {
		return receipt, err
	}
	cleared, err := store.NextVersion(reconciled)
	if err != nil {
		return receipt, err
	}
	var record store.RecoveryRecord
	if err := s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		record, err = store.ReadRecovery(ctx, tx, id)
		return err
	}); err != nil {
		return receipt, err
	}
	if !record.Evidence.Valid || record.Evidence.String != evidence || !((record.Status == "reconciled" && record.Version == reconciled) || (record.Status == "cleared" && record.Version == cleared)) {
		return receipt, store.RecoveryRequired
	}
	markers, err := s.config.Markers.List(ctx)
	if err != nil {
		return receipt, store.RecoveryRequired
	}
	if record.Status == "cleared" {
		for _, marker := range markers {
			if marker.IncidentID == id {
				return receipt, store.RecoveryRequired
			}
		}
	}
	if err := s.config.Markers.Remove(ctx, markerForRecord(record)); err != nil {
		return receipt, err
	}
	if _, err := s.maintenance.Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		changed, err := store.ClearRecovery(ctx, tx, id, reconciled, evidence)
		return store.TransitionResult{Changed: changed}, err
	}, nil); err != nil {
		return receipt, err
	}
	markers, err = s.config.Markers.List(ctx)
	if err != nil {
		return receipt, store.RecoveryRequired
	}
	var held bool
	if err := s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		held, err = store.RecoveryHeld(ctx, tx)
		return err
	}); err != nil {
		return receipt, err
	}
	s.stateMu.Lock()
	s.held = held || len(markers) > 0 || len(s.pending) > 0
	s.stateMu.Unlock()
	return receipt, nil
}
