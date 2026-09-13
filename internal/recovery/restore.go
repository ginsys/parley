package recovery

import (
	"context"
	"database/sql"

	"github.com/ginsys/parley/internal/store"
)

type RecoveryCompleteRequest struct {
	OperationID, IncidentID, DispositionRef string
	ExpectedRecoveryVersion                 int64
}
type ReviewedIncident struct {
	ID      string
	Version int64
}

// RestoreDisposition comes only from a trusted human evidence resolver. It must
// account for lost credential, operation/event, grant and delivery history. All
// outstanding work remains held. Unenumerable identity histories must be retired.
// ClockFloor additionally requires reviewed configured-source evidence, retirement
// of affected authority, and an exact list of every outstanding clock incident.
type RestoreDisposition struct {
	Retire         []store.NamespaceRetirement
	PendingWork    []store.WorkRef
	ClockFloor     *store.ReviewedClockFloor
	ClockIncidents []ReviewedIncident
}
type RestoreAdministration struct {
	administration *Administration
	resolve        func(context.Context, RecoveryCompleteRequest) (RestoreDisposition, error)
}

func NewRestoreAdministration(a *Administration, resolve func(context.Context, RecoveryCompleteRequest) (RestoreDisposition, error)) (*RestoreAdministration, error) {
	if a == nil || resolve == nil {
		return nil, store.InvalidRequest
	}
	return &RestoreAdministration{a, resolve}, nil
}
func (a *RestoreAdministration) Complete(ctx context.Context, p store.CommandPrincipal, r RecoveryCompleteRequest) (store.CommandReceipt, error) {
	if !canonicalID(r.IncidentID) || !canonicalID(r.DispositionRef) || r.ExpectedRecoveryVersion < 1 {
		return store.CommandReceipt{}, store.InvalidRequest
	}
	request, err := store.NewCommandRequest("recovery.complete", r.OperationID, store.Field{Name: "incident_id", Value: r.IncidentID}, store.Field{Name: "expected_recovery_version", Value: r.ExpectedRecoveryVersion}, store.Field{Name: "disposition_ref", Value: r.DispositionRef})
	if err != nil {
		return store.CommandReceipt{}, err
	}
	admin := a.administration
	s := admin.config.Service
	c := s.config.Store.Coordinator()
	prior, found, err := c.LookupCommand(ctx, p, request, admin.authorization(p))
	if err != nil {
		return store.CommandReceipt{}, err
	}
	if found {
		return a.finish(prior, r.DispositionRef)
	}
	evidenceCtx, cancel := context.WithTimeout(ctx, store.ReadinessDeadline)
	disposition, evidenceErr := a.resolve(evidenceCtx, r)
	if evidenceCtx.Err() != nil {
		evidenceErr = store.HostUnverified
	}
	cancel()
	var newFloor *int64
	receipt, err := c.Execute(ctx, p, request, admin.authorization(p), func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
		if err := store.CheckRetiredMutation(ctx, tx, p.ID); err != nil {
			return store.CommandResult{}, err
		}
		record, err := store.ReadRecovery(ctx, tx, r.IncidentID)
		if err != nil {
			return store.CommandResult{}, err
		}
		if record.Kind != "restore" {
			return rejection(store.InvalidRequest)
		}
		if record.Version != r.ExpectedRecoveryVersion {
			return rejection(store.VersionConflict)
		}
		if record.Status != "held" {
			return rejection(store.RequestTerminal)
		}
		if evidenceErr != nil {
			return rejection(store.HostUnverified)
		}
		if len(disposition.ClockIncidents) > 99 || len(disposition.Retire) > 1000 || len(disposition.PendingWork) > 1000 {
			return store.CommandResult{}, store.CapacityExceeded
		}
		if disposition.ClockFloor == nil && len(disposition.ClockIncidents) != 0 {
			return rejection(store.InvalidRequest)
		}
		now, err := store.InstantNanos(store.AuthorityTime(ctx, s.config.Now))
		if err != nil {
			return store.CommandResult{}, err
		}
		seen := map[string]bool{}
		for _, retirement := range disposition.Retire {
			if seen[retirement.PrincipalID] {
				return rejection(store.InvalidRequest)
			}
			seen[retirement.PrincipalID] = true
			if err := store.RetireRecoveryNamespace(ctx, tx, r.IncidentID, retirement, now); err != nil {
				return store.CommandResult{}, err
			}
		}
		// This implementation supports conservative retirement, not import of
		// surviving grant/receipt history. Every restored binding must therefore
		// be retired before release, so snapshot budgets cannot authorize new work.
		var activeNamespaces bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM bindings b WHERE NOT EXISTS(SELECT 1 FROM retired_namespaces n WHERE n.binding_id=b.binding_id))").Scan(&activeNamespaces); err != nil {
			return store.CommandResult{}, err
		}
		if activeNamespaces {
			return rejection(store.VersionConflict)
		}
		if err := store.HoldRestoredWork(ctx, tx, r.IncidentID, disposition.PendingWork); err != nil {
			return store.CommandResult{}, err
		}
		var changes []store.ResourceChange
		if floor := disposition.ClockFloor; floor != nil {
			// The trusted source has reviewed this floor; current writer time must also
			// be at least that floor. Ordinary clock.reconcile never takes this path.
			if now < floor.Reviewed {
				return store.CommandResult{}, store.RecoveryRequired
			}
			clocks := map[string]int64{}
			for _, incident := range disposition.ClockIncidents {
				if !canonicalID(incident.ID) || incident.Version < 1 || clocks[incident.ID] != 0 {
					return rejection(store.InvalidRequest)
				}
				clocks[incident.ID] = incident.Version
			}
			rows, err := tx.QueryContext(ctx, "SELECT incident_id,recovery_version,status FROM recovery_incidents WHERE kind='clock' AND status!='cleared'")
			if err != nil {
				return store.CommandResult{}, err
			}
			count := 0
			valid := true
			for rows.Next() {
				var id, status string
				var version int64
				if err := rows.Scan(&id, &version, &status); err != nil {
					rows.Close()
					return store.CommandResult{}, err
				}
				count++
				if clocks[id] != version || status != "held" {
					valid = false
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return store.CommandResult{}, err
			}
			if !valid || count != len(clocks) {
				return rejection(store.VersionConflict)
			}
			if err := store.ResetReviewedClockFloor(ctx, tx, r.IncidentID, *floor, p.ID, r.OperationID); err != nil {
				return store.CommandResult{}, err
			}
			for _, incident := range disposition.ClockIncidents {
				change, err := store.ReconcileRecovery(ctx, tx, incident.ID, incident.Version, r.DispositionRef)
				if err != nil {
					return store.CommandResult{}, err
				}
				changes = append(changes, change)
			}
			value := floor.Reviewed
			newFloor = &value
		}
		change, err := store.ReconcileRecovery(ctx, tx, r.IncidentID, r.ExpectedRecoveryVersion, r.DispositionRef)
		if err != nil {
			return store.CommandResult{}, err
		}
		changes = append(changes, change)
		return store.CommandResult{Resources: changes}, nil
	}, func(store.CommitView) {
		if newFloor != nil {
			s.stateMu.Lock()
			s.trusted = newFloor
			s.stateMu.Unlock()
		}
	})
	if err != nil {
		return receipt, err
	}
	return a.finish(receipt, r.DispositionRef)
}
func (a *RestoreAdministration) finish(receipt store.CommandReceipt, evidence string) (store.CommandReceipt, error) {
	if receipt.Result.Code != "" {
		return receipt, nil
	}
	for _, change := range receipt.Result.Resources {
		if change.Kind != "recovery_incident" {
			return receipt, store.RecoveryRequired
		}
		if _, err := a.administration.finish(receipt, change.ID, change.Before, evidence); err != nil {
			return receipt, err
		}
	}
	return receipt, nil
}
