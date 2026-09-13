package recovery

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	runtimeowner "github.com/ginsys/parley/internal/runtime"
	"github.com/ginsys/parley/internal/store"
)

const restoredBinding = "30000000-0000-4000-8000-000000000001"

func restoreAdministration(t *testing.T) (*RestoreAdministration, *Service, *time.Time, RecoveryCompleteRequest) {
	t.Helper()
	s, now, _ := recoveryFixture(t)
	marker := Marker{IncidentID: "60000000-0000-4000-8000-000000000001", ServerID: s.serverID, Kind: "restore"}
	if err := s.config.Markers.Put(context.Background(), marker); err != nil {
		t.Fatal(err)
	}
	a, err := NewAdministration(AdministrationConfig{Service: s, Authorize: func(context.Context, *sql.Tx, store.CommandPrincipal) error { return nil }, VerifyTime: func(context.Context, ClockReconcileRequest, store.RecoveryRecord) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	restore, err := NewRestoreAdministration(a, func(context.Context, RecoveryCompleteRequest) (RestoreDisposition, error) {
		return RestoreDisposition{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return restore, s, now, RecoveryCompleteRequest{OperationID: recoveryOperation, IncidentID: marker.IncidentID, DispositionRef: recoveryEvidence, ExpectedRecoveryVersion: 1}
}
func seedRestoredWork(t *testing.T, s *Service) {
	t.Helper()
	_, err := s.maintenance.Transition(context.Background(), func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		b := store.BindingRecord{ID: restoredBinding, PeerID: "restored", HostKind: "codex_cli", NamespaceID: "synthetic", SessionID: "native", ConnectorUID: 1000, Status: "enabled", Version: 1}
		c := store.CredentialRecord{BindingID: b.ID, ID: "40000000-0000-4000-8000-000000000001", Version: 1, Status: "current", ExpiresAtNS: 300000000000}
		if err := store.InsertBindingCredential(ctx, tx, b, c); err != nil {
			return store.TransitionResult{}, err
		}
		if err := store.EnsureConversation(ctx, tx, "restore", "restore", "2026-01-01T00:00:00Z"); err != nil {
			return store.TransitionResult{}, err
		}
		if err := store.InsertGrant(ctx, tx, store.Grant{Conversation: "restore", GrantVersion: 1, PeerAID: "restored", PeerBID: "recipient", Direction: store.Bidirectional, MaxExchanges: 10, GrantedAt: "2026-01-01T00:00:00Z", Status: store.GrantActive}); err != nil {
			return store.TransitionResult{}, err
		}
		for index, state := range []string{"queued", "dispatching", "handed_off", "uncertain", "acked"} {
			id := fmt.Sprintf("50000000-0000-4000-8000-%012d", index+1)
			e := store.Envelope{ID: id, Conversation: "restore", FromPeer: "restored", ToPeer: "recipient", Text: "synthetic", GrantVersion: 1, CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z"}
			if err := store.InsertQueued(ctx, tx, e); err != nil {
				return store.TransitionResult{}, err
			}
			if err := store.RecordAuthenticatedEnvelope(ctx, tx, id, b.ID, 1); err != nil {
				return store.TransitionResult{}, err
			}
			if _, err := tx.ExecContext(ctx, "UPDATE envelopes SET state=?,dispatch_attempt=4 WHERE id=?", state, id); err != nil {
				return store.TransitionResult{}, err
			}
		}
		return store.TransitionResult{Changed: true}, nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
}
func TestRestoreRetiresNamespaceWithoutReplayingSnapshotEffects(t *testing.T) {
	a, s, _, r := restoreAdministration(t)
	seedRestoredWork(t, s)
	ctx := context.Background()
	// Missing the affected binding cannot release restored grant budgets.
	receipt, err := a.Complete(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, r)
	if err != nil || receipt.Result.Code != store.VersionConflict {
		t.Fatalf("incomplete disposition=%+v %v", receipt, err)
	}
	r.OperationID = "80000000-0000-4000-8000-000000000004"
	a.resolve = func(context.Context, RecoveryCompleteRequest) (RestoreDisposition, error) {
		return RestoreDisposition{Retire: []store.NamespaceRetirement{{PrincipalID: restoredBinding, BindingVersion: 1, CredentialVersion: 1}}}, nil
	}
	markers := &removalFailure{Markers: s.config.Markers, fail: true}
	s.config.Markers = markers
	receipt, err = a.Complete(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, r)
	if err != store.TemporarilyUnavailable || receipt.Result.Code != "" {
		t.Fatalf("committed restore=%+v %v", receipt, err)
	}
	markers.fail = false
	a.resolve = func(context.Context, RecoveryCompleteRequest) (RestoreDisposition, error) {
		t.Fatal("replay re-resolved disposition")
		return RestoreDisposition{}, nil
	}
	replay, err := a.Complete(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, r)
	if err != nil || !replay.Replayed || replay.AuditID != receipt.AuditID {
		t.Fatalf("restore replay=%+v %v", replay, err)
	}
	if mode, err := s.InspectRecovery(ctx, s.config.Store); err != nil || mode != runtimeowner.Normal {
		t.Fatalf("completed=%v %v", mode, err)
	}
	err = s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		b, err := store.ReadBinding(ctx, tx, restoredBinding)
		if err != nil {
			return err
		}
		if b.Status != "retired" {
			t.Errorf("revival=%+v", b)
		}
		if err := store.CheckRetiredMutation(ctx, tx, restoredBinding); err != store.RecoveryRequired {
			t.Errorf("namespace reusable=%v", err)
		}
		grant, err := store.CurrentGrant(ctx, tx, "restore")
		if err != nil {
			return err
		}
		if grant.ExchangesUsed != 0 {
			t.Errorf("budget rewritten=%+v", grant)
		}
		for index, state := range []string{"queued", "dispatching", "handed_off", "uncertain", "acked"} {
			e, err := store.GetByID(ctx, tx, fmt.Sprintf("50000000-0000-4000-8000-%012d", index+1))
			if err != nil {
				return err
			}
			if string(e.State) != state || e.DispatchAttempt != 4 {
				t.Errorf("snapshot outcome rewritten=%+v", e)
			}
			held, err := store.WorkHeld(ctx, tx, store.WorkRef{Kind: "envelope", ID: e.ID})
			if err != nil {
				return err
			}
			if held != (state != "acked") {
				t.Errorf("state=%s held=%v", state, held)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestReviewedBadFloorRequiresExactClockSetAndCannotBeRaisedByOldPublication(t *testing.T) {
	a, s, now, r := restoreAdministration(t)
	ctx := context.Background()
	// Establish a trusted process observation before the clock becomes unusable.
	if err := s.config.Store.Coordinator().Inspect(ctx, func(context.Context, *sql.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	*now = time.Unix(100, 0)
	if err := rejectOrdinary(t, s); err != store.RecoveryRequired {
		t.Fatal(err)
	}
	var floor store.ClockCheckpoint
	var clocks []ReviewedIncident
	if err := s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		floor, err = store.ReadClockCheckpoint(ctx, tx)
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, "SELECT incident_id,recovery_version FROM recovery_incidents WHERE kind='clock' AND status='held'")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var c ReviewedIncident
			if err := rows.Scan(&c.ID, &c.Version); err != nil {
				return err
			}
			clocks = append(clocks, c)
		}
		return rows.Err()
	}); err != nil {
		t.Fatal(err)
	}
	disposition := RestoreDisposition{Retire: []store.NamespaceRetirement{{PrincipalID: recoveryPrincipal}}, ClockFloor: &store.ReviewedClockFloor{CheckpointVersion: floor.Version, Previous: floor.Instant.Int64, Reviewed: now.UnixNano()}}
	a.resolve = func(context.Context, RecoveryCompleteRequest) (RestoreDisposition, error) { return disposition, nil }
	receipt, err := a.Complete(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, r)
	if err != nil || receipt.Result.Code != store.VersionConflict {
		t.Fatalf("unreviewed clock cleared=%+v %v", receipt, err)
	}
	// Rejected effect must also roll back the attempted self-retirement.
	r.OperationID = "80000000-0000-4000-8000-000000000004"
	disposition.ClockIncidents = clocks
	receipt, err = a.Complete(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, r)
	if err != nil || receipt.Result.Code != "" {
		t.Fatalf("bad-floor completion=%+v %v", receipt, err)
	}
	if mode, err := s.InspectRecovery(ctx, s.config.Store); err != nil || mode != runtimeowner.Normal {
		t.Fatalf("bad floor revived=%v %v", mode, err)
	}
	if err := s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		got, err := store.ReadClockCheckpoint(ctx, tx)
		if got.Instant.Int64 != now.UnixNano() {
			t.Errorf("old publication restored floor=%+v", got)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	replay, err := a.Complete(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, r)
	if err != nil || !replay.Replayed {
		t.Fatalf("retired admin cannot finish receipt=%+v %v", replay, err)
	}
	r.OperationID = "80000000-0000-4000-8000-000000000005"
	if _, err := a.Complete(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, r); err != store.RecoveryRequired {
		t.Fatalf("retired principal new mutation=%v", err)
	}
}
