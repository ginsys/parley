package recovery

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/controller"
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
	a, s, now, r := restoreAdministration(t)
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
		var missing int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM security_holds WHERE created_at_ns != ?", now.UnixNano()).Scan(&missing); err != nil {
			return err
		}
		if missing != 0 {
			t.Errorf("holds missing trusted creation instant: %d", missing)
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

func TestLegacyControllerCannotBypassRecoveryOwner(t *testing.T) {
	for _, operation := range []string{"grant", "revoke", "renew"} {
		t.Run(operation, func(t *testing.T) {
			_, s, _, _ := restoreAdministration(t)
			seedRestoredWork(t, s)
			c := controller.New(s.config.Store)
			ctx := context.Background()
			var err error
			switch operation {
			case "grant":
				_, err = c.Grant(ctx, controller.GrantParams{Conversation: "new", PeerAID: "a", PeerBID: "b", Direction: store.Bidirectional, MaxExchanges: 1})
			case "revoke":
				_, err = c.Revoke(ctx, "restore")
			case "renew":
				_, err = c.Renew(ctx, controller.RenewParams{Conversation: "restore", MaxExchanges: 20})
			}
			if err != store.RecoveryRequired {
				t.Fatalf("legacy %s bypass=%v", operation, err)
			}
			if err := s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
				var grants, conversations int
				if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM grants").Scan(&grants); err != nil {
					return err
				}
				if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM conversations").Scan(&conversations); err != nil {
					return err
				}
				g, err := store.CurrentGrant(ctx, tx, "restore")
				if err != nil {
					return err
				}
				if grants != 1 || conversations != 1 || g.GrantVersion != 1 || g.MaxExchanges != 10 {
					t.Errorf("mutated grants=%d conversations=%d grant=%+v", grants, conversations, g)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRestoreCanRetireMoreThanThousandBindings(t *testing.T) {
	a, s, _, r := restoreAdministration(t)
	ctx := context.Background()
	var retire []store.NamespaceRetirement
	_, err := s.maintenance.Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		for i := 1; i <= 1001; i++ {
			b := store.BindingRecord{ID: fmt.Sprintf("30000000-0000-4000-8000-%012d", i), PeerID: fmt.Sprintf("peer-%d", i), HostKind: "codex_cli", NamespaceID: "synthetic", SessionID: fmt.Sprintf("session-%d", i), ConnectorUID: 1000, Status: "enabled", Version: 1}
			c := store.CredentialRecord{BindingID: b.ID, ID: fmt.Sprintf("40000000-0000-4000-8000-%012d", i), Version: 1, Status: "current", ExpiresAtNS: 300000000000}
			if err := store.InsertBindingCredential(ctx, tx, b, c); err != nil {
				return store.TransitionResult{}, err
			}
			retire = append(retire, store.NamespaceRetirement{PrincipalID: b.ID, BindingVersion: 1, CredentialVersion: 1})
		}
		return store.TransitionResult{Changed: true}, nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.resolve = func(context.Context, RecoveryCompleteRequest) (RestoreDisposition, error) {
		return RestoreDisposition{Retire: retire}, nil
	}
	receipt, err := a.Complete(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, r)
	if err != nil || receipt.Result.Code != "" {
		t.Fatalf("large restore=%+v %v", receipt, err)
	}
	if err := s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM retired_namespaces WHERE binding_id IS NOT NULL").Scan(&count); err != nil {
			return err
		}
		if count != 1001 {
			t.Errorf("retired=%d", count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if mode, err := s.InspectRecovery(ctx, s.config.Store); err != nil || mode != runtimeowner.Normal {
		t.Fatalf("large restore held=%v %v", mode, err)
	}
}

func TestFloorResetRejectsRollbackLatchedAtWriterTime(t *testing.T) {
	a, s, now, r := restoreAdministration(t)
	ctx := context.Background()
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
	a.resolve = func(context.Context, RecoveryCompleteRequest) (RestoreDisposition, error) {
		samples := 0
		s.config.Now = func() time.Time {
			samples++
			if samples == 1 {
				return time.Unix(100, 0)
			}
			return time.Unix(101, 0)
		}
		return RestoreDisposition{Retire: []store.NamespaceRetirement{{PrincipalID: recoveryPrincipal}}, ClockFloor: &store.ReviewedClockFloor{CheckpointVersion: floor.Version, Previous: floor.Instant.Int64, Reviewed: time.Unix(100, 0).UnixNano()}, ClockIncidents: clocks}, nil
	}
	receipt, err := a.Complete(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, r)
	if err != store.RecoveryRequired {
		t.Fatalf("unreviewed writer evidence reset floor: receipt=%+v err=%v", receipt, err)
	}
	if err := s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		got, err := store.ReadClockCheckpoint(ctx, tx)
		if err != nil {
			return err
		}
		if got != floor {
			t.Errorf("floor changed: %+v want %+v", got, floor)
		}
		var clocks, retired, receipts int
		for query, target := range map[string]*int{"SELECT count(*) FROM recovery_incidents WHERE kind='clock' AND status='held'": &clocks, "SELECT count(*) FROM retired_namespaces": &retired, "SELECT count(*) FROM operation_results": &receipts} {
			if err := tx.QueryRowContext(ctx, query).Scan(target); err != nil {
				return err
			}
		}
		if clocks != 2 || retired != 0 || receipts != 0 {
			t.Errorf("clock/retirement/receipt=%d/%d/%d", clocks, retired, receipts)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFloorResetCanCoverMoreThanNinetyNineClockIncidents(t *testing.T) {
	a, s, now, r := restoreAdministration(t)
	s.config.Markers.(*Directory).capacity = 200
	ctx := context.Background()
	var clocks []ReviewedIncident
	floor := time.Unix(110, 0).UnixNano()
	for i := 1; i <= 100; i++ {
		observed := time.Unix(100, 0).UnixNano() + int64(i)
		marker := Marker{IncidentID: fmt.Sprintf("61000000-0000-4000-8000-%012d", i), ServerID: s.serverID, Kind: "clock", Floor: &floor, Observed: &observed}
		if err := s.config.Markers.Put(ctx, marker); err != nil {
			t.Fatal(err)
		}
		clocks = append(clocks, ReviewedIncident{ID: marker.IncidentID, Version: 1})
	}
	*now = time.Unix(100, 100)
	if err := s.prepare(ctx); err != nil {
		t.Fatal(err)
	}
	var checkpoint store.ClockCheckpoint
	if err := s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		checkpoint, err = store.ReadClockCheckpoint(ctx, tx)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	a.resolve = func(context.Context, RecoveryCompleteRequest) (RestoreDisposition, error) {
		return RestoreDisposition{Retire: []store.NamespaceRetirement{{PrincipalID: recoveryPrincipal}}, ClockFloor: &store.ReviewedClockFloor{CheckpointVersion: checkpoint.Version, Previous: floor, Reviewed: now.UnixNano()}, ClockIncidents: clocks}, nil
	}
	markers := &removalFailure{Markers: s.config.Markers, fail: true}
	s.config.Markers = markers
	receipt, err := a.Complete(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, r)
	if err != store.TemporarilyUnavailable || receipt.Result.Code != "" || len(receipt.Result.Resources) != 101 {
		t.Fatalf("large floor reset=%+v %v", receipt, err)
	}
	markers.fail = false
	a.resolve = func(context.Context, RecoveryCompleteRequest) (RestoreDisposition, error) {
		t.Fatal("replay re-resolved evidence")
		return RestoreDisposition{}, nil
	}
	replay, err := a.Complete(ctx, store.CommandPrincipal{ID: recoveryPrincipal}, r)
	if err != nil || !replay.Replayed || replay.AuditID != receipt.AuditID {
		t.Fatalf("large reset cleanup replay=%+v %v", replay, err)
	}
	if mode, err := s.InspectRecovery(ctx, s.config.Store); err != nil || mode != runtimeowner.Normal {
		t.Fatalf("large floor reset held=%v %v", mode, err)
	}
	if err := s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM recovery_incidents WHERE status='cleared'").Scan(&count); err != nil {
			return err
		}
		if count != 101 {
			t.Errorf("cleared=%d", count)
		}
		got, err := store.ReadClockCheckpoint(ctx, tx)
		if got.Instant.Int64 != now.UnixNano() {
			t.Errorf("reset floor=%+v", got)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
