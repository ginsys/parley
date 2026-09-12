package store

import (
	"context"
	"database/sql"
	"testing"
)

func retainedWorkFixture(t *testing.T) (*DB, BindingRecord, string, string) {
	t.Helper()
	db := commandDB(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	b := BindingRecord{ID: "30000000-0000-4000-8000-000000000001", PeerID: "author", HostKind: "codex_cli", NamespaceID: "synthetic", SessionID: "native", ConnectorUID: 1000, Status: "enabled", Version: 1}
	c := CredentialRecord{BindingID: b.ID, ID: "40000000-0000-4000-8000-000000000001", Version: 1, Status: "current", ExpiresAtNS: 200000000000}
	if err := InsertBindingCredential(ctx, tx, b, c); err != nil {
		t.Fatal(err)
	}
	if err := EnsureConversation(ctx, tx, "retained", "retained", "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err := InsertGrant(ctx, tx, Grant{Conversation: "retained", GrantVersion: 1, PeerAID: b.PeerID, PeerBID: "recipient", Direction: Bidirectional, MaxExchanges: 10, GrantedAt: "2026-01-01T00:00:00Z", Status: GrantActive}); err != nil {
		t.Fatal(err)
	}
	authored, incoming := "50000000-0000-4000-8000-000000000001", "50000000-0000-4000-8000-000000000002"
	for _, id := range []string{authored, incoming} {
		from, to := b.PeerID, "recipient"
		if id == incoming {
			from, to = to, from
		}
		e := Envelope{ID: id, Conversation: "retained", FromPeer: from, ToPeer: to, Text: "synthetic", GrantVersion: 1, CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z"}
		if err := InsertQueued(ctx, tx, e); err != nil {
			t.Fatal(err)
		}
		if id == authored {
			if err := RecordAuthenticatedEnvelope(ctx, tx, id, b.ID, 1); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return db, b, authored, incoming
}
func TestRevocationHoldsAuthoredWorkAndBarrierAtomically(t *testing.T) {
	db, b, authored, incoming := retainedWorkFixture(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	change, err := RevokeBinding(ctx, tx, RevocationRequest{BindingID: b.ID, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1, IncidentID: "60000000-0000-4000-8000-000000000001", NowNS: 110000000000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if change.BindingVersion != 2 || change.BarrierVersion != 1 {
		t.Fatalf("change=%+v", change)
	}
	for _, id := range []string{authored, incoming} {
		held, err := WorkHeld(ctx, tx, WorkRef{"envelope", id})
		if err != nil || held != (id == authored) {
			t.Fatalf("work=%s held=%v %v", id, held, err)
		}
	}
	after, err := ReadBinding(ctx, tx, b.ID)
	if err != nil || after.Status != "revoked" {
		t.Fatalf("binding=%+v %v", after, err)
	}
	credential, err := LatestCredential(ctx, tx, b.ID)
	if err != nil || credential.Status != "revoked" {
		t.Fatalf("credential=%+v %v", credential, err)
	}
	e, err := GetByID(ctx, tx, authored)
	if err != nil || e.State != Queued {
		t.Fatalf("held work changed state=%+v %v", e, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
func TestRevocationFailureRollsBackIdentityAndHolds(t *testing.T) {
	db, b, authored, _ := retainedWorkFixture(t)
	ctx := context.Background()
	if _, err := db.sql.Exec("CREATE TRIGGER fail_barrier BEFORE INSERT ON ingestion_barriers BEGIN SELECT RAISE(ABORT,'synthetic'); END"); err != nil {
		t.Fatal(err)
	}
	request := testRequest(t, testOperation)
	_, err := db.Coordinator().Execute(ctx, CommandPrincipal{ID: testPrincipal}, request, allowed, func(ctx context.Context, tx *sql.Tx) (CommandResult, error) {
		_, err := RevokeBinding(ctx, tx, RevocationRequest{BindingID: b.ID, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1, IncidentID: "60000000-0000-4000-8000-000000000001", NowNS: 110000000000}, nil)
		return CommandResult{}, err
	}, nil)
	if err == nil {
		t.Fatal("failed barrier committed revocation")
	}
	if err := db.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		after, err := ReadBinding(ctx, tx, b.ID)
		if err != nil {
			return err
		}
		if after != b {
			t.Errorf("partial binding mutation=%+v", after)
		}
		held, err := WorkHeld(ctx, tx, WorkRef{"envelope", authored})
		if held {
			t.Error("partial authored hold")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRevocationAcrossCredentialVersionsKeepsIndependentHolds(t *testing.T) {
	db, b, work, _ := retainedWorkFixture(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO ingestion_cursors(binding_id,source_id,cursor) VALUES(?,?,'before-revoke')", b.ID, "70000000-0000-4000-8000-000000000001"); err != nil {
		t.Fatal(err)
	}
	next := CredentialRecord{BindingID: b.ID, ID: "40000000-0000-4000-8000-000000000002", Version: 2, Status: "current", ExpiresAtNS: 300000000000}
	if err := RotateCredential(ctx, tx, b.ID, 1, 1, next); err != nil {
		t.Fatal(err)
	}
	first := "60000000-0000-4000-8000-000000000001"
	if _, err := RevokeBinding(ctx, tx, RevocationRequest{BindingID: b.ID, ExpectedBindingVersion: 2, ExpectedCredentialVersion: 2, IncidentID: first}, nil); err != nil {
		t.Fatal(err)
	}
	next.ID = "40000000-0000-4000-8000-000000000003"
	next.Version = 3
	if err := ReenrollCredential(ctx, tx, b.ID, 3, 2, next); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE ingestion_cursors SET cursor='later',cursor_version=2 WHERE binding_id=?", b.ID); err != nil {
		t.Fatal(err)
	}
	second := "60000000-0000-4000-8000-000000000002"
	if _, err := RevokeBinding(ctx, tx, RevocationRequest{BindingID: b.ID, ExpectedBindingVersion: 4, ExpectedCredentialVersion: 3, IncidentID: second}, nil); err != nil {
		t.Fatal(err)
	}
	var count int
	var cursor string
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM security_holds WHERE work_id=?", work).Scan(&count); err != nil || count != 2 {
		t.Fatalf("holds=%d %v", count, err)
	}
	if err := tx.QueryRowContext(ctx, "SELECT paused_cursor FROM ingestion_barriers WHERE binding_id=?", b.ID).Scan(&cursor); err != nil || cursor != "before-revoke" {
		t.Fatalf("barrier=%s %v", cursor, err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE security_holds SET status='released',hold_version=2 WHERE incident_id=?", first); err != nil {
		t.Fatal(err)
	}
	if held, err := WorkHeld(ctx, tx, WorkRef{"envelope", work}); err != nil || !held {
		t.Fatalf("second incident lost: %v %v", held, err)
	}
	var credential int64
	if err := tx.QueryRowContext(ctx, "SELECT credential_version FROM work_provenance WHERE work_id=?", work).Scan(&credential); err != nil || credential != 1 {
		t.Fatalf("original credential=%d %v", credential, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestReplyCarryPreservesOriginalProvenanceAndHold(t *testing.T) {
	db, b, work, originalID := retainedWorkFixture(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "UPDATE envelopes SET is_trusted_reply=1,in_reply_to=? WHERE id=?", originalID, work); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE envelopes SET state='acked' WHERE id=?", originalID); err != nil {
		t.Fatal(err)
	}
	if _, err := RevokeBinding(ctx, tx, RevocationRequest{BindingID: b.ID, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1, IncidentID: "60000000-0000-4000-8000-000000000001"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := SetGrantStatus(ctx, tx, "retained", 1, GrantSuperseded, "2026-01-02T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err := InsertGrant(ctx, tx, Grant{Conversation: "retained", GrantVersion: 2, PeerAID: b.PeerID, PeerBID: "recipient", Direction: Bidirectional, MaxExchanges: 10, GrantedAt: "2026-01-02T00:00:00Z", Status: GrantActive}); err != nil {
		t.Fatal(err)
	}
	if n, err := CarryForwardQueuedReplies(ctx, tx, "retained", 1, 2, "2026-01-02T00:00:00Z"); err != nil || n != 1 {
		t.Fatalf("carry=%d %v", n, err)
	}
	e, err := GetByID(ctx, tx, work)
	if err != nil || e.GrantVersion != 2 || !e.TrustedReply {
		t.Fatalf("reply=%+v %v", e, err)
	}
	var original, credential int64
	if err := tx.QueryRowContext(ctx, "SELECT original_grant_version,credential_version FROM work_provenance WHERE work_id=?", work).Scan(&original, &credential); err != nil || original != 1 || credential != 1 {
		t.Fatalf("provenance=%d/%d %v", original, credential, err)
	}
	if held, err := WorkHeld(ctx, tx, WorkRef{"envelope", work}); err != nil || !held {
		t.Fatalf("carry lost hold=%v %v", held, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
