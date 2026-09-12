package store

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
)

func seedVersionFive(t *testing.T) (*sql.DB, string) {
	t.Helper()
	seed, path := seedVersionFour(t)
	tx, err := seed.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := addConnectionRegistry(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("PRAGMA user_version=5"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return seed, path
}
func seedLegacyWork(t *testing.T, seed *sql.DB) []string {
	t.Helper()
	ctx := context.Background()
	tx, err := seed.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	sender := strings.Repeat("legacy", 60) + "\n☃"
	if _, err := tx.Exec(`INSERT INTO grants(conversation,grant_version,peer_a_id,peer_b_id,direction,max_exchanges,exchanges_used,granted_at,status) VALUES('old',1,?,'receiver','bidirectional',10,2,'2026-01-01T00:00:00Z','active')`, sender); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, state := range []EnvelopeState{Queued, Dispatching, HandedOff, Acked, Failed, Cancelled, Uncertain} {
		id := " legacy " + string(state) + "☃\n"
		ids = append(ids, id)
		e := Envelope{ID: id, Conversation: "old", FromPeer: sender, ToPeer: "receiver", Text: "synthetic historical body", GrantVersion: 1, CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z", TrustedReply: state == Queued}
		if err := InsertQueued(ctx, tx, e); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec("UPDATE envelopes SET state=?,dispatch_attempt=3,error_code='synthetic',error_detail='retained' WHERE id=?", state, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return ids
}
func legacySnapshot(t *testing.T, seed *sql.DB, ids []string) []Envelope {
	t.Helper()
	tx, err := seed.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var values []Envelope
	for _, id := range ids {
		e, err := GetByID(context.Background(), tx, id)
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, *e)
	}
	return values
}
func TestRetentionMigrationPreservesLegacyEvidenceAndReruns(t *testing.T) {
	seed, path := seedVersionFive(t)
	ids := seedLegacyWork(t, seed)
	before := legacySnapshot(t, seed, ids)
	var incident string
	for attempt := 0; attempt < 2; attempt++ {
		db, err := Open(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
		if got := legacySnapshot(t, seed, ids); !reflect.DeepEqual(got, before) {
			t.Fatal("migration changed delivery evidence")
		}
		var got string
		if err := seed.QueryRow("SELECT incident_id FROM migration_incidents").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if attempt == 0 {
			incident = got
		} else if got != incident {
			t.Fatal("rerun replaced migration incident")
		}
		var tagged, held, identities int
		if err := seed.QueryRow("SELECT count(*) FROM work_provenance WHERE provenance='legacy' AND binding_id IS NULL AND credential_version IS NULL").Scan(&tagged); err != nil {
			t.Fatal(err)
		}
		if err := seed.QueryRow("SELECT count(*) FROM migration_quarantine WHERE status='held'").Scan(&held); err != nil {
			t.Fatal(err)
		}
		if err := seed.QueryRow("SELECT (SELECT count(*) FROM bindings)+(SELECT count(*) FROM credentials)").Scan(&identities); err != nil {
			t.Fatal(err)
		}
		if tagged != 7 || held != 4 || identities != 0 {
			t.Fatalf("tagged=%d held=%d invented identities=%d", tagged, held, identities)
		}
		var used int
		if err := seed.QueryRow("SELECT exchanges_used FROM grants WHERE conversation='old'").Scan(&used); err != nil || used != 2 {
			t.Fatalf("budget=%d %v", used, err)
		}
	}
	for _, statement := range []string{
		"UPDATE work_provenance SET provenance='authenticated'", "DELETE FROM work_provenance", "UPDATE migration_incidents SET incident_id='forged'", "DELETE FROM migration_quarantine",
	} {
		if _, err := seed.Exec(statement); err == nil {
			t.Fatalf("immutable evidence changed: %s", statement)
		}
	}
}
func TestRetentionMigrationFailureIsAtomic(t *testing.T) {
	for _, kind := range []string{"late_ddl_collision", "invalid_legacy_id"} {
		t.Run(kind, func(t *testing.T) {
			seed, path := seedVersionFive(t)
			ids := seedLegacyWork(t, seed)
			if kind == "late_ddl_collision" {
				if _, err := seed.Exec("CREATE TABLE recovery_incidents(collision TEXT)"); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := seed.Exec("UPDATE envelopes SET id=NULL WHERE id=?", ids[0]); err != nil {
					t.Fatal(err)
				}
			}
			before := schemaSnapshot(t, seed)
			if db, err := Open(context.Background(), path); err == nil {
				db.Close()
				t.Fatal("invalid migration succeeded")
			}
			if !reflect.DeepEqual(before, schemaSnapshot(t, seed)) {
				t.Fatal("partial migration schema")
			}
			var version int
			if err := seed.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 5 {
				t.Fatalf("version=%d err=%v", version, err)
			}
		})
	}
}

func TestQuarantineIdentityVersionAndCancellationGuards(t *testing.T) {
	for _, kind := range []string{"retarget", "skip_version", "revive"} {
		t.Run(kind, func(t *testing.T) {
			seed, path := seedVersionFive(t)
			ids := seedLegacyWork(t, seed)
			db, err := Open(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var statement string
			var args []any
			switch kind {
			case "retarget":
				statement = "UPDATE migration_quarantine SET work_id=? WHERE work_id=?"
				args = []any{ids[3], ids[0]}
			case "skip_version":
				statement = "UPDATE migration_quarantine SET quarantine_version=quarantine_version+2 WHERE work_id=?"
				args = []any{ids[0]}
			case "revive":
				if _, err := seed.Exec("UPDATE migration_quarantine SET status='cancelled',quarantine_version=2 WHERE work_id=?", ids[0]); err != nil {
					t.Fatal(err)
				}
				statement = "UPDATE migration_quarantine SET status='held',quarantine_version=3 WHERE work_id=?"
				args = []any{ids[0]}
			}
			if _, err := seed.Exec(statement, args...); err == nil {
				t.Fatalf("accepted unsafe quarantine %s", kind)
			}
		})
	}
}

func TestReplacementCannotEraseRetainedEvidence(t *testing.T) {
	db := commandDB(t)
	ctx := context.Background()
	req := testRequest(t, testOperation)
	if _, err := db.Coordinator().Execute(ctx, CommandPrincipal{ID: testPrincipal}, req, allowed, func(context.Context, *sql.Tx) (CommandResult, error) { return CommandResult{}, nil }, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.sql.Exec(`INSERT OR REPLACE INTO operation_results SELECT principal_id,operation_id,operation_kind,request_digest,'{"code":"forbidden","resources":null}' FROM operation_results`); err == nil {
		t.Fatal("replacement erased retained operation result")
	}
}
