package dispatch_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	bridgefixture "github.com/ginsys/parley/internal/testfixture/bridge"
	"reflect"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/bridgetext"
	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/store"
	"github.com/ginsys/parley/internal/testfixture/identity"
	"github.com/google/uuid"
)

// Insert through the storage primitives to model historical data, not new enrollment.
func legacyIdentifiers(t *testing.T, c, a, b string) (*store.DB, store.Envelope) {
	t.Helper()
	ctx := context.Background()
	db := openTestDB(t)
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := store.EnsureConversation(ctx, tx, c, c, now); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertGrant(ctx, tx, store.Grant{Conversation: c, GrantVersion: 1, PeerAID: a, PeerBID: b, Direction: store.Bidirectional, MaxExchanges: 3, GrantedAt: now}); err != nil {
		t.Fatal(err)
	}
	e := store.Envelope{ID: "legacy", Conversation: c, FromPeer: a, ToPeer: b, Text: "payload", GrantVersion: 1, State: store.Queued, CreatedAt: now, UpdatedAt: now}
	if err := store.InsertQueued(ctx, tx, e); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return db, e
}

func identifierState(t *testing.T, db *store.DB, c string) (*store.Grant, *store.Envelope, int) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	g, err := store.CurrentGrant(ctx, tx, c)
	if err != nil {
		t.Fatal(err)
	}
	e, err := store.GetByID(ctx, tx, "legacy")
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM envelopes").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return g, e, n
}

func assertCompatibleBindings(t *testing.T, db *store.DB) {
	t.Helper()
	tx, err := db.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	rows, err := tx.Query("SELECT peer_id FROM bindings")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var peer string
		if err := rows.Scan(&peer); err != nil {
			t.Fatal(err)
		}
		if bridgetext.ValidateMetadata(peer) != nil {
			t.Errorf("fixture enrolled incompatible historical peer %q", peer)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestIncompatibleHistoryRejectsNewWorkWithoutMutation(t *testing.T) {
	for _, bad := range []string{"a\xff", "a\xfe", "café", "a\ufffd", "\x7f", "   "} {
		for field := 0; field < 3; field++ {
			t.Run(fmt.Sprintf("%x/%d", bad, field), func(t *testing.T) {
				ids := []string{"c", "a", "b"}
				ids[field] = bad
				c, a, b := ids[0], ids[1], ids[2]
				db, e := legacyIdentifiers(t, c, a, b)
				ctx := context.Background()
				beforeG, beforeE, beforeN := identifierState(t, db, c)
				tr := newFakeTransport()
				bridge := bridgefixture.New(t, db, tr)
				if _, err := controller.New(db).Renew(ctx, controller.RenewParams{Conversation: c, MaxExchanges: 5}); !errors.Is(err, bridgetext.ErrInvalidMetadata) {
					t.Errorf("renew: %v", err)
				}
				// Keep the private author valid; place each incompatible key in
				// the real request so fixture enrollment cannot mask rejection.
				author, recipient := a, b
				if field == 1 {
					author, recipient = b, a
				}
				session, err := bridge.Identity.Session(author)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := bridge.AuthenticatedBridge.Send(ctx, session, connection.SendRequest{OperationID: uuid.NewString(), Conversation: c, Recipient: recipient, Text: "new"}); !errors.Is(err, store.InvalidRequest) {
					t.Errorf("send: %v", err)
				}
				outcome, err := bridge.DispatchOutcome(ctx, e.ID)
				if !errors.Is(err, store.InvalidRequest) || outcome.State != store.Queued || outcome.Attempted || outcome.ErrorCode != "incompatible_identifier" || outcome.ErrorDetail == "" {
					t.Errorf("dispatch: %+v, %v", outcome, err)
				}
				if len(tr.delivered) != 0 {
					t.Error("transport invoked")
				}
				assertCompatibleBindings(t, db)
				afterG, afterE, afterN := identifierState(t, db, c)
				if !reflect.DeepEqual(beforeG, afterG) || !reflect.DeepEqual(beforeE, afterE) || beforeN != afterN {
					t.Fatal("rejection changed historical state")
				}
				if _, err := controller.New(db).Revoke(ctx, c); err != nil {
					t.Fatalf("exact historical revocation: %v", err)
				}
				tx, err := db.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				var storedC, storedA, storedB, status string
				if err := tx.QueryRowContext(ctx, "SELECT conversation,peer_a_id,peer_b_id,status FROM grants").Scan(&storedC, &storedA, &storedB, &status); err != nil {
					t.Fatal(err)
				}
				if storedC != c || storedA != a || storedB != b || status != "revoked" {
					t.Fatal("revocation changed identity or failed to revoke")
				}
			})
		}
	}
}

func TestIncompatibleReplyCannotAcknowledgeOriginal(t *testing.T) {
	for field := 0; field < 3; field++ {
		t.Run(fmt.Sprint(field), func(t *testing.T) {
			ids := []string{"c", "a", "b"}
			ids[field] = "a\xff"
			c, a, b := ids[0], ids[1], ids[2]
			db, e := legacyIdentifiers(t, c, a, b)
			ctx := context.Background()
			tx, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, "UPDATE envelopes SET state='handed_off' WHERE id=?", e.ID); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			beforeG, beforeE, beforeN := identifierState(t, db, c)
			payload, err := json.Marshal(map[string]string{"to": "a", "in_reply_to": e.ID, "text": "reply"})
			if err != nil {
				t.Fatal(err)
			}
			turn := "```BRIDGE-REPLY\n" + string(payload) + "\n```"
			if _, err := bridgefixture.IngestTurn(t, ctx, db, "c", "b", "a", turn); !errors.Is(err, store.InvalidRequest) {
				t.Errorf("ingest: %v", err)
			}
			assertCompatibleBindings(t, db)
			afterG, afterE, afterN := identifierState(t, db, c)
			if !reflect.DeepEqual(beforeG, afterG) || !reflect.DeepEqual(beforeE, afterE) || beforeN != afterN {
				t.Fatal("reply rejection acknowledged or queued work")
			}
		})
	}
}

func TestCompatibleConversationWorksAlongsideIncompatibleHistory(t *testing.T) {
	db, _ := legacyIdentifiers(t, "old\xff", "a\xff", "b")
	ctx := context.Background()
	c, a, b := " c | [review] ", " a! ", "a!"
	g, err := controller.New(db).Grant(ctx, controller.GrantParams{Conversation: c, PeerAID: a, PeerBID: b, Direction: store.Bidirectional, MaxExchanges: 2})
	if err != nil {
		t.Fatal(err)
	}
	if g.Conversation != c || g.PeerAID != a || g.PeerBID != b {
		t.Fatal("accepted keys changed")
	}
	tr := newFakeTransport()
	bridge := bridgefixture.New(t, db, tr)
	e, err := bridge.Send(ctx, c, a, b, "世界", nil)
	if err != nil {
		t.Fatal(err)
	}
	if e.Conversation != c || e.FromPeer != a || e.ToPeer != b || e.Text != "世界" {
		t.Fatal("accepted envelope changed")
	}
	if state, err := bridge.Dispatch(ctx, e.ID); err != nil || state != store.HandedOff {
		t.Fatalf("dispatch: %s %v", state, err)
	}
	if len(tr.delivered) != 1 {
		t.Fatal("unaffected delivery did not run exactly once")
	}
}

func TestSyntheticSessionRejectsIncompatiblePeerBeforeStorage(t *testing.T) {
	db := openTestDB(t)
	f := identity.For(t, db)
	for _, peer := range []string{"a\xff", "café", "   "} {
		if _, err := f.Session(peer); !errors.Is(err, identity.ErrInvalidPeer) {
			t.Errorf("expected distinct fixture validation for %q: %v", peer, err)
		}
	}
	tx, err := db.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var bindings, credentials int
	if err := tx.QueryRow("SELECT (SELECT count(*) FROM bindings), (SELECT count(*) FROM credentials)").Scan(&bindings, &credentials); err != nil {
		t.Fatal(err)
	}
	if bindings != 0 || credentials != 0 {
		t.Fatalf("invalid fixture mutated identity: bindings=%d credentials=%d", bindings, credentials)
	}
}
