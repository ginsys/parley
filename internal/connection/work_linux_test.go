package connection

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/store"
)

func readyCapability(t *testing.T, m *Manager, a Authentication) *Session {
	t.Helper()
	ctx := context.Background()
	s, err := m.Attach(ctx, acceptSocket(t, m), a, 0)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := m.BeginReadiness(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Acknowledge(ctx, s, probe); err != nil {
		t.Fatal(err)
	}
	return s
}
func workFixture(t *testing.T) (*Manager, *Session, Authentication) {
	t.Helper()
	m, auth, _ := attachmentFixture(t)
	s := readyCapability(t, m, auth)
	ctx := context.Background()
	var secret [32]byte
	secret[0] = 42
	b := store.BindingRecord{ID: "30000000-0000-4000-8000-000000000002", PeerID: "other", HostKind: "codex_cli", NamespaceID: "synthetic", SessionID: "other-session", ConnectorUID: uint32(os.Geteuid()), Status: "enabled", Version: 1}
	c := store.CredentialRecord{BindingID: b.ID, ID: "40000000-0000-4000-8000-000000000002", Version: 1, Status: "current", ExpiresAtNS: time.Unix(130, 0).UnixNano(), Verifier: sha256.Sum256(secret[:])}
	_, err := m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		return store.TransitionResult{Changed: true}, store.InsertBindingCredential(ctx, tx, b, c)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.New(m.store).Grant(ctx, controller.GrantParams{Conversation: "work", PeerAID: s.PeerID(), PeerBID: b.PeerID, Direction: store.Bidirectional, MaxExchanges: 5}); err != nil {
		t.Fatal(err)
	}
	other, err := NewAuthentication(c.ID, secret[:], NativeTuple{b.HostKind, b.NamespaceID, b.SessionID})
	if err != nil {
		t.Fatal(err)
	}
	return m, s, other
}
func TestAuthenticatedSendDerivesAuthorAndAllowsOfflineRecipient(t *testing.T) {
	m, s, _ := workFixture(t)
	ctx := context.Background()
	r := SendRequest{OperationID: targetID, Conversation: "work", Recipient: "other", Text: "synthetic"}
	receipt, err := m.Send(ctx, s, r)
	if err != nil || receipt.Result.Code != "" {
		t.Fatalf("send=%+v %v", receipt, err)
	}
	id := receipt.Result.Resources[0].ID
	if err := m.store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		e, err := store.GetByID(ctx, tx, id)
		if err != nil {
			return err
		}
		if e.FromPeer != s.PeerID() || e.TrustedReply || e.State != store.Queued {
			t.Errorf("acceptance=%+v", e)
		}
		var author string
		var cv int64
		err = tx.QueryRowContext(ctx, "SELECT binding_id,credential_version FROM work_provenance WHERE work_id=?", id).Scan(&author, &cv)
		if author != s.token.BindingID || cv != 1 {
			t.Errorf("provenance=%s/%d", author, cv)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	replay, err := m.Send(ctx, s, r)
	if err != nil || !replay.Replayed || replay.Result.Resources[0].ID != id {
		t.Fatalf("send replay=%+v %v", replay, err)
	}
	r.Text = "changed"
	if _, err := m.Send(ctx, s, r); err != store.OperationConflict {
		t.Fatalf("changed send=%v", err)
	}
	s.socket.Close()
	r.Text = "synthetic"
	if _, err := m.Send(ctx, s, r); err != store.AuthenticationFailed {
		t.Fatalf("closed capability replay=%v", err)
	}
}
func TestAuthenticatedIngestionWaitsThenCommitsExactlyOneReply(t *testing.T) {
	m, author, auth := workFixture(t)
	ctx := context.Background()
	recipient := readyCapability(t, m, auth)
	receipt, err := m.Send(ctx, author, SendRequest{OperationID: targetID, Conversation: "work", Recipient: recipient.PeerID(), Text: "synthetic original"})
	if err != nil || receipt.Result.Code != "" {
		t.Fatalf("send=%+v %v", receipt, err)
	}
	id := receipt.Result.Resources[0].ID
	_, err = m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		ok, err := store.TransitionToDispatching(ctx, tx, id, "2026-01-01T00:00:00Z")
		return store.TransitionResult{Changed: ok}, err
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ingestor, err := NewIngestor(IngestorConfig{Manager: m, Verify: func(context.Context, NativeTuple, Token, IngestRequest) error { return nil }, Origin: func(context.Context, NativeTuple, Token, string, string) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	source := "70000000-0000-4000-8000-000000000001"
	if err := ingestor.Initialize(ctx, recipient, source, "start"); err != nil {
		t.Fatal(err)
	}
	request := IngestRequest{Event: NativeEvent{ID: "native-turn-1", SourceID: source, Revision: "1", Before: "start", After: "one"}, Conversation: "work", Recipient: author.PeerID(), Text: fmt.Sprintf("```BRIDGE-REPLY\n{\"in_reply_to\":%q,\"to\":%q,\"text\":\"synthetic reply\"}\n```", id, author.PeerID())}
	pending, err := ingestor.Ingest(ctx, recipient, request)
	if err != nil || pending.Classification != "pending" {
		t.Fatalf("before settlement=%+v %v", pending, err)
	}
	_, err = m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		e, err := store.GetByID(ctx, tx, id)
		if err != nil {
			return store.TransitionResult{}, err
		}
		ok, err := store.SettleDispatch(ctx, tx, e, store.HandedOff, e.GrantVersion, "", "", "2026-01-01T00:00:00Z")
		return store.TransitionResult{Changed: ok}, err
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := ingestor.Ingest(ctx, recipient, request)
	if err != nil || accepted.Classification != "accepted" {
		t.Fatalf("accepted=%+v %v", accepted, err)
	}
	ingestor.config.Verify = func(context.Context, NativeTuple, Token, IngestRequest) error {
		t.Fatal("terminal replay or conflict consulted source")
		return store.HostUnverified
	}
	replay, err := ingestor.Ingest(ctx, recipient, request)
	if err != nil || !replay.Replayed || replay.EnvelopeID != accepted.EnvelopeID {
		t.Fatalf("ingest replay=%+v %v", replay, err)
	}
	request.Text += "\nchanged"
	if _, err := ingestor.Ingest(ctx, recipient, request); err != store.EventConflict {
		t.Fatalf("changed event=%v", err)
	}
	if err := m.store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		original, err := store.GetByID(ctx, tx, id)
		if err != nil {
			return err
		}
		reply, err := store.GetByID(ctx, tx, accepted.EnvelopeID)
		if err != nil {
			return err
		}
		if original.State != store.Acked || !reply.TrustedReply || reply.FromPeer != recipient.PeerID() {
			t.Errorf("original=%+v reply=%+v", original, reply)
		}
		var count int
		var cv int64
		var cursor string
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM envelopes").Scan(&count); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, "SELECT credential_version FROM work_provenance WHERE work_id=?", reply.ID).Scan(&cv); err != nil {
			return err
		}
		err = tx.QueryRowContext(ctx, "SELECT cursor FROM ingestion_cursors WHERE binding_id=?", recipient.token.BindingID).Scan(&cursor)
		if count != 2 || cv != recipient.token.CredentialVersion || cursor != "one" {
			t.Errorf("count=%d cv=%d cursor=%s", count, cv, cursor)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRejectedSendPersistsRecipientExpiry(t *testing.T) {
	m, author, auth := workFixture(t)
	other := readyCapability(t, m, auth)
	ctx := context.Background()
	m.now = func() time.Time { return time.Unix(131, 0) }
	request := SendRequest{OperationID: targetID, Conversation: "work", Recipient: "other", Text: "synthetic"}
	receipt, err := m.Send(ctx, author, request)
	if err != nil || receipt.Result.Code != store.BindingUnavailable {
		t.Fatalf("expiry rejection=%+v %v", receipt, err)
	}
	if other.Context().Err() == nil {
		t.Fatal("expired recipient retained live capability")
	}
	if err := m.store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		c, err := store.LatestCredential(ctx, tx, other.token.BindingID)
		if err != nil {
			return err
		}
		if c.Status != "expired" {
			t.Errorf("observed expiry not durable: %+v", c)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	m.now = func() time.Time { return time.Unix(110, 0) }
	request.OperationID = "80000000-0000-4000-8000-000000000001"
	receipt, err = m.Send(ctx, author, request)
	if err != nil || receipt.Result.Code != store.BindingUnavailable {
		t.Fatalf("clock reversal revived credential=%+v %v", receipt, err)
	}
}
func readyIngestion(t *testing.T) (*Manager, *Session, *Session, *Ingestor, IngestRequest, string) {
	t.Helper()
	m, author, auth := workFixture(t)
	recipient := readyCapability(t, m, auth)
	ctx := context.Background()
	receipt, err := m.Send(ctx, author, SendRequest{OperationID: targetID, Conversation: "work", Recipient: recipient.PeerID(), Text: "synthetic"})
	if err != nil || receipt.Result.Code != "" {
		t.Fatalf("send=%+v %v", receipt, err)
	}
	id := receipt.Result.Resources[0].ID
	_, err = m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		return store.TransitionResult{Changed: true}, store.SetState(ctx, tx, id, store.Queued, store.HandedOff, "1970-01-01T00:01:50Z")
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ingestor, err := NewIngestor(IngestorConfig{Manager: m, Verify: func(context.Context, NativeTuple, Token, IngestRequest) error { return nil }, Origin: func(context.Context, NativeTuple, Token, string, string) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	request := IngestRequest{Event: NativeEvent{ID: "native-turn-1", SourceID: "70000000-0000-4000-8000-000000000001", Revision: "1", Before: "start", After: "one"}, Conversation: "work", Recipient: author.PeerID(), Text: fmt.Sprintf("```BRIDGE-REPLY\n{\"in_reply_to\":%q,\"to\":%q,\"text\":\"reply\"}\n```", id, author.PeerID())}
	if err := ingestor.Initialize(ctx, recipient, request.Event.SourceID, request.Event.Before); err != nil {
		t.Fatal(err)
	}
	return m, author, recipient, ingestor, request, id
}
func TestIngestionRejectsInvalidUTF8BeforeHashing(t *testing.T) {
	_, _, recipient, ingestor, request, _ := readyIngestion(t)
	for _, text := range []string{string([]byte{0x80}), string([]byte{0x81})} {
		request.Text = text
		if _, err := ingestor.Ingest(context.Background(), recipient, request); err != store.InvalidRequest {
			t.Fatalf("invalid source=%v", err)
		}
	}
}
func TestCancelledOriginalDoesNotPinCursor(t *testing.T) {
	m, author, recipient, ingestor, request, id := readyIngestion(t)
	ctx := context.Background()
	lifecycle := testLifecycle(t, m)
	actor := store.CommandPrincipal{ID: adminID}
	revoke, err := lifecycle.Revoke(ctx, actor, BindingLifecycleRequest{OperationID: "80000000-0000-4000-8000-000000000001", BindingID: author.token.BindingID, ExpectedBindingVersion: 1, ExpectedCredentialVersion: 1})
	if err != nil || revoke.Result.Code != "" {
		t.Fatalf("revoke=%+v %v", revoke, err)
	}
	disposition, err := lifecycle.HoldDisposition(ctx, actor, HoldDispositionRequest{OperationID: "80000000-0000-4000-8000-000000000002", Work: store.WorkRef{Kind: "envelope", ID: id}, IncidentID: revoke.Result.Resources[1].ID, ExpectedHoldVersion: 1, Action: "cancel", Reason: DispositionReason{Code: "compromise"}})
	if err != nil || disposition.Result.Code != "" {
		t.Fatalf("cancel=%+v %v", disposition, err)
	}
	held, err := ingestor.Ingest(ctx, recipient, request)
	if err != nil || held.Classification != "held" {
		t.Fatalf("cancelled original=%+v %v", held, err)
	}
	request.Event.ID = "native-turn-2"
	request.Event.Before = "one"
	request.Event.After = "two"
	request.Text = "ordinary conversation"
	result, err := ingestor.Ingest(ctx, recipient, request)
	if err != nil || result.Classification != "no_marker" {
		t.Fatalf("cursor remained pinned=%+v %v", result, err)
	}
	if err := m.store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		e, err := store.GetByID(ctx, tx, id)
		if err == nil && e.State != store.HandedOff {
			t.Errorf("cancelled original ACKed: %+v", e)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
func TestIngestionVerifierCancelledWithSession(t *testing.T) {
	_, _, recipient, ingestor, request, _ := readyIngestion(t)
	started := make(chan struct{})
	done := make(chan error, 1)
	ingestor.config.Verify = func(ctx context.Context, _ NativeTuple, _ Token, _ IngestRequest) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	go func() { _, err := ingestor.Ingest(context.Background(), recipient, request); done <- err }()
	<-started
	recipient.socket.Close()
	select {
	case err := <-done:
		if err != store.HostUnverified {
			t.Fatalf("cancelled verifier=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("source verifier outlived connection")
	}
}

func TestFailedExpiryPersistenceKeepsOnlyExactCredentialDenied(t *testing.T) {
	m, author, auth := workFixture(t)
	other := readyCapability(t, m, auth)
	ctx := context.Background()
	_, err := m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		_, err := tx.ExecContext(ctx, "CREATE TRIGGER fail_expiry BEFORE UPDATE OF status ON credentials WHEN NEW.status='expired' BEGIN SELECT RAISE(ABORT,'synthetic'); END")
		return store.TransitionResult{Changed: true}, err
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.now = func() time.Time { return time.Unix(131, 0) }
	request := SendRequest{OperationID: targetID, Conversation: "work", Recipient: "other", Text: "synthetic"}
	if _, err := m.Send(ctx, author, request); err != store.TemporarilyUnavailable {
		t.Fatalf("injected expiry persistence=%v", err)
	}
	m.now = func() time.Time { return time.Unix(110, 0) }
	if err := m.Heartbeat(ctx, author); err != nil {
		t.Fatalf("expiry failure blocked unrelated principal: %v", err)
	}
	// The exact observed credential remains denied while storage is unavailable.
	request.OperationID = "80000000-0000-4000-8000-000000000001"
	if _, err := m.Send(ctx, author, request); err != store.TemporarilyUnavailable {
		t.Fatalf("failed observation lost after clock reversal: %v", err)
	}
	if !m.store.CredentialExpiryObserved(other.socket.credentialID) {
		t.Fatal("lost failed expiry observation")
	}
	_, err = m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		_, err := tx.ExecContext(ctx, "DROP TRIGGER fail_expiry")
		return store.TransitionResult{Changed: true}, err
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.OperationID = "80000000-0000-4000-8000-000000000002"
	receipt, err := m.Send(ctx, author, request)
	if err != nil || receipt.Result.Code != store.BindingUnavailable {
		t.Fatalf("expiry retry=%+v %v", receipt, err)
	}
	if other.Context().Err() == nil {
		t.Fatal("persisted expiry retained old capability")
	}
	if err := m.store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		c, err := store.ReadCredential(ctx, tx, other.socket.credentialID)
		if err == nil && c.Status != "expired" {
			t.Errorf("recovered storage did not retain expiry: %+v", c)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticationExpiryObservationSurvivesWriteFailure(t *testing.T) {
	for _, method := range []string{"heartbeat", "inspect", "attach"} {
		t.Run(method, func(t *testing.T) {
			m, auth, now := attachmentFixture(t)
			ctx := context.Background()
			var s *Session
			var socket *Socket
			if method != "attach" {
				socket = acceptSocket(t, m)
				var err error
				s, err = m.Attach(ctx, socket, auth, 0)
				if err != nil {
					t.Fatal(err)
				}
				for _, second := range []int64{130, 150, 170, 190} {
					*now = time.Unix(second, 0)
					if err := m.Heartbeat(ctx, s); err != nil {
						t.Fatal(err)
					}
				}
			}
			_, err := m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
				_, err := tx.ExecContext(ctx, "CREATE TRIGGER fail_expiry BEFORE UPDATE OF status ON credentials WHEN NEW.status='expired' BEGIN SELECT RAISE(ABORT,'synthetic'); END")
				return store.TransitionResult{Changed: true}, err
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			*now = time.Unix(201, 0)
			if method == "attach" {
				socket = acceptSocket(t, m)
			}
			attempt := func() error {
				switch method {
				case "heartbeat":
					return m.Heartbeat(ctx, s)
				case "inspect":
					_, err := m.Inspect(ctx, socket, auth)
					return err
				default:
					_, err := m.Attach(ctx, socket, auth, 0)
					return err
				}
			}
			if err := attempt(); err != store.TemporarilyUnavailable {
				t.Fatalf("failed expiry=%v", err)
			}
			*now = time.Unix(199, 0)
			if err := attempt(); err != store.TemporarilyUnavailable {
				t.Fatalf("observation lost after earlier time=%v", err)
			}
			_, err = m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
				_, err := tx.ExecContext(ctx, "DROP TRIGGER fail_expiry")
				return store.TransitionResult{Changed: true}, err
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := attempt(); err != store.AuthenticationFailed {
				t.Fatalf("terminal expiry=%v", err)
			}
			if m.store.CredentialExpiryObserved(auth.credentialID) {
				t.Fatal("persisted observation retained unnecessary deny entry")
			}
		})
	}
}

func TestAuthenticatedIngestionCursorFailureRollsBackACKReplyAndEvidence(t *testing.T) {
	m, _, recipient, ingestor, request, id := readyIngestion(t)
	ctx := context.Background()
	_, err := m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		_, err := tx.ExecContext(ctx, "CREATE TRIGGER fail_ingestion_cursor BEFORE UPDATE ON ingestion_cursors BEGIN SELECT RAISE(ABORT,'synthetic'); END")
		return store.TransitionResult{Changed: true}, err
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor.Ingest(ctx, recipient, request); err != store.TemporarilyUnavailable {
		t.Fatalf("cursor fault=%v", err)
	}
	if err := m.store.Coordinator().Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		e, err := store.GetByID(ctx, tx, id)
		if err != nil {
			return err
		}
		if e.State != store.HandedOff {
			t.Errorf("partial ACK=%s", e.State)
		}
		var count int
		for query, want := range map[string]int{"SELECT count(*) FROM envelopes": 1, "SELECT count(*) FROM work_provenance": 1, "SELECT count(*) FROM ingestion_evidence": 0} {
			if err := tx.QueryRowContext(ctx, query).Scan(&count); err != nil {
				return err
			}
			if count != want {
				t.Errorf("%s=%d", query, count)
			}
		}
		var cursor string
		if err := tx.QueryRowContext(ctx, "SELECT cursor FROM ingestion_cursors WHERE binding_id=?", recipient.token.BindingID).Scan(&cursor); err != nil {
			return err
		}
		if cursor != "start" {
			t.Errorf("partial cursor=%s", cursor)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	_, err = m.store.Coordinator().Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		_, err := tx.ExecContext(ctx, "DROP TRIGGER fail_ingestion_cursor")
		return store.TransitionResult{Changed: true}, err
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := ingestor.Ingest(ctx, recipient, request); err != nil || result.Classification != "accepted" {
		t.Fatalf("retry=%+v %v", result, err)
	}
}

func TestIngestionInitializationPersistsExpiryObservedDuringAuthorization(t *testing.T) {
	m, _, auth := workFixture(t)
	recipient := readyCapability(t, m, auth)
	now := time.Unix(110, 0)
	m.now = func() time.Time { return now }
	// Cross the recipient's 130s credential deadline after sessionTransition's
	// identity check, while its 30s socket liveness window is still valid.
	m.guard = func(context.Context, *sql.Tx, string) error { now = time.Unix(131, 0); return nil }
	ingestor, err := NewIngestor(IngestorConfig{Manager: m, Verify: func(context.Context, NativeTuple, Token, IngestRequest) error { return nil }, Origin: func(context.Context, NativeTuple, Token, string, string) error {
		t.Fatal("expired origin consulted")
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ingestor.Initialize(context.Background(), recipient, "70000000-0000-4000-8000-000000000001", "start"); err != store.BindingUnavailable {
		t.Fatalf("initialization=%v", err)
	}
	if err := m.store.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		c, err := store.LatestCredential(ctx, tx, recipient.token.BindingID)
		if err != nil {
			return err
		}
		if c.Status != "expired" {
			t.Errorf("initialization lost terminal expiry: %s", c.Status)
		}
		var cursors int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM ingestion_cursors").Scan(&cursors); err != nil {
			return err
		}
		if cursors != 0 {
			t.Errorf("expired initialization wrote %d cursors", cursors)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if recipient.Context().Err() == nil {
		t.Error("expired initialization retained live capability")
	}
	now = time.Unix(110, 0)
	m.guard = func(context.Context, *sql.Tx, string) error { return nil }
	if err := m.Heartbeat(context.Background(), recipient); err != store.AuthenticationFailed {
		t.Fatalf("backward wall revived initialization credential: %v", err)
	}
}
