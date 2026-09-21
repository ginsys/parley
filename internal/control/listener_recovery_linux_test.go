//go:build linux

package control

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ginsys/parley/internal/connection"
	"github.com/ginsys/parley/internal/recovery"
	"github.com/ginsys/parley/internal/store"
)

// Review 5259801666 (comment 4056340196): the recovery hooks a membership
// mutation runs through (internal/recovery.Service.before/after) serialize
// marker I/O under a mutex and write markers through the Markers interface.
// Neither the mutex wait nor a marker write is bounded by the request
// context -- after()'s own five-second background context bounds only the
// flush it performs once the mutex is held, not the wait for it -- and Go
// cannot cancel an arbitrary filesystem write anyway. The tests below prove
// that the request path still answers within RequestDeadline while that
// finalization continues, owned and drained, in the background.

// gatedMarkers is memoryMarkersForTest with test-controlled blocking on List
// and Put. A blocked call ignores its context on purpose: it stands in for a
// marker-directory write that Go cannot interrupt.
type gatedMarkers struct {
	*memoryMarkersForTestType
	blockList, blockPut atomic.Bool
	listEntered         chan struct{}
	putEntered          chan struct{}
	listOnce, putOnce   sync.Once
	release             chan struct{}
}

func newGatedMarkers() *gatedMarkers {
	return &gatedMarkers{
		memoryMarkersForTestType: memoryMarkersForTest(),
		listEntered:              make(chan struct{}),
		putEntered:               make(chan struct{}),
		release:                  make(chan struct{}),
	}
}

func (m *gatedMarkers) List(ctx context.Context) ([]recovery.Marker, error) {
	if m.blockList.Load() {
		m.listOnce.Do(func() { close(m.listEntered) })
		<-m.release
	}
	return m.memoryMarkersForTestType.List(ctx)
}

func (m *gatedMarkers) Put(ctx context.Context, marker recovery.Marker) error {
	if m.blockPut.Load() {
		m.putOnce.Do(func() { close(m.putEntered) })
		<-m.release
	}
	return m.memoryMarkersForTestType.Put(ctx, marker)
}

// installListenerRecovery installs a real recovery.Service on the fixture's
// writer -- the same composition cmd/parleyd serve builds -- so every
// coordinator call the listener makes runs the real Before/Time/After hooks.
// FailStop failing the test keeps the fail-stop requirement observable.
func installListenerRecovery(t *testing.T, fx *listenerFixture, markers recovery.Markers, now func() time.Time) *recovery.Service {
	t.Helper()
	svc, err := recovery.New(context.Background(), recovery.Config{
		Store: fx.db, Markers: markers, Now: now,
		FailStop: func() { t.Error("unexpected fail-stop") },
	})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

// negotiatedConn dials the fixture and completes server.hello on one
// connection, returning it with the reader every later reply comes from.
func negotiatedConn(t *testing.T, fx *listenerFixture) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := connection.DialTrustedServer(context.Background(), fx.socketPath, fx.serverUID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(RequestDeadline + restartTeardownWait)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	if _, err := conn.Write([]byte(`{"jsonrpc":"2.0","id":"1","method":"server.hello","params":{"protocol":"parley-control/1"}}` + "\n")); err != nil {
		t.Fatal(err)
	}
	if hello := readReply(t, br); hello["error"] != nil {
		t.Fatalf("hello failed: %#v", hello)
	}
	return conn, br
}

func readReply(t *testing.T, br *bufio.Reader) map[string]any {
	t.Helper()
	line, err := br.ReadBytes('\n')
	if err != nil {
		t.Fatalf("no reply: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(line, &decoded); err != nil {
		t.Fatalf("reply %q: %v", line, err)
	}
	return decoded
}

// replyCode returns a reply's domain error code, or "" for a success.
func replyCode(resp map[string]any) string {
	wireErr, _ := resp["error"].(map[string]any)
	data, _ := wireErr["data"].(map[string]any)
	code, _ := data["code"].(string)
	return code
}

func revokeLine(id, op, conv string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":"membership.revoke","params":{"operation_id":%q,"conversation":%q,"expected_grant_version":"1"}}`+"\n", id, op, conv)
}

func enrollLine(id, op, conv string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":"membership.enroll","params":{"operation_id":%q,"conversation":%q,"expected_grant_version":"0","members":[{"peer_id":"peer-a","role":"member"},{"peer_id":"peer-b","role":"member"}],"policy":{"kind":"open"},"max_exchanges":"5"}}`+"\n", id, op, conv)
}

// expectBoundedReply reads one reply and checks its correlation ID, domain
// code and that it arrived about RequestDeadline after started: not early
// (the deadline, not something else, bounded it) and not late (the request
// path stopped waiting). The lower bound tolerates handlerContextLead.
func expectBoundedReply(t *testing.T, br *bufio.Reader, started time.Time, id, code string) map[string]any {
	t.Helper()
	resp := readReply(t, br)
	elapsed := time.Since(started)
	if resp["id"] != id || replyCode(resp) != code {
		t.Fatalf("reply for %s: want %s, got %#v", id, code, resp)
	}
	if elapsed < RequestDeadline-time.Second || elapsed > RequestDeadline+2*time.Second {
		t.Fatalf("reply %s after %v, want about RequestDeadline (%v) from arrival", id, elapsed, RequestDeadline)
	}
	return resp
}

func operationResultCount(t *testing.T, db *store.DB, op string) int {
	t.Helper()
	var count int
	if err := db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT count(*) FROM operation_results WHERE operation_id=?", op).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

// handshakeClock is a recovery clock the test pauses at chosen calls: while
// armed, each Now hands the test a proceed channel and waits for it to be
// closed. Abort -- idempotent, registered as cleanup when the clock is
// created -- lets every paused or later call return at once, so a test that
// fails between two handshakes cannot strand the handler inside Now (and with
// it serveSession's drain and the fixture's Listener.Wait).
type handshakeClock struct {
	armed     atomic.Bool
	calls     chan chan struct{}
	abort     chan struct{}
	abortOnce sync.Once
}

func newHandshakeClock(t *testing.T) *handshakeClock {
	c := &handshakeClock{calls: make(chan chan struct{}), abort: make(chan struct{})}
	t.Cleanup(c.Abort)
	return c
}

func (c *handshakeClock) Abort() { c.abortOnce.Do(func() { close(c.abort) }) }

func (c *handshakeClock) Now() time.Time {
	if c.armed.Load() {
		proceed := make(chan struct{})
		select {
		case c.calls <- proceed:
			select {
			case <-proceed:
			case <-c.abort:
			}
		case <-c.abort:
		}
	}
	return time.Unix(1000, 0)
}

// next returns the proceed channel of the next paused call, failing within
// restartTeardownWait if the clock is never consulted.
func (c *handshakeClock) next(t *testing.T) chan struct{} {
	t.Helper()
	select {
	case p := <-c.calls:
		return p
	case <-time.After(restartTeardownWait):
		t.Fatal("the recovery clock was not consulted")
		return nil
	}
}

// TestListenerHandshakeClockSettlesAfterEarlyReturn: a test that fails
// between two clock handshakes leaves the handler paused inside Now; the
// clock's cleanup abort must let it finish so that serveSession's drain and
// the fixture's Listener.Wait complete. The subtest returns right after the
// first handshake without ever closing it -- the shape of a t.Fatal there --
// and its teardown must settle within a bound. Without the abort this hangs
// until the package timeout.
func TestListenerHandshakeClockSettlesAfterEarlyReturn(t *testing.T) {
	started := time.Now()
	t.Run("returns while a clock call is paused", func(t *testing.T) {
		fx := newListenerFixture(t)
		seedEnabledBinding(t, fx.db, 1, "peer-a")
		seedEnabledBinding(t, fx.db, 2, "peer-b")
		clock := newHandshakeClock(t)
		installListenerRecovery(t, fx, memoryMarkersForTest(), clock.Now)
		conn, _ := negotiatedConn(t, fx)
		clock.armed.Store(true)
		if _, err := conn.Write([]byte(enrollLine("2", "80000000-0000-4000-8000-00000000f701", "conv-abort"))); err != nil {
			t.Fatal(err)
		}
		_ = clock.next(t) // paused inside prepare's checkpoint; never closed
	})
	if elapsed := time.Since(started); elapsed > restartTeardownWait {
		t.Fatalf("the fixture took %v to settle after an early return", elapsed)
	}
}

// TestListenerServiceAnswersUncertainWhileRecoveryPreparationBlocks holds a
// mutation inside Before: a clock rollback latches a pending marker and the
// marker write blocks under the recovery I/O mutex, before any business
// execution. The request must be answered outcome_unknown at RequestDeadline
// -- nothing proves non-commitment from outside the handler -- while the
// write is still blocked; requests queued behind it expire at their own
// deadlines with temporarily_unavailable (they never dispatched); and once
// the write completes, the deferred After still records the incident
// durably, the external marker is retained, the service is held, and the
// socket -- whose correlation ID was released with the reply -- keeps
// serving. Before this change the reply waited for the write.
func TestListenerServiceAnswersUncertainWhileRecoveryPreparationBlocks(t *testing.T) {
	fx := newListenerFixture(t)
	markers := newGatedMarkers()
	var clock atomic.Int64
	clock.Store(110)
	installListenerRecovery(t, fx, markers, func() time.Time { return time.Unix(clock.Load(), 0) })
	releaseOnce := sync.OnceFunc(func() { close(markers.release) })
	t.Cleanup(releaseOnce)

	conn, br := negotiatedConn(t, fx)
	clock.Store(100) // the next hooked operation observes a rollback
	markers.blockPut.Store(true)
	const op = "80000000-0000-4000-8000-00000000f101"
	started := time.Now()
	if _, err := conn.Write([]byte(revokeLine("2", op, "conv-recovery") +
		revokeLine("3", "80000000-0000-4000-8000-00000000f102", "conv-recovery") +
		`{"jsonrpc":"2.0","id":"4","method":"no.such.method","params":{}}` + "\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-markers.putEntered:
	case <-time.After(restartTeardownWait):
		t.Fatal("the marker write was never reached")
	}
	expectBoundedReply(t, br, started, "2", string(store.OutcomeUnknown))
	expectBoundedReply(t, br, started, "3", string(store.TemporarilyUnavailable))
	expectBoundedReply(t, br, started, "4", string(store.TemporarilyUnavailable))
	select {
	case <-markers.release:
		t.Fatal("the marker write was released before the replies; the deadline did not bound them")
	default:
	}

	markers.blockPut.Store(false)
	releaseOnce()
	if err := conn.SetDeadline(time.Now().Add(restartTeardownWait)); err != nil {
		t.Fatal(err)
	}
	// The socket is still owned and serving; "2" is reusable because its
	// reply was written. The read path is not hooked, so this also shows the
	// blocked handler did not stall the session once answered.
	if _, err := conn.Write([]byte(`{"jsonrpc":"2.0","id":"2","method":"operation.get","params":{"operation_id":"` + op + `"}}` + "\n")); err != nil {
		t.Fatal(err)
	}
	if resp := readReply(t, br); resp["id"] != "2" || replyCode(resp) != string(OperationNotFound) {
		t.Fatalf("expected operation_not_found for the never-committed operation, got %#v", resp)
	}
	// The hold latched by the abandoned request governs later mutations.
	if _, err := conn.Write([]byte(revokeLine("5", "80000000-0000-4000-8000-00000000f103", "conv-recovery"))); err != nil {
		t.Fatal(err)
	}
	if resp := readReply(t, br); replyCode(resp) != string(store.RecoveryRequired) {
		t.Fatalf("expected recovery_required after the latched rollback, got %#v", resp)
	}
	// Independent recovery evidence survived the request path giving up:
	// the exact external marker and its durable incident both exist.
	list, err := markers.memoryMarkersForTestType.List(context.Background())
	if err != nil || len(list) != 1 || list[0].Kind != "clock" || list[0].Floor == nil || list[0].Observed == nil ||
		*list[0].Floor != 110*int64(time.Second) || *list[0].Observed != 100*int64(time.Second) {
		t.Fatalf("expected one retained clock marker (floor 110s, observed 100s), got %#v err=%v", list, err)
	}
	var incidents int
	if err := fx.db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT count(*) FROM recovery_incidents WHERE kind='clock' AND status='held'").Scan(&incidents)
	}); err != nil || incidents != 1 {
		t.Fatalf("expected one held durable clock incident, got %d err=%v", incidents, err)
	}
	if n := operationResultCount(t, fx.db, op); n != 0 {
		t.Fatalf("the abandoned request must not have recorded a result: %d", n)
	}
}

// TestListenerWaitDrainsOwnedRecoveryFinalization: shutdown must account for
// a handler whose recovery finalization outlived its request. Listener.Wait
// may not return -- and so the runtime may not close the writer -- while
// that handler still runs; it returns once the handler does.
func TestListenerWaitDrainsOwnedRecoveryFinalization(t *testing.T) {
	fx := newListenerFixture(t)
	markers := newGatedMarkers()
	var clock atomic.Int64
	clock.Store(110)
	installListenerRecovery(t, fx, markers, func() time.Time { return time.Unix(clock.Load(), 0) })
	releaseOnce := sync.OnceFunc(func() { close(markers.release) })
	t.Cleanup(releaseOnce)

	conn, br := negotiatedConn(t, fx)
	clock.Store(100)
	markers.blockPut.Store(true)
	started := time.Now()
	if _, err := conn.Write([]byte(revokeLine("2", "80000000-0000-4000-8000-00000000f201", "conv-drain"))); err != nil {
		t.Fatal(err)
	}
	select {
	case <-markers.putEntered:
	case <-time.After(restartTeardownWait):
		t.Fatal("the marker write was never reached")
	}
	expectBoundedReply(t, br, started, "2", string(store.OutcomeUnknown))

	if err := fx.ln.StopAdmission(); err != nil {
		t.Fatal(err)
	}
	fx.cancel()
	waited := make(chan error, 1)
	go func() { waited <- fx.ln.Wait() }()
	select {
	case err := <-waited:
		t.Fatalf("Wait returned (err=%v) while a handler's recovery finalization was still running", err)
	case <-time.After(300 * time.Millisecond):
	}
	markers.blockPut.Store(false)
	releaseOnce()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(restartTeardownWait):
		t.Fatal("Wait did not return after the finalization completed")
	}
}

// TestListenerServiceReportsUncertaintyAfterCommittedOutcomeAndReplays: the
// business transaction commits, then the deferred After blocks on the
// recovery I/O mutex held by a concurrent operation whose marker listing is
// stuck. The request is answered outcome_unknown at RequestDeadline while
// After is still blocked; the committed receipt is retained, and a
// same-operation retry replays it (same audit_id) once finalization ends.
// The recovery clock hands the test the two moments it needs: inside
// prepare's checkpoint (the concurrent operation is started) and inside the
// business transaction's time validation (the concurrent operation has taken
// the mutex and is blocked in List). Before this change the reply waited for
// the mutex.
func TestListenerServiceReportsUncertaintyAfterCommittedOutcomeAndReplays(t *testing.T) {
	fx := newListenerFixture(t)
	seedEnabledBinding(t, fx.db, 1, "peer-a")
	seedEnabledBinding(t, fx.db, 2, "peer-b")
	markers := newGatedMarkers()
	clock := newHandshakeClock(t)
	installListenerRecovery(t, fx, markers, clock.Now)
	releaseOnce := sync.OnceFunc(func() { close(markers.release) })
	t.Cleanup(releaseOnce)

	conn, br := negotiatedConn(t, fx)
	const op = "80000000-0000-4000-8000-00000000f301"
	clock.armed.Store(true)
	started := time.Now()
	if _, err := conn.Write([]byte(enrollLine("2", op, "conv-commit"))); err != nil {
		t.Fatal(err)
	}
	// 1. prepare's checkpoint: the request holds the mutex and the gate.
	inPrepare := clock.next(t)
	markers.blockList.Store(true)
	concurrent := make(chan error, 1)
	go func() {
		_, err := fx.db.Coordinator().Transition(context.Background(), func(context.Context, *sql.Tx, store.CommitView) (store.TransitionResult, error) {
			return store.TransitionResult{}, nil
		}, nil)
		concurrent <- err
	}()
	close(inPrepare)
	// 2. the business transaction's time validation: prepare released the
	// mutex, so the concurrent operation now holds it and is blocked in List
	// (no trusted instant exists yet, so its own flush did not need the
	// gate this request holds). After will need that mutex post-commit.
	inTransaction := clock.next(t)
	select {
	case <-markers.listEntered:
	case <-time.After(restartTeardownWait):
		t.Fatal("the concurrent operation never reached List")
	}
	clock.armed.Store(false)
	close(inTransaction)

	expectBoundedReply(t, br, started, "2", string(store.OutcomeUnknown))
	select {
	case <-markers.release:
		t.Fatal("the mutex holder was released before the reply; the deadline did not bound it")
	default:
	}
	markers.blockList.Store(false)
	releaseOnce()
	awaitHeld(t, concurrent)

	// The outcome was a commit: the receipt exists and a same-operation
	// retry replays it rather than executing again.
	if n := operationResultCount(t, fx.db, op); n != 1 {
		t.Fatalf("expected exactly one committed result for the operation, got %d", n)
	}
	var auditID string
	if err := fx.db.Coordinator().Inspect(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT audit_id FROM command_audit WHERE operation_id=?", op).Scan(&auditID)
	}); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(restartTeardownWait)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte(enrollLine("3", op, "conv-commit"))); err != nil {
		t.Fatal(err)
	}
	resp := readReply(t, br)
	if resp["id"] != "3" || resp["error"] != nil {
		t.Fatalf("expected the retained receipt on retry, got %#v", resp)
	}
	raw, _ := json.Marshal(resp["result"])
	var receipt CommandReceiptResult
	if err := json.Unmarshal(raw, &receipt); err != nil {
		t.Fatal(err)
	}
	if !receipt.Usable(op, "conv-commit") || receipt.AuditID != auditID {
		t.Fatalf("retry must replay the committed receipt (audit %s), got %#v", auditID, receipt)
	}
	if n := operationResultCount(t, fx.db, op); n != 1 {
		t.Fatalf("the retry must not have executed again: %d results", n)
	}
}

// TestListenerServiceDoesNotWaitForDeferredAfterWhenBeforeRefuses: Before
// refuses because the coordinator gate is held (its own wait honors the
// request context), but the deferred After then waits for the same gate
// under its independent background deadline. A prepare-only fix would still
// hold the reply for that second wait; the request must instead be answered
// outcome_unknown at RequestDeadline while the gate is still held, nothing
// commits, and the socket keeps serving. Before this change the reply came
// only when After's background deadline expired.
func TestListenerServiceDoesNotWaitForDeferredAfterWhenBeforeRefuses(t *testing.T) {
	fx := newListenerFixture(t)
	installListenerRecovery(t, fx, memoryMarkersForTest(), func() time.Time { return time.Unix(1000, 0) })
	// One hooked transaction first, so a trusted instant exists and After's
	// flush actually needs the gate (recovery.Service.flush checkpoints it).
	if _, err := fx.db.Coordinator().Transition(context.Background(), func(context.Context, *sql.Tx, store.CommitView) (store.TransitionResult, error) {
		return store.TransitionResult{}, nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	release, held := holdCoordinator(t, fx.db, nil)

	conn, br := negotiatedConn(t, fx)
	const op = "80000000-0000-4000-8000-00000000f401"
	started := time.Now()
	if _, err := conn.Write([]byte(revokeLine("2", op, "conv-gate"))); err != nil {
		t.Fatal(err)
	}
	expectBoundedReply(t, br, started, "2", string(store.OutcomeUnknown))
	select {
	case err := <-held:
		t.Fatalf("the gate was released before the reply (err=%v); the deadline did not bound it", err)
	default:
	}
	release()
	awaitHeld(t, held)
	if n := operationResultCount(t, fx.db, op); n != 0 {
		t.Fatalf("a request refused before execution must not have recorded a result: %d", n)
	}
	if err := conn.SetDeadline(time.Now().Add(restartTeardownWait)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte(`{"jsonrpc":"2.0","id":"2","method":"server.hello","params":{"protocol":"parley-control/1"}}` + "\n")); err != nil {
		t.Fatal(err)
	}
	if resp := readReply(t, br); resp["id"] != "2" || resp["error"] != nil {
		t.Fatalf("the socket must keep serving after the finalization drained, got %#v", resp)
	}
}

// TestListenerServiceCompletesMutationWithRealRecoveryHooks is the healthy
// path through the same composition: a real recovery.Service on the
// listener's writer, a committed enrollment answered normally, its receipt
// readable, a follow-up mutation on the new version accepted, and no
// fail-stop.
func TestListenerServiceCompletesMutationWithRealRecoveryHooks(t *testing.T) {
	fx := newListenerFixture(t)
	seedEnabledBinding(t, fx.db, 1, "peer-a")
	seedEnabledBinding(t, fx.db, 2, "peer-b")
	installListenerRecovery(t, fx, memoryMarkersForTest(), time.Now)

	conn, br := negotiatedConn(t, fx)
	const op = "80000000-0000-4000-8000-00000000f501"
	started := time.Now()
	if _, err := conn.Write([]byte(enrollLine("2", op, "conv-healthy"))); err != nil {
		t.Fatal(err)
	}
	resp := readReply(t, br)
	if resp["id"] != "2" || resp["error"] != nil {
		t.Fatalf("expected a committed enrollment, got %#v", resp)
	}
	if elapsed := time.Since(started); elapsed >= RequestDeadline-handlerContextLead {
		t.Fatalf("a healthy mutation took %v; the hooks must not consume the request budget", elapsed)
	}
	raw, _ := json.Marshal(resp["result"])
	var receipt CommandReceiptResult
	if err := json.Unmarshal(raw, &receipt); err != nil {
		t.Fatal(err)
	}
	if !receipt.Usable(op, "conv-healthy") {
		t.Fatalf("receipt is not usable: %#v", receipt)
	}
	if _, err := conn.Write([]byte(`{"jsonrpc":"2.0","id":"3","method":"operation.get","params":{"operation_id":"` + op + `"}}` + "\n")); err != nil {
		t.Fatal(err)
	}
	if got := readReply(t, br); got["id"] != "3" || got["error"] != nil {
		t.Fatalf("operation.get must find the committed receipt, got %#v", got)
	}
	if _, err := conn.Write([]byte(revokeLine("4", "80000000-0000-4000-8000-00000000f502", "conv-healthy"))); err != nil {
		t.Fatal(err)
	}
	if got := readReply(t, br); got["id"] != "4" || got["error"] != nil {
		t.Fatalf("a follow-up revoke on the enrolled version must succeed, got %#v", got)
	}
}

// liveHandlerConn negotiates a connection and pipelines one membership.revoke
// whose marker write blocks inside Before (clock rollback under the recovery
// I/O mutex), followed by extra frames, returning once that write is reached.
// The revoke is answered outcome_unknown at RequestDeadline while its handler
// stays alive until release is called (cleanup calls it too).
func liveHandlerConn(t *testing.T, extra string) (conn net.Conn, br *bufio.Reader, started time.Time, markers *gatedMarkers, release func()) {
	t.Helper()
	fx := newListenerFixture(t)
	markers = newGatedMarkers()
	clock := new(atomic.Int64)
	clock.Store(110)
	installListenerRecovery(t, fx, markers, func() time.Time { return time.Unix(clock.Load(), 0) })
	release = sync.OnceFunc(func() {
		markers.blockPut.Store(false)
		close(markers.release)
	})
	t.Cleanup(release)

	conn, br = negotiatedConn(t, fx)
	clock.Store(100)
	markers.blockPut.Store(true)
	started = time.Now()
	if _, err := conn.Write([]byte(revokeLine("2", "80000000-0000-4000-8000-00000000f601", "conv-live") + extra)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-markers.putEntered:
	case <-time.After(restartTeardownWait):
		t.Fatal("the marker write was never reached")
	}
	return conn, br, started, markers, release
}

// TestListenerServiceClassifiesFramesExpiringBehindLiveHandler: review
// 5264346948 (comment 4060293322). A frame dequeued behind a handler that
// outlives its deadline is still an ID-less object or an envelope violation
// when its own deadline passes: the first closes the socket without any
// response, the second is answered with its envelope error under its own ID.
// Before this change both were answered temporarily_unavailable as if they
// were valid requests -- the ID-less one under an empty ID, socket left open.
func TestListenerServiceClassifiesFramesExpiringBehindLiveHandler(t *testing.T) {
	t.Run("envelope violation keeps its error and id", func(t *testing.T) {
		_, br, started, _, _ := liveHandlerConn(t, `{"jsonrpc":"2.0","id":"3","method":"membership.revoke","params":[]}`+"\n")
		expectBoundedReply(t, br, started, "2", string(store.OutcomeUnknown))
		resp := readReply(t, br)
		elapsed := time.Since(started)
		wireErr, _ := resp["error"].(map[string]any)
		if resp["id"] != "3" || wireErr["code"] != float64(InvalidRequest) || replyCode(resp) != "" {
			t.Fatalf("expected the -32600 envelope error under id 3, got %#v", resp)
		}
		if elapsed < RequestDeadline-time.Second || elapsed > RequestDeadline+2*time.Second {
			t.Fatalf("violation answered after %v, want about RequestDeadline (%v) from arrival", elapsed, RequestDeadline)
		}
	})
	t.Run("id-less object closes the socket without a response", func(t *testing.T) {
		_, br, started, _, _ := liveHandlerConn(t, `{"jsonrpc":"2.0","method":"server.hello","params":{"protocol":"parley-control/1"}}`+"\n")
		expectBoundedReply(t, br, started, "2", string(store.OutcomeUnknown))
		_, err := br.ReadBytes('\n')
		assertConnectionActuallyClosed(t, err)
		if elapsed := time.Since(started); elapsed > RequestDeadline+2*time.Second {
			t.Fatalf("socket closed after %v; the close must not wait for the live handler", elapsed)
		}
	})
}

// frameKind names one pipelined frame shape for the capacity tests: a valid
// request (tracked), an envelope violation whose ID cannot be echoed
// (untracked, answered under a null ID) or an ID-less object (untracked,
// closes the socket when it is settled).
type frameKind int

const (
	validFrame frameKind = iota
	unechoableFrame
	idlessFrame
)

// burstOf renders frames in order; valid requests get ids from firstID up.
func burstOf(kinds []frameKind, firstID int) string {
	var b strings.Builder
	id := firstID
	for _, kind := range kinds {
		switch kind {
		case validFrame:
			fmt.Fprintf(&b, `{"jsonrpc":"2.0","id":%q,"method":"no.such.method","params":{}}`+"\n", fmt.Sprint(id))
			id++
		case unechoableFrame:
			b.WriteString(`{"jsonrpc":"2.0","id":7,"method":"no.such.method","params":{}}` + "\n")
		case idlessFrame:
			b.WriteString(`{"jsonrpc":"2.0","method":"no.such.method","params":{}}` + "\n")
		}
	}
	return b.String()
}

func repeatKind(kind frameKind, n int) []frameKind {
	kinds := make([]frameKind, n)
	for i := range kinds {
		kinds[i] = kind
	}
	return kinds
}

// expectRepliesInOrder reads one reply per frame: a valid request's reply
// carries its id (from firstID up) and an unechoable violation's carries
// null with the -32600 envelope code.
func expectRepliesInOrder(t *testing.T, br *bufio.Reader, kinds []frameKind, firstID int) {
	t.Helper()
	id := firstID
	for i, kind := range kinds {
		resp := readReply(t, br)
		switch kind {
		case validFrame:
			if resp["id"] != fmt.Sprint(id) {
				t.Fatalf("reply %d: want id %d, got %#v", i, id, resp)
			}
			id++
		case unechoableFrame:
			wireErr, _ := resp["error"].(map[string]any)
			if resp["id"] != nil || wireErr["code"] != float64(InvalidRequest) {
				t.Fatalf("reply %d: want a null-id -32600 for the unechoable frame, got %#v", i, resp)
			}
		default:
			t.Fatalf("reply %d: an ID-less frame gets no reply", i)
		}
	}
}

// TestListenerServiceKeepsExecutingSlotOccupiedAfterDeadlineReply: review
// 5264346948 (comment 4060293329) and 5264559000 (comment 4060461953). After
// a mutation is answered outcome_unknown its handler still holds the socket's
// one executing slot, so the reader admits exactly MaxQueuedPerSocket more
// frames behind it -- of any kind: ID-less objects and unechoable violations
// occupy capacity like requests -- and the next one closes the socket. Before
// batch 25 the reply released the slot with the correlation ID and nine
// requests were admitted; before this change untracked frames were not
// counted, so ten mixed frames waited behind the handler. The boundary cases
// prove the bound itself is admitted, served once the handler returns, and
// its capacity reusable afterwards.
func TestListenerServiceKeepsExecutingSlotOccupiedAfterDeadlineReply(t *testing.T) {
	overflow := map[string][]frameKind{
		"one more request than the bound":          repeatKind(validFrame, MaxQueuedPerSocket+1),
		"one more unechoable frame than the bound": repeatKind(unechoableFrame, MaxQueuedPerSocket+1),
		"an id-less ninth behind a mixed bound": append(append(repeatKind(validFrame, MaxQueuedPerSocket/2),
			repeatKind(unechoableFrame, MaxQueuedPerSocket-MaxQueuedPerSocket/2)...), idlessFrame),
	}
	for name, kinds := range overflow {
		t.Run(name+" closes the socket", func(t *testing.T) {
			conn, br, started, _, _ := liveHandlerConn(t, "")
			expectBoundedReply(t, br, started, "2", string(store.OutcomeUnknown))
			// Shorter than the queued frames' own deadlines: a close must
			// come from the queue bound, not be confused with expiry replies.
			if err := conn.SetDeadline(time.Now().Add(RequestDeadline - 2*time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Write([]byte(burstOf(kinds, 3))); err != nil {
				t.Fatal(err)
			}
			_, err := br.ReadBytes('\n')
			assertConnectionActuallyClosed(t, err)
		})
	}
	boundary := map[string][]frameKind{
		"the bound of requests":   repeatKind(validFrame, MaxQueuedPerSocket),
		"the bound of unechoable": repeatKind(unechoableFrame, MaxQueuedPerSocket),
		"a mixed bound": append(repeatKind(validFrame, MaxQueuedPerSocket/2),
			repeatKind(unechoableFrame, MaxQueuedPerSocket-MaxQueuedPerSocket/2)...),
	}
	for name, kinds := range boundary {
		t.Run(name+" is admitted, served and its capacity reusable", func(t *testing.T) {
			conn, br, started, _, release := liveHandlerConn(t, "")
			expectBoundedReply(t, br, started, "2", string(store.OutcomeUnknown))
			if _, err := conn.Write([]byte(burstOf(kinds, 3))); err != nil {
				t.Fatal(err)
			}
			release()
			if err := conn.SetDeadline(time.Now().Add(restartTeardownWait)); err != nil {
				t.Fatal(err)
			}
			expectRepliesInOrder(t, br, kinds, 3)
			// Settled: the slots are free again for a full new burst of
			// MaxExecutingPerSocket + MaxQueuedPerSocket frames.
			again := append(repeatKind(validFrame, MaxExecutingPerSocket), kinds...)
			if _, err := conn.Write([]byte(burstOf(again, 20))); err != nil {
				t.Fatal(err)
			}
			expectRepliesInOrder(t, br, again, 20)
		})
	}
}
