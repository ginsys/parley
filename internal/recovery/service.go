package recovery

import (
	"context"
	"database/sql"
	"sync"
	"time"

	runtimeowner "github.com/ginsys/parley/internal/runtime"
	"github.com/ginsys/parley/internal/store"
	"github.com/google/uuid"
)

type Config struct {
	Store   *store.DB
	Markers Markers
	Now     func() time.Time
	// FailStop must stop admission and arrange supervised recovery before an
	// unattended restart when external recovery evidence cannot be made durable.
	// It must be nonblocking and must not reenter the store or perform I/O.
	FailStop func()
}
type Service struct {
	config      Config
	maintenance *store.RecoveryMaintenance
	// Lock order: I/O serialization -> maintenance coordinator -> short state.
	// Writer Time/Guard take state only and never acquire ioMu.
	ioMu          sync.Mutex
	stateMu       sync.Mutex
	held          bool
	pending       []Marker
	trusted       *int64
	serverID      string
	newIncidentID func() (uuid.UUID, error)
}

func New(ctx context.Context, c Config) (*Service, error) {
	if c.Store == nil || c.Markers == nil || c.FailStop == nil {
		return nil, store.InvalidRequest
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	s := &Service{config: c, newIncidentID: uuid.NewRandom}
	maintenance, err := c.Store.Coordinator().InstallRecovery(store.RecoveryHooks{Before: s.before, Time: s.transactionTime, After: s.after})
	if err != nil {
		return nil, err
	}
	s.maintenance = maintenance
	if err := s.maintenance.Inspect(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT server_id FROM installation WHERE singleton=1").Scan(&s.serverID)
	}); err != nil {
		return nil, err
	}
	if err := s.prepare(ctx); err != nil {
		return nil, err
	}
	return s, nil
}
func humanRecovery(kind string) bool {
	return kind == "human_inspection" || kind == "clock.reconcile" || kind == "recovery.complete"
}
func (s *Service) before(ctx context.Context, kind string) error {
	if err := s.prepare(ctx); err != nil {
		return err
	}
	s.stateMu.Lock()
	held := s.held
	s.stateMu.Unlock()
	if held && !humanRecovery(kind) && kind != "dispatch.settle" {
		return store.RecoveryRequired
	}
	return nil
}
func (s *Service) latch(floor, observed int64) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.held = true
	// Repeated observations of the same pair reuse pending evidence; a new
	// detection never replaces or suppresses a different pending incident.
	for _, marker := range s.pending {
		if marker.Kind == "clock" && *marker.Floor == floor && *marker.Observed == observed {
			return nil
		}
	}
	s.pending = append(s.pending, Marker{ServerID: s.serverID, Kind: "clock", Floor: &floor, Observed: &observed})
	return nil
}

// latchRollback deduplicates only the same still-held immutable observation.
// A different floor/observed pair needs independent evidence even during cleanup.
func (s *Service) latchRollback(ctx context.Context, tx *sql.Tx, floor, observed int64) error {
	var recorded bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM recovery_incidents WHERE kind='clock' AND status='held' AND last_trusted_ns=? AND observed_ns=?)", floor, observed).Scan(&recorded); err != nil {
		// Detection already happened. An unavailable deduplication read cannot
		// discard the observation before the independent After flush.
		if latchErr := s.latch(floor, observed); latchErr != nil {
			return latchErr
		}
		return store.TemporarilyUnavailable
	}
	if recorded {
		return nil
	}
	return s.latch(floor, observed)
}

func (s *Service) transactionTime(ctx context.Context, tx *sql.Tx, kind string) (time.Time, error) {
	instant := s.config.Now()
	ns, err := store.InstantNanos(instant)
	if err != nil {
		return time.Time{}, err
	}
	checkpoint, err := s.authorizationFloor(ctx, tx)
	if err != nil {
		return time.Time{}, err
	}
	if checkpoint.Instant.Valid && ns < checkpoint.Instant.Int64 {
		if err := s.latchRollback(ctx, tx, checkpoint.Instant.Int64, ns); err != nil {
			return time.Time{}, err
		}
		if !humanRecovery(kind) && kind != "dispatch.settle" {
			return time.Time{}, store.RecoveryRequired
		}
		if kind == "dispatch.settle" {
			// Late outcome evidence remains recordable, but its timestamp
			// cannot regress below the trusted floor. The marker retains the
			// actual observed rollback sample independently.
			instant = time.Unix(0, checkpoint.Instant.Int64)
		}
	} else {
		if _, err := store.AdvanceClockCheckpoint(ctx, tx, ns); err != nil {
			return time.Time{}, err
		}
		s.stateMu.Lock()
		if s.trusted == nil || ns > *s.trusted {
			value := ns
			s.trusted = &value
		}
		s.stateMu.Unlock()
	}
	durableHeld, err := store.RecoveryHeld(ctx, tx)
	if err != nil {
		return time.Time{}, err
	}
	s.stateMu.Lock()
	held := s.held || durableHeld
	s.stateMu.Unlock()
	if held && !humanRecovery(kind) && kind != "dispatch.settle" {
		return time.Time{}, store.RecoveryRequired
	}
	if principal, ok := store.CommandIdentity(ctx); ok && !humanRecovery(kind) {
		var retired bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM retired_namespaces WHERE principal_id=?)", principal.ID).Scan(&retired); err != nil {
			return time.Time{}, store.TemporarilyUnavailable
		}
		if retired {
			return time.Time{}, store.RecoveryRequired
		}
	}
	return instant, nil
}
func (s *Service) flush(ctx context.Context) error {
	// Caller owns ioMu; no filesystem operation occurs under coordinator/state.
	s.stateMu.Lock()
	pending := append([]Marker(nil), s.pending...)
	hasTrusted := s.trusted != nil
	s.stateMu.Unlock()
	for _, marker := range pending {
		if marker.IncidentID == "" {
			id, err := s.newIncidentID()
			if err != nil {
				s.config.FailStop()
				return store.RecoveryRequired
			}
			marker.IncidentID = id.String()
			// ioMu owns removal from the pending prefix; concurrent latches
			// can only append. Assign once before either fallible persistence.
			s.stateMu.Lock()
			s.pending[0].IncidentID = marker.IncidentID
			s.stateMu.Unlock()
		}
		if err := s.config.Markers.Put(ctx, marker); err != nil {
			s.config.FailStop()
			return store.RecoveryRequired
		}
		if _, err := s.maintenance.Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
			changed, err := store.RecordRecovery(ctx, tx, marker.record())
			return store.TransitionResult{Changed: changed}, err
		}, nil); err != nil {
			return err
		}
		s.stateMu.Lock()
		if len(s.pending) > 0 && s.pending[0].IncidentID == marker.IncidentID {
			s.pending = s.pending[1:]
		}
		s.stateMu.Unlock()
	}
	if hasTrusted {
		if _, err := s.maintenance.Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
			// Read the current publication only after acquiring the writer: a
			// reviewed floor reset may have committed while flush waited.
			s.stateMu.Lock()
			var trusted int64
			present := s.trusted != nil
			if present {
				trusted = *s.trusted
			}
			s.stateMu.Unlock()
			if !present {
				return store.TransitionResult{}, nil
			}
			checkpoint, err := store.ReadClockCheckpoint(ctx, tx)
			if err != nil {
				return store.TransitionResult{}, err
			}
			if checkpoint.Instant.Valid && checkpoint.Instant.Int64 >= trusted {
				return store.TransitionResult{}, nil
			}
			changed, err := store.AdvanceClockCheckpoint(ctx, tx, trusted)
			return store.TransitionResult{Changed: changed}, err
		}, nil); err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) after() error {
	ctx, cancel := context.WithTimeout(context.Background(), store.AuthenticationDeadline)
	defer cancel()
	s.ioMu.Lock()
	defer s.ioMu.Unlock()
	return s.flush(ctx)
}
func (s *Service) prepare(ctx context.Context) error {
	s.ioMu.Lock()
	defer s.ioMu.Unlock()
	if err := s.flush(ctx); err != nil {
		return err
	}
	markers, err := s.config.Markers.List(ctx)
	if err != nil {
		s.stateMu.Lock()
		s.held = true
		s.stateMu.Unlock()
		return store.RecoveryRequired
	}
	for _, marker := range markers {
		if marker.ServerID != s.serverID {
			s.stateMu.Lock()
			s.held = true
			s.stateMu.Unlock()
			return store.RecoveryRequired
		}
		s.stateMu.Lock()
		s.held = true
		s.stateMu.Unlock()
		if _, err := s.maintenance.Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
			changed, err := store.RecordRecovery(ctx, tx, marker.record())
			if err != nil {
				return store.TransitionResult{}, err
			}
			record, err := store.ReadRecovery(ctx, tx, marker.IncidentID)
			if err != nil {
				return store.TransitionResult{}, err
			}
			if record.Status == "cleared" {
				return store.TransitionResult{}, store.RecoveryRequired
			}
			return store.TransitionResult{Changed: changed}, nil
		}, nil); err != nil {
			return err
		}
	}
	// Sample, compare and checkpoint in one writer acquisition. In particular a
	// concurrent authorization must not invalidate a cached floor comparison.
	_, err = s.maintenance.Transition(ctx, func(ctx context.Context, tx *sql.Tx, _ store.CommitView) (store.TransitionResult, error) {
		checkpoint, err := s.authorizationFloor(ctx, tx)
		if err != nil {
			return store.TransitionResult{}, err
		}
		durableHeld, err := store.RecoveryHeld(ctx, tx)
		if err != nil {
			return store.TransitionResult{}, err
		}
		s.stateMu.Lock()
		s.held = len(markers) > 0 || durableHeld || len(s.pending) > 0
		s.stateMu.Unlock()
		ns, err := store.InstantNanos(s.config.Now())
		if err != nil {
			return store.TransitionResult{}, err
		}
		if checkpoint.Instant.Valid && ns < checkpoint.Instant.Int64 {
			return store.TransitionResult{}, s.latchRollback(ctx, tx, checkpoint.Instant.Int64, ns)
		}
		changed, err := store.AdvanceClockCheckpoint(ctx, tx, ns)
		return store.TransitionResult{Changed: changed}, err
	}, nil)
	if err != nil {
		return err
	}
	return s.flush(ctx)
}
func (s *Service) Guard(ctx context.Context, tx *sql.Tx, principal string) error {
	s.stateMu.Lock()
	held := s.held
	s.stateMu.Unlock()
	if held {
		return store.RecoveryRequired
	}
	if principal == "" {
		return nil
	}
	var retired bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM retired_namespaces WHERE principal_id=?)", principal).Scan(&retired); err != nil {
		return store.TemporarilyUnavailable
	}
	if retired {
		return store.RecoveryRequired
	}
	return nil
}

// InspectRecovery is the runtime.Config inspector. It never starts workers or
// clears holds and rejects use with any writer other than this service's owner.
func (s *Service) InspectRecovery(ctx context.Context, db *store.DB) (runtimeowner.RecoveryMode, error) {
	if db != s.config.Store {
		return 0, store.InvalidRequest
	}
	if err := s.prepare(ctx); err != nil {
		return 0, err
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.held {
		return runtimeowner.Held, nil
	}
	return runtimeowner.Normal, nil
}

// authorizationFloor includes observations awaiting independent persistence after
// a rejected/rolled-back business transaction. Caller holds the writer gate.
func (s *Service) authorizationFloor(ctx context.Context, tx *sql.Tx) (store.ClockCheckpoint, error) {
	checkpoint, err := store.ReadClockCheckpoint(ctx, tx)
	if err != nil {
		return checkpoint, err
	}
	s.stateMu.Lock()
	if s.trusted != nil && (!checkpoint.Instant.Valid || *s.trusted > checkpoint.Instant.Int64) {
		checkpoint.Instant = sql.NullInt64{Int64: *s.trusted, Valid: true}
	}
	s.stateMu.Unlock()
	return checkpoint, nil
}
