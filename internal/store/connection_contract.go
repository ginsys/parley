package store

import (
	"errors"
	"math"
	"time"

	"github.com/google/uuid"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Connection contract constants. These are not evidence of a live host integration.
const (
	MaxIdentityBytes       = 256
	MaxLocatorBytes        = 4096
	AuthenticationDeadline = 5 * time.Second
	ReadinessDeadline      = 30 * time.Second
	HeartbeatInterval      = 10 * time.Second
	LivenessDeadline       = 30 * time.Second
	ReconnectRetries       = 3
	ReconnectInterval      = time.Second
)

// Code is safe for diagnostics. Never replace it with a transport/SQLite error string.
type Code string

const (
	InvalidRequest         Code = "invalid_request"
	AuthenticationFailed   Code = "authentication_failed"
	NotFound               Code = "not_found"
	Forbidden              Code = "forbidden"
	IdentityConflict       Code = "identity_conflict"
	BindingUnavailable     Code = "binding_unavailable"
	HostUnverified         Code = "host_unverified"
	NotReady               Code = "not_ready"
	AlreadyConnected       Code = "already_connected"
	GenerationConflict     Code = "generation_conflict"
	VersionConflict        Code = "version_conflict"
	RequestExpired         Code = "request_expired"
	RequestTerminal        Code = "request_terminal"
	OperationConflict      Code = "operation_conflict"
	EventConflict          Code = "event_conflict"
	SecurityHold           Code = "security_hold"
	RecoveryRequired       Code = "recovery_required"
	CapacityExceeded       Code = "capacity_exceeded"
	TemporarilyUnavailable Code = "temporarily_unavailable"
	OutcomeUnknown         Code = "outcome_unknown"
)

func (c Code) Error() string { return string(c) }
func (c Code) valid() bool {
	switch c {
	case "", InvalidRequest, AuthenticationFailed, NotFound, Forbidden, IdentityConflict, BindingUnavailable, HostUnverified, NotReady, AlreadyConnected, GenerationConflict, VersionConflict, RequestExpired, RequestTerminal, OperationConflict, EventConflict, SecurityHold, RecoveryRequired, CapacityExceeded, TemporarilyUnavailable, OutcomeUnknown:
		return true
	}
	return false
}
func validUUID(s string) bool {
	id, err := uuid.Parse(s)
	return err == nil && id != uuid.Nil && id.String() == s
}

// NextVersion never lets SQLite promote an overflowing INTEGER to REAL.
func NextVersion(current int64) (int64, error) {
	if current < 0 || current == math.MaxInt64 {
		return 0, InvalidRequest
	}
	return current + 1, nil
}

// InstantNanos rejects the wraparound permitted by time.Time.UnixNano.
func InstantNanos(t time.Time) (int64, error) {
	n := t.UnixNano()
	if !time.Unix(0, n).Equal(t) {
		return 0, InvalidRequest
	}
	return n, nil
}
func storageCode(err error) error {
	var code Code
	if errors.As(err, &code) && code.valid() && code != "" {
		return code
	}
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) && sqliteErr.Code()&255 == sqlite3.SQLITE_FULL {
		return CapacityExceeded
	}
	return TemporarilyUnavailable
}
