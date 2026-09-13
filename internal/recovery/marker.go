// Package recovery implements internal clock/restore recovery. It grants no
// execution authority and has no human endpoint or live-session integration.
package recovery

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/ginsys/parley/internal/store"
	"github.com/google/uuid"
)

type Marker struct {
	IncidentID string `json:"incident_id"`
	ServerID   string `json:"server_id"`
	Kind       string `json:"kind"`
	Floor      *int64 `json:"last_trusted_ns,omitempty"`
	Observed   *int64 `json:"observed_ns,omitempty"`
}

func canonicalID(s string) bool {
	id, err := uuid.Parse(s)
	return err == nil && id != uuid.Nil && id.String() == s
}
func (m Marker) valid() bool {
	return canonicalID(m.IncidentID) && canonicalID(m.ServerID) && ((m.Kind == "clock" && m.Floor != nil && m.Observed != nil) || (m.Kind == "restore" && m.Floor == nil && m.Observed == nil))
}
func (m Marker) encode() ([]byte, error) {
	if !m.valid() {
		return nil, store.InvalidRequest
	}
	return json.Marshal(m)
}
func (m Marker) record() store.RecoveryRecord {
	r := store.RecoveryRecord{ID: m.IncidentID, ServerID: m.ServerID, Kind: m.Kind, Version: 1, Status: "held"}
	if m.Floor != nil {
		r.Floor = sql.NullInt64{Int64: *m.Floor, Valid: true}
		r.Observed = sql.NullInt64{Int64: *m.Observed, Valid: true}
	}
	return r
}
func sameMarker(a, b Marker) bool {
	if a.IncidentID != b.IncidentID || a.ServerID != b.ServerID || a.Kind != b.Kind {
		return false
	}
	if (a.Floor == nil) != (b.Floor == nil) || (a.Observed == nil) != (b.Observed == nil) {
		return false
	}
	return a.Floor == nil || (*a.Floor == *b.Floor && *a.Observed == *b.Observed)
}

// Markers is trusted external storage, independent of the restored database.
// Put is durable/non-replacing. Remove matches exact content and syncs even when
// an earlier removal left the entry absent. List must bound materialization.
type Markers interface {
	List(context.Context) ([]Marker, error)
	Put(context.Context, Marker) error
	Remove(context.Context, Marker) error
}
