package store

import "time"

// Direction of a grant: which peer(s) may send.
type Direction string

const (
	Bidirectional Direction = "bidirectional"
	AToB          Direction = "a_to_b"
	BToA          Direction = "b_to_a"
)

// GrantStatus is the lifecycle state of one grant version.
type GrantStatus string

const (
	GrantActive     GrantStatus = "active"
	GrantRevoked    GrantStatus = "revoked"
	GrantSuperseded GrantStatus = "superseded"
)

// EnvelopeState is the delivery state machine for one message.
type EnvelopeState string

const (
	Queued      EnvelopeState = "queued"
	Dispatching EnvelopeState = "dispatching"
	HandedOff   EnvelopeState = "handed_off"
	Acked       EnvelopeState = "acked"
	Failed      EnvelopeState = "failed"
	Cancelled   EnvelopeState = "cancelled"
	Uncertain   EnvelopeState = "uncertain"
)

// Grant is one versioned membership record for a conversation.
type Grant struct {
	Conversation  string
	GrantVersion  int64
	PeerAID       string
	PeerBID       string
	Direction     Direction
	MaxExchanges  int64
	ExchangesUsed int64
	GrantedAt     string
	ExpiresAt     *string
	Status        GrantStatus
	RevokedAt     *string
}

// Expired reports whether this grant's ExpiresAt has passed as of now. A
// nil ExpiresAt never expires. An ExpiresAt that fails to parse is treated
// as expired (fail closed), not ignored.
func (g Grant) Expired(now time.Time) bool {
	if g.ExpiresAt == nil {
		return false
	}
	exp, err := time.Parse(time.RFC3339Nano, *g.ExpiresAt)
	return err != nil || !now.Before(exp)
}

// Permits reports whether this grant allows a message from "from" to "to" at
// "now" — both identities must be exactly the grant's enrolled pair (not a
// third identity, and not swapped beyond what Direction allows), Direction
// must permit that specific direction rather than only the reverse one, the
// grant must not be revoked, and it must not have expired. The only current
// caller resolves g via CurrentGrant, whose query already excludes non-active
// grants, so RevokedAt is never actually set here in practice today — but
// Permits is the named authorization gate for this type, and a future caller
// resolving a Grant some other way must not have to separately remember to
// check RevokedAt or ExpiresAt itself.
func (g Grant) Permits(from, to string, now time.Time) bool {
	if g.RevokedAt != nil {
		return false
	}
	if g.Expired(now) {
		return false
	}
	switch {
	case from == g.PeerAID && to == g.PeerBID:
		return g.Direction == Bidirectional || g.Direction == AToB
	case from == g.PeerBID && to == g.PeerAID:
		return g.Direction == Bidirectional || g.Direction == BToA
	default:
		return false
	}
}

// Envelope is one message, durable from the moment Send accepts it.
type Envelope struct {
	ID           string
	Conversation string
	FromPeer     string
	ToPeer       string
	Text         string
	GrantVersion int64
	InReplyTo    *string
	// TrustedReply is set only by codex.IngestTurn, the one path that
	// atomically acks the original envelope and validates in_reply_to
	// against it (replymarker.Validate) before queuing this row. Send has no
	// parameter for it and always leaves it false — a bare non-nil
	// InReplyTo is a caller-supplied claim, never proof of a genuine reply.
	TrustedReply bool
	State        EnvelopeState
	CreatedAt    string
	UpdatedAt    string
}
