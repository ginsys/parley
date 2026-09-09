package store

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

// Envelope is one message, durable from the moment Send accepts it.
type Envelope struct {
	ID           string
	Conversation string
	FromPeer     string
	ToPeer       string
	Text         string
	GrantVersion int64
	InReplyTo    *string
	State        EnvelopeState
	CreatedAt    string
	UpdatedAt    string
}
