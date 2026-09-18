// Package membership implements the transport-independent members/policy
// model from docs/specifications/membership.md and its exact translation to
// and from the existing pair-based grant storage (store.Grant's peer_a_id,
// peer_b_id and direction columns). It creates no new storage: the first
// runtime and two-peer inbox stage keeps membership state entirely in the
// existing columns, per that specification's scope table.
package membership

import (
	"sort"

	"github.com/ginsys/parley/internal/bridgetext"
	"github.com/ginsys/parley/internal/store"
)

// Role is a member's role within a conversation for one grant version.
type Role string

const (
	RoleMember Role = "member"
	RoleLead   Role = "lead"
)

// PolicyKind is the policy tagged union's discriminant.
type PolicyKind string

const (
	PolicyOpen     PolicyKind = "open"
	PolicyLeadOnly PolicyKind = "lead_only"
	PolicyDirected PolicyKind = "directed"
)

// Member is one entry of a Model's members array.
type Member struct {
	PeerID string
	Role   Role
}

// Edge is one entry of a directed Policy's edges array.
type Edge struct {
	From, To string
}

// Policy is the tagged-union policy object. Edges is meaningful only when
// Kind is PolicyDirected; Validate rejects a nonempty Edges on any other kind.
type Policy struct {
	Kind  PolicyKind
	Edges []Edge
}

// Model is one complete, transport-independent membership object: the
// members array plus its policy, exactly as decoded from a wire request or
// derived from a stored grant.
type Model struct {
	Members []Member
	Policy  Policy
}

// Validate checks Model against membership.md's full "Data contract" shape
// rules -- the invalid_membership class -- independent of whether the shape
// is supported by the first-runtime subset (see Supported). It is safe to
// call on a decoded wire object before any durable mutation: it performs no
// I/O and mutates nothing.
func Validate(m Model) error {
	if len(m.Members) < 2 {
		return store.InvalidMembership
	}
	seen := make(map[string]bool, len(m.Members))
	leads := 0
	for _, mem := range m.Members {
		if err := bridgetext.ValidateMetadata(mem.PeerID); err != nil {
			return store.InvalidMembership
		}
		if seen[mem.PeerID] {
			return store.InvalidMembership // duplicate member ID
		}
		seen[mem.PeerID] = true
		switch mem.Role {
		case RoleMember:
		case RoleLead:
			leads++
		default:
			return store.InvalidMembership // unknown role tag
		}
	}
	switch m.Policy.Kind {
	case PolicyOpen:
		if len(m.Policy.Edges) != 0 {
			return store.InvalidMembership // edges is a directed-only field
		}
		if leads != 0 {
			return store.InvalidMembership // open: "all roles are member"
		}
	case PolicyLeadOnly:
		if len(m.Policy.Edges) != 0 {
			return store.InvalidMembership
		}
		if leads != 1 {
			return store.InvalidMembership // lead_only: "exactly one lead"
		}
	case PolicyDirected:
		if leads != 0 {
			return store.InvalidMembership // directed: "all roles are member"
		}
		edgeSeen := make(map[Edge]bool, len(m.Policy.Edges))
		for _, e := range m.Policy.Edges {
			if e.From == e.To {
				return store.InvalidMembership // self-send is always rejected
			}
			if !seen[e.From] || !seen[e.To] {
				return store.InvalidMembership // "endpoints outside membership"
			}
			if edgeSeen[e] {
				return store.InvalidMembership // duplicate edge
			}
			edgeSeen[e] = true
		}
	default:
		return store.InvalidMembership // unknown policy tag
	}
	return nil
}

// Supported reports whether an already-Validate'd Model falls inside the
// first-runtime subset: exactly two `member` roles, with `open` or a
// one-edge `directed` policy. Every other well-formed model (lead_only,
// two-edge/empty directed, more than two members) is valid but out of
// scope -- unsupported_membership, not invalid_membership. Callers must
// call Validate first; Supported does not repeat shape validation.
func Supported(m Model) bool {
	if len(m.Members) != 2 {
		return false
	}
	switch m.Policy.Kind {
	case PolicyOpen:
		return true
	case PolicyDirected:
		return len(m.Policy.Edges) == 1
	default:
		return false
	}
}

// ToGrantFields translates a Validate'd, Supported Model into the exact
// (peerA, peerB, direction) triple stored by store.Grant, per
// membership.md's translation table. A and B are chosen in canonical member
// order (sorted by exact identifier bytes), and direction is derived from
// the requested edge -- never the reverse. The caller must have already
// confirmed Validate(m) == nil and Supported(m); ToGrantFields does not
// repeat those checks and its result is meaningless for an unsupported or
// invalid model.
func ToGrantFields(m Model) (peerA, peerB string, direction store.Direction) {
	ids := []string{m.Members[0].PeerID, m.Members[1].PeerID}
	sort.Strings(ids)
	peerA, peerB = ids[0], ids[1]
	if m.Policy.Kind == PolicyOpen {
		return peerA, peerB, store.Bidirectional
	}
	edge := m.Policy.Edges[0]
	if edge.From == peerA && edge.To == peerB {
		return peerA, peerB, store.AToB
	}
	return peerA, peerB, store.BToA
}

// FromGrant derives the canonical-order Model a stored grant represents,
// per membership.md's translation table read in reverse. Both A→B and B→A
// direction values are represented as a one-edge directed policy: only
// Bidirectional maps back to `open`. The returned Model's Members and
// Policy.Edges are already in the canonical output order (sorted by exact
// identifier bytes) that membership.md requires for responses; the grant's
// stored A/B positions are never treated as meaningful output order on
// their own.
func FromGrant(peerA, peerB string, direction store.Direction) Model {
	members := []Member{{PeerID: peerA, Role: RoleMember}, {PeerID: peerB, Role: RoleMember}}
	sort.Slice(members, func(i, j int) bool { return members[i].PeerID < members[j].PeerID })
	if direction == store.Bidirectional {
		return Model{Members: members, Policy: Policy{Kind: PolicyOpen}}
	}
	var edge Edge
	if direction == store.AToB {
		edge = Edge{From: peerA, To: peerB}
	} else {
		edge = Edge{From: peerB, To: peerA}
	}
	return Model{Members: members, Policy: Policy{Kind: PolicyDirected, Edges: []Edge{edge}}}
}
