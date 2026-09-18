package membership

import (
	"testing"

	"github.com/ginsys/parley/internal/store"
)

func TestValidateAcceptsOpenAndDirected(t *testing.T) {
	open := Model{
		Members: []Member{{PeerID: "peer-a", Role: RoleMember}, {PeerID: "peer-b", Role: RoleMember}},
		Policy:  Policy{Kind: PolicyOpen},
	}
	if err := Validate(open); err != nil {
		t.Fatalf("open: %v", err)
	}
	if !Supported(open) {
		t.Fatal("open two-member model must be supported")
	}
	directed := Model{
		Members: []Member{{PeerID: "peer-a", Role: RoleMember}, {PeerID: "peer-b", Role: RoleMember}},
		Policy:  Policy{Kind: PolicyDirected, Edges: []Edge{{From: "peer-a", To: "peer-b"}}},
	}
	if err := Validate(directed); err != nil {
		t.Fatalf("directed: %v", err)
	}
	if !Supported(directed) {
		t.Fatal("one-edge directed two-member model must be supported")
	}
}

func TestValidateRejectsMalformedShapes(t *testing.T) {
	cases := map[string]Model{
		"too few members": {
			Members: []Member{{PeerID: "peer-a", Role: RoleMember}},
			Policy:  Policy{Kind: PolicyOpen},
		},
		"duplicate member": {
			Members: []Member{{PeerID: "peer-a", Role: RoleMember}, {PeerID: "peer-a", Role: RoleMember}},
			Policy:  Policy{Kind: PolicyOpen},
		},
		"unknown role": {
			Members: []Member{{PeerID: "peer-a", Role: "captain"}, {PeerID: "peer-b", Role: RoleMember}},
			Policy:  Policy{Kind: PolicyOpen},
		},
		"unknown policy kind": {
			Members: []Member{{PeerID: "peer-a", Role: RoleMember}, {PeerID: "peer-b", Role: RoleMember}},
			Policy:  Policy{Kind: "quorum"},
		},
		"open with lead": {
			Members: []Member{{PeerID: "peer-a", Role: RoleLead}, {PeerID: "peer-b", Role: RoleMember}},
			Policy:  Policy{Kind: PolicyOpen},
		},
		"open with edges": {
			Members: []Member{{PeerID: "peer-a", Role: RoleMember}, {PeerID: "peer-b", Role: RoleMember}},
			Policy:  Policy{Kind: PolicyOpen, Edges: []Edge{{From: "peer-a", To: "peer-b"}}},
		},
		"lead_only without a lead": {
			Members: []Member{{PeerID: "peer-a", Role: RoleMember}, {PeerID: "peer-b", Role: RoleMember}},
			Policy:  Policy{Kind: PolicyLeadOnly},
		},
		"lead_only with two leads": {
			Members: []Member{{PeerID: "peer-a", Role: RoleLead}, {PeerID: "peer-b", Role: RoleLead}},
			Policy:  Policy{Kind: PolicyLeadOnly},
		},
		"directed self-edge": {
			Members: []Member{{PeerID: "peer-a", Role: RoleMember}, {PeerID: "peer-b", Role: RoleMember}},
			Policy:  Policy{Kind: PolicyDirected, Edges: []Edge{{From: "peer-a", To: "peer-a"}}},
		},
		"directed edge outside membership": {
			Members: []Member{{PeerID: "peer-a", Role: RoleMember}, {PeerID: "peer-b", Role: RoleMember}},
			Policy:  Policy{Kind: PolicyDirected, Edges: []Edge{{From: "peer-a", To: "peer-c"}}},
		},
		"directed duplicate edge": {
			Members: []Member{{PeerID: "peer-a", Role: RoleMember}, {PeerID: "peer-b", Role: RoleMember}},
			Policy: Policy{Kind: PolicyDirected, Edges: []Edge{
				{From: "peer-a", To: "peer-b"}, {From: "peer-a", To: "peer-b"},
			}},
		},
		"directed with a lead": {
			Members: []Member{{PeerID: "peer-a", Role: RoleLead}, {PeerID: "peer-b", Role: RoleMember}},
			Policy:  Policy{Kind: PolicyDirected, Edges: []Edge{{From: "peer-a", To: "peer-b"}}},
		},
		"non-ASCII peer id": {
			Members: []Member{{PeerID: "peer-é", Role: RoleMember}, {PeerID: "peer-b", Role: RoleMember}},
			Policy:  Policy{Kind: PolicyOpen},
		},
	}
	for name, model := range cases {
		t.Run(name, func(t *testing.T) {
			if err := Validate(model); err != store.InvalidMembership {
				t.Fatalf("expected invalid_membership, got %v", err)
			}
		})
	}
}

func TestSupportedExcludesOutOfScopeButValidShapes(t *testing.T) {
	cases := map[string]Model{
		"lead_only": {
			Members: []Member{{PeerID: "peer-a", Role: RoleLead}, {PeerID: "peer-b", Role: RoleMember}},
			Policy:  Policy{Kind: PolicyLeadOnly},
		},
		"three members open": {
			Members: []Member{{PeerID: "peer-a", Role: RoleMember}, {PeerID: "peer-b", Role: RoleMember}, {PeerID: "peer-c", Role: RoleMember}},
			Policy:  Policy{Kind: PolicyOpen},
		},
		"directed with no edges": {
			Members: []Member{{PeerID: "peer-a", Role: RoleMember}, {PeerID: "peer-b", Role: RoleMember}},
			Policy:  Policy{Kind: PolicyDirected},
		},
	}
	for name, model := range cases {
		t.Run(name, func(t *testing.T) {
			if err := Validate(model); err != nil {
				t.Fatalf("expected a well-formed model, got %v", err)
			}
			if Supported(model) {
				t.Fatal("expected unsupported")
			}
		})
	}
}

func TestToGrantFieldsCanonicalOrderAndDirection(t *testing.T) {
	// Members supplied out of sorted order; ToGrantFields must still emit
	// peerA < peerB by exact byte order, independent of input order.
	open := Model{
		Members: []Member{{PeerID: "peer-z", Role: RoleMember}, {PeerID: "peer-a", Role: RoleMember}},
		Policy:  Policy{Kind: PolicyOpen},
	}
	a, b, dir := ToGrantFields(open)
	if a != "peer-a" || b != "peer-z" || dir != store.Bidirectional {
		t.Fatalf("open: got %q %q %v", a, b, dir)
	}

	aToB := Model{
		Members: []Member{{PeerID: "peer-z", Role: RoleMember}, {PeerID: "peer-a", Role: RoleMember}},
		Policy:  Policy{Kind: PolicyDirected, Edges: []Edge{{From: "peer-a", To: "peer-z"}}},
	}
	a, b, dir = ToGrantFields(aToB)
	if a != "peer-a" || b != "peer-z" || dir != store.AToB {
		t.Fatalf("a_to_b: got %q %q %v", a, b, dir)
	}

	bToA := Model{
		Members: []Member{{PeerID: "peer-z", Role: RoleMember}, {PeerID: "peer-a", Role: RoleMember}},
		Policy:  Policy{Kind: PolicyDirected, Edges: []Edge{{From: "peer-z", To: "peer-a"}}},
	}
	a, b, dir = ToGrantFields(bToA)
	if a != "peer-a" || b != "peer-z" || dir != store.BToA {
		t.Fatalf("b_to_a: got %q %q %v", a, b, dir)
	}
}

func TestFromGrantRoundTripsToGrantFields(t *testing.T) {
	for _, dir := range []store.Direction{store.Bidirectional, store.AToB, store.BToA} {
		model := FromGrant("peer-a", "peer-z", dir)
		if err := Validate(model); err != nil {
			t.Fatalf("%v: %v", dir, err)
		}
		if !Supported(model) {
			t.Fatalf("%v: expected supported", dir)
		}
		a, b, gotDir := ToGrantFields(model)
		if a != "peer-a" || b != "peer-z" || gotDir != dir {
			t.Fatalf("%v: round trip mismatch: %q %q %v", dir, a, b, gotDir)
		}
	}
}

func TestFromGrantOutputOrderIsCanonicalNotStoragePosition(t *testing.T) {
	// FromGrant's canonical output order is exact-byte-sorted, independent
	// of which of peerA/peerB the caller happens to pass first.
	model := FromGrant("peer-z", "peer-a", store.AToB)
	if model.Members[0].PeerID != "peer-a" || model.Members[1].PeerID != "peer-z" {
		t.Fatalf("members not canonically ordered: %+v", model.Members)
	}
	// direction is still derived from the stored A/B positions as passed,
	// i.e. "peer-z -> peer-a" here, not from the now-reordered output.
	if len(model.Policy.Edges) != 1 || model.Policy.Edges[0] != (Edge{From: "peer-z", To: "peer-a"}) {
		t.Fatalf("unexpected edge: %+v", model.Policy.Edges)
	}
}
