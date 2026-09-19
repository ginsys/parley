package control

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"regexp"
	"strconv"
	"time"

	"github.com/ginsys/parley/internal/bridgetext"
	"github.com/ginsys/parley/internal/controller"
	"github.com/ginsys/parley/internal/membership"
	"github.com/ginsys/parley/internal/store"
)

// membership.enroll/renew/replace/revoke: docs/specifications/control.md's
// wire mutation methods, translating an authenticated administrator's
// request into a store.Coordinator.Execute call. Each reuses
// internal/membership for the members/policy <-> grant translation and
// internal/controller's *Tx functions for the actual grant mutation --
// this file owns wire decode/digest/response shape only, never grant
// business logic of its own.
//
// authorize is a trivial no-op for every method here, not because
// administrators are exempt from retirement enforcement, but because that
// enforcement already happens one layer up and does not need repeating:
// store.Coordinator.Execute installs the calling principal into ctx before
// authorize ever runs, and its own transactionContext/Before hook path
// (internal/recovery/service.go) unconditionally checks that principal
// against retired_namespaces for every kind absent from
// recovery.humanRecovery's allowlist -- which every membership.* kind is.
// A retired administrator is therefore already refused before authorize is
// reached, the same automatic check internal/connection/lifecycle.go's
// legacy.disposition/hold.disposition and internal/recovery/restore.go's
// recovery.complete rely on for the mutation-eligibility half of their own
// explicit store.CheckRetiredMutation calls -- those calls exist only
// because their kinds are humanRecovery-exempt and so skip the automatic
// hook check; membership.* kinds are not exempt, so no explicit call is
// needed here. This authorize therefore only needs to exist to satisfy
// Execute's non-nil precondition -- it is deliberately not a second,
// redundant retirement check.
func noopAuthorize(context.Context, *sql.Tx) error { return nil }

// auditResourceID returns id verbatim when it satisfies
// store.Coordinator.Execute's own resource-audit rule (bridgetext byte
// shape plus store.MaxIdentityBytes), or a fixed-width hex SHA-256 digest
// of id otherwise. AGENTS.md's exact-key human revocation must remain
// available for legacy conversation identifiers containing bytes outside
// 0x20-0x7E, or oversized ones -- without this, Execute's own resource
// validation (internal/store/coordinator.go) would abort the whole revoke
// command before it ever reaches RevokeTx, silently defeating the one
// escape path parseCommand's revoke branch deliberately skips
// bridgetext.ValidateMetadata for. Only the durable audit record's
// resource ID is affected; RevokeTx itself still receives the conversation
// identifier verbatim, so the actual mutation target is exact either way.
func auditResourceID(id string) string {
	if len(id) <= store.MaxIdentityBytes && bridgetext.ValidateMetadata(id) == nil {
		return id
	}
	sum := sha256.Sum256([]byte(id))
	return "legacy-sha256:" + hex.EncodeToString(sum[:])
}

func (sess *Session) principal() store.CommandPrincipal {
	return store.CommandPrincipal{ID: sess.identity.PrincipalID, ConnectorUID: sess.identity.UID}
}

// mutationResponse converts one Coordinator.Execute outcome into a wire
// Response. err is an infrastructure/pre-principal failure (the mutation
// never durably ran); a domain rejection is instead carried in
// receipt.Result.Code with err == nil -- Coordinator.execute records a
// terminal rejection's audit and returns it as a normal, non-error receipt
// (internal/store/coordinator.go), so a wire caller must check
// Result.Code even on the success path, not branch on err alone.
func mutationResponse(id string, receipt store.CommandReceipt, err error) Response {
	if err != nil {
		return domainErrorResponse(&id, domainCode(err))
	}
	if receipt.Result.Code != "" {
		return domainErrorResponse(&id, DomainCode(receipt.Result.Code))
	}
	return successResponse(id, CommandReceiptResult{
		Result:      toWireCommandResult(receipt.Result),
		AuditID:     receipt.AuditID,
		OperationID: receipt.OperationID,
		CommitView:  CommitView{Epoch: receipt.View.Epoch, Revision: strconv.FormatInt(receipt.View.Revision, 10)},
	})
}

// CommandReceiptResult is the successful wire result shape shared by every
// membership mutation: a command receipt per control.md's "Command
// atomicity, idempotency and audit" section.
//
// OperationID echoes the exact operation_id this receipt was durably
// recorded under (store.CommandReceipt.OperationID). It is not redundant
// with the response's own JSON-RPC id: that id is a separate, per-connection
// incrementing correlation number (see internal/control/client.go's
// Client.nextID), never the operation UUID a caller supplied in
// params.operation_id -- an earlier version of this doc comment incorrectly
// claimed otherwise. Without this field a caller has no way to confirm,
// from the receipt alone, which operation_id the server actually recorded
// this result under.
type CommandReceiptResult struct {
	Result      wireCommandResult `json:"result"`
	AuditID     string            `json:"audit_id"`
	OperationID string            `json:"operation_id"`
	CommitView  CommitView        `json:"commit_view"`
}

// Usable reports whether r is a genuine command receipt for the exact
// operation the caller requested, not merely a structurally nonempty one.
// AuditID/OperationID/CommitView.Epoch/CommitView.Revision are always
// nonempty and Result.Code is always "" on every successful receipt this
// server actually produces (mutationResponse only reaches successResponse,
// with an empty Result.Code, after a real store.Coordinator.Execute call
// that did not terminally reject -- see mutationResponse's own doc comment);
// a response failing any of these checks did not carry a real, matching
// receipt. Critically, r.OperationID must equal requestedOperationID: two
// nonempty strings that merely differ (a stale or cross-operation receipt)
// previously passed this check, letting a caller print its own locally
// generated operation ID as if the server had confirmed that exact
// operation. A caller must never treat r as a proven successful completion
// of requestedOperationID without checking this first.
func (r CommandReceiptResult) Usable(requestedOperationID string) bool {
	return r.AuditID != "" &&
		r.OperationID != "" &&
		r.OperationID == requestedOperationID &&
		r.CommitView.Epoch != "" &&
		r.CommitView.Revision != "" &&
		r.Result.Code == ""
}

// toWireCommandResult re-encodes a live store.CommandResult with
// decimal-string Before/After fields, mirroring recodeCommandResult's
// stored-JSON re-encoding (server.go) for the live-mutation path.
func toWireCommandResult(result store.CommandResult) wireCommandResult {
	resources := make([]wireResourceChange, len(result.Resources))
	for i, r := range result.Resources {
		resources[i] = wireResourceChange{
			Kind:   r.Kind,
			ID:     r.ID,
			Before: strconv.FormatInt(r.Before, 10),
			After:  strconv.FormatInt(r.After, 10),
		}
	}
	return wireCommandResult{Code: result.Code, Resources: resources}
}

// rejection and domainRejection mirror internal/connection/provisioning.go's
// identically named helpers exactly. Packages do not share these -- each
// coordinator-calling package defines its own copy, the codebase's
// established convention rather than an oversight.
func rejection(code store.Code) (store.CommandResult, error) {
	return store.CommandResult{Code: code}, nil
}
func domainRejection(err error) (store.CommandResult, error) {
	var code store.Code
	if errors.As(err, &code) && code != "" {
		return rejection(code)
	}
	return store.CommandResult{}, err
}

// handleMembershipEnroll implements membership.enroll.
func (sess *Session) handleMembershipEnroll(ctx context.Context, req Request) (Response, bool) {
	p, ok := decodeEnrollParams(req.Params)
	if !ok {
		return envelopeErrorResponse(InvalidParams, &req.ID), false
	}
	if err := bridgetext.ValidateMetadata(p.conversation); err != nil {
		return domainErrorResponse(&req.ID, IncompatibleIdentifier), false
	}
	model := membership.Model{Members: p.members, Policy: p.policy}
	// membership.Validate must run before membersField/NewCommandRequest:
	// a malformed shape (e.g. a duplicate member) produces a members Set
	// with two identical encoded entries, which store.NewCommandRequest's
	// own digest canonicalization already refuses on structural grounds
	// (duplicate Set content -> InvalidRequest) before ever reaching
	// Execute -- there is no operation_id-scoped audit possible for input
	// this malformed, since the very shape needed to build the
	// CommandRequest is broken. membership.Supported, by contrast, only
	// inspects member count/policy kind and never produces a duplicate
	// Set entry, so it can safely run inside the mutate callback below
	// and be durably audited like any other domain rejection.
	//
	// Known asymmetry (from repair-batch-1 review): invalid_membership
	// rejections here are therefore NOT recorded through
	// operation_results/command_audit (they never reach Execute at all),
	// while unsupported_membership rejections below are. A retry with the
	// same operation_id and a still-malformed payload re-runs this exact
	// check and gets the same InvalidMembership response every time --
	// there is no OperationConflict risk from the missing audit row,
	// only the absence of a durable record of the earlier attempt. See
	// docs/specifications/control.md's invalid_membership row for the
	// documented exception this asymmetry corresponds to.
	if err := membership.Validate(model); err != nil {
		return domainErrorResponse(&req.ID, DomainCode(store.InvalidMembership)), false
	}
	fields := []store.Field{
		{Name: "conversation", Value: p.conversation},
		{Name: "expected_grant_version", Value: p.expectedVersion},
		membersField(model), policyField(model.Policy),
		{Name: "max_exchanges", Value: p.maxExchanges},
	}
	if p.expiresAt != nil {
		fields = append(fields, store.Field{Name: "expires_at", Value: p.expiresAtText})
	}
	request, err := store.NewCommandRequest("membership.enroll", p.operationID, fields...)
	if err != nil {
		return envelopeErrorResponse(InvalidParams, &req.ID), false
	}
	expectedVersion := p.expectedVersion
	receipt, err := sess.server.Store.Coordinator().Execute(ctx, sess.principal(), request, noopAuthorize,
		func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
			// Run inside the mutate callback, not before Execute: an
			// unsupported-shape rejection must be recorded through
			// operation_results/command_audit like any other terminal
			// domain rejection, so a retry reusing the same operation_id
			// with a corrected payload hits OperationConflict instead of
			// silently succeeding.
			if !membership.Supported(model) {
				return rejection(store.UnsupportedMembership)
			}
			peerA, peerB, direction := membership.ToGrantFields(model)
			now := store.AuthorityTime(ctx, sess.server.now())
			if _, err := store.EnabledPeer(ctx, tx, peerA, now); err != nil {
				return domainRejection(err)
			}
			if _, err := store.EnabledPeer(ctx, tx, peerB, now); err != nil {
				return domainRejection(err)
			}
			g, err := controller.GrantTx(ctx, tx, controller.GrantParams{
				Conversation: p.conversation, PeerAID: peerA, PeerBID: peerB, Direction: direction,
				MaxExchanges: p.maxExchanges, ExpiresAt: p.expiresAt, ExpectedVersion: &expectedVersion,
			})
			if err != nil {
				return domainRejection(err)
			}
			return store.CommandResult{Resources: []store.ResourceChange{{Kind: "grant", ID: p.conversation, Before: expectedVersion, After: g.GrantVersion}}}, nil
		}, nil)
	return mutationResponse(req.ID, receipt, err), false
}

// handleMembershipRenew implements membership.renew.
func (sess *Session) handleMembershipRenew(ctx context.Context, req Request) (Response, bool) {
	p, ok := decodeRenewParams(req.Params)
	if !ok {
		return envelopeErrorResponse(InvalidParams, &req.ID), false
	}
	if err := bridgetext.ValidateMetadata(p.conversation); err != nil {
		return domainErrorResponse(&req.ID, IncompatibleIdentifier), false
	}
	fields := renewalFields(p.conversation, p.expectedVersion, p.maxExchanges, p.expiresAtText, p.cancelPendingReplies)
	request, err := store.NewCommandRequest("membership.renew", p.operationID, fields...)
	if err != nil {
		return envelopeErrorResponse(InvalidParams, &req.ID), false
	}
	expectedVersion := p.expectedVersion
	receipt, err := sess.server.Store.Coordinator().Execute(ctx, sess.principal(), request, noopAuthorize,
		func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
			result, err := controller.RenewTx(ctx, tx, controller.RenewParams{
				CancelPendingReplies: p.cancelPendingReplies, Conversation: p.conversation,
				MaxExchanges: p.maxExchanges, ExpiresAt: p.expiresAt, ExpectedVersion: &expectedVersion,
			})
			if err != nil {
				return domainRejection(err)
			}
			return store.CommandResult{Resources: []store.ResourceChange{
				{Kind: "grant", ID: p.conversation, Before: p.expectedVersion, After: result.Grant.GrantVersion},
				{Kind: "queued_carried", ID: p.conversation, After: result.Carried},
				{Kind: "queued_cancelled", ID: p.conversation, After: result.Cancelled},
				{Kind: "queued_already_dispatching", ID: p.conversation, After: result.AlreadyDispatching},
				{Kind: "queued_already_handed_off", ID: p.conversation, After: result.AlreadyHandedOff},
			}}, nil
		}, nil)
	return mutationResponse(req.ID, receipt, err), false
}

// handleMembershipReplace implements membership.replace.
func (sess *Session) handleMembershipReplace(ctx context.Context, req Request) (Response, bool) {
	p, ok := decodeReplaceParams(req.Params)
	if !ok {
		return envelopeErrorResponse(InvalidParams, &req.ID), false
	}
	if err := bridgetext.ValidateMetadata(p.conversation); err != nil {
		return domainErrorResponse(&req.ID, IncompatibleIdentifier), false
	}
	model := membership.Model{Members: p.members, Policy: p.policy}
	// See handleMembershipEnroll's identical comment: Validate must run
	// before membersField/NewCommandRequest (a duplicate member would
	// otherwise trip store.NewCommandRequest's own Set-uniqueness rule
	// first); Supported is safe to defer into the mutate callback below.
	if err := membership.Validate(model); err != nil {
		return domainErrorResponse(&req.ID, DomainCode(store.InvalidMembership)), false
	}
	fields := renewalFields(p.conversation, p.expectedVersion, p.maxExchanges, p.expiresAtText, p.cancelPendingReplies)
	fields = append(fields, membersField(model), policyField(model.Policy))
	request, err := store.NewCommandRequest("membership.replace", p.operationID, fields...)
	if err != nil {
		return envelopeErrorResponse(InvalidParams, &req.ID), false
	}
	expectedVersion := p.expectedVersion
	receipt, err := sess.server.Store.Coordinator().Execute(ctx, sess.principal(), request, noopAuthorize,
		func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
			// See handleMembershipEnroll's identical comment: only the
			// Supported check runs inside Execute's callback.
			if !membership.Supported(model) {
				return rejection(store.UnsupportedMembership)
			}
			peerA, peerB, direction := membership.ToGrantFields(model)
			now := store.AuthorityTime(ctx, sess.server.now())
			if _, err := store.EnabledPeer(ctx, tx, peerA, now); err != nil {
				return domainRejection(err)
			}
			if _, err := store.EnabledPeer(ctx, tx, peerB, now); err != nil {
				return domainRejection(err)
			}
			result, err := controller.ReplaceTx(ctx, tx, controller.ReplaceParams{
				CancelPendingReplies: p.cancelPendingReplies, Conversation: p.conversation,
				PeerAID: peerA, PeerBID: peerB, Direction: direction,
				MaxExchanges: p.maxExchanges, ExpiresAt: p.expiresAt, ExpectedVersion: &expectedVersion,
			})
			if err != nil {
				return domainRejection(err)
			}
			return store.CommandResult{Resources: []store.ResourceChange{
				{Kind: "grant", ID: p.conversation, Before: p.expectedVersion, After: result.Grant.GrantVersion},
				{Kind: "queued_carried", ID: p.conversation, After: result.Carried},
				{Kind: "queued_cancelled", ID: p.conversation, After: result.Cancelled},
				{Kind: "queued_already_dispatching", ID: p.conversation, After: result.AlreadyDispatching},
				{Kind: "queued_already_handed_off", ID: p.conversation, After: result.AlreadyHandedOff},
			}}, nil
		}, nil)
	return mutationResponse(req.ID, receipt, err), false
}

// handleMembershipRevoke implements membership.revoke.
func (sess *Session) handleMembershipRevoke(ctx context.Context, req Request) (Response, bool) {
	p, ok := decodeRevokeParams(req.Params)
	if !ok {
		return envelopeErrorResponse(InvalidParams, &req.ID), false
	}
	request, err := store.NewCommandRequest("membership.revoke", p.operationID,
		store.Field{Name: "conversation", Value: p.conversation},
		store.Field{Name: "expected_grant_version", Value: p.expectedVersion})
	if err != nil {
		return envelopeErrorResponse(InvalidParams, &req.ID), false
	}
	id := auditResourceID(p.conversation)
	receipt, err := sess.server.Store.Coordinator().Execute(ctx, sess.principal(), request, noopAuthorize,
		func(ctx context.Context, tx *sql.Tx) (store.CommandResult, error) {
			result, err := controller.RevokeTx(ctx, tx, p.conversation, &p.expectedVersion)
			if err != nil {
				return domainRejection(err)
			}
			return store.CommandResult{Resources: []store.ResourceChange{
				{Kind: "grant", ID: id, Before: p.expectedVersion, After: p.expectedVersion},
				{Kind: "queued_cancelled", ID: id, After: int64(result.Cancelled)},
				{Kind: "queued_already_dispatching", ID: id, After: int64(result.AlreadyDispatching)},
				{Kind: "queued_already_handed_off", ID: id, After: int64(result.AlreadyHandedOff)},
			}}, nil
		}, nil)
	return mutationResponse(req.ID, receipt, err), false
}

// renewalFields builds the digest fields common to renew and replace.
// expiresAtText is the raw wire RFC3339 string, or "" if expires_at was
// omitted -- omitted, never digested, matching the field's optionality.
func renewalFields(conversation string, expectedVersion, maxExchanges int64, expiresAtText string, cancelPendingReplies bool) []store.Field {
	fields := []store.Field{
		{Name: "conversation", Value: conversation},
		{Name: "expected_grant_version", Value: expectedVersion},
		{Name: "cancel_pending_replies", Value: cancelPendingReplies},
	}
	if maxExchanges != 0 {
		fields = append(fields, store.Field{Name: "max_exchanges", Value: maxExchanges})
	}
	if expiresAtText != "" {
		fields = append(fields, store.Field{Name: "expires_at", Value: expiresAtText})
	}
	return fields
}

func membersField(model membership.Model) store.Field {
	members := make(store.Set, len(model.Members))
	for i, m := range model.Members {
		members[i] = store.Fields{{Name: "peer_id", Value: m.PeerID}, {Name: "role", Value: string(m.Role)}}
	}
	return store.Field{Name: "members", Value: members}
}

func policyField(p membership.Policy) store.Field {
	edges := make(store.Set, len(p.Edges))
	for i, e := range p.Edges {
		edges[i] = store.Fields{{Name: "from", Value: e.From}, {Name: "to", Value: e.To}}
	}
	return store.Field{Name: "policy", Value: store.Fields{{Name: "kind", Value: string(p.Kind)}, {Name: "edges", Value: edges}}}
}

// --- param decoding ---
//
// Every wire identifier byte-shape rule (conversation/peer ASCII exact-key
// validation) is enforced by membership.Validate against the members list,
// and by validateGrant/validateRenewalInput inside internal/controller for
// the top-level conversation field -- this file only checks JSON shape
// (right key set, right JSON type per field) and produces InvalidParams
// for a violation.
//
// The top-level conversation field is the one exception: enroll/renew/
// replace each apply bridgetext.ValidateMetadata to p.conversation
// themselves, immediately after decode and before building the digest
// fields or calling Execute, and return a deterministic IncompatibleIdentifier
// rejection rather than letting a malformed identifier reach
// controller.GrantTx/RenewTx/ReplaceTx -- which reject it with a plain Go
// error, not a store.Code, and would otherwise degrade to an
// infrastructure-style TemporarilyUnavailable via domainRejection's
// fallback. Like membership.Validate's invalid_membership check above, this
// runs before store.NewCommandRequest/Execute, so it is not durably audited
// through operation_results/command_audit -- the same known asymmetry
// (see handleMembershipEnroll's comment): a retry with the same
// operation_id and a still-malformed conversation re-runs this exact check
// and gets the same IncompatibleIdentifier response every time, with no
// OperationConflict risk. handleMembershipRevoke deliberately does not
// apply this check: AGENTS.md's exact-key human revocation escape must
// remain reachable for a byte-malformed historical conversation identifier
// (see auditResourceID's own doc comment for the matching audit-side
// accommodation).

type enrollParams struct {
	operationID     string
	conversation    string
	expectedVersion int64
	members         []membership.Member
	policy          membership.Policy
	maxExchanges    int64
	expiresAt       *time.Time
	expiresAtText   string
}

func decodeEnrollParams(params map[string]any) (enrollParams, bool) {
	if !paramKeysAllowed(params, "operation_id", "conversation", "expected_grant_version", "members", "policy", "max_exchanges", "expires_at") {
		return enrollParams{}, false
	}
	var p enrollParams
	var ok bool
	if p.operationID, ok = paramOperationID(params); !ok {
		return enrollParams{}, false
	}
	if p.conversation, ok = paramString(params, "conversation"); !ok {
		return enrollParams{}, false
	}
	if p.expectedVersion, ok = paramDecimal(params, "expected_grant_version"); !ok {
		return enrollParams{}, false
	}
	if p.maxExchanges, ok = paramDecimal(params, "max_exchanges"); !ok || p.maxExchanges <= 0 {
		return enrollParams{}, false
	}
	membersRaw, ok := params["members"]
	if !ok {
		return enrollParams{}, false
	}
	if p.members, ok = decodeMembers(membersRaw); !ok {
		return enrollParams{}, false
	}
	policyRaw, ok := params["policy"]
	if !ok {
		return enrollParams{}, false
	}
	if p.policy, ok = decodePolicy(policyRaw); !ok {
		return enrollParams{}, false
	}
	if p.expiresAt, p.expiresAtText, ok = paramOptionalExpiresAt(params); !ok {
		return enrollParams{}, false
	}
	return p, true
}

type renewParams struct {
	operationID          string
	conversation         string
	expectedVersion      int64
	maxExchanges         int64
	expiresAt            *time.Time
	expiresAtText        string
	cancelPendingReplies bool
}

func decodeRenewParams(params map[string]any) (renewParams, bool) {
	if !paramKeysAllowed(params, "operation_id", "conversation", "expected_grant_version", "max_exchanges", "expires_at", "cancel_pending_replies") {
		return renewParams{}, false
	}
	p, ok := decodeRenewalCore(params)
	return p, ok
}

type replaceParams struct {
	renewParams
	members []membership.Member
	policy  membership.Policy
}

func decodeReplaceParams(params map[string]any) (replaceParams, bool) {
	if !paramKeysAllowed(params, "operation_id", "conversation", "expected_grant_version", "max_exchanges", "expires_at", "cancel_pending_replies", "members", "policy") {
		return replaceParams{}, false
	}
	core, ok := decodeRenewalCore(params)
	if !ok {
		return replaceParams{}, false
	}
	membersRaw, ok := params["members"]
	if !ok {
		return replaceParams{}, false
	}
	members, ok := decodeMembers(membersRaw)
	if !ok {
		return replaceParams{}, false
	}
	policyRaw, ok := params["policy"]
	if !ok {
		return replaceParams{}, false
	}
	policy, ok := decodePolicy(policyRaw)
	if !ok {
		return replaceParams{}, false
	}
	return replaceParams{renewParams: core, members: members, policy: policy}, true
}

// decodeRenewalCore decodes the fields renew and replace share: required
// operation_id/conversation/expected_grant_version(positive), optional
// max_exchanges(positive if present)/expires_at, and
// cancel_pending_replies defaulting false when omitted.
func decodeRenewalCore(params map[string]any) (renewParams, bool) {
	var p renewParams
	var ok bool
	if p.operationID, ok = paramOperationID(params); !ok {
		return renewParams{}, false
	}
	if p.conversation, ok = paramString(params, "conversation"); !ok {
		return renewParams{}, false
	}
	if p.expectedVersion, ok = paramDecimal(params, "expected_grant_version"); !ok || p.expectedVersion < 1 {
		return renewParams{}, false
	}
	if _, present := params["max_exchanges"]; present {
		if p.maxExchanges, ok = paramDecimal(params, "max_exchanges"); !ok || p.maxExchanges <= 0 {
			return renewParams{}, false
		}
	}
	if p.expiresAt, p.expiresAtText, ok = paramOptionalExpiresAt(params); !ok {
		return renewParams{}, false
	}
	if _, present := params["cancel_pending_replies"]; present {
		if p.cancelPendingReplies, ok = paramBool(params, "cancel_pending_replies"); !ok {
			return renewParams{}, false
		}
	}
	return p, true
}

type revokeParams struct {
	operationID     string
	conversation    string
	expectedVersion int64
}

func decodeRevokeParams(params map[string]any) (revokeParams, bool) {
	if !paramKeysAllowed(params, "operation_id", "conversation", "expected_grant_version") {
		return revokeParams{}, false
	}
	var p revokeParams
	var ok bool
	if p.operationID, ok = paramOperationID(params); !ok {
		return revokeParams{}, false
	}
	if p.conversation, ok = paramString(params, "conversation"); !ok {
		return revokeParams{}, false
	}
	if p.expectedVersion, ok = paramDecimal(params, "expected_grant_version"); !ok || p.expectedVersion < 1 {
		return revokeParams{}, false
	}
	return p, true
}

func decodeMembers(raw any) ([]membership.Member, bool) {
	arr, ok := raw.([]any)
	if !ok {
		return nil, false
	}
	members := make([]membership.Member, len(arr))
	for i, item := range arr {
		obj, ok := item.(map[string]any)
		if !ok || !paramKeysAllowed(obj, "peer_id", "role") {
			return nil, false
		}
		peerID, ok := paramString(obj, "peer_id")
		if !ok {
			return nil, false
		}
		role, ok := paramString(obj, "role")
		if !ok {
			return nil, false
		}
		members[i] = membership.Member{PeerID: peerID, Role: membership.Role(role)}
	}
	return members, true
}

// decodePolicy enforces the tagged union's field-presence rule, not merely
// its decoded content: membership.md's policy object carries an edges field
// only when kind is "directed" -- open/lead_only must omit the key
// entirely, never send it present-but-empty. Checking only decoded length
// (as an earlier version of this function did) cannot tell "edges omitted"
// and "edges sent as an empty array" apart, since both produce a
// zero-length Go slice -- letting a client wire an open/lead_only policy
// with a stray "edges":[] pass decode unnoticed, silently accepted by
// membership.Validate's own len(Edges)==0 check even though the wire shape
// itself violated the tagged union. A directed policy conversely requires
// the key present (even an empty array is a syntactically valid directed
// policy at this layer; membership.Supported separately treats a
// zero/multi-edge directed policy as unsupported_membership, a domain
// rejection, not a decode failure).
func decodePolicy(raw any) (membership.Policy, bool) {
	obj, ok := raw.(map[string]any)
	if !ok || !paramKeysAllowed(obj, "kind", "edges") {
		return membership.Policy{}, false
	}
	kindStr, ok := paramString(obj, "kind")
	if !ok {
		return membership.Policy{}, false
	}
	kind := membership.PolicyKind(kindStr)
	edgesRaw, present := obj["edges"]
	switch kind {
	case membership.PolicyOpen, membership.PolicyLeadOnly:
		if present {
			return membership.Policy{}, false
		}
		return membership.Policy{Kind: kind}, true
	case membership.PolicyDirected:
		if !present {
			return membership.Policy{}, false
		}
	default:
		// An unrecognized kind is membership.Validate's rejection to make
		// (invalid_membership, a domain code), not this decoder's -- letting
		// edges be either present or absent here avoids a wire-level
		// InvalidParams masking that intended domain rejection.
		return membership.Policy{Kind: kind}, true
	}
	arr, ok := edgesRaw.([]any)
	if !ok {
		return membership.Policy{}, false
	}
	edges := make([]membership.Edge, len(arr))
	for i, item := range arr {
		eobj, ok := item.(map[string]any)
		if !ok || !paramKeysAllowed(eobj, "from", "to") {
			return membership.Policy{}, false
		}
		from, ok := paramString(eobj, "from")
		if !ok {
			return membership.Policy{}, false
		}
		to, ok := paramString(eobj, "to")
		if !ok {
			return membership.Policy{}, false
		}
		edges[i] = membership.Edge{From: from, To: to}
	}
	return membership.Policy{Kind: kind, Edges: edges}, true
}

// paramOptionalExpiresAt decodes and validates the wire profile's permitted
// expires_at form and range (control.md's RFC3339-UTC timestamp codec).
// Form: RFC3339Nano syntax (fractional seconds permitted, per parleyctl's
// own -expires-at retry-safe path in cmd/parleyctl/main.go) and a literal
// "Z" UTC suffix -- a numeric zone offset is rejected outright rather than
// normalized. Accepting an offset and silently converting it (as an earlier
// version of parleyctl's own -expires-at flag parsing did on the client
// side) would make the wire byte string that store.NewCommandRequest digests
// depend on which equivalent-instant spelling happened to be sent, rather
// than on the instant itself -- exactly the property a same-operation-id
// retry's digest must not depend on. Range: store.InstantNanos, the same
// bound store.Coordinator.Execute itself applies to its own authority
// instant, so an expiry outside the range a stored nanosecond timestamp can
// represent is rejected here rather than surfacing later as a storage
// failure.
// expiresAtGrammar is the exact lexical shape this wire field accepts:
// 4-digit year, 2-digit month/day/hour/minute/second, an optional
// dot-separated fraction of 1-9 digits, and a literal "Z". time.Parse with
// time.RFC3339Nano alone is not sufficient to enforce this (MC-03):
// verified directly against this Go toolchain, it silently truncates a
// 10-digit fraction to 9 (e.g. "12:00:00.1234567891Z" parses without error,
// losing the last digit), accepts a comma in place of the fraction's dot
// (a legal ISO 8601 alternative RFC3339Nano itself does not document
// rejecting), and accepts a single-digit hour ("T1:00:00Z"). This grammar
// check runs before time.Parse and rejects all three. It deliberately does
// NOT reject a valid trailing-zero fraction spelling such as ".750Z" --
// three digits, all significant per the grammar above -- merely because
// time.Time's own String()/Format output would later render the same
// instant more compactly (".75Z"): validity is a property of the original
// input's lexical form, not of whether a reformatted round-trip happens to
// look shorter.
var expiresAtGrammar = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?Z$`)

func paramOptionalExpiresAt(params map[string]any) (*time.Time, string, bool) {
	raw, present := params["expires_at"]
	if !present {
		return nil, "", true
	}
	s, ok := raw.(string)
	if !ok || !expiresAtGrammar.MatchString(s) {
		return nil, "", false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil, "", false
	}
	if _, err := store.InstantNanos(t); err != nil {
		return nil, "", false
	}
	return &t, s, true
}

func paramKeysAllowed(params map[string]any, allowed ...string) bool {
	set := make(map[string]bool, len(allowed))
	for _, k := range allowed {
		set[k] = true
	}
	for k := range params {
		if !set[k] {
			return false
		}
	}
	return true
}

func paramString(params map[string]any, key string) (string, bool) {
	v, ok := params[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

func paramBool(params map[string]any, key string) (bool, bool) {
	v, ok := params[key]
	if !ok {
		return false, false
	}
	b, ok := v.(bool)
	return b, ok
}

// paramOperationID decodes the fixed operation_id field: a canonical UUID
// string, the CommandRequest idempotency key every mutation method shares.
func paramOperationID(params map[string]any) (string, bool) {
	s, ok := paramString(params, "operation_id")
	if !ok || !canonicalUUID(s) {
		return "", false
	}
	return s, true
}

// paramDecimal parses key as a canonical nonnegative decimal string -- the
// wire profile's 64-bit counter/version/budget codec (control.md): ASCII
// digits only, no sign, no leading zero unless the value is exactly "0".
// Any other spelling of the same number is rejected rather than
// normalized, so two different wire spellings of one value can never
// collide in the durable command digest.
func paramDecimal(params map[string]any, key string) (int64, bool) {
	s, ok := paramString(params, key)
	if !ok {
		return 0, false
	}
	return parseCanonicalNonNegative(s)
}

func parseCanonicalNonNegative(s string) (int64, bool) {
	if s == "" || len(s) > 20 {
		return 0, false
	}
	if s == "0" {
		return 0, true
	}
	if s[0] < '1' || s[0] > '9' {
		return 0, false
	}
	for i := 1; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
