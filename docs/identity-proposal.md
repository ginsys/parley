# Peer identity and conversation admission proposal

This is the proposed resolution of [#21](https://github.com/ginsys/parley/issues/21), for owner
review. Only [actual-pair admission](architecture.md#accepted-conversation-admission) is already
approved. The credential mechanism, discovery policy and session-lifetime choices below are
recommendations, not accepted authority or implemented behavior. After the owner ruling, the
connection specification turns these decisions into schemas, operations and executable fixtures.

## A concrete first connection

Alice has a Claude session and Bob has a Codex session. They need not know which conversation
they will join, and there may be several conversations waiting for partners.

1. The human registers each existing host session with the bridge. Registration fixes its host
   kind, configured account/installation namespace and native session ID, assigns a peer ID, and
   provisions a private credential to its deterministic adapter. It grants no conversation access.
2. Alice's adapter authenticates. Alice asks to create `Review authentication`, with a short public
   purpose, and explicitly advertises it to registered peers. It waits without a communication
   grant. An unadvertised conversation can instead be offered to specific registered peers by its
   initiator or human administration. An invitation permits discovery/requesting, not admission.
3. Bob authenticates and lists eligible waiting conversations. The list may also contain
   `Debug deployment`. Bob chooses `Review authentication`; the bridge records a join request.
4. The human sees both registered peers, their host bindings, the selected conversation and
   communication limits. Approval atomically establishes the actual pair. Readiness gates delivery.
5. Bob disconnects and resumes the same host session. His adapter authenticates again, obtains a
   new connection generation and resets readiness. Existing memberships and budgets are unchanged.
   Bob can also explicitly discover/request another conversation; there is no single implicit room.

Registration is a separate human setup action from pair approval in this proposal. It can happen
before any conversation exists and is not repeated for ordinary reconnects. No command in this
document instructs an agent to run the protected controller or create actual credentials.

## Four identities with different jobs

These are logical field names, not a new SQL schema or finalized wire encoding.

| Record | Meaning |
| --- | --- |
| `server_id` | The enrolled bridge installation; the adapter must recognize the intended server before presenting its credential |
| `peer_id` and immutable `binding_id` | One registered host session in one administrator-configured namespace; usable in several conversations |
| `credential_id`, `credential_version`, verifier, expiry, status | Revocable proof of possession for that binding, with no administrator privileges |
| `server_epoch`, `connection_generation` | This server process lifetime and this authenticated connection; old readiness/events cannot authorize a new connection |

The binding contains `(host_kind, host_namespace_id, host_session_id)` and the allowed OS identity
for the local connector. The namespace is configured by the administrator, not an arbitrary
client-supplied HOME path. Host-native IDs are opaque locators; they are not peer IDs or secrets.
Their syntax is validated by the applicable host adapter and never silently normalized. New peer
IDs and conversation names obey the accepted printable-ASCII rule. Display labels are not routing
keys. A caller cannot authenticate by presenting a peer name, native session ID or PID alone.

One host binding has one peer identity, even when that peer joins several conversations. Enforce
binding uniqueness and reject duplicate registration; it must not manufacture a second identity
to bypass a membership limit. Preserve historical peer IDs and bindings rather than recycling
them. Existing grants do not authenticate their peers: enrollment of legacy identities requires
human review of their existing grants before the binding becomes usable.

The server constructs the principal from the verified credential record and connection. Agent
operations do not accept an authoritative `from_peer`, credential-selected role or administrator
flag. Explicit recipients and conversation selection remain necessary; they do not establish
the sender. Replies still resolve and validate their original envelope under the membership
contract, after the ingestion source has been bound to the authenticated peer.

Record the accepting binding and credential version with every admitted message, reply and pending
request. Preserve that provenance through grant renewal/carry and credential rotation; do not
replace it with the version current at dispatch. Existing work without credential provenance must
be reviewed before enabling a legacy binding, not silently attributed to a new credential.

An enabled binding has current, non-revoked enrollment and a current unexpired credential. This
does not mean its adapter is online. New sends/replies require an authenticated current sender and
an enabled recipient binding; an offline recipient may queue work without a host attempt. Dispatch
requires both bindings still enabled and the recipient connection ready; it does not require the
original sender to stay online after acceptance. An unavailable binding defers queued work without
consuming budget or silently changing its envelope state; grant lifecycle rules still apply.

## Credential provisioning and protection

Recommend a separate random 256-bit opaque bearer credential per binding, generated with the
standard cryptographic random source. Store only a cryptographic hash verifier in server-owned
storage, alongside the credential ID/version, binding and human-selected finite expiry. Compare
verifiers in constant time. This is a uniformly random machine secret, not a user password;
do not use a human-chosen code as its long-lived replacement.

The trusted human administration path delivers the secret to a private adapter credential file,
outside repositories and host prompts. Proposed location: the adapter account's XDG state
directory, under `parley/credentials/`, named by an opaque generated credential ID rather than a
host-supplied path component. Require private parent directories and an owner-only regular file
(`0700` directories, `0600` file on Unix), safe creation without symlink following, and atomic
publication. The operator provisions cross-account ownership through the trusted setup path.
Do not place the secret in command arguments, environment values, diagnostics, history or fixtures.
An environment/config value may identify the file path; the deterministic adapter reads the secret.
File permissions do not hide it from another process with the same account's access.

The server commits enrollment before the credential becomes usable. Failed or ambiguous secret
publication never falls back to a shared credential or logs the secret; the human revokes/rotates
the orphaned enrollment. Rotation uses the expected credential version, invalidates the old secret
and existing connections, and keeps the same immutable host binding. A lost rotation response
requires human recovery with another rotation; it cannot reactivate the old credential.

For the proposed Linux local transport, require both a valid binding credential and the enrolled
connector UID obtained from the kernel. The client checks the expected server UID and a configured
socket path beneath administrator-controlled directories before sending its secret. A pathname or
server ID supplied by an untrusted peer is not server authentication. Same-UID server impersonation
remains a development limitation; production needs the separate trusted server account and its
protected path. This identity design does not settle the control-plane wire framing decision.

No bearer credential may be sent over unauthenticated plaintext TCP. A future remote transport
must provide authenticated confidentiality, server verification and a defined replacement for
local UID checks before it is enabled. Neither remote transport nor a certificate infrastructure
is introduced here. A credential is scoped to its enrolled server and never accepted as an
administrative credential, even on an endpoint with an administrative name.

## Session lifetime and reconnect

Recommend keeping a peer bound to one persistent host session. This is independent of choosing
which conversations it joins. Resuming the same session retains its peer; creating a new native
session creates a new binding/peer and requires explicit admission to any conversation.
Do not reinterpret a changed native session ID as a harmless reconnect or migrate old inbox work
implicitly. Stable role handover can be designed later if needed, with its own history policy.

There is at most one live authenticated adapter connection per binding. Credential version,
connection generation and grant version are separate counters; reconnect never renews a grant.

| Event | Proposed result |
| --- | --- |
| First connection | Validate credential, expiry, UID and immutable binding; allocate a generation; remain not-ready until host verification/readiness |
| Same socket repeats authentication after a lost response | Return its existing result; do not allocate a second generation |
| Authentication response and its socket are lost | Authenticate a read-only generation lookup on a new socket, then use the observed generation for an ordinary reconnect once the old slot is inactive |
| Another socket connects while the first is live | Reject as already connected; never silently evict the live connection |
| Prior connection has closed or failed its bounded liveness check | Authenticate again; compare-and-increment the durable generation; one concurrent reconnect wins |
| A second reconnect loses the race | Return a conflict; do not retry by automatically displacing the winner |
| Stuck connection | Human may disconnect it explicitly; identity remains bound to the same host session |
| Server restart | New random server epoch; no connection starts ready; reauthenticate, advance generation and re-establish host readiness |
| Different native host session or namespace | Reject as binding mismatch; human registration/admission of a new peer is required |
| Credential expires, rotates or is revoked | Stop authorizing its connections and new work; cancel their readiness contexts |

Generation allocation and eligibility checks are serialized with the server's writer. Client
reconnect requests use an expected generation; failed attempts cannot mutate it. Generation
overflow fails closed. A reconnect response must include server-owned generation/epoch, never
trust an arbitrary client epoch as proof that an old connection is current.

Provide an authenticated read-only lookup of the binding's current server epoch, committed
generation and active/inactive connection status. Validate credential, expiry, UID and immutable
binding before returning only that binding's state. The lookup grants no operational principal or
readiness, allocates no generation, and cannot reserve or evict a connection. It is available before
ordinary attachment so a lost response cannot strand a client behind its own committed generation.
For example, if attachment advanced generation 5 to 6 but its response and socket were lost, a new
socket observes 6; after the old slot is inactive, its normal compare-and-increment attaches at 7.
Repeated lookups have no effect. A concurrent winner still causes a conflict, never a takeover.
Close/liveness callbacks must match their exact epoch/generation before releasing a slot; an old
callback cannot clear the winner. Liveness durations and bounded retry limits belong in the
connection specification and its fixtures.

Reject stale-connection mutations using current credential/binding status and generation in the
same writer transaction as message acceptance, admission changes or reply ingestion. A check at
socket establishment alone is insufficient. Pure reads also recheck current authorization before
materializing a bounded result. No transaction is held while streaming output; already-sent bytes
cannot be recalled when revocation races with a response.

## Revocation, in-flight work and durable ingestion

Credential revocation disables that binding across all conversations and atomically places a
durable security hold on its outstanding authored work, including pending admission requests and
never-attempted messages/replies. Cover all its credential versions by default: a stolen old
credential may have queued work before a routine rotation. Keep acceptance provenance for review.
The hold is independent of the envelope's delivery state and grant lifecycle; its storage and
operations require specification. Revocation does not rewrite grants, reset budgets or authorize
another peer. Disable discovery, new create/join requests and admission of a revoked binding;
neither a stale approval nor later restoration may consume a held pending request.

Human rotation/re-enrollment for the same verified native session can restore access under still
valid membership, but cannot release these holds. Neither grant renewal/carry nor ordinary expiry
and rotation may remove an existing hold. Only explicit human disposition may cancel or release
reviewed individual work; release still requires every current membership, provenance, readiness
and budget check. It cannot revive cancelled messages or revoked grants, refund/reset budgets,
reset an acknowledged original or authorize replay of an uncertain attempt. Pending rows and grant
history remain available to administration. The human must separately revoke conversation grants
when withdrawing conversation permission.

Routine expiry/rotation closes the authentication gate without declaring earlier work compromised;
queued work may resume when the required bindings are enabled, subject to any existing hold.
Messages authored by an unaffected peer for an unavailable recipient wait for that recipient's
restored access; they are not automatically attributed to its compromised credential. Replacing
the host session is a different-peer admission and follows the approved membership/version rules.

Before a dispatch claim consumes budget, validate the current required bindings, absence of a
security/recovery hold and recipient readiness with the grant in the writer transaction. After a
claim, connection invalidation cancels
the old transport context and must never redirect that attempt to a newly connected transport.
Cancellation is cooperative: an already-started host attempt may complete and cannot be recalled.
Normal settlement still records the exact original grant version and durable attempt token;
revoked credentials do not erase accounting evidence. Ambiguous outcomes remain uncertain with
no automatic retry. Do not treat an internal settlement as a new request from a revoked agent.
If settlement proves no host attempt occurred, any requeued/carried work retains the security hold,
including when settlement arrives after revocation or restoration. Apply any ordinary exact-token
refund only once. A held trusted reply does not undo its original's already-recorded ACK.

Host ingestion sources are tied to immutable bindings, not a caller-supplied transcript path.
The adapter must verify native session provenance using a supported host-specific mechanism.
If the host provides insufficient identity evidence, refuse that integration instead of accepting
a caller's assertion. No currently investigated host mechanism is declared proven by this design.

Durable ingestion cursors/deduplication are scoped to the binding and native event identity, so a
reconnect does not replay previously committed final turns. Recheck binding, generation and reply
provenance before committing ACK/reply/cursor effects atomically. A stale connection cannot ACK an
original or advance the authoritative cursor. A valid event racing with original delivery remains
pending until the original is handed off; do not discard it by advancing the cursor. Never ingest
a new host session's events through an old binding. Retained context and inbox handling must not
override these rules.

Client operation replay is separate from reconnect. The control specification must provide durable
operation IDs scoped to principal and operation, with a request-content check and atomic result
recording. Same-ID/same-request retries do not create grants, replenish budgets or insert another
message; same-ID/different-request attempts fail. Reauthentication/current access is still required
to retrieve a prior result. The wire format, retention and expired-ID behavior require explicit
contracts; a cache eviction must not turn an old mutation into an authorized new one.

### Backup restoration

Restoring an old database can undo revocations and rewind operation results, ingestion cursors and
deduplication, envelope states/attempt tokens, membership/grant versions and consumed budgets.
Credential rotation and a new process epoch cannot reconcile effects already observed by a host.

Before starting from a restored database, the human must establish a recovery hold in trusted
startup configuration outside the restored SQLite state. While held, permit human recovery
inspection and explicit recovery dispositions, but no ordinary agent admission, pending-request
or approval operations, dispatch or ingestion. Repeated restarts and credential rotation cannot
clear the hold. The operational specification must define this startup gate and its explicit
human completion procedure; do not
claim automatic restore detection or make ordinary startup safe after an undisclosed restore.

Recovery must reconcile credentials/revocations, security holds, operation results/tombstones,
ingestion cursors/deduplication, membership/grant versions/budgets and envelope states/attempts
against surviving host and operational evidence. Work whose external outcome or authorization
cannot be established stays held or uncertain; it is never automatically requeued, re-ingested
or given a replenished budget from the old snapshot. The human may retire affected work and
explicitly authorize fresh work, but cannot claim to reconstruct missing evidence. Only explicit
human disposition of the affected state allows the startup hold to be lifted. Unresolved work
retains its own durable hold after that global gate opens. Restore fixtures must exercise lost
delivery and replay evidence as well as restored credential verifiers.

## Discovery and pending conversations

Recommend explicit advertising to all registered, non-revoked peers of this server, or invitations
to named registered peers. An unadvertised waiting conversation is visible only to its initiator,
invitees and human administration. Naming a hidden conversation ID does not bypass this rule.
Publishing a waiting name/purpose is an explicit disclosure to that audience; context, transcript,
host-native IDs, filesystem locations and credentials are excluded from that listing.

An advertised conversation is only an opportunity to request admission. The human still approves
the actual pair. Once full, remove it from the public waiting list; members and administration
retain their authorized view, while other candidates receive their own request's unavailable
result without transcript access. Every page is bounded; selection must be revalidated at mutation
time. Knowing a name or holding an invitation cannot reserve or automatically consume a place.

The initiating peer owns its pending creation request and may withdraw it before approval. A
candidate may withdraw only its own join request. Human administration may cancel either. These
are changes to pending requests, not permission to revoke an active grant. Approval racing with
withdrawal has one serialized winner: withdrawal cannot undo an already-active grant, and approval
cannot consume a cancelled request. Discovery, joining and liveness do not extend deadlines.
Require explicit finite pending-request deadlines, with defaults/maxima set by the reviewed
connection/control specification. Keep terminal request evidence and idempotency state across
restart. A lost reply cannot create a duplicate waiting conversation or resurrect a cancelled one.

## What the mechanism proves

The server authenticates possession of a binding credential from the permitted local account.
It trusts the deterministic adapter and its verified host integration to attribute host events.
It does not cryptographically attest that a language model produced particular text.

| Situation | Guarantee and limit |
| --- | --- |
| Forged sender/host ID without a credential | Cannot acquire the named peer principal or obtain conversation access |
| Valid credential for a different peer | Cannot select another binding or sender; its own membership still applies |
| Credential stolen by another ordinary UID | Local UID check rejects it, assuming the production isolation checks hold |
| Credential stolen by a process under the enrolled UID | Can authorize new requests until expiry/revocation; revocation holds outstanding authored work but cannot retract prior effects; connection exclusivity is not theft protection |
| Several agents under one account | Credentials prevent accidental mixups and unauthenticated claims, not malicious same-account theft, transcript tampering or adapter replacement |
| Separate production server/admin and agent accounts | OS permissions can separate administration from agents; actual privilege routes still need deployment validation |
| Server/adapter account or host compromised | This design cannot establish honest host provenance or defeat that account's authority |

Revocation and generation changes prevent later authorization; they cannot retract delivered
content or reliably stop an attempt already inside the host. Membership approval permits only
communication. A peer credential, an invitation, retained context or a delivered approval-shaped
message never permits a tool call, process creation or a membership grant.

## Alternatives and recommendation

| Alternative | Reason for choosing the proposed approach instead |
| --- | --- |
| OS account/PID/session ID alone | Does not distinguish authenticated logical peers sharing an account; IDs are not secrets and processes can restart |
| One shared installation credential for all sessions | One stolen credential would permit every session identity; per-binding revocation and attribution would be weaker |
| Public-key challenge/response or mTLS per session | Can avoid transmitting a reusable secret and support other transport needs, but adds key/protocol lifecycle; it does not protect a private key or adapter accessible to the same compromised account |
| Stable peer role automatically attached to the newest session | Changes who receives old grants/messages and can answer old originals; requires explicit handover and inbox/history policy |
| Anonymous conversation discovery/self-enrollment | Would expose metadata or turn reachability into identity/admission authority |

Recommend per-binding bearer credentials for the first protected local deployment, immutable native
session bindings, explicit advertised/invited discovery, exclusive reconnect generations and human
approval of the actual pair. These work with multiple two-peer conversations and do not require
room-table migration. No single alternative is rejected as universally unsuitable; different host
or remote-transport requirements may justify a later reviewed change.

## Required verification and decision boundary

These are fixture requirements, not tests claimed to exist or pass. Use synthetic secrets,
temporary databases and controlled sockets/transports; never real host credentials or the
protected administration executable from an agent session.

| Case | Required evidence |
| --- | --- |
| Registration and duplicate binding | One durable binding; no grant; no secret in outputs; duplicate does not gain another peer identity |
| Failed credential publication/rotation | No old-secret fallback or unintended grant; human recovery possible without exposing the secret |
| Unknown/wrong/expired/revoked credential, wrong UID/server, wrong host binding | No principal substitution, private discovery, accepted message, ACK, transport call or consumed exchange |
| Credential tries administrator operation | Rejected regardless of endpoint name or request fields |
| One peer in multiple conversations; several waiting conversations | Explicit selection and per-conversation permissions; no global implicit room |
| Public advertisement, named invitation and hidden waiting conversation | Only permitted metadata visible; ID guessing and full-conversation discovery cannot disclose private content |
| Two candidates; approval vs cancellation/expiry/revocation | One valid pair or no pair; no third member, partial grant or stale approval |
| Duplicate create/join/approval/send, including restart and conflicting payload | One durable effect; conflict rejected; grant budget never silently reset |
| Two reconnects, old connection still live, same-socket authentication replay | Exclusive winner; no repeated takeover or duplicate generation mutation |
| Generation committed, response lost, socket closed | Authenticated lookup recovers the committed generation without mutation; one new attachment advances it once, resets readiness and preserves grants/budgets; stale close cannot clear it |
| Old epoch/generation readiness ACK or ingestion event | Cannot ready a new transport, mutate a queue, ACK an original or advance the ingestion cursor |
| Server restart and same native session resume | Fresh connection readiness, preserved memberships/budgets and no duplicate final-turn ingestion |
| Fresh native session, same label or recycled PID | Cannot inherit the old binding or message rights |
| Revocation before claim, after claim and after host startup | No pre-claim budget use; no retargeted attempt; exact settlement and conservative uncertainty retained |
| Stolen credential queues work while recipient offline; revoke then rotate/re-enroll/renew | Outstanding authored work across credential versions stays held until explicit human disposition; no dispatch or stale admission after access is restored |
| Revocation followed by late never-attempted settlement or trusted-reply carry | Requeue/carry retains hold and original acceptance provenance; exact refund at most once; original ACK is not reset |
| Restore snapshot predating delivery, ACK, approval, revocation or budget use | External recovery hold survives rotation/restart and blocks admission, mutation, dispatch and ingestion until all affected state is reconciled or explicitly retired/held; no duplicate host effect or snapshot-based budget replenishment |
| Same-account stolen credential/compromised adapter | Demonstrate and document the limit; do not label it impersonation-proof |

Owner review selects or amends this model. The follow-up specification must close every operation,
error, deadline, storage migration, idempotency-retention and host-evidence detail before binding
implementation. Approval of this proposal alone does not establish live compatibility or lift
the repository's live-connection fixture gate.

## Source basis

At `27e0c9707d64b500b9c4bb530ec098f596017de6`, [Send](../internal/dispatch/dispatch.go) and
[IngestTurn](../internal/adapter/codex/ingest.go) accept peer identity from their caller;
[Codex transport](../internal/adapter/codex/transport.go) separately stores native thread and peer
IDs. [Claude readiness](../internal/adapter/claude/handshake.go) has process-local generations;
these are not authenticated durable bindings. The [membership specification](specifications/membership.md)
owns grants, routing and reply/version semantics. The [host investigation](host-probes.md) does not
claim a proven session authenticator.

Linux [Unix socket documentation](https://man7.org/linux/man-pages/man7/unix.7.html) defines pathname
permissions and `SO_PEERCRED`. [RFC 6750](https://datatracker.ietf.org/doc/html/rfc6750#section-5)
explains possession-based bearer credentials and disclosure/replay risks; this proposal does not
claim OAuth conformance. Go's [crypto/rand](https://pkg.go.dev/crypto/rand#Read) provides the proposed
random source. The [XDG directory specification](https://specifications.freedesktop.org/basedir/latest/)
distinguishes persistent state from login-lifetime runtime storage; a credential file still needs
the explicit permissions and provisioning rules above.
