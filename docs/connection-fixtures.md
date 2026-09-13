# Controlled connection fixture handoff

This maps #29's implemented subset of the [connection acceptance fixtures](specifications/connections.md#required-controlled-fixtures)
to reproducible synthetic tests. It is not a live-connection approval or a second issue-status tracker.
GitHub issues own remaining scope and completion. All privileged operations use internal test
providers; no fixture invokes the protected controller executable or an installed host CLI.

Run from a trusted isolated worktree:

```sh
mise run verify
mise exec -- go test -race ./internal/...
mise exec -- go test ./internal/adapter/codex -run '^TestConnectionRuntime' -count=1
```

The runtime composition fixtures create private databases and recovery directories under `/tmp`,
attach synthetic peers over local Unix sockets, and inject the test executable into `ExecSender`.
A child emits a controlled startup signal before cancellation. Parent synchronization distinguishes
cancellation after process startup from a process that never ran; no installed `codex` is launched.
These tests compose the existing internal APIs without introducing a runnable daemon, protocol
listener, human administration endpoint, host-native source reader or production supervisor.

| ID | Implemented controlled evidence | Named boundary or deferral |
| --- | --- | --- |
| C01 | [Registry constraints](../internal/store/registry_test.go), `TestRegistrationPublicationAndReplay` in [provisioning](../internal/connection/provisioning_test.go) | Registration creates identity, not a conversation grant |
| C02 | [Private publication](../internal/connection/publication_linux_test.go): non-replacement, unsafe targets, ambiguous rename and controlled crash; provisioning replay/rotation | Real credential distribution remains operator wiring |
| C03 | `TestAuthenticationFailureClosesWithoutDisclosure` in [attachment](../internal/connection/attachment_linux_test.go); unauthorized registration and required capabilities in provisioning; [authenticated send](../internal/connection/work_linux_test.go) derives its author | No agent membership endpoint is exposed; complete human admission is #28 |
| C04 | Attachment one-winner concurrency, repeat/lost-response generation, restart readiness; [bounded reconnect](../internal/connection/client_linux_test.go) | Actual-host reconnect compatibility is #30/#31 |
| C05 | [Readiness fences](../internal/connection/readiness_linux_test.go), exact [administrative disconnect](../internal/connection/disconnect_linux_test.go), stale timer and close fixtures | Tokens supplied as data do not create Session capabilities |
| C06 | Readiness verification failure/stale completion/cancellation with injected evidence providers | Real host verification is #30/#31 |
| C07 | Current identity/authority guards and exact replay authorization | Full discover/list eligibility is #28 |
| C08 | [Coordinator](../internal/store/coordinator_test.go) atomic effects/audit, rejection rollback and current replay authorization; [recovery hooks](../internal/store/recovery_hooks_test.go) gate replay | Admission and approval races are #28 |
| C09 | Offline recipient acceptance; `TestConnectionRuntimeMultipleConversationsAndAtomicReply` in [runtime composition](../internal/adapter/codex/connection_integration_linux_test.go) shares two bindings across two separate grants | Waiting creators and join requests are #28 |
| C10 | [Ingestion storage](../internal/store/ingestion_test.go) event identity/conflict/pending cursor; authenticated ingestion verifies ACK/reply/provenance/cursor rollback and replay; runtime composition keeps the other conversation unacknowledged | Host-native event identity and cursor production are #31 |
| C11 | [Revocation holds](../internal/store/work_retention_test.go), [lifecycle](../internal/connection/lifecycle_test.go) and re-enrollment barrier preservation | Pending-request extension is #28 |
| C12 | [Held exact-attempt settlement](../internal/dispatch/holds_test.go); [authenticated claim](../internal/dispatch/authenticated_linux_test.go); runtime composition cancels a recipient or shuts down after child startup, retains uncertainty/budget, and holds the writer lease until settlement drains | Delivery remains cooperative; accepted host work cannot be recalled |
| C13 | Reviewed held interval in ingestion storage; [human resume](../internal/connection/ingestion_resume_test.go) stale barrier, replay and renewed authorization | Real source-interval production and larger-interval tooling are #31; oversized intervals remain held |
| C14 | Coordinator retained results survive reopen and cannot be deleted/replaced; [retention guards](../internal/store/retention_test.go); authenticated event replay/conflict | No TTL eviction or recovery by emptying identity history |
| C15 | Retention migration preserves all legacy evidence and rolls back/reruns; [dispositions](../internal/store/dispositions_test.go); [identifier rejection](../internal/dispatch/identifiers_test.go); renewal/carry regressions | Historical exact identifiers remain unchanged |
| C16 | [Restore administration](../internal/recovery/restore_test.go) preserves ACK/state/attempt/budget evidence and retires every restored binding; runtime composition restores a stopped pre-delivery/ACK/revoke/budget snapshot and skips ordinary service admission across marked restarts | Pending approval is #28; surviving-history import is not implemented; operators must mark every restore |
| C17 | Registry key/locator constraints, signed-counter and SQLite-capacity fixtures; attachment deadline equality; dispatch attempt overflow; [expiry failure](../internal/connection/work_linux_test.go) retains exact denial without blocking unrelated credentials | Pending TTL is #28; physical storage limits can still prevent persistence |
| C18 | Wrong-UID rejection and accepted same-UID credential possession in attachment; trusted socket path/kernel checks in the client | No claim of production account isolation or protection from a compromised trusted verifier |
| C19 | [Recovery service](../internal/recovery/service_test.go) marker/DB failure, corrected-time restart and synchronized clock races; [clock reconciliation](../internal/recovery/administration_test.go) stale/independent incident and lost-response cleanup; runtime composition turns external-persistence failure into a supervised stop | Real supervisor prevention of unattended restart and cross-host deployment validation belong to #37/operator integration |

The current restore implementation is deliberately conservative: it disables every restored binding
and holds outstanding work instead of importing surviving history. It does not rewrite ACKs, delivery
states, attempt tokens, budgets, deadlines or persisted revocation. A new native identity/admission is
separate work; merely restarting or rotating credentials cannot remove a global recovery incident.

Fixture coverage does not waive the [live-connection gate](../AGENTS.md#record-evidence-and-decisions).
#28 owns discovery/admission and human endpoints, #30/#31 own real host capabilities and native source
production, and #37 owns cross-host/runtime deployment validation. Those capabilities require their
own evidence. No production account creation, execution approval or nah integration is supplied here.
