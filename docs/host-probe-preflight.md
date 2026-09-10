# Host probe preflight — 2026-09-10

Source baseline: `a8beeffd1bbbfaf0033e086e68af74c5108cb56a`. These are command-availability
observations on the development workstation, not wake or delivery results. The
[trial protocol](host-probes.md#trial-protocol) still requires disposable host sessions and
separate outcomes for every trial.

| Command/check | Observed result |
| --- | --- |
| `claude --version` | Exit 0: `2.1.267 (Claude Code)` |
| `codex --version` | Exit 0: `codex-cli 0.153.4`; warning about inability to create PATH aliases in the restricted environment |
| `opencode --version` | Exit 0: `1.18.30` |
| `codex queue --help` | Exit 0; advertises queuing a message for an existing session using thread and message arguments |
| `claude --help` | Exit 0; MCP config options advertised; Channels absent from this help output, which does not establish lack of support |
| `opencode auth --help` | Exit 0; provider-list command advertised |
| `opencode --pure auth list` | Exit 1 in the restricted environment; provider availability unresolved, no credential values retained |
| Expected Codex IPC path existence | No socket found at the configured/default Codex HOME's `ipc/ipc.sock`; no connection attempted |
| `command -v herdr` | Not found; harness does not depend on herdr |

Commands had a ten-second timeout, except provider listing with fifteen seconds. None timed out.
The provider/socket observations were recorded at 17:20 UTC. No message was queued, no ordinary
session was contacted and no provider request was made. Presence of a command is not evidence
that it wakes a session, and absence of an IPC path in this environment is not product-wide
unsupported status. OpenCode remains the intended third host; resolve its provider preflight or
record an explicit owner scope amendment before treating the investigation as complete.

The controlled harness fixtures are independently reproducible with `mise run python`; their
success does not fill any real-host result cell. [Issue #18](https://github.com/ginsys/parley/issues/18)
owns the remaining investigation evidence and acceptance.
